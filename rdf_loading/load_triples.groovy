import org.openrdf.rio.*
import org.openrdf.rio.ntriples.*
import org.openrdf.rio.helpers.*
import org.openrdf.model.*
import org.apache.commons.compress.compressors.*

// Titan configuration & schema definitions
def prepareTitan(String storageDirectory, ArrayList langCodes) {
    def conf = new BaseConfiguration()
    conf.setProperty("storage.backend", "cassandra")
    conf.setProperty("storage.directory", storageDirectory)
    conf.setProperty("storage.batch-loading", true)
    conf.setProperty("storage.infer-schema", false)
      
    def g = TitanFactory.open(conf)
    g.makeKey("qname").dataType(String).single().unique().indexed(Vertex).make()
    _partition = g.makeKey("_partition").dataType(String).single().indexed(Vertex).indexed(Edge).make()
    createdAt = g.makeKey("created_at").dataType(Date).make()
    provenance = g.makeKey("provenance").dataType(String).make()
    langCodes.each {
        g.makeKey("rdfs:label@" + it).dataType(String).make()
        g.makeKey("rdfs:comment@" + it).dataType(String).make()
        g.makeKey("skos:prefLabel@" + it).dataType(String).make()
    }
    g.makeKey("rdfs:label").dataType(String).make()
    
    // TODO: add definitions for all predicates with literal objects
    [
        "rdf:type", "dcterms:subject", "dbp-owl:wikiPageWikiLink",
        "dbp-owl:wikiPageDisambiguates", "skos:broader", "skos:related",
    ].each {
        g.makeLabel(it).sortKey(createdAt).sortOrder(Order.DESC).signature(provenance).make()
    }
    // TODO: add definitions for all edge types
    g.commit()

    bg = new BatchGraph(g, VertexIDType.STRING, 10000L)
    bg.setVertexIdKey("qname")
    // For incremental loading, may need: bg.setLoadingFromScratch(false)
    pg = new PartitionGraph(bg, '_partition', 'dbp')
    return pg
}

// Creates vertices and edges from RDF statements
class StatementsToGraphDB extends RDFHandlerBase {
    def bg, sourceFilename
    StatementsToGraphDB(g, sFn) { bg = g; sourceFilename = sFn }

    def tripleCount = 0L
    // Human formatting for large numbers
    def oom = ['k', 'M', 'G', 'T'] as char[]
    def humanFormat(double n, iteration=0) {
        double d = ((long) n / 100) / 10.0;
        boolean isRound = (d * 10) %10 == 0;
        return (d < 1000?
            ((d > 99.9 || isRound || (!isRound && d > 9.99)?
             (int) d * 10 / 10 : d + ""
             ) + "" + oom[iteration]) 
            : humanFormat(d, iteration+1));

    }

    // Define known namespaces with their prefix
    def namespaces = [
        'http://dbpedia.org/resource/': 'dbp',
        'http://dbpedia.org/ontology/': 'dbp-owl',
        'http://dbpedia.org/property/': 'dbpprop',
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#': 'rdf',
        'http://www.w3.org/2000/01/rdf-schema#': 'rdfs',
        'http://www.w3.org/2002/07/owl#': 'owl',
        'http://xmlns.com/foaf/0.1/': 'foaf',
        'http://schema.org/': 'schema',
        'http://rdf.freebase.com/ns/': 'freebase',
        'http://www.wikidata.org/entity/': 'wikidata',
        'http://purl.org/dc/terms/': 'dcterms',
        'http://www.w3.org/2004/02/skos/core#': 'skos',
        'http://purl.org/ontology/bibo/': 'bibo',
        'http://www.opengis.net/gml/': 'gml',
        'http://www.w3.org/2004/02/skos/core#': 'skos'
    ]

    def unknownNamespaces = new HashSet()
   
    def countedStatements = [:].withDefault{0}
   
    void handleStatement(Statement st) {
        def subject = qName(st.subject)
        def predicate = qName(st.predicate)
        def object, vObj, edge, langCode, propKey

        def vSubj = bg.getVertex(subject) ?: bg.addVertex(subject)

        if (st.object instanceof URI) {
            object = qName(st.object)
            vObj = bg.getVertex(object) ?: bg.addVertex(object)
            edge = bg.addEdge(null, vSubj, vObj, predicate)
            edge.setProperty("created_at", new Date())
            edge.setProperty("provenance", sourceFilename)
        } else {
            // TODO: handle literal datatypes
            object = st.object.getLabel()
            langCode = st.object.getLanguage()
            propKey = predicate + '@' + langCode
            vSubj.setProperty(propKey, object)
            vSubj.setProperty("created_at", new Date())
            vSubj.setProperty("provenance", sourceFilename)
        }

        countedStatements[qName(st.predicate)] += 1
        if (++tripleCount%100000L == 0L) {
            println( (new Date()).toString() + \
            " Processed ${humanFormat(tripleCount)} triples")
        }
    }
    
    def getCountedStatements() {
        return countedStatements.sort { a, b -> b.value <=> a.value }
    }

    def qName(URI uri) {
        def nspc = uri.getNamespace()
        if (nspc.split("/resource/").size() > 1) {
            nspc = nspc[0..(-nspc.split("/resource/")[-1].size()-1)]
        }
        def prefix = namespaces[nspc]
        if (prefix == null) {
            // resolve i18n DBpedia resources
            def matcher = nspc =~ /http:\/\/([a-z\-]+).dbpedia.org\/resource\//
            try {
                prefix = 'dbp-' + matcher[0][1]
            } catch (IndexOutOfBoundsException) {
                // warn about unknown namespace when first encountered
                if ( !(nspc in unknownNamespaces) ) {
                    unknownNamespaces += nspc
                    println('Unknown namespace: ' + nspc)
                }
                return uri.stringValue()
            }
        }
        return prefix + ':' + uri.getLocalName()
    }

}


class Helpers {
    def printLoadingTime(long epoch1, long epoch2, String filename){
        long runningTime = epoch2 - epoch1
        long diffMinutes = Math.round(runningTime / (60 * 1000)) % 60
        long diffHours = Math.round(runningTime / (60 * 60 * 1000)) % 24
        long diffDays = Math.round(runningTime / (24 * 60 * 60 * 1000))

        println("\nLoading ${filename} took ${diffDays} days, ${diffHours} hours, ${diffMinutes} minutes.\n")
    }
}


def loadRdfFromFile(Graph graph, String filepath) {
    // Initialize a stream that feeds bz2-compressed triples
    def fin = new FileInputStream(filepath)
    def bis = new BufferedInputStream(fin)
    def cisFactory = new CompressorStreamFactory()
    cisFactory.setDecompressConcatenated(true)
    def cis = cisFactory.createCompressorInputStream(CompressorStreamFactory.BZIP2, bis)

    def sourceFilename = filepath.split('/')[-1][0..-5]
    def graphCommitter = new StatementsToGraphDB(graph, sourceFilename)
    def rdfParser = new NTriplesParser()
    rdfParser.setRDFHandler(graphCommitter)
    def startTime = System.currentTimeMillis()

    try {
        rdfParser.parse(cis, "http://dbpedia.org/resource/")
    } catch (IOException e) {
        // handle IO problems (e.g. the file could not be read)
        println(e)
    } catch (RDFParseException e) {
        // handle unrecoverable parse error
        println(e)
    } catch (RDFHandlerException e) {
        // handle a problem encountered by the RDFHandler
        println(e)
    }
    
    graph.baseGraph.commit()

    def endTime = System.currentTimeMillis()
    def helpers = new Helpers()
    helpers.printLoadingTime(startTime, endTime, sourceFilename)

    println(graphCommitter.getCountedStatements())
    println("Unknown namespaces: " + graphCommitter.unknownNamespaces)

    return graphCommitter
}

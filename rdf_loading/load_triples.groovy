/**
 * Script for bulk loading RDF (e.g. DBpedia) into Titan
 *
 * Known issues:
 * - assign >= mx 1024mb to the Gremlin shell JVM
 * - Cassandra resource limits: http://www.datastax.com/documentation/cassandra/2.0/cassandra/troubleshooting/trblshootInsufficientResources_r.html
 */
import org.openrdf.rio.*
import org.openrdf.rio.ntriples.*
import org.openrdf.rio.helpers.*
import org.openrdf.model.*
import org.apache.commons.compress.compressors.*

// Titan configuration & schema definitions
def prepareTitan(String storageDirectory, ArrayList langCodes) {
    def conf = new BaseConfiguration()
    conf.setProperty("storage.backend", "cassandra")
    conf.setProperty("storage.hostname", "127.0.0.1")
    conf.setProperty("storage.batch-loading", true)
    conf.setProperty("schema.default", null)
    conf.setProperty("storage.index.search.backend", "elasticsearch")
    conf.setProperty("storage.index.search.directory", storageDirectory + "/searchindex")
    conf.setProperty("storage.index.search.client-only", false)
    conf.setProperty("storage.index.search.local-mode", true)
      
    def g = TitanFactory.open(conf)
    // Types should only be defined once
    // Assumption: if "qname" exists, all keys and labels exist.
    def mgmt = g.getManagementSystem()
    if (mgmt.containsPropertyKey("qname") == false) {
        qname = mgmt.makePropertyKey("qname").dataType(String).make()
        _partition = mgmt.makePropertyKey("_partition").dataType(String).make()
        // Define composite (key) indexes
        mgmt.buildIndex('by_qname', Vertex).addKey(qname).unique().buildCompositeIndex()
        mgmt.buildIndex('v_by_partition', Vertex).addKey(_partition).buildCompositeIndex()
        mgmt.buildIndex('e_by_partition', Edge).addKey(_partition).buildCompositeIndex()
        
        createdAt = mgmt.makePropertyKey("created_at").dataType(Long).make()
        provenance = mgmt.makePropertyKey("provenance").dataType(String).make()
        flow = mgmt.makePropertyKey("flow").dataType(Precision).make()
        langCodes.each {
            mgmt.makePropertyKey("rdfs:label@" + it).dataType(String).make()
            mgmt.makePropertyKey("rdfs:comment@" + it).dataType(String).make()
            mgmt.makePropertyKey("skos:prefLabel@" + it).dataType(String).make()
            mgmt.makePropertyKey("georss:point@" + it).dataType(String).make()
        }
        mgmt.makePropertyKey("rdfs:label").dataType(String).make()
        mgmt.makePropertyKey("georss:point").dataType(String).make()
        lat = mgmt.makePropertyKey("geo:lat").dataType(Double).make()
        lon = mgmt.makePropertyKey("geo:long").dataType(Double).make()
        // Define mixed indexes
        // mgmt.buildIndex('latlon',Vertex.class).addKey(lat).addKey(lon).buildMixedIndex("search")
        
        // TODO: add definitions for all predicates with literal objects
        [
            "rdf:type", "dcterms:subject", "dbp-owl:wikiPageWikiLink",
            "dbp-owl:wikiPageDisambiguates", "skos:broader", "skos:related",
            "owl:sameAs", "dbp-owl:wikiPageRedirects",
        ].each {
            itLabel = mgmt.makeEdgeLabel(it).signature(createdAt,provenance).make()
            mgmt.buildEdgeIndex(itLabel,"${it.replace(':', '_')}_by_created_at", Direction.BOTH,Order.DESC,createdAt)
        }
        categoryFlow = mgmt.makeEdgeLabel("category_flow").signature(flow,createdAt,provenance).make()
        mgmt.buildEdgeIndex(categoryFlow,'cat_flow_by_flow_and_created_at',Direction.BOTH,Order.DESC,flow,createdAt)
        // TODO: add definitions for all edge types
        mgmt.commit()
    }
    bg = new BatchGraph(g, VertexIDType.STRING, 10000L)
    bg.setVertexIdKey("qname")
    // Assumption: if "qname" exists, we are not loading from scratch.
    if (mgmt.containsPropertyKey("qname")) {
        bg.setLoadingFromScratch(false)
    }
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
            ((isRound ? (int) d * 10 / 10 : d + ""
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
        'http://wikidata.org/entity/': 'wikidata',
        'http://purl.org/dc/terms/': 'dcterms',
        'http://www.w3.org/2004/02/skos/core#': 'skos',
        'http://purl.org/ontology/bibo/': 'bibo',
        'http://www.opengis.net/gml/': 'gml',
        'http://www.w3.org/2003/01/geo/wgs84_pos#': 'geo',
        'http://www.georss.org/georss/': 'georss',
        'http://www.w3.org/2004/02/skos/core#': 'skos',
        'http://www.ontologydesignpatterns.org/ont/d0.owl#': 'odp-d0',
        'http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#': 'odp-dul'
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
            edge.setProperty("created_at", System.currentTimeMillis())
            edge.setProperty("provenance", sourceFilename)
        } else {
            // TODO: handle additional literal datatypes
            // http://openrdf.callimachus.net/sesame/2.7/apidocs/org/openrdf/model/impl/LiteralImpl.html
            def datatype = st.object.getDatatype()
            if (datatype && datatype.getLocalName() == "float") {
                object = st.object.floatValue()
                propKey = predicate
            } else {
                object = st.object.getLabel()
                langCode = st.object.getLanguage()
                if (langCode) {
                    propKey = predicate + '@' + langCode
                } else {
                    propKey = predicate
                }
            }
            vSubj.setProperty(propKey, object)
            vSubj.setProperty("created_at", System.currentTimeMillis())
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

import org.openrdf.rio.*
import org.openrdf.rio.ntriples.*
import org.openrdf.rio.helpers.*
import org.openrdf.model.*
import org.apache.commons.compress.compressors.*

// Titan configuration & schema definitions
def prepareTitan(String storage_directory) {
    def conf = new BaseConfiguration()
    conf.setProperty("storage.backend", "cassandra")
    conf.setProperty("storage.directory", storage_directory)
    conf.setProperty("storage.batch-loading", true)
    conf.setProperty("storage.infer-schema", false)
      
    def g = TitanFactory.open(conf)
    g.makeKey("qname").dataType(String).single().unique().indexed(Vertex).make()
    _partition = g.makeKey("_partition").dataType(String).single().indexed(Vertex).make()
    createdAt = g.makeKey("created_at").dataType(Date).make()
    provenance = g.makeKey("provenance").dataType(String).make()
    rdfsLabel = g.makeKey("rdfs:label").dataType(String).make()
    // TODO: add definitions for all predicates with literal objects
    g.makeLabel("rdf:type").signature(createdAt, provenance).make()
    // TODO: add definitions for all edge types
    g.commit()

    bg = new BatchGraph(g, VertexIDType.STRING, 10000)
    bg.setVertexIdKey("qname")
    return bg
}

// Creates vertices and edges from RDF statements
class StatementsToGraphDB extends RDFHandlerBase {
    def tripleCount = 0L

    // Define known namespaces with their prefix
    def namespaces = [
        'http://dbpedia.org/resource/': 'dbp',
        'http://dbpedia.org/ontology/': 'dbp-owl',
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#': 'rdf',
        'http://www.w3.org/2000/01/rdf-schema#': 'rdfs',
        'http://www.w3.org/2002/07/owl#': 'owl',
        'http://xmlns.com/foaf/0.1/': 'foaf',
        'http://schema.org/': 'schema',
        'http://rdf.freebase.com/ns/': 'freebase',
        'http://www.wikidata.org/entity/': 'wikidata',
        'http://purl.org/dc/terms/': 'dcterms',
        'http://www.w3.org/2004/02/skos/core#': 'skos'
    ]

    def unknownNamespaces = new HashSet()
   
    def countedStatements = [:].withDefault{0}
   
    void handleStatement(Statement st) {
        def subject = qName(st.subject)
        def predicate = qName(st.predicate)
        def object
        if (st.object instanceof URI) {
            object = qName(st.object)
        } else {
            // TODO: handle literal datatypes
            object = st.object.getLabel()
        }
        println([subject, predicate, object])

        countedStatements[qName(st.predicate)] += 1
        if (++tripleCount%100000L == 0L) {
            println( (new Date()).toString() + \
            " Processed ${tripleCount} triples")
        }
    }
    
    def getCountedStatements() {
        return countedStatements
    }

    def qName(URI uri) {
        def nspc = uri.getNamespace()
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


def loadRdfFromFile(String filepath) {
    // Initialize a stream that feeds bz2-compressed triples
    def fin = new FileInputStream(filepath)
    def bis = new BufferedInputStream(fin)
    def cisFactory = new CompressorStreamFactory()
    cisFactory.setDecompressConcatenated(true)
    def cis = cisFactory.createCompressorInputStream(CompressorStreamFactory.BZIP2, bis)

    def graphCommitter = new StatementsToGraphDB()
    def rdfParser = new NTriplesParser()
    rdfParser.setRDFHandler(graphCommitter)

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

    return graphCommitter
}


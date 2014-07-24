import org.openrdf.rio.*
import org.openrdf.rio.ntriples.*
import org.openrdf.rio.helpers.*
import org.openrdf.model.*

// Creates vertices and edges from RDF statements
class StatementsToGraphDB extends RDFHandlerBase {

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
    documentUrl = new URL(filepath)
    def inputStream = documentUrl.openStream()
    def graphCommitter = new StatementsToGraphDB()
    def rdfParser = new NTriplesParser()
    rdfParser.setRDFHandler(graphCommitter)

    try {
        rdfParser.parse(inputStream, documentUrl.toString())
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


/**
 * Script for bulk loading RDF (e.g. DBpedia) into Titan
 *
 * Known issues:
 * - assign >= mx 1024mb to the Gremlin shell JVM
 * - Cassandra resource limits: http://www.datastax.com/documentation/cassandra/2.0/cassandra/troubleshooting/trblshootInsufficientResources_r.html
 */
import org.openrdf.rio.*
import org.openrdf.rio.turtle.*
import org.openrdf.rio.helpers.*
import org.openrdf.model.*
import org.apache.commons.compress.compressors.*
import javax.xml.bind.DatatypeConverter

// Creates vertices and edges from RDF statements
class StatementsToGraphDB extends RDFHandlerBase {
    TitanGraph graph
    String sourceFilename
    Long skipLines
    Long tripleCount
    HashSet unknownNamespaces
    Map countedStatements
    GraphTraversalSource g

    StatementsToGraphDB(TitanGraph graph_, String sourceFilename_, Long skipLines_) {
        graph = graph_
        sourceFilename = sourceFilename_
        skipLines = skipLines_
        tripleCount = 0L
        unknownNamespaces = new HashSet()
        countedStatements = [:].withDefault{0}
        g = graph.traversal()
    }

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
        'http://www.w3.org/2001/XMLSchema#': 'xsd',
        'http://dbpedia.org/resource/': 'dbr-en',
        'http://dbpedia.org/resource/Category:': 'dbc-en',
        'http://dbpedia.org/ontology/': 'dbo',
        'http://dbpedia.org/property/': 'dbp',
        'http://dbpedia.org/datatype/': 'dbdt',
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#': 'rdf',
        'http://www.w3.org/2000/01/rdf-schema#': 'rdfs',
        'http://www.w3.org/2002/07/owl#': 'owl',
        'http://xmlns.com/foaf/0.1/': 'foaf',
        'http://schema.org/': 'schema',
        'http://rdf.freebase.com/ns/': 'freebase',
        'http://www.wikidata.org/entity/': 'wikidata',
        'http://wikidata.org/entity/': 'wikidata',
        'http://wikidata.dbpedia.org/resource/': 'dbr-wikidata',
        'http://purl.org/dc/terms/': 'dct',
        'http://purl.org/dc/elements/1.1/': 'dce',
        'http://www.w3.org/2004/02/skos/core#': 'skos',
        'http://purl.org/ontology/bibo/': 'bibo',
        'http://www.opengis.net/gml/': 'gml',
        'http://www.w3.org/2003/01/geo/wgs84_pos#': 'geo',
        'http://www.georss.org/georss/': 'georss',
        'http://www.ontologydesignpatterns.org/ont/d0.owl#': 'odp-d0',
        'http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#': 'odp-dul'
    ]
    
    def dtc = new DatatypeConverter()
   
    void handleStatement(Statement st) {
        // Increment triple count and return early if this line should be skipped
        if (++tripleCount <= skipLines) {
            if (tripleCount%100000L == 0L) {
                println( (new Date()).toString() + \
                " Skipped ${humanFormat(tripleCount)} triples")
            }
            return
        }    
    
        def subject = qName(st.subject)
        def predicate = qName(st.predicate)
        def object, vObj, langCode, propKey

        def vSubj = getOrCreateVertex(subject)

        if (st.object instanceof URI) {
            object = qName(st.object, false)
            
            if (["rdf:type", "owl:sameAs"].contains(predicate)) {
                vSubj.property(Cardinality.SET, predicate, object)
            } else {            
                vObj = getOrCreateVertex(object)
                vSubj.addEdge(predicate, vObj)
                     .property("created_at", System.currentTimeMillis())
                     .property("provenance", sourceFilename)
                     .next()
            }
        } else {
            // TODO: handle additional literal datatypes
            // http://openrdf.callimachus.net/sesame/2.7/apidocs/org/openrdf/model/impl/LiteralImpl.html
            def datatype = st.object.getDatatype()
            propKey = predicate
            if (datatype?.getLocalName() == "float") {
                object = st.object.floatValue()
            } else if (datatype?.getLocalName() in ["date", "gYearMonth"]) {
                object = dtc.parseDate(st.object.getLabel()).getTime()
            } else if (datatype?.getLocalName() == "double") {
                object = st.object.doubleValue()
            } else if (datatype?.getLocalName() == "boolean") {
                object = st.object.booleanValue()
            } else if ( datatype && 
                ["gYear", "integer", "nonNegativeInteger", "positiveInteger"].contains(datatype.getLocalName())
            ) {
                def bint = st.object.integerValue()
                // if the value is to large for Integer, assign the min value
                object = bint == (int) bint ? bint : Integer.MIN_VALUE
            } else if (datatype?.getNamespace() == "http://dbpedia.org/datatype/") {
                object = "${st.object.getLabel()}^^${qName(datatype)}"
            } else {
                object = st.object.getLabel()
                langCode = st.object.getLanguage()
                if (langCode) {
                    propKey = predicate + '@' + langCode
                }
            }
            vSubj.property(propKey, object)
            vSubj.property("created_at", System.currentTimeMillis())
            vSubj.property("provenance", sourceFilename)
        }

        graph.tx().commit()

        countedStatements[qName(st.predicate)] += 1
        if (tripleCount%100000L == 0L) {
            println( (new Date()).toString() + \
            " Processed ${humanFormat(tripleCount)} triples")
        }
    }
    
    def getCountedStatements() {
        return countedStatements.sort { a, b -> b.value <=> a.value }
    }

    def qName(URI uri, Boolean validateNamespace=true) {
        // TODO: deal with categories -> dbc:
        def nspc = uri.getNamespace()
        if (nspc.split("/resource/").size() > 1) {
            nspc = nspc[0..(-nspc.split("/resource/")[-1].size()-1)]
        }
        def prefix = namespaces[nspc]
        if (prefix == null) {
            // resolve i18n DBpedia resources
            def matcher = nspc =~ /http:\/\/([a-z\-]+).dbpedia.org\/resource\//
            try {
                prefix = 'dbr-' + matcher[0][1]
            } catch (IndexOutOfBoundsException) {
                // warn about unknown namespace when first encountered
                if ( validateNamespace && !(nspc in unknownNamespaces) ) {
                    unknownNamespaces += nspc
                    println('Unknown namespace: ' + nspc)
                }
                return uri.stringValue()
            }
        }
        return prefix + ':' + uri.getLocalName()
    }

    def getOrCreateVertex(String qname) {
        try {
            return g.V().has('qname', qname).next()
        } catch (FastNoSuchElementException) {
            return graph.addVertex('qname', qname)
        }
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

// Titan configuration & schema definitions
def prepareTitan(String inferredSchema, ArrayList langCodes) {
    def conf = new BaseConfiguration()
    conf.setProperty("storage.backend", "cassandra")
    conf.setProperty("storage.hostname", "127.0.0.1")
    conf.setProperty("storage.batch-loading", true)
    conf.setProperty("schema.default", null)
    conf.setProperty("storage.index.search.backend", "elasticsearch")
    conf.setProperty("storage.index.search.client-only", true)
    //conf.setProperty("storage.index.search.local-mode", true)
      
    def graph = TitanFactory.open(conf)
    // Types should only be defined once
    // Assumption: if "qname" exists, all keys and labels exist.
    def mgmt = graph.openManagement()
    if (mgmt.containsPropertyKey("qname") == false) {
        // Make property keys
        qname = mgmt.makePropertyKey("qname").dataType(String).make()
        _partition = mgmt.makePropertyKey("_partition").dataType(String).make()
        // Define composite (key) indexes
        mgmt.buildIndex('by_qname', Vertex).addKey(qname).unique().buildCompositeIndex()
        
        createdAt = mgmt.makePropertyKey("created_at").dataType(Long).make()
        provenance = mgmt.makePropertyKey("provenance").dataType(String).make()
        flow = mgmt.makePropertyKey("flow").dataType(Double).make()
        hops = mgmt.makePropertyKey("hops").dataType(Integer).make()

        langCodes.each {
            mgmt.makePropertyKey("rdfs:label@" + it).dataType(String).make()
            mgmt.makePropertyKey("rdfs:comment@" + it).dataType(String).make()
            mgmt.makePropertyKey("skos:prefLabel@" + it).dataType(String).make()
            mgmt.makePropertyKey("georss:point@" + it).dataType(String).make()
            mgmt.makePropertyKey("foaf:nick@" + it).dataType(String).make()
            mgmt.makePropertyKey("foaf:name@" + it).dataType(String).make()
            mgmt.makePropertyKey("dce:language@" + it).dataType(String).make()
        }
        mgmt.makePropertyKey("rdfs:label").dataType(String).make()
        mgmt.makePropertyKey("foaf:name").dataType(String).make()
        mgmt.makePropertyKey("dce:description").dataType(String).make()
        mgmt.makePropertyKey("georss:point").dataType(String).make()
        lat = mgmt.makePropertyKey("geo:lat").dataType(Double).make()
        lon = mgmt.makePropertyKey("geo:long").dataType(Double).make()
        
        rdfType = mgmt.makePropertyKey("rdf:type").dataType(String).cardinality(Cardinality.SET).make()
        sameAs = mgmt.makePropertyKey("owl:sameAs").dataType(String).cardinality(Cardinality.SET).make()
        mgmt.buildIndex('by_type', Vertex).addKey(rdfType).buildCompositeIndex()
        mgmt.buildIndex('by_sameas', Vertex).addKey(sameAs).buildCompositeIndex()
        
        // Define mixed indexes
        // mgmt.buildIndex('latlon',Vertex.class).addKey(lat).addKey(lon).buildMixedIndex("search")
        
        // Make edge labels
        [
            "dct:subject", "dbo:wikiPageWikiLink",
            "dbo:wikiPageDisambiguates", "dbo:wikiPageRedirects",
            "skos:broader", "skos:related",
        ].each {
            itLabel = mgmt.makeEdgeLabel(it).multiplicity(Multiplicity.SIMPLE).signature(createdAt, provenance).make()
        }
        superordinateCategory = mgmt.makeEdgeLabel("superordinate_category")
                                    .multiplicity(Multiplicity.SIMPLE)
                                    .signature(hops, createdAt).make()

        // FIXME: https://github.com/thinkaurelius/titan/issues/1275
        //mgmt.buildEdgeIndex(superordinateCategory, 'superordinate_category_by_hops', Direction.BOTH, hops)
    }
    
    // Make keys and labels from an inferred datatype schema
    def namespaces = new StatementsToGraphDB(graph, "schema", 0).namespaces
    def createdAt = mgmt.getPropertyKey("created_at")
    def provenance = mgmt.getPropertyKey("provenance")
    new File(inferredSchema).eachLine { line ->
        def fields = line.split()
        def predUri = fields[0], range = fields[1]
        def lname = predUri.split(/[\/#]/)[-1]
        def nspc = predUri[0..-(lname.size()+1)]
        def label = "${namespaces[nspc]}:${lname}"
        if (range[0] == "@") {
            label += range
            range = "string"
        }
        if (mgmt.containsRelationType(label) == false) {
            println "..making RelationType ${label} for range ${range}"
            if (range == "uri") {
                mgmt.makeEdgeLabel(label).signature(createdAt,provenance).make()            
            } else if (range == "string") {
                mgmt.makePropertyKey(label).dataType(String).make()
            } else if (range[0..3] == "http") {
                def dtLname = range.split(/[\/#]/)[-1]
                def dtNspc = range[0..-(dtLname.size()+1)]
                if (dtLname in ["date", "gYearMonth"]) {
                    mgmt.makePropertyKey(label).dataType(Date).make()
                } else if (dtLname == "double") {
                    mgmt.makePropertyKey(label).dataType(Double).make()
                } else if (dtLname == "float") {
                    mgmt.makePropertyKey(label).dataType(Float).make()
                } else if (dtLname == "boolean") {
                    mgmt.makePropertyKey(label).dataType(Boolean).make()
                } else if (["gYear", "integer", "nonNegativeInteger", "positiveInteger"].contains(dtLname)) {
                    mgmt.makePropertyKey(label).dataType(Integer).make()
                } else if (dtNspc == "http://dbpedia.org/datatype/" || dtLname == "anyURI") {
                    mgmt.makePropertyKey(label).dataType(String).make()
                } else {
                    throw new Exception("Datatype ${range} not recognized")
                }
            }
        }
    }
    
    mgmt.commit()
    
    return graph
}

def loadRdfFromFile(Graph graph, String filepath, Long skipLines=0L) {
    // Initialize a stream that feeds bz2-compressed triples
    def fin = new FileInputStream(filepath)
    def bis = new BufferedInputStream(fin)
    def cisFactory = new CompressorStreamFactory(true)
    def cis = cisFactory.createCompressorInputStream(CompressorStreamFactory.BZIP2, bis)

    def sourceFilename = filepath.split('/')[-1][0..-5]
    def graphCommitter = new StatementsToGraphDB(graph, sourceFilename, skipLines)
    def rdfParser = new TurtleParser()
    def pConfig = rdfParser.getParserConfig()
    pConfig.addNonFatalError(BasicParserSettings.VERIFY_DATATYPE_VALUES)
    pConfig.addNonFatalError(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES)
    rdfParser.setRDFHandler(graphCommitter)
    def startTime = System.currentTimeMillis()
    
    if (skipLines) {
        println "Skipping the first ${skipLines} triples/lines..."
    }

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
    
    graph.tx().commit()

    def endTime = System.currentTimeMillis()
    def helpers = new Helpers()
    helpers.printLoadingTime(startTime, endTime, sourceFilename)

    println(graphCommitter.getCountedStatements())
    println("Unknown namespaces: " + graphCommitter.unknownNamespaces)

    return graphCommitter
}

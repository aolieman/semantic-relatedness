// Follows http://s3.thinkaurelius.com/docs/titan/current/hadoop-distributed-computing.html

def g

// create a Titan database connection that exists for the life of the mapper
def setup(args) {
    conf = new BaseConfiguration()
    conf.setProperty("storage.backend", "cassandra")
    conf.setProperty("storage.hostname", "127.0.0.1")
    conf.setProperty("schema.default", null)
    conf.setProperty("storage.index.search.backend", "elasticsearch")
    conf.setProperty("storage.index.search.client-only", false)
    conf.setProperty("storage.index.search.local-mode", true)
    g = TitanFactory.open(conf)
}

// process each FaunusVertex
//  - lookup the vertex in the TitanGraph using the FaunusVertex's numeric ID
//  - for each outgoing edge: collect vertices with the same label, and call findDuplicates
//  - findDuplicates groups edges by incoming vertex, keeps the newest, and removes all others

def map(u, args) {
    def v = g.v(u.id) // the Faunus vertex id is the same as the original Titan vertex id
    def eSameLabel = []
    def lastLabel = null
    def removeMsgs = []
    
    def findDuplicates = { edgeList ->
        eg = [:];
        toRemoveList = [];
        edgeList._().groupBy(eg){it.inV().next()}{it}.iterate();
        
        def keepNewest = { duplicates ->
            newest = duplicates.max{it.created_at};
            msg = "keep ${newest}";
            duplicates.findAll{it != newest}.each{
                msg += "\nremv ${it}";
                it.remove();
            }
            msg
        }
        
        eg.each{ key,val ->
            if(val.size>1){toRemoveList += keepNewest(val)}
        };
        toRemoveList
    }
    
    for (e in v.getEdges()){
        if (e.outV.next() != v) {continue}
        if (e.getLabel() == lastLabel){eSameLabel << e}
        else {
            if(eSameLabel.size>1){removeMsgs += findDuplicates(eSameLabel);}
            eSameLabel = [e]; lastLabel = e.label;
        }
    }
    removeMsgs += findDuplicates(eSameLabel)
    return removeMsgs ? removeMsgs.join("\n") : null
}


// close the Titan database connection
def cleanup(args) {
    g.commit()
    g.shutdown()
}


// without Hadoop
def removeDupEdges(v) {
    def eSameLabel = []
    def lastLabel = null
    def removeMsgs = []
    
    def findDuplicates = { edgeList ->
        eg = [:];
        toRemoveList = [];
        edgeList._().groupBy(eg){it.inV().next()}{it}.iterate();
        
        def keepNewest = { duplicates ->
            newest = duplicates.max{it.created_at};
            msg = "keep ${newest}";
            duplicates.findAll{it != newest}.each{
                msg += "\nremv ${it}";
                it.remove();
            }
            msg
        }
        
        eg.each{ key,val ->
            if(val.size>1){toRemoveList += keepNewest(val)}
        };
        toRemoveList
    }
    
    for (e in v.getEdges()){
        if (e.outV.next() != v) {continue}
        if (e.getLabel() == lastLabel){eSameLabel << e}
        else {
            if(eSameLabel.size>1){removeMsgs += findDuplicates(eSameLabel);}
            eSameLabel = [e]; lastLabel = e.label;
        }
    }
    removeMsgs += findDuplicates(eSameLabel)
    removeMsgs ? println(removeMsgs.join("\n")) : null
}

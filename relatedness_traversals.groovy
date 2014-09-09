/* Gremlin traversals to measure semantic relatedness between entities.
   Copy this file into Rexster's configured Gremlin script directory as "relatedness.gremlin".
*/

// Vertex getter by qName
def vqName(qname) {
    res = g.getVertices("qname", qname)
    if (res.size() == 0) {return null}
    else if (res.size() == 1) {return res.take(1)[0]}
    else {
        throw new IllegalArgumentException("$qname is associated with multiple vertices!")
    }
}

// Define categories to ignore (e.g. supernodes)
ign = ['dbp-nl:Categorie:Alles', 'dbp-nl:Categorie:Lijsten', 'dbp-nl:Categorie:Wikipedia',
       'dbp:Category:Container_categories', 'dbp:Category:Fundamental_categories',
       'dbp:Category:Main_topic_classifications']

def combine( Map... m ) {
  m.collectMany { it.entrySet() }.inject( [:] ) { result, e ->
    result << [ (e.key):e.value + ( result[ e.key ] ?: 0 ) ]
  }
}

def getCatFlowMap( topic_list, lang_code, max_topics=0 ){
    total_flow = [:]

    for (topic_slug in topic_list) {
        // Process one topic URI
        flow = [:]
        if (lang_code == "nl") {
            v = vqName("dbp-nl:$topic_slug")
        } else {
            v = vqName("dbp:$topic_slug")
        }

        // Count flow (Sibling, Narrower, Broader, and Cousin topics)
        if (v) {
            v.out('dcterms:subject').dedup().filter{!ign.contains(it.qname)}.as('mothers')
             .in('dcterms:subject').groupCount(flow){it.qname}{it.b+1.0}  // Sibling w1.0
             .back('mothers')
             .both('skos:broader').filter{!ign.contains(it.qname)}.in('dcterms:subject')
             .groupCount(flow){it.qname}{it.b+0.5}.iterate()              // Narrower & Broader w0.5

             // Normalize flow (as fraction)
             flow.remove(v.qname)
             flow_sum = flow.values().sum()
             flow.each{ it -> flow[it.key] = it.value / flow_sum }

             // Update total_flow to reflect average flow
             if (topic_list.size() > 1) {
                 flow.each{ it -> flow[it.key] = it.value / topic_list.size() }
             }
             total_flow = combine(total_flow, flow)
        }
    }

    // Sort by flow count
    flow_sorted = total_flow.sort{ a,b -> b.value <=> a.value }

    if (max_topics > 0) {
        if (total_flow.size() < max_topics){max_topics = total_flow.size()}
        flow_sorted = flow_sorted[0..max_topics-1]
    }

    return flow_sorted
}
// Define categories to ignore (e.g. supernodes)
def ign = [g.uri('dbp-nl:Categorie:Alles'), g.uri('dbp-nl:Categorie:Lijsten'),
           g.uri('dbp-nl:Categorie:Wikipedia'), g.uri('dbp:Category:Container_categories'),
           g.uri('dbp:Category:Fundamental_categories'), g.uri('dbp:Category:Main_topic_classifications')]

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
            v = g.v("http://nl.dbpedia.org/resource/$topic_slug")
            nsp_len = "http://nl.dbpedia.org/resource/".size()
        } else {
            v = g.v("http://dbpedia.org/resource/$topic_slug")
            nsp_len = "http://dbpedia.org/resource/".size()
        }

        // Count flow (Sibling, Narrower, Broader, and Cousin topics)

        v.out('dcterms:subject').dedup().filter{!ign.contains(it.id)}.as('mothers')
         .in('dcterms:subject').groupCount(flow){it.id.substring(nsp_len)}{it.b+1.0}  // Sibling w1.0
         .back('mothers')
         .both('skos:broader').filter{!ign.contains(it.id)}.in('dcterms:subject')
         .groupCount(flow){it.id.substring(nsp_len)}{it.b+0.5}.iterate()              // Narrower & Broader w0.5

         // Normalize flow (as fraction)
         flow.remove(v.id)
         flow_sum = flow.values().sum()
         flow.each{ it -> flow[it.key] = it.value / flow_sum }

         // Update total_flow to reflect average flow
         if (topic_list.size() > 1) {
             flow.each{ it -> flow[it.key] = it.value / topic_list.size() }
         }
         total_flow = combine(total_flow, flow)

    }

    // Sort by flow count
    flow_sorted = total_flow.sort{ a,b -> b.value <=> a.value }

    if (max_topics > 0) {
        if (total_flow.size() < max_topics){max_topics = total_flow.size()}
        flow_sorted = flow_sorted[0..max_topics-1]
    }

    return flow_sorted
}
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
def ign = [vqName('dbp-nl:Categorie:Alles').id, vqName('dbp-nl:Categorie:Lijsten').id,
           vqName('dbp-nl:Categorie:Wikipedia').id, vqName('dbp:Category:Container_categories').id,
           vqName('dbp:Category:Fundamental_categories').id, vqName('dbp:Category:Main_topic_classifications').id]

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

        v.out('dcterms:subject').dedup().filter{!ign.contains(it.id)}.as('mothers')
         .in('dcterms:subject').groupCount(flow){it.qname}{it.b+1.0}  // Sibling w1.0
         .back('mothers')
         .both('skos:broader').filter{!ign.contains(it.id)}.in('dcterms:subject')
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

    // Sort by flow count
    flow_sorted = total_flow.sort{ a,b -> b.value <=> a.value }

    if (max_topics > 0) {
        if (total_flow.size() < max_topics){max_topics = total_flow.size()}
        flow_sorted = flow_sorted[0..max_topics-1]
    }

    return flow_sorted
}

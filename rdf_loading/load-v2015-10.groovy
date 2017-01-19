graph = prepareTitan("semantic-relatedness/rdf_loading/inferred_schema.txt", ['en', 'nl'])

handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/en/category_labels_en.ttl.bz2") // 1.3M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/en/skos_categories_en.ttl.bz2") // 5.4M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/en/article_categories_en.ttl.bz2") // 21.6M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/en/disambiguations_en.ttl.bz2") // 1.4M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/en/long_abstracts_en.ttl.bz2") // 4.6M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/en/instance_types_en.ttl.bz2") // 5.1M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/en/interlanguage_links_en.ttl.bz2") // 33.0M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/en/labels_en.ttl.bz2") // 12.0M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/en/redirects_en.ttl.bz2") // 7.1M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/en/short_abstracts_en.ttl.bz2") // 4.6M

handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/nl/category_labels_nl.ttl.bz2") // 92k
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/nl/skos_categories_nl.ttl.bz2") // 379k
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/nl/article_categories_nl.ttl.bz2") // 2.7M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/nl/disambiguations_nl.ttl.bz2") // 285k
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/nl/long_abstracts_nl.ttl.bz2") // 1.8M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/nl/instance_types_nl.ttl.bz2") // 1.7M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/nl/interlanguage_links_nl.ttl.bz2") // 17.0M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/nl/labels_nl.ttl.bz2") // 2.5M
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/nl/redirects_nl.ttl.bz2") // 660k
handler = loadRdfFromFile(graph, "dbpedia-dumps/v2015-10/nl/short_abstracts_nl.ttl.bz2") // 1.8M

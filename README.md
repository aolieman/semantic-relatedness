Semantic Relatedness
====================

Facilitates relatedness measurements between KB entries for, e.g., collective disambiguation.

Semantic Relatedness depends on a knowledge base to perform relatedness measurements between entities. At this stage of development, the module includes a bulk loading script that facilitates loading RDF triples from DBpedia to a local Graph DB. Other knowledge bases may be supported in the future.

Work on this module has been supported by:
- [University of Amsterdam](http://www.illc.uva.nl/)
- [Stamkracht BV](http://www.stamkracht.com/)

Usage
-----

```bash
$ /path/to/semantic-relatedness/rdf_loading/download_dumps.sh /path/to/dbpedia_dumps en
    Downloading English DBpedia dumps
$ /path/to/semantic-relatedness/rdf_loading/download_dumps.sh /path/to/dbpedia_dumps nl
    Downloading Dutch DBpedia dumps
    
$ ./titan/bin/gremlin.sh /path/to/semantic-relatedness/rdf_loading/load_triples.groovy

         \,,,/
         (o o)
-----oOOo-(_)-oOOo-----

gremlin> bg = prepareTitan("semantic-relatedness/rdf_loading/inferred_schema.txt", ['en', 'nl'])
gremlin> handler = loadRdfFromFile(bg, "/path/to/dbpedia_dumps/v2014/en/skos_categories_en.nt.bz2")
```

License
-------

This project is licensed under the terms of the [MIT license](http://opensource.org/licenses/MIT).

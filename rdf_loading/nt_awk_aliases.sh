#!/bin/bash
# N-Triples aliases from http://blog.datagraph.org/2010/03/grepping-ntriples
alias rdf-subjects="awk '/^\s*[^#]/ { print \$1 }' | uniq"
alias rdf-predicates="awk '/^\s*[^#]/ { print \$2 }' | uniq"
alias rdf-objects="awk '/^\s*[^#]/ { ORS=\"\"; for (i=3;i<=NF-1;i++) print \$i \" \"; print \"\n\" }' | uniq"
alias rdf-datatypes="awk -F'\x5E' '/\"\^\^</ { print substr(\$3, 2, length(\$3)-4) }' | uniq"

# Custom N-Triples aliases
alias rdf-pred-dt="awk -F'> [<\"]' '/\"\^\^</ { match(\$2, /(#|y\/)([a-zA-z]+)/, pred); match(\$3, /#([a-zA-Z]+)>/, obj); print pred[2], obj[1] }' | uniq"

# TODO: find pred with plain literals /\" \./
# TODO: find pred with lang-code literals /\"@/
# TODO: find pred with URI objects
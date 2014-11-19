#!/bin/bash
# Download DBpedia dumps to the given directory

USGSTR="Usage: download_dumps.sh <download_root_dir> <language_code (e.g. 'en')>"
DBP_VERSION=2014
echo "Downloading dumps for DBpedia v$DBP_VERSION. Edit the script source to change this version."

if [ -n "$1" ] && [ -n "$2" ]; then
  target_dir="$1/v$DBP_VERSION"
  lc="$2"
else  
  echo $USGSTR
  exit 1
fi

FNAMES=( "article_categories_$lc.nt.bz2" "category_labels_$lc.nt.bz2" "disambiguations_$lc.nt.bz2" 
         "geo_coordinates_$lc.nt.bz2" "instance_types_$lc.nt.bz2" 
         "interlanguage_links_$lc.nt.bz2" "labels_$lc.nt.bz2" "page_links_$lc.nt.bz2" "redirects_$lc.nt.bz2" 
         "short_abstracts_$lc.nt.bz2" "skos_categories_$lc.nt.bz2" "mappingbased_properties_$lc.nt.bz2")
if [ $lc -eq "en" ]; then
    FNAMES+=( "instance_types_heuristic_$lc.nt.bz2" )
fi
         
for fn in "${FNAMES[@]}"
do
    fpath="$target_dir/$lc/$fn"
    if [ -s "$fpath" ]; then
        echo "$fpath already exists"
    else
        url="http://data.dws.informatik.uni-mannheim.de/dbpedia/$DBP_VERSION/$lc/$fn"
        echo "Downloading $url..."
        wget -P "$target_dir/$lc/" "$url"
    fi
done

echo "All done!"
exit 0
        

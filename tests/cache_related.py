import os
from rexster_client import *

def get_category_map(topic_slug_list, language_code, port=8182):

    # Set up language-specific processing
    def slug_esc(s):
        return s

    if language_code == u"nl":
        prefix = u"dbp-nl:"
    else:
        prefix = u"dbp:"

        # English DBpedia uses URI instead of IRI, thus needs to escape non-ASCII
        def slug_esc(s):
            return encode_non_ascii(s)

    # Build the Gremlin script string
    prefixed_slugs = [u'"{0}{1}"'.format(prefix, slug_esc(slug)) for slug in topic_slug_list
                      if slug is not None and is_possible_wiki_slug(slug)]
    script_value = u"getCatMap([{0}], '{1}')".format(u", ".join(prefixed_slugs), language_code)

    url = base_url.format(port=port, path="graphs/dbp-sail/tp/gremlin")
    payload = {'script': script_value, 'load': "[v0_categories]"}

    print url, json.dumps(payload)

    resp = requests.post(url, data=payload)

    try:
        resp_dict = resp.json()
        if resp.status_code is not 200:
            print resp_dict
            # Somehow POSTs can give Rexster the hiccups
            # and a GET puts it in a working state again
            requests.get(url, params=payload)
            return get_category_map(topic_slug_list, language_code)
        resp_dict['n_found_cats'] = len(resp_dict["results"][0])
        return resp_dict
    except Exception as e:
        print "Server error:", e
        return {"n_found_cats": 0}
        
def cache_related_resources(dir_path, start=0):
    ann_file_paths = [os.path.join(dir_path, fn) for fn 
        in os.listdir(dir_path) if fn.endswith("_annotations.json")]
    for i, fp in enumerate(ann_file_paths):
        print "{:3d} {}".format(i, fp[len(dir_path):])
        
    for fp in ann_file_paths[start:]:
        with open(fp, 'rb') as f:
            annotations = json.load(f)
        print fp
        for ann in annotations:
            try:
                for cand in ann[u'resource']:
                    slug = unicode(cand[u'uri'])
                    cat_map = get_category_map([slug], 'en')
                    print "N found cats:", cat_map['n_found_cats'], "\n"
                    flow_map = get_cat_flow_map(list([slug]), 'en')
                    print "N related resources:", flow_map['n_related_topics'], "\n"
            except KeyError:
                pass

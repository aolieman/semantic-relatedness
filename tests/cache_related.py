# -*- coding: utf-8 -*-
import os, urllib2, requests, json

# Define the Rexster base URL
base_url = u"http://your_server:{port:d}/{path}"

def encode_non_ascii(s):
    return urllib2.quote(s.encode('utf8'),
                         ''.join([chr(i) for i in range(128)]))
                         
def is_possible_wiki_slug(s):
    if s.islower():
        # User-generated slugs are lowercase
        if not s[0].isdigit():
            return False
        else:
            # Some wikislugs are lowercase but start with a digit
            return True
    else:
        # Most wikislugs contain a capital character
        return True

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

def get_flow_map(topic_slug_list, language_code, port=8182):
    # Build the Gremlin script string
    if language_code == u'en':
        escaped_slugs = [u'"{0}"'.format(encode_non_ascii(slug)) for slug in topic_slug_list
                         if slug is not None and is_possible_wiki_slug(slug)]
    else:
        escaped_slugs = [u'"{0}"'.format(slug) for slug in topic_slug_list
                         if slug is not None and is_possible_wiki_slug(slug)]
    script_value = u"getFlowMap([{0}], '{1}')".format(u", ".join(escaped_slugs), language_code)

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
            return get_flow_map(topic_slug_list, language_code)
        resp_dict['n_related_topics'] = len(resp_dict["results"][0])
        return resp_dict
    except Exception as e:
        print "Server error:", e
        return {"n_related_topics": 0}
        
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
                    flow_map = get_flow_map(list([slug]), 'en')
                    print "N related resources:", flow_map['n_related_topics'], "\n"
            except KeyError:
                pass
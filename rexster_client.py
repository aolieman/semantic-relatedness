# -*- coding: utf-8 -*-
import urllib2, requests, json

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

def get_cat_flow_map(topic_slug_list, language_code, max_topics=0, port=8182):
    # Build the Gremlin script string
    if language_code == u'en':
        escaped_slugs = [u'"{0}"'.format(encode_non_ascii(slug)) for slug in topic_slug_list
                         if slug is not None and is_possible_wiki_slug(slug)]
    else:
        escaped_slugs = [u'"{0}"'.format(slug) for slug in topic_slug_list
                         if slug is not None and is_possible_wiki_slug(slug)]
    script_value = u"getCatFlowMap([{0}], '{1}', {2})".format(u", ".join(escaped_slugs), language_code, max_topics)

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
            get_script_value = u"getCatFlowMap([{0}], '{1}')".format(u", ".join(escaped_slugs[:2]), language_code)
            get_payload = {'script': get_script_value, 'load': "[v0_categories]"}
            requests.get(url, params=get_payload)
            return get_cat_flow_map(topic_slug_list, language_code)
        resp_dict['n_related_topics'] = len(resp_dict["results"][0])
        return resp_dict
    except Exception as e:
        print "Server error:", e
        return {"n_related_topics": 0}
from collections import defaultdict
from operator import iadd, isub
from rexster_client import get_cat_flow_map

def oper_vals_to_avg(oper, divisor, avg_dict, *dicts):
    """
    Applies oper(oldval, value / divisor) to flow dicts by key;
    returns a defaultdict(float).
    Assumes values can be added to 0.0; raises when needed.
    """
    sum_dict = defaultdict(float)
    for dict in dicts:
        for k, v in dict.iteritems():
            oper(sum_dict[k], v / divisor)
    return sum_dict

def rerank_unweighted(annotations):
    # Get per-annotation and average flow
    avg_flow_map = {}
    n_ann = len(annotations)
    for ann in annotations:
        slug_set = set()
        try:
            for cand in ann['resource']:
                cand['uri'] = unicode(cand['uri'])
                slug_set.add(cand['uri'])
            ann['cat_flow_map'] = get_cat_flow_map(list(slug_set), 'en')
            avg_flow_map = oper_vals_to_avg(iadd, n_ann, avg_flow_map, ann['cat_flow_map'])
        except KeyError:
            ann['resource'] = []

    # Rerank candidates by cat_flow_score
    min_flow = 0.9 * min(avg_flow_map.itervalues())
    for ann in annotations:
        context_flow = oper_vals_to_avg(isub, n_ann, avg_flow_map, ann['cat_flow_map'])
        for cand in ann['resource']:
            cand['cat_flow'] = context_flow[cand['uri']]
            cand['cat_flow_score'] = cand['finalScore'] * max(min_flow, cand['cat_flow'])
        ann['resource'] = sorted(ann['resource'], key=lambda c: c['cat_flow_score'])

    return annotations
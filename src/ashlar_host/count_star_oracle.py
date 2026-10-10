"""Finite independent original-commerce occurrence bags and COUNT(*) HAVING.

Original object values and edge ordinal occurrences determine results. Compiler
SQL, native result observations and distinct destinations never determine counts.
"""
import hashlib
import json
from ashlar.commerce_source import SOURCE_SHA, GRAPH_SHA

PLAIN='SELECT COUNT(*) AS rows FROM order_lines l'
GROUPED='SELECT l.product_id,COUNT(*) AS rows FROM order_lines l GROUP BY l.product_id ORDER BY l.product_id'
HAVING='SELECT l.product_id,COUNT(*) AS rows FROM order_lines l GROUP BY l.product_id HAVING COUNT(*)>0 ORDER BY l.product_id'
EXPANDED='SELECT l.id,COUNT(*) AS rows FROM order_lines l CROSS JOIN EXPAND_PATHS(l."order_lines.product_id", "products.supplier_id") AS p GROUP BY l.id HAVING COUNT(*)>0 ORDER BY l.id'

def commerce_count_star_cases():
    return (('plain-count',PLAIN),('grouped-count',GROUPED),('grouped-count-having',HAVING),('expanded-count-having',EXPANDED))

def evaluate_complete_bags(bags, *, threshold=None, maximum_occurrences=1000):
    """Check every full group before selection; preserve parallel occurrences."""
    if type(maximum_occurrences)is not int or not 1<=maximum_occurrences<=1000 or (threshold is not None and (type(threshold)is not int or not 0<=threshold<9223372036854775808)):
        raise ValueError('invalid-count-bounds')
    if type(bags)is not list or len(bags)>1000:raise ValueError('complete-bag-bound')
    total=0;seen=set();rows=[]
    for item in bags:
        if type(item)is not tuple or len(item)!=2:raise ValueError('group-shape')
        key,occurrences=item
        if type(key)is not str or not key or key in seen or type(occurrences)is not list:raise ValueError('group-identity')
        seen.add(key);total+=len(occurrences)
        if total>maximum_occurrences or len(occurrences)>9223372036854775807:raise ValueError('complete-bag-capacity')
        identities=set()
        for occurrence in occurrences:
            if type(occurrence)is not tuple or not occurrence or any(type(atom)is not str or not atom for atom in occurrence)or occurrence in identities:raise ValueError('occurrence-integrity')
            identities.add(occurrence)
        if occurrences and (threshold is None or len(occurrences)>threshold):rows.append([key,str(len(occurrences))])
    rows.sort(key=lambda row:row[0].encode('utf8'))
    return rows,total

def original_commerce_count_star_oracle(model,graph,request):
    if type(model)is not bytes or type(graph)is not bytes or hashlib.sha256(model).hexdigest()!=SOURCE_SHA or hashlib.sha256(graph).hexdigest()!=GRAPH_SHA:
        raise ValueError('original-commerce-source-required')
    if type(request)is not dict or request.get('sql')not in dict(commerce_count_star_cases()).values():raise ValueError('uncovered-count-query')
    document=json.loads(graph);objects={o['key']:o for o in document['objects']}
    if len(objects)!=len(document['objects']):raise ValueError('duplicate-object')
    lines=[o for o in document['objects']if o['type']['element']=='order_lines'];edges=list(enumerate(document['edges'],1));sql=request['sql'];groups={};pairs=[]
    for line in lines:
        if sql==EXPANDED:
            identity=line['values']['order_lines.id'];bag=groups.setdefault(identity,[])
            for first_id,first in edges:
                if first['relationship']['id']!='order_lines.product_id'or first['source']!=line['key']:continue
                product=objects[first['target']]
                for second_id,second in edges:
                    if second['relationship']['id']!='products.supplier_id'or second['source']!=product['key']:continue
                    objects[second['target']]
                    occurrence=(str(first_id),str(second_id));bag.append(occurrence);pairs.append(list(occurrence))
        else:
            identity=line['values']['order_lines.product_id'];groups.setdefault(identity,[]).append((line['key'],))
    rows,total=evaluate_complete_bags(list(groups.items()),threshold=0 if sql in (HAVING,EXPANDED)else None)
    if sql==PLAIN:rows=[[str(total)]]
    return {'rows':rows,'witnesses':{'occurrences':total,'edge_pairs':pairs},'scope':'Four exact original-commerce COUNT(*) cases; complete independent source occurrence bags before HAVING; no source/native authority'}

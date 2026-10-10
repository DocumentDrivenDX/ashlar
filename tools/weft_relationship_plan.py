"""Closed original-commerce one-hop proof; no two-hop or numeric widening.

The trusted indexed public compiler must recompute the complete artifact before
callbacks. Exact equality preserves every emitted slot/check/descriptor; it is
not replaced by a host reconstruction of compiler SQL semantics. Parallel-edge
versus distinct-neighbor multiplicity remains an upstream qualification gap.
"""
import hashlib,json
from run_commerce_publication_weft import compiler_request
from local_delta_custody import encoded

RELATION='products.supplier_id'
SQL='SELECT p.id, RELATED_KEYS(p."products.supplier_id", 2) AS suppliers FROM products p ORDER BY p.id'
BACKEND={'backendId':'ashlar.databricks','backendVersion':'0.1.0-candidate','interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql-candidate'}
OBLIGATIONS={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.candidate.relationshipIntegrity'}

def relationship_request(model: bytes, bindings: dict, manifest: dict, registry: list, aliases: dict) -> dict:
    request=compiler_request(SQL,model,bindings,manifest,registry,aliases,fields=True)
    binding=json.loads(request['target']['bindingJson']);document=json.loads(model)
    definitions=[r for m in document['modules'] if m['id']=='domain' for r in m.get('relationships',[]) if r['id']==RELATION]
    if len(definitions)!=1:raise ValueError('Exact original relationship required')
    definition=definitions[0]
    if definition['source']!=[{'module':'domain','element':'products'}] or definition['target']!=[{'module':'domain','element':'suppliers','key':'identity'}] or definition['directed']is not True:raise ValueError('Original relation endpoints/key changed')
    types=[r for r in bindings['types'] if r['identity']==['edge','domain',RELATION]]
    tables=[i for i,t in enumerate(binding['publication']['tables']) if t['name'][-1]=='edge_current']
    if len(types)!=1 or len(tables)!=1:raise ValueError('Separate original edge carrier required')
    records={r['logical']['element']:r for r in binding['records']};pin=binding['modelPins'][0]
    binding['relationships']=[{'logical':{'documentId':pin['documentId'],'module':'domain','relationship':RELATION,'revision':pin['revision']},'acceptedDefinition':definition,'table':tables[0],'kind':'edge','sourceSystem':records['products']['sourceSystem'],'typeId':types[0]['type_id'],'schemaRevision':pin['revision'],'source':records['products']['logical'],'target':records['suppliers']['logical']}]
    raw=encoded(binding);request['target']['bindingJson']=raw;request['target']['bindingSha256']=hashlib.sha256(raw.encode()).hexdigest()
    return request

def admit_relationship_artifact(request: dict, artifact: dict, recompiled: dict) -> dict:
    if request['sql']!=SQL or encoded(artifact)!=encoded(recompiled):raise ValueError('Exact public recompilation required before callbacks')
    binding=json.loads(request['target']['bindingJson'])
    if hashlib.sha256(request['target']['bindingJson'].encode()).hexdigest()!=request['target']['bindingSha256']:raise ValueError('Original binding bytes differ from request hash')
    if artifact.get('status')!='compiled' or artifact.get('backend')!=BACKEND or artifact.get('bindingSha256')!=request['target']['bindingSha256'] or artifact.get('modelPins')!=binding['modelPins']:raise ValueError('Original compiler custody differs')
    obligations=artifact.get('obligations')
    if type(obligations)is not list or len(obligations)!=3 or {o['id'] for o in obligations}!=OBLIGATIONS:raise ValueError('Closed complete relationship obligations required')
    p=next(o['parameters'] for o in obligations if o['id']=='ashlar.candidate.publication')
    for name in ('publication','modelPins','layoutRevision','layoutSha256'):
        if p[name]!=binding[name]:raise ValueError('Original publication obligation differs')
    for o in obligations:
        if o['id']== 'ashlar.candidate.publication':continue
        p=o['parameters']
        if p.get('phase')!='before-user-query' or p.get('samePublicationRequired')is not True or type(p.get('checks'))is not list or not p['checks']:raise ValueError('Complete before-query guards required')
    slots=artifact.get('parameters')
    if type(slots)is not list or any(type(s['position'])is not int or s['position']!=i for i,s in enumerate(slots,1)):raise ValueError('Complete ordered compiler slot inventory required')
    return binding

def decode_relationship_rows(rows: list) -> list:
    answer=[]
    if type(rows)is not list:raise ValueError('Complete ordered rows required')
    for row in rows:
        if type(row)is not dict or set(row)!={'id','suppliers'} or type(row['id'])is not str or type(row['suppliers'])is not str:raise ValueError('Exact native String carriers required')
        def pairs(items):
            d={}
            for k,v in items:
                if k in d:raise ValueError('Duplicate related carrier member')
                d[k]=v
            return d
        cell=json.loads(row['suppliers'],object_pairs_hook=pairs)
        if type(cell)is not dict or set(cell)!={'items','truncated'} or type(cell['truncated'])is not bool or type(cell['items'])is not list or len(cell['items'])>2:raise ValueError('Closed bounded relation envelope required')
        if any(type(k)is not list or len(k)!=1 or type(k[0])is not str for k in cell['items']):raise ValueError('Exact original String key tuples required')
        if cell['items']!=sorted(cell['items'],key=lambda k:k[0].encode('utf-8')) or (cell['truncated'] and len(cell['items'])!=2):raise ValueError('Exact ordered lookahead envelope required')
        answer.append({'id':row['id'],'suppliers':cell})
    return answer

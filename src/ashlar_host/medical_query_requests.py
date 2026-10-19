# SPDX-License-Identifier: MIT OR Apache-2.0
"""Finite original medical cases; pure source witnesses, never authority."""
from collections import Counter
import hashlib,json
from ashlar.medical_source import SOURCE_SHA,GRAPH_SHA
from .delta_custody import encoded
from .medical_singleton_request import medical_singleton_request

FIELDS=('resource_key','patient_key','status','class_system','class_code')
FILTERED_LIMIT="SELECT "+', '.join('e.'+f for f in FIELDS)+" FROM encounters e ORDER BY e.resource_key LIMIT 2"
SINGLETON="SELECT "+', '.join('e.'+f for f in FIELDS)+" FROM encounters e WHERE e.resource_key = 'Encounter/example'"


def _original(model,graph):
    if (type(model)is not bytes or type(graph)is not bytes or hashlib.sha256(model).hexdigest()!=SOURCE_SHA
            or hashlib.sha256(graph).hexdigest()!=GRAPH_SHA):raise PermissionError('Exact original medical source required')
    return json.loads(model),json.loads(graph)


def medical_object_key(document,record,atoms):
    """Original declared one-STRING key subset consumed by these two cases."""
    elements={e['id']:e for m in document['modules']if m['id']=='domain'for e in m['elements']}
    owner=elements[record];keys=[k for k in owner['keys']if k.get('primary')is True]
    if len(keys)!=1 or len(keys[0]['fields'])!=1 or len(atoms)!=1 or type(atoms[0])is not str:
        raise ValueError('Exact original required STRING primary key required')
    field=elements[keys[0]['fields'][0]['element']]
    if field['scalarType']!='string' or field['nullability']!='required':raise ValueError('Original required STRING key subset only')
    return json.dumps([document['id'],'domain',record,list(atoms)],ensure_ascii=False,separators=(',',':'))


def prepare_medical_queries(model,graph,baseline,bindings,manifest,registry,aliases,cases):
    if type(cases)is not tuple or not cases or len(set(cases))!=len(cases) or any(c not in ('singleton','two-hop','filtered-limit','all-objects')for c in cases):
        raise ValueError('Closed unique finite medical cases required')
    document,original=_original(model,graph)
    request=medical_singleton_request(model,graph,baseline,bindings,manifest,registry,aliases)
    binding=json.loads(request['target']['bindingJson']);records={(r['logical']['module'],r['logical']['element']):r for r in binding['records']}
    types={tuple(t['identity']):t['type_id']for t in bindings['types']}
    definitions=[(m['id'],r)for m in document['modules']for r in m.get('relationships',[])]
    edges_table=next(i for i,t in enumerate(binding['publication']['tables'])if t['name'][-1]=='edge_current')
    relations=[]
    for module,rel in definitions:
        if rel['directed']is not True or len(rel['source'])!=1 or len(rel['target'])!=1:raise ValueError('Original directed binary relationship required')
        a,b=rel['source'][0],rel['target'][0];source=records[(a['module'],a['element'])];target=records[(b['module'],b['element'])]
        relations.append({'logical':{'documentId':document['id'],'revision':SOURCE_SHA,'module':module,'relationship':rel['id']},
            'acceptedDefinition':rel,'table':edges_table,'kind':'edge','sourceSystem':source['sourceSystem'],
            'typeId':types[('edge',module,rel['id'])],'schemaRevision':SOURCE_SHA,'source':source['logical'],'target':target['logical']})
    if len(relations)!=11:raise ValueError('Complete original eleven medical relationships required')
    if 'all-objects'in cases:
        fields={(m['id'],e['id']):e for m in document['modules']for e in m['elements']}
        for obj in original['objects']:
            record=fields[obj['type']['module'],obj['type']['element']]
            if any(ref['element']not in obj['values']for ref in record['members']):raise ValueError('Original present-key medical subset required')
        for record in binding['records']:
            for prop in record['properties']:
                field=prop['logical']
                if fields[field['module'],field['element']].get('nullability')=='absent-allowed':
                    prop['home']['encoding']='ashlar-weft-json-native-null/0.1-candidate'
    binding['relationships']=relations;binding_raw=encoded(binding)
    request['target'].update(bindingJson=binding_raw,bindingSha256=hashlib.sha256(binding_raw.encode()).hexdigest())
    objects={o['key']:o for o in original['objects']};edges=original['edges']
    assigned={(v['kind'],v['originalKey']):v for v in bindings['entities']};prepared=[];expected_paths=[]
    def add(name,sql,rows,witnesses):
        selected=json.loads(encoded(request));selected['sql']=sql
        prepared.append({'id':name,'request':selected,'oracle':{'rows':rows,'witnesses':witnesses,'scope':'Independent complete original medical graph; selected finite original medical cases only.'}})
    if 'all-objects'in cases:
        for module in document['modules']:
            fields={e['id']:e for e in module['elements']}
            for record in module['elements']:
                if record['kind']!='record':continue
                members=[ref['element']for ref in record['members']];population=[o for o in original['objects']if o['type']=={'document':document['id'],'module':module['id'],'element':record['id']}];rows=[]
                for obj in population:
                    row=[]
                    for member in members:
                        token=obj['values'][member];field=fields[member]
                        if field.get('nullability')=='absent-allowed':
                            if token is None:value={'state':'null'}
                            else:
                                atom=token
                                if field['scalarType']=='boolean':
                                    proofs=[p for p in json.loads(baseline)['booleanReceipts']if p['request']['sourceContext']['objectKey']==obj['key']and p['request']['field']['element']==member]
                                    if len(proofs)!=1:raise ValueError('Original Boolean receipt required')
                                    atom=proofs[0]['value']['boolean']
                                value={'state':'value','value':atom}
                            row.append(encoded(value))
                        else:row.append(token)
                    rows.append(row)
                sql='SELECT '+', '.join('r.'+m.split('.',1)[1]for m in members)+' FROM '+record['id']+' r'
                add('all-objects-'+record['id'],sql,rows,{'original_rows':population,'members':members,'record':record['id']})
    if 'singleton'in cases:
        first=min(original['objects'],key=lambda o:o['key'])
        if first['type']['element']!='encounters':raise ValueError('Original singleton record correspondence differs')
        add('singleton',SINGLETON,[[first['values']['encounters.'+f]for f in FIELDS]],{'original_rows':[first]})
    if 'filtered-limit'in cases:
        first=min(original['objects'],key=lambda o:o['key'])
        population=sorted((o for o in original['objects']if o['type']==first['type']),key=lambda o:o['key'])
        if first['type']['element']!='encounters' or len(population)!=1:
            raise ValueError('Pinned original encounter population correspondence differs')
        selected=population[:2]
        add('filtered-limit',FILTERED_LIMIT,[[o['values']['encounters.'+f]for f in FIELDS]for o in selected],
            {'original_rows':selected,'complete_population':len(population),'limit':2,'original_key_order':[o['key']for o in population]})
    if 'two-hop'in cases:
        pairs=[(a,b)for _,a in definitions for _,b in definitions if a['target'][0]['module']==b['source'][0]['module']and a['target'][0]['element']==b['source'][0]['element']]
        if len(pairs)!=5:raise ValueError('Complete five original medical two-hop pairs required')
        for number,(a,b)in enumerate(pairs):
            record=a['source'][0]['element'];middle_record=a['target'][0]['element'];terminal_record=b['target'][0]['element'];rows=[];witnesses=[]
            for source in original['objects']:
                if source['type']['element']!=record:continue
                items=[]
                for first in edges:
                    if first['relationship']['id']!=a['id']or first['source']!=source['key']:continue
                    for second in edges:
                        if second['relationship']['id']!=b['id']or second['source']!=first['target']:continue
                        middle,target=objects[first['target']],objects[second['target']]
                        items.append({'intermediate':[middle['values'][middle_record+'.resource_key']],
                            'terminal':[target['values'][terminal_record+'.resource_key']],
                            'edges':[assigned['edge',first['key']]['id'],assigned['edge',second['key']]['id']]})
                        witnesses.append([source['key'],first['key'],middle['key'],second['key'],target['key']])
                items.sort(key=lambda p:(tuple(s.encode()for s in p['intermediate']),tuple(s.encode()for s in p['terminal']),tuple(int(s)for s in p['edges'])))
                rows.append([source['values'][record+'.resource_key'],json.dumps({'items':items,'truncated':False},ensure_ascii=False,separators=(',',':'))])
            sql='SELECT s.resource_key, RELATED_PATHS(s.'+encoded(a['id'])+', '+encoded(b['id'])+', 63) AS paths FROM '+record+' s'
            add('two-hop-'+str(number+1),sql,rows,{'relationships':[a['id'],b['id']],'records':[record,middle_record,terminal_record],'occurrences':witnesses})
            expected_paths.extend(witnesses)
        complete=[[a['source'],a['key'],a['target'],b['key'],b['target']]for a in edges for b in edges if a['target']==b['source']]
        if len(complete)!=20 or Counter(map(encoded,expected_paths))!=Counter(map(encoded,complete)):raise ValueError('Complete original twenty two-hop occurrence bag differs')
    return tuple(prepared)


def require_medical_case_bag(model,graph,bindings,prepared,artifact,evidence):
    """Check actual decoded native witnesses against the independent original graph."""
    from ashlar.weft_path_decode import decode_related_paths,PathDecodeConfig
    document,original=_original(model,graph);rows=evidence['native_result']['rows'];wanted=prepared['oracle']['witnesses']
    if prepared['id'].startswith('all-objects-'):
        actual=[];members=wanted['members'];record=wanted['record'];fields={e['id']:e for m in document['modules']for e in m['elements']}
        for row in rows:
            if len(row)!=len(members):raise ValueError('Complete original medical row width required')
            values={}
            for member,cell in zip(members,row):
                field=fields[member]
                if field.get('nullability')=='absent-allowed':
                    item=json.loads(cell)
                    if item=={'state':'null'}:value=None
                    elif type(item)is dict and set(item)=={'state','value'}and item['state']=='value':value=item['value']
                    else:raise ValueError('Original present medical field state required')
                    if field['scalarType']=='boolean'and value is not None:
                        candidates={o['values'][member]for o in wanted['original_rows']if (o['values'][member]in ('true','True'))is value}
                        if type(value)is not bool or len(candidates)!=1:raise ValueError('Original Boolean lexical correspondence required')
                        value=candidates.pop()
                else:value=cell
                values[member]=value
            keyfields=[k for k in fields[record]['keys']if k.get('primary')is True]
            keyvalues=[values[r['element']]for r in keyfields[0]['fields']]
            key=json.dumps([document['id'],'domain',record,keyvalues],ensure_ascii=False,separators=(',',':'))
            actual.append({'key':key,'type':{'document':document['id'],'module':'domain','element':record},'values':values})
        if Counter(map(encoded,actual))!=Counter(map(encoded,wanted['original_rows'])):raise ValueError('Complete original medical object bag differs')
        return actual
    if prepared['id']in ('singleton','filtered-limit'):
        actual=[{'key':medical_object_key(document,'encounters',(row[0],)),
            'type':{'document':document['id'],'module':'domain','element':'encounters'},
            'values':{'encounters.'+f:cell for f,cell in zip(FIELDS,row)}}for row in rows if len(row)==len(FIELDS)]
        if len(actual)!=len(rows)or Counter(map(encoded,actual))!=Counter(map(encoded,wanted['original_rows'])):raise ValueError('Full original singleton row bag differs')
        if prepared['id']=='filtered-limit':
            population=sorted((o for o in original['objects']if o['type']==actual[0]['type']),key=lambda o:o['key'])
            if (wanted['complete_population']!=len(population) or wanted['limit']!=2
                    or wanted['original_key_order']!=[o['key']for o in population] or actual!=population[:2]):
                raise ValueError('Original encounter key order, population or limit differs')
        return actual
    first,second=wanted['relationships'];source_record,middle_record,target_record=wanted['records']
    definitions={r['id']:r for m in document['modules']for r in m.get('relationships',[])}
    by_edge={(v['type_id'],v['id']):v['originalKey']for v in bindings['entities']if v['kind']=='edge'}
    types={tuple(t['identity']):t['type_id']for t in bindings['types']};actual=[]
    representation=artifact['columns'][1]['representation']
    for root,cell in rows:
        decoded=decode_related_paths(representation,cell,model_pins=artifact['modelPins'],config=PathDecodeConfig(1048576))
        if decoded.truncated is not False:raise ValueError('Complete medical two-hop collection required')
        for item in decoded.items:
            try:keys=[by_edge[(types[('edge','domain',name)],token)]for name,token in zip((first,second),item.edges)]
            except KeyError:raise ValueError('Original typed medical edge identity differs')from None
            actual.append([medical_object_key(document,source_record,(root,)),keys[0],medical_object_key(document,middle_record,item.intermediate),keys[1],medical_object_key(document,target_record,item.terminal)])
    if Counter(map(encoded,actual))!=Counter(map(encoded,wanted['occurrences'])):raise ValueError('Complete medical two-hop edge-occurrence bag differs')
    return actual

"""Pure fixed common041 requests; no compiler, read or native authority."""
import hashlib
import json
from types import MappingProxyType

from .delta_custody import encoded
from .graphframes_pack_scenarios import common_oracle
from .supply_chain_oracle import original_graph_oracle
from .supply_chain_path_request import supply_chain_path_request
from .supply_chain_request import (source_snapshot,supply_chain_cases,
                                   supply_chain_count_star_request)

PARTS=MappingProxyType({'relationships-0-6':tuple(range(0,7)),'relationships-7-12':tuple(range(7,13))})


def _request(sql,pack,model,graph,bindings,manifest,registry,aliases):
    # Select the genuine existing CountStar041 request owner. The separately
    # public pure relationship builder contributes original binding DATA only;
    # no Paths artifact/dialect is relabeled or claimed accepted by CountStar.
    request=supply_chain_count_star_request('sensor',pack,model,graph,bindings,manifest,registry,aliases)
    binding=json.loads(request['target']['bindingJson'])
    shared=supply_chain_path_request(sql,pack,model,graph,bindings,manifest,registry,aliases)
    mapping=json.loads(shared['target']['bindingJson']);relationships=mapping.pop('relationships')
    if mapping!=binding:raise ValueError('Original shared relationship binding differs')
    binding['relationships']=relationships;raw=encoded(binding)
    request['sql']=sql
    request['target'].update(bindingJson=raw,bindingSha256=hashlib.sha256(raw.encode()).hexdigest())
    return request


def _container_rows(objects,document):
    definitions=[element for module in document['modules']for element in module['elements']
                 if element['id']=='containers'and element['kind']=='record']
    if len(definitions)!=1 or [m['element']for m in definitions[0]['members']]!=['containers.id','containers.parent_id','containers.kind']:
        raise ValueError('Complete original container projection required')
    selected=[obj for obj in objects if obj['type']['element']=='containers']
    selected.sort(key=lambda obj:obj['values']['containers.id'].encode())
    rows=[]
    for obj in selected:
        value=obj['values']['containers.parent_id']
        parent=json.dumps({'state':'null'}if value is None else {'state':'value','value':value},ensure_ascii=False,separators=(',',':'))
        rows.append([obj['values']['containers.id'],parent,obj['values']['containers.kind']])
    return selected,rows


def prepare_supply_chain_common_count_requests(pack,model,graph,bindings,manifest,registry,aliases,*,family,part):
    """Complete fixed source requests/oracles only; execution admits separately.

    all-objects/all-edges belong to the public original graph-release owner.
    Grouped actual relationship occurrence counts need their own compiler
    operation; property joins or host counts do not substitute for it here.
    """
    if type(family)is not str or type(part)is not str:
        raise ValueError('Exact fixed common family and part required')
    if family=='grouped-count':
        raise ValueError('Native one-hop relationship grouped counts are not selected by this041 builder')
    if family not in ('one-hop','singleton','filtered-limit','two-hop-count'):
        raise ValueError('Unknown fixed common041 family')
    if (family=='one-hop'and part not in PARTS)or(family!='one-hop'and part!='complete'):
        raise ValueError('Exact fixed complete common partition required')
    supply_chain_cases(pack,model,graph)
    bindings,manifest,registry,aliases=source_snapshot([bindings,manifest,registry,aliases])
    # Admit complete original binding correspondence before deriving any IDs.
    supply_chain_count_star_request('sensor',pack,model,graph,bindings,manifest,registry,aliases)
    original=original_graph_oracle(model,graph);common=common_oracle(original)
    document=json.loads(model)
    objects={row['key']:row for row in original['objects']}
    assigned={(row['kind'],row['originalKey']):row for row in bindings['entities']}
    def request(sql):return _request(sql,pack,model,graph,bindings,manifest,registry,aliases)
    queries=[];occurrences=[]
    if family=='two-hop-count':
        relations=('items.lot_id','lots.product_id')
        definitions=[r for module in document['modules']for r in module.get('relationships',[])if r['id']in relations]
        by_id={r['id']:r for r in definitions}
        if set(by_id)!=set(relations)or by_id[relations[0]]['source'][0]['element']!='items'or by_id[relations[0]]['target'][0]['element']!=by_id[relations[1]]['source'][0]['element']:raise ValueError('Original items-to-lots-to-products pair required')
        first_edges=[edge for edge in original['edges']if edge['relationship']['id']==relations[0]]
        second_edges=[edge for edge in original['edges']if edge['relationship']['id']==relations[1]]
        rows=[];witnesses=[]
        for source in original['objects']:
            if source['type']['element']!='items':continue
            count=0
            for first in first_edges:
                if first['source']!=source['key']:continue
                for second in second_edges:
                    if second['source']!=first['target']:continue
                    count+=1
                    occurrence=[source['key'],first['key'],first['target'],second['key'],second['target']]
                    occurrences.append(occurrence)
                    witnesses.append({'original_edges':[first['key'],second['key']],
                        'edge_ids':[assigned['edge',first['key']]['id'],assigned['edge',second['key']]['id']],
                        'source':source['key'],'intermediate':first['target'],'target':second['target']})
            # CROSS expansion excludes roots without complete paths; no zero-degree claim.
            if count:rows.append([source['values']['items.id'],str(count)])
        rows.sort(key=lambda row:row[0].encode())
        if not rows:raise ValueError('Original two-hop grouped count must be nonvacuous')
        sql='SELECT s.id, COUNT(*) AS paths FROM items s CROSS JOIN EXPAND_PATHS(s."items.lot_id", "lots.product_id") AS p GROUP BY s.id ORDER BY s.id'
        queries.append({'ordinal':0,'relationships':list(relations),'request':request(sql),'expected_rows':rows,'original_edge_witnesses':witnesses,'original_path_occurrences':list(occurrences)})
    elif family=='one-hop':
        definitions=[r for module in document['modules']for r in module.get('relationships',[])]
        if len(definitions)!=13:raise ValueError('Complete original thirteen relationships required')
        for ordinal in PARTS[part]:
            definition=definitions[ordinal];record=definition['source'][0]['element']
            sql='SELECT s.id, RELATED_KEYS(s.'+encoded(definition['id'])+', 24) AS targets FROM '+record+' s ORDER BY s.id'
            req=request(sql);rows=[];witnesses=[]
            for obj in original['objects']:
                if obj['type']['element']!=record:continue
                edges=[edge for edge in original['edges']if edge['source']==obj['key']and edge['relationship']['id']==definition['id']]
                items=[]
                for edge in edges:
                    target=objects[edge['target']];target_record=target['type']['element']
                    items.append([target['values'][target_record+'.id']])
                    occurrence=[edge['source'],edge['key'],edge['target']];occurrences.append(occurrence)
                    witnesses.append({'original_edge':edge['key'],'edge_id':assigned['edge',edge['key']]['id'],
                        'edge_type_id':assigned['edge',edge['key']]['type_id'],
                        'source':edge['source'],'target':edge['target'],'relationship':definition['id']})
                items.sort(key=lambda key:tuple(value.encode()for value in key))
                rows.append([obj['values'][record+'.id'],json.dumps({'items':items,'truncated':False},ensure_ascii=False,separators=(',',':'))])
            rows.sort(key=lambda row:row[0].encode())
            queries.append({'ordinal':ordinal,'relationships':[definition['id']],'request':req,
                            'expected_rows':rows,'original_edge_witnesses':witnesses})
        selected={definitions[i]['id']for i in PARTS[part]}
        wanted=[row for row in common['one-hop']if next(e for e in original['edges']if e['key']==row[1])['relationship']['id']in selected]
        if sorted(map(encoded,occurrences))!=sorted(map(encoded,wanted)):
            raise ValueError('Complete original partition occurrence bag differs')
    elif family in ('singleton','filtered-limit'):
        containers,rows=_container_rows(original['objects'],document)
        if len(containers)!=3 or not rows:
            raise ValueError('Original nonvacuous three-container source required')
        first=containers[0]
        if common['singleton']!=[first['key']]or common['filtered-limit']!=[first['key']]or common['filtered-total']!=3:
            raise ValueError('Original singleton/filtered complete oracle differs')
        fields='SELECT s.id, s.parent_id, s.kind FROM containers s'
        if family=='singleton':
            key=first['values']['containers.id'];sql=fields+" WHERE s.id='"+key.replace("'","''")+"' ORDER BY s.id"
            queries.append({'ordinal':0,'request':request(sql),'expected_rows':[rows[0]],'original_object_keys':[first['key']]})
        else:
            queries.append({'ordinal':0,'request':request('SELECT COUNT(*) AS total FROM containers s'),
                            'expected_rows':[['3']],'original_object_keys':[obj['key']for obj in containers]})
            queries.append({'ordinal':1,'request':request(fields+' ORDER BY s.id LIMIT 1'),
                            'expected_rows':[rows[0]],'original_object_keys':[first['key']]})
    return {'case':'QUERY/supply-chain/'+family,'family':family,'part':part,'queries':queries,
            'expected_occurrences':occurrences,
            'qualification':'Pure original fixed041 requests and independent source rows only. No compiler/admission/native/read/publication/edge-witness authority; actual full native graph-release witness and held execution remain required.'}

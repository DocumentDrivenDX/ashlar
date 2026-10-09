"""Explicit commerce CSV graph oracle using original public UMF value/key APIs.

No Record/relationship-instance validator or accepted storage identities are
implied. Python maps a fixed source profile; UMF alone encodes/evaluates key values.
"""
import argparse
import base64
from collections import Counter
from decimal import Decimal
import json
from pathlib import Path
import subprocess

from domain_pack_inventory import GitSource, InventoryError, build_inventory, encoded, read_json, sha256, PIN
from prepare_commerce_values import prepare_values

BUN_CHECK = r'''
import {pathToFileURL} from 'node:url';import {join} from 'node:path';
const [root,modelPath,requestPath]=process.argv.slice(1);
const load=p=>import(pathToFileURL(join(root,p)).href);
const [{readDocument},{validateCoreFieldValue},{encodeCoreKeyTuple}]=await Promise.all([load('src/model/document.ts'),load('src/model/schema-properties.ts'),load('src/model/key-tuple.ts')]);
const source=readDocument(await Bun.file(modelPath).text(),'json');
const requests=JSON.parse(await Bun.file(requestPath).text());
const fields=requests.fields.map(request=>({request,validation:validateCoreFieldValue(source,request.field,request.value)}));
if(fields.some(x=>!x.validation.valid||!x.validation.complete))throw Error('Original field requests refused');
const keys=requests.keys.map(request=>({request,receipt:encodeCoreKeyTuple(source,request.identity,request.values)}));
console.log(JSON.stringify({fields,keys}));
'''


def binding_requests(prepared, model, tables):
    elements={(m['id'],e['id']):e for m in model['modules'] for e in m['elements']}
    relationships={(m['id'],r['id']):r for m in model['modules'] for r in m.get('relationships',[])}
    requests={'fields':[], 'keys':[]}
    links=[]
    for row in prepared['rows']:
        requests['fields'].extend({'field':f['field'],'value':f['value']} for f in row['fields'])
        fields={f['column']:f for f in row['fields']}
        record=row['record']['element']
        keys=row['declaredKeys']
        if len(keys)!=1 or not keys[0].get('primary') or len(keys[0]['fields'])!=1:
            raise InventoryError('Fixed commerce profile requires one declared primary single-field key')
        key=keys[0]
        keyfield=elements[(key['fields'][0]['module'],key['fields'][0]['element'])]
        value=fields[keyfield['name']]['value']
        requests['keys'].append({'role':'record','sourceRowKey':row['sourceRowKey'],
                                'identity':{'module':'domain','element':record,'key':key['id']},'values':[value]})
        for fk in tables[record].get('relationships',{}).get('foreign_keys',[]):
            relid=record+'.'+fk['column'];rel=relationships.get(('domain',relid))
            target=elements[('domain',fk['references_table'])]
            targetkeys=[k for k in target['keys'] if k.get('primary')]
            if len(targetkeys)!=1 or len(targetkeys[0]['fields'])!=1:
                raise InventoryError('Fixed commerce FK target requires one primary single-field key')
            targetkey=targetkeys[0]
            targetfield=elements[(targetkey['fields'][0]['module'],targetkey['fields'][0]['element'])]
            if not rel or rel['source']!=[{'module':'domain','element':record}] or rel['target']!=[{'module':'domain','element':target['id'],'key':targetkey['id']}] or not rel['directed'] or targetfield['name']!=fk['references_column']:
                raise InventoryError('Explicit TableSpec FK/authored relationship binding differs')
            # Bound profile uses strings; no numeric or SQL implicit FK coercion.
            if 'string' not in fields[fk['column']]['value'] or targetfield.get('scalarType')!='string':
                raise InventoryError('Fixed commerce FK equality profile requires strings')
            key_index=len(requests['keys'])
            requests['keys'].append({'role':'foreign-key','sourceRowKey':row['sourceRowKey'],
                'identity':{'module':'domain','element':target['id'],'key':targetkey['id']},'values':[fields[fk['column']]['value']]})
            links.append({'sourceRowKey':row['sourceRowKey'],'relationship':{'document':model['id'],'module':'domain','id':relid},'targetKeyRequestIndex':key_index})
    return requests,links


def materialize_source_graph(prepared, public, links):
    records={r['sourceRowKey']:r for r in prepared['rows']}
    nodes=[];key_index={};source_index={}
    for observation in public['keys']:
        request=observation['request'];receipt=observation['receipt']
        identity=request['identity']
        if receipt.get('operation')!='encode-core-key-tuple' or receipt.get('version')!='3.0.0' or receipt.get('profile')!='umf-key-tuple-v1' or receipt['identity']!=identity or receipt['values']!=request['values'] or receipt['source']['id']!=prepared['documentId'] or receipt['source']['umf']!='0.8.0':
            raise InventoryError('Original key receipt correspondence differs')
        if request['role']!='record':continue
        qualified=[prepared['documentId'],identity['module'],identity['element'],identity['key'],receipt['bytesHex']]
        key=json.dumps(qualified,ensure_ascii=False,separators=(',',':'))
        if key in key_index:raise InventoryError('Duplicate original key; no repair or ID allocation')
        row=records[request['sourceRowKey']]
        node={'key':key,'record':row['record'],'sourceRowKey':row['sourceRowKey'],
              'values':{f['field']['element']:f['value'] for f in row['fields']}}
        key_index[key]=node;source_index[row['sourceRowKey']]=node;nodes.append(node)
    edges=[]
    for link in links:
        receipt=public['keys'][link['targetKeyRequestIndex']]['receipt'];identity=receipt['identity']
        targetkey=json.dumps([prepared['documentId'],identity['module'],identity['element'],identity['key'],receipt['bytesHex']],ensure_ascii=False,separators=(',',':'))
        if targetkey not in key_index:raise InventoryError('Original FK endpoint missing')
        sourcekey=source_index[link['sourceRowKey']]['key'];relationship=link['relationship']
        edgekey=json.dumps([relationship,sourcekey,targetkey],ensure_ascii=False,sort_keys=True,separators=(',',':'))
        edges.append({'key':edgekey,'relationship':relationship,'source':sourcekey,'target':targetkey})
    if len({e['key'] for e in edges})!=len(edges):raise InventoryError('Source-profile edge identity collision')
    if len(nodes)!=len(prepared['rows']):raise InventoryError('Complete source-row coverage required')
    return {'nodes':nodes,'edges':edges,'counts':{'nodes':len(nodes),'edges':len(edges)},
            'relationship_counts':dict(Counter(e['relationship']['id'] for e in edges)),
            'qualification':'Explicit commerce original-CSV source-profile graph oracle. Public UMF field/key receipts establish only those selected literal/key operations. No complete Record/relationship-instance admission, graph-fixture seeded-ID binding or accepted storage IDs.'}


def decimal_scenarios(prepared):
    tables={}
    for row in prepared['rows']:
        fields={f['column']:f['lexical'] for f in row['fields']}
        tables.setdefault(row['record']['element'],{})[fields['id']]=fields
    partial=[];over=[];settled=[];refund=[]
    for f in tables['fulfillments'].values():
        line=tables['order_lines'][f['line_id']];fq=int(f['quantity']);lq=int(line['quantity'])
        if fq>lq:over.append(f['id'])
        for r in tables['returns'].values():
            if r['line_id']==line['id'] and fq<lq:partial.append([fq,int(r['quantity']),lq-fq+int(r['quantity'])])
    for p in tables['payments'].values():
        invoice=tables['invoices'][p['invoice_id']]
        if Decimal(p['amount'])==Decimal(invoice['amount']) and p['currency']==invoice['currency']:settled.append(p['id'])
    for r in tables['refunds'].values():
        returned=tables['returns'][r['return_id']];line=tables['order_lines'][returned['line_id']];product=tables['products'][line['product_id']]
        if Decimal(r['amount'])==int(returned['quantity'])*Decimal(product['unit_price']):refund.append(r['id'])
    return {'partial-return':sorted(partial),'fulfillment':[[v] for v in sorted(over)],
            'settlement':[[v] for v in sorted(settled)],'refund':[[v] for v in sorted(refund)],
            'qualification':'Independent direct row arithmetic and FK walks; no tested SQL/Weft query is used. Integer/Decimal arithmetic occurs after original public literal validation.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ['umf-repo','source','inventory','inspection','output']:parser.add_argument('--'+key,required=True)
    args=parser.parse_args();source=GitSource(args.umf_repo)
    if source._git('rev-parse','HEAD').decode().strip()!=PIN or source._git('status','--porcelain').strip():raise InventoryError('Clean exact UMF source required')
    inventory=build_inventory(source)
    if Path(args.inventory).read_bytes()!=encoded(inventory):raise InventoryError('Original pinned inventory differs')
    inspection_raw=Path(args.inspection).read_bytes();inspection=read_json(inspection_raw)
    prepared=prepare_values(args.source,inventory,inspection)
    prepared['inspectionSha256']=sha256(inspection_raw)
    model=read_json((Path(args.source)/'ontology.json').read_bytes())
    tables={p.stem:read_json(p.read_bytes()) for p in (Path(args.source)/'umf').glob('*.json')}
    requests,links=binding_requests(prepared,model,tables)
    import tempfile
    with tempfile.TemporaryDirectory() as directory:
        request_path=Path(directory)/'requests.json';request_path.write_bytes(encoded(requests))
        result=subprocess.run(['bun','-e',BUN_CHECK,str(Path(args.umf_repo).resolve()),str((Path(args.source)/'ontology.json').resolve()),str(request_path)],capture_output=True)
        if result.returncode:raise InventoryError('Public UMF field/key operation refused: '+result.stderr.decode())
        public=read_json(result.stdout)
    if source._git('rev-parse','HEAD').decode().strip()!=PIN or source._git('status','--porcelain').strip():raise InventoryError('UMF source drift')
    if any(p['receipt']['source']!=model for p in public['keys']):raise InventoryError('Original complete key source context differs')
    if [p['request'] for p in public['fields']]!=requests['fields'] or [p['request'] for p in public['keys']]!=requests['keys']:raise InventoryError('Original public request coverage differs')
    # Recheck copied inputs/interpretation after execution before releasing output.
    if Path(args.inspection).read_bytes()!=inspection_raw:raise InventoryError('Original inspection byte custody drift')
    closing=prepare_values(args.source,inventory,inspection);closing['inspectionSha256']=sha256(inspection_raw)
    if closing!=prepared:raise InventoryError('Original source custody drift')
    graph=materialize_source_graph(prepared,public,links)
    output={'format':'ashlar.commerce-source-graph-oracle','version':'0.1','upstreamCommit':PIN,
            'ontologySha256':prepared['ontologySha256'],'preparedRequests':prepared,
            'publicUmfOperations':public,'graph':graph,'scenarios':decimal_scenarios(prepared)}
    Path(args.output).write_bytes(encoded(output))
    print(json.dumps({'fields':len(public['fields']),'keys':len(public['keys']),'counts':graph['counts'],'outputSha256':sha256(encoded(output))}))


if __name__=='__main__':main()

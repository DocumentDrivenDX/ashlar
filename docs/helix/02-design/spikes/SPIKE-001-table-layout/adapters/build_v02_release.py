"""Build consumer-neutral typed release tables from the native pinned export.
No engine deployment or compatibility claim; all output is local JSON.
"""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent
source=B.parent/'out/native/ashlar_layout_v02_export_20261006_r67/export.json'
raw=source.read_bytes(); export=json.loads(raw)
assert export['profile']=='ashlar-delta/0.2'
root=B/'release-r66'; root.mkdir(exist_ok=True)
tables={}
for v in export['vertices']:
 label='Type'+v['type_id']; name='node_'+v['type_id']
 row={**v,'native_id':v['id'],'id':v['node_key']}
 tables.setdefault(name,{'kind':'node','label':label,'rows':[]})['rows'].append(row)
for e in export['edges']:
 name='edge_'+e['rel_type_id']+'_'+e['source_type']+'_'+e['target_type']
 label='Rel'+e['rel_type_id']+'_'+e['source_type']+'_'+e['target_type']
 row={**e,'native_id':e['id'],'id':e['edge_key']}
 tables.setdefault(name,{'kind':'edge','label':label,'sourceLabel':'Type'+e['source_type'],'targetLabel':'Type'+e['target_type'],'rows':[]})['rows'].append(row)
# Fabric has differing documented ingestion/runtime string bounds: use smaller.
# Reject the whole release, never truncate bags or silently drop rows.
for table in tables.values():
 for row in table['rows']:
  assert all(len(value.encode('utf8'))<=65534 for value in row.values() if isinstance(value,str)), 'Oversize Fabric string: explicit residual/release decision required'
keys={r['node_key'] for t in tables.values() if t['kind']=='node' for r in t['rows']}
assert len(keys)==3
edgekeys=[]
for t in tables.values():
 if t['kind']=='edge':
  for row in t['rows']:
   assert row['src'] in keys and row['dst'] in keys
   edgekeys.append(row['edge_key'])
assert len(edgekeys)==len(set(edgekeys))==3
files={}
for name,table in tables.items():
 payload=(json.dumps(table['rows'],indent=2)+'\n').encode()
 (root/(name+'.json')).write_bytes(payload)
 files[name]={'file':name+'.json','sha256':hashlib.sha256(payload).hexdigest(),'rows':len(table['rows']),**{k:v for k,v in table.items() if k!='rows'}}
plan={'format':'ashlar-mapping-plan/0.2','not_vendor_api_payload':True,'publication':export['publication'],'canonical_versions':export['versions'],'source_sha256':hashlib.sha256(raw).hexdigest(),'tables':files,'targets':{'GraphFrames':{'vertices':'union all node tables; id=node_key','edges':'union all edge tables; src,dst,edge_id=edge_key'},'PuppyGraph':{'nodes':'one label per node type; key node_key','edges':'one label per (relationship,source type,target type); key edge_key; retain rel_type_id for cross-label queries','source':'immutable release Delta tables; Databricks connection/version profile separately qualified'},'Microsoft Fabric Graph':{'nodes':'one node type per node table; key node_key','edges':'one edge type per endpoint type combination; src/dst foreign keys; edge_key explicit property; uniqueness validated by publisher','source':'immutable OneLake scalar Delta copies, independently verified','provisional_release_budget_elements':100000000,'budget_basis':'Conservative planning assumption below documented 500M instability guidance; not proved capacity.','string_bytes_limit':65534}},'residuals':export['residuals'],'state':'local release shape verified; Delta materialization and all engine execution pending','checked_at':'2026-10-06','sources':['https://docs.puppygraph.com/modeling/','https://docs.puppygraph.com/connecting/connecting-to-delta-lake/','https://learn.microsoft.com/fabric/graph/limitations','https://learn.microsoft.com/en-us/fabric/graph/monitor-graph-performance']}
(root/'mapping-plan.json').write_text(json.dumps(plan,indent=2)+'\n')
print('Release shape passed: 2 node tables, 2 endpoint-typed edge tables; 3 nodes/3 edges')

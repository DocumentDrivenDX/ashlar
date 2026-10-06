"""Read-only export conformance against the actual r66 immutable publication."""
import json
from pathlib import Path
from collections import Counter
from persistent_sql import Client
B=Path(__file__).resolve().parent
N='client_dev.ashlar_layout_v02_20261006_r65'
out=B/'out/native/ashlar_layout_v02_export_20261006_r67'
c=Client(out)
rows=c.sql('manifest',f"SELECT publication_id,profile_version,table_versions_json FROM {N}.publication_manifest WHERE publication_id='r66:publication1'")
assert len(rows)==1
publication,profile,raw=rows[0]; vector=json.loads(raw)
def key(kind,s,t,i):
 assert s.isascii()
 return f'{kind}{len(s)}:{s}:{int(t)}:{int(i)}'
def read(table,fields):
 names=fields.split(',')
 data=c.sql(table,f"SELECT {fields} FROM {N}.{table} VERSION AS OF {int(vector[N+'.'+table])}")
 return [dict(zip(names,row)) for row in data]
vertices=read('object_current','source_system,type_id,id,logical_key_json,props_json,retained_json')
edges=read('edge_current','source_system,rel_type_id,id,source_type,source_id,target_type,target_id,props_json,retained_json')
for v in vertices:v['node_key']=key('N',v['source_system'],v['type_id'],v['id'])
for e in edges:
 e['edge_key']=key('E',e['source_system'],e['rel_type_id'],e['id'])
 e['src']=key('N',e['source_system'],e['source_type'],e['source_id'])
 e['dst']=key('N',e['source_system'],e['target_type'],e['target_id'])
keys={v['node_key'] for v in vertices}
assert len(keys)==len(vertices)==3
assert len({e['edge_key'] for e in edges})==len(edges)==3
assert all(e['src'] in keys and e['dst'] in keys for e in edges)
assert Counter((e['src'],e['dst']) for e in edges)[('N5:pilot:1:1','N5:pilot:2:1')]==2
assert sum(e['src']==e['dst'] for e in edges)==1
assert 'N5:pilot:1:-1' in keys-{e[k] for e in edges for k in ('src','dst')}
assert all(v['props_json']=='{"101":null,"102":9007199254740993,"103":"verbatim"}' for v in vertices)
assert all(x['retained_json']=='{"future":{"unrecognized":[null,1,"x"]}}' for x in vertices+edges)
export={'publication':publication,'profile':profile,'versions':vector,'vertices':vertices,'edges':edges,'residuals':['History, tombstones, degree summaries and coordination records remain in pinned canonical tables.','No selected property scalar conversion: exact JSON text retained.'],'scope':'Native Delta reads and Python export conformance only; external engines unexecuted.'}
(out/'export.json').write_text(json.dumps(export,indent=2)+'\n')
(out/'summary.json').write_text(json.dumps({'state':'passed','publication':publication,'vertices':3,'edges':3,'parallel_edges':2,'self_loops':1,'isolates':1,'scope':export['scope']},indent=2)+'\n')
print('Publication-scoped 0.2 export passed',flush=True)

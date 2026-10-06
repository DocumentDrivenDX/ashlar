"""Check authored adapter examples against the independent consumer fixture."""
from pathlib import Path
import json
base=Path(__file__).resolve().parent
original=json.loads((base.parents[2]/'03-test/fixtures/consumer-conformance.json').read_text())['publications']['graph']['current']
export=json.loads((base/'adapters/fixture-export.json').read_text())
vertices,edges=export['vertices'],export['edges']
node_keys={v['node_key'] for v in vertices}
assert len(node_keys)==len(vertices)
assert len({e['edge_key'] for e in edges})==len(edges)
assert all(e['src'] in node_keys and e['dst'] in node_keys for e in edges)
by_identity=lambda rows:{json.dumps(x['logical_identity'],sort_keys=True):x for x in rows}
encoded=by_identity(vertices+edges)
for row in original:
 recovered=encoded[json.dumps(row['identity'],sort_keys=True)]
 assert json.loads(recovered['props_json'])==row['value']['properties']
 assert recovered['retained_json']=='{}'
a=next(v['node_key'] for v in vertices if v['logical_identity']['key']=='A1')
a0=next(v['node_key'] for v in vertices if v['logical_identity']['key']=='A0')
paths=[(e['edge_key'],f['edge_key'],f['dst']) for e in edges for f in edges
       if e['rel_type_id']==7 and e['src']==a and f['rel_type_id']==8 and e['dst']==f['src']]
assert len(paths)==4 and len({x[2] for x in paths})==2
assert [list(x) for x in paths]==export['expected_two_hop_paths']
assert not any(e['src']==a0 or e['dst']==a0 for e in edges)
for file in ('run.py','databricks_spike.py','adapters/graphframes.py'):
 compile((base/file).read_text(),file,'exec')
for file in ('puppygraph-model.json','fabric-projection.json'):
 json.loads((base/'adapters'/file).read_text())
print('Adapter fixture and Python syntax checks passed; external graph engines unexecuted. Native Delta evidence is recorded separately.')

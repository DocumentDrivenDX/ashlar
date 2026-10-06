"""Generated parameterized adjacency reads against the fixed r74 vector."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;out=B/'out/native/ashlar_native_adjacency_builder_20261006_r76';c=Client(out)
queries=json.loads((B/'out/native-adjacency-queries.json').read_text());results=[]
for i,q in enumerate(queries):
 rows=c.sql('page-'+str(i),q['sql'],[{'name':k,'value':v,'type':'STRING'} for k,v in q['parameters'].items()])
 assert [r[2] for r in rows]==q['expectedEdges']
 for row in rows:
  assert row[0]==q['sourceSystem']
  offset=3 if q['direction']=='out' else 5
  assert row[offset:offset+2]==[q['typeId'],q['id']]
 results.append({'direction':q['direction'],'edges':[r[2] for r in rows]})
assert results[0]['edges']+results[1]['edges']==['1','2','3']
(out/'summary.json').write_text(json.dumps({'state':'passed','pages':results,'scope':'Four bounded manifest-pinned generated adjacency reads: two forward pages preserving parallel edges/self-loop, reverse typed endpoint and isolate. No service cursor custody, concurrent latest reads, performance/scale or producer/engine claim.'},indent=2)+'\n')
print('Native typed adjacency pagination passed',flush=True)

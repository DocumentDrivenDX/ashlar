"""Execute generated portable builder queries with SDK named parameters."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;out=B/'out/native/ashlar_native_read_builder_20261006_r75';c=Client(out)
queries=json.loads((B/'out/native-read-queries.json').read_text())
results=[]
for i,q in enumerate(queries):
 params=[{'name':k,'value':v,'type':'STRING'} for k,v in q['parameters'].items()]
 rows=c.sql('bound-'+str(i),q['sql'],params);assert len(rows)==q['expectedRows']
 if rows:
  # Physical core ordering retains native tuple and exact bags.
  assert rows[0][:3]==[q['sourceSystem'],q['typeId'],q['id']]
  pi=6 if q['kind']=='node' else 9
  expected='{"101":null,"102":9007199254740993,"103":"verbatim"}' if q['kind']=='node' else '{}'
  assert rows[0][pi]==expected
 results.append({'kind':q['kind'],'type':q['typeId'],'id':q['id'],'rows':len(rows)})
(out/'summary.json').write_text(json.dumps({'state':'passed','queries':results,'scope':'Four generated manifest-pinned singleton queries with exact SQL identity predicates and named SDK string parameters; typed repeated IDs, edge identity, exact bags and hostile-looking source parameter. No latency, scale, backend authorization or full producer-support claim.'},indent=2)+'\n')
print('Portable parameterized native singleton builder passed',flush=True)

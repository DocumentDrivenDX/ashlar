"""Read-only deep keyset page sensitivity on r108's pinned synthetic graph."""
import json,math
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_deep_pages_r109'
assert not (O/'statements.jsonl').exists(),'Inspect existing query evidence before repeating'
A='client_dev.ashlar_entropy_20261006_r86.adjacency_hub_r108'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
for source,hub in [('pilot',32),('other',160)]:
 for cursor in (4000000,4400000,4800000,5000000):
  expected=[]
  ordinal=cursor-4000000+1
  while ordinal<=1000000 and len(expected)<100:
   if (ordinal%10!=0)==(source=='pilot'):expected.append([str(4000000+ordinal),str(9000+ordinal%32+1),str(ordinal%32+1),str(ordinal)])
   ordinal+=1
  if len(expected)<100:
   for hop in range(2,6):
    edge=hop*4000000+hub;dst=hub+hop*10
    if edge>cursor:expected.append([str(edge),str(1004+hop),str(dst%32+1),str(dst)])
  for i in range(5):
   rows=c.sql(f'page-{source}-{cursor}-{i}',f"SELECT edge_id,rel_type_id,target_type,target_id FROM {A} VERSION AS OF 0 WHERE source_system='{source}' AND source_type=1 AND source_id={hub} AND edge_id>{cursor} ORDER BY edge_id LIMIT 100")
   assert rows==expected,(source,cursor,rows[:1],expected[:1])
 for i in range(2):
  assert c.sql(f'end-{source}-{i}',f"SELECT edge_id FROM {A} VERSION AS OF 0 WHERE source_system='{source}' AND source_type=1 AND source_id={hub} AND edge_id>{20000000+hub} ORDER BY edge_id LIMIT 100")==[]
(O/'summary.json').write_text(json.dumps({'state':'44 exact deep/tail/end reads passed; final history audit pending','table':A,'version':0,'qualification':'Warm uncached synthetic hub graph, stable keyset order with unique edge IDs; no actual source or billion-scale claim.'},indent=2)+'\n')
c.history();c.close();print('Passed 44 exact deep/tail/end reads')

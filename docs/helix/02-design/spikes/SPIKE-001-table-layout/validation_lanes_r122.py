"""Matched exact read-only raw/journal validation scheduling screen."""
import json,time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_validation_lanes_r122'
assert not (O/'summary.json').exists(),'Inspect prior handles'
rs=[json.loads(l) for l in (B/'out/native/ashlar_isolation_r121/statements.jsonl').read_text().splitlines()]
jr=[json.loads(l) for l in (B/'out/native/ashlar_isolation_r121/journal-lane/statements.jsonl').read_text().splitlines()]
queries={role:next(r['sql'] for r in rs+jr if r['label']==label) for role,label in [('raw','raw-parity-r121-b1'),('journal','journal-parity-r121-b1')]}
assert all(q.startswith('SELECT count(*)') for q in queries.values())
def lane(mode,round_,role):
 c=DriverClient(O/f'{mode}-{round_}-{role}')
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=180')
  started=time.monotonic();assert c.sql('exact-validation',queries[role])==[['0']]
  elapsed=time.monotonic()-started;c.history()
  return {'role':role,'validation_s':elapsed,'statement_id':c.records[-1]['statement_id']}
 finally:c.close()
results=[]
for round_ in range(2):
 for mode in (('serial','parallel') if round_==0 else ('parallel','serial')):
  start=time.monotonic()
  if mode=='serial':values=[lane(mode,round_,role) for role in ('raw','journal')]
  else:
   with ThreadPoolExecutor(max_workers=2) as pool:values=list(pool.map(lambda role:lane(mode,round_,role),('raw','journal')))
  results.append({'mode':mode,'round':round_,'whole_lane_wall_s':time.monotonic()-start,'lanes':values})
  print(mode,round_,round(results[-1]['whole_lane_wall_s'],3),flush=True)
(O/'summary.json').write_text(json.dumps({'state':'8 exact read-only validation checks passed; final metrics pending','results':results,'qualification':'Two balanced rounds, identical pinned100k raw/journal checks from r121; separate worker-owned clients. Whole wall includes connection setup/close and history retrieval; query execution intervals are needed for publication-path comparison. No writes or whole-publication p95 claim.'},indent=2)+'\n')

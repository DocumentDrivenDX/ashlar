"""Matched pinned post-change checks with/without staged-side broadcast hint."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;source=B/'out/native/ashlar_fenced_scheduled_20261005_r13r1';out=B/'out/native/ashlar_validation_hint_20261005_r15';c=DriverClient(out)
records=[json.loads(x) for x in (source/'statements.jsonl').read_text().splitlines()];queries={int(r['label'].rsplit('-',1)[1]):r['sql'] for r in records if r['label'].startswith('changed-check-')};assert len(queries)==4
results=[]
for round in range(2):
 for batch in range(1,5):
  order=['plain','broadcast'] if (round+batch)%2 else ['broadcast','plain']
  for variant in order:
   sql=queries[batch];sql=sql.replace('SELECT count(*)','SELECT /*+ BROADCAST(s) */ count(*)',1) if variant=='broadcast' else sql
   assert c.sql(f'{variant}-round{round}-batch{batch}',sql)==[['200000','0']]
   results.append({'variant':variant,'round':round,'batch':batch,'wall_ms':c.records[-1]['wall_ms'],'statement_id':c.records[-1]['statement_id']})
   (out/'progress.json').write_text(json.dumps(results,indent=2))
(out/'summary.json').write_text(json.dumps({'state':'passed','checks':results,'scope':'16 exact matched pinned checks; alternated ordering; cache confounding possible; no integrated throughput result'},indent=2));c.history();c.close();print('Matched validation checks passed')

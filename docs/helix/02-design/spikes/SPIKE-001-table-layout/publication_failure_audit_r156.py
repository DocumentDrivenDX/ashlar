"""Finalize failed-lane native evidence and exact old snapshot; never replay writes."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from persistent_sql import Client
from property_apply_queries import COLS,TEXTS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_failure_r156'
s=json.loads((O/'summary.json').read_text());assert s['refused_rows']==1000
Q=O/'old-snapshot';assert not (Q/'statements.jsonl').exists(),'Inspect prior snapshot handle; no repeat'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
E='client_dev.ashlar_entropy_20261006_r86.edge_failure_r156';S='client_dev.ashlar_entropy_20261006_r86.schedule_r139_1'
fields=','.join(f"hex(encode(e.{col},'UTF-8')) AS {col}" if col in TEXTS else 'e.'+col for col in COLS)
old=f'SELECT {fields} FROM {E} VERSION AS OF 0 e'
base=f'SELECT {fields} FROM {S} VERSION AS OF 0 e INNER JOIN {E} VERSION AS OF 0 k ON e.source_system=k.source_system AND e.rel_type_id=k.rel_type_id AND e.id=k.id'
assert c.sql('old-exact',f'SELECT count(*) FROM (({old} EXCEPT ALL {base}) UNION ALL ({base} EXCEPT ALL {old}))')==[['0']]
c.close()
records=[];h={}
for out in (O,O/'apply-lane',O/'journal-lane',Q):
 client=Client(out);client.records=[json.loads(l) for l in (out/'statements.jsonl').read_text().splitlines()];records.extend(client.records)
 for attempt in range(3):
  history={q['query_id']:q for q in client.history()}
  if all(r['statement_id'] in history and history[r['statement_id']]['is_final'] for r in client.records):break
  if attempt<2:time.sleep(2)
 h.update(history)
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in records),'Refresh same IDs only'
assert not any(r['label']=='publish-new' for r in records)
s['costs']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in records}
s['cost_totals']={'read_bytes':sum(v['metrics'].get('read_bytes',0) for v in s['costs'].values()),'write_remote_bytes':sum(v['metrics'].get('write_remote_bytes',0) for v in s['costs'].values())}
s['state']='Failed raw validation blocks new manifest; old publication and exact1000 old carriers preserved; all native IDs final'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'state':s['state'],'cost_totals':s['cost_totals']},indent=2))

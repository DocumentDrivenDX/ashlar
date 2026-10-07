"""Finalize saved native IDs; never repeat the workload for delayed metrics."""
import json,time
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_journal_validation_r154'
s=json.loads((O/'summary.json').read_text());assert len(s['refused'])==11
c=Client(O);c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
for attempt in range(3):
 h={q['query_id']:q for q in c.history()}
 if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
 if attempt<2:time.sleep(2)
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records),'Refresh same IDs only'
s['costs']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records}
for mode in ('control','keyed'):
 rs=[r for r in c.records if r['label'].startswith(mode+'-')];assert len(rs)==3
 assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
 s[mode]={'caller_ms':[r['wall_ms'] for r in rs],'engine_ms':[h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs],'files':[h[r['statement_id']]['metrics']['read_files_count'] for r in rs],'read_bytes':[h[r['statement_id']]['metrics']['read_bytes'] for r in rs],'remote_bytes':[h[r['statement_id']]['metrics']['read_remote_bytes'] for r in rs],'spill_bytes':[h[r['statement_id']]['metrics'].get('spill_to_disk_bytes',0) for r in rs]}
s['cost_totals']={'read_bytes':sum(v['metrics'].get('read_bytes',0) for v in s['costs'].values()),'write_remote_bytes':sum(v['metrics'].get('write_remote_bytes',0) for v in s['costs'].values())}
s['state']='Three exact journal pairs and11 corruption refusals finalized; no publication admission'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({k:s[k] for k in ['control','keyed','refused','cost_totals']},indent=2))

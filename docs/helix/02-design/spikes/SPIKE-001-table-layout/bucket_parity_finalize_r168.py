import sys,json,time
from pathlib import Path
B=Path('/Users/erik/Projects/ashlar/docs/helix/02-design/spikes/SPIKE-001-table-layout');sys.path.insert(0,str(B))
from persistent_sql import Client
O=B/'out/native/ashlar_bucket_parity_r168';c=Client(O);c.records=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()]
for i in range(12):
 h={q['query_id']:q for q in c.history()}
 if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
 time.sleep(5)
assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records)
s=json.loads((O/'summary.json').read_text());s['final_metrics']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records};prior=json.loads((B/'out/native/ashlar_bucket_parity_r167/finalized-first-range.json').read_text());s['costs']={k:prior['costs'][k]+sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ('read_bytes','write_remote_bytes','spill_to_disk_bytes')};s['qualification']='Two hash ranges passed exact20-field parity; native metrics finalized by same IDs after lag. No exact-0 replay. Two remaining ranges unproved.'
(O/'finalized-two-ranges.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'costs':s['costs'],'exact0':s['final_metrics']['exact-1']},indent=2))

"""Finalize the same read-only probe IDs; never replay SQL."""
import json,time
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent
for run in ('r162','r163'):
 O=B/('out/native/ashlar_bucket_chunk_'+run);c=Client(O);c.records=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()]
 for attempt in range(3):
  h={q['query_id']:q for q in c.history()}
  if all(x['statement_id'] in h and h[x['statement_id']]['is_final'] for x in c.records):break
  if attempt<2:time.sleep(2)
 assert all(h[x['statement_id']]['is_final'] and h[x['statement_id']]['status']=='FINISHED' for x in c.records),'Inspect same IDs'
 s=json.loads((O/'summary.json').read_text());s['final_metrics']={x['label']:{'caller_ms':x['wall_ms'],'metrics':h[x['statement_id']]['metrics']} for x in c.records};s['total_read_bytes']=sum(h[x['statement_id']]['metrics'].get('read_bytes',0) for x in c.records)
 s['qualification']='Read-only complete disjoint counts at E23; final native metrics. Narrow reads do not predict full-carrier copy time or prove value preservation; no mutations or scale admission.'
 (O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(run,json.dumps({k:{n:v['metrics'].get(n) for n in ('read_bytes','read_files_count','pruned_files_count')} for k,v in s['final_metrics'].items()}))

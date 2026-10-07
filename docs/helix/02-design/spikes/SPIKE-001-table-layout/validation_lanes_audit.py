"""Same-ID final metrics and actual validation interval comparison."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_validation_lanes_r122';s=json.loads((O/'summary.json').read_text());comparison=[]
for group in s['results']:
 intervals=[]
 for lane in group['lanes']:
  out=O/f"{group['mode']}-{group['round']}-{lane['role']}";c=Client(out)
  c.records=[json.loads(l) for l in (out/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in c.history()}
  r=next(r for r in c.records if r['label']=='exact-validation');v=h[r['statement_id']]
  assert v['is_final'] and v['status']=='FINISHED' and not v['metrics'].get('result_from_cache'),'Refresh same IDs only'
  lane['metrics']=v['metrics'];intervals.append((r['start_epoch'],r['start_epoch']+r['wall_ms']/1000))
 serial=sum(end-start for start,end in intervals)
 span=max(end for start,end in intervals)-min(start for start,end in intervals)
 overlap=max(0,min(end for start,end in intervals)-max(start for start,end in intervals))
 comparison.append({'mode':group['mode'],'round':group['round'],'sum_query_caller_s':serial,'query_interval_span_s':span,'overlap_s':overlap})
s.update(state='8 exact pinned validation checks audited; all final uncached',comparison=comparison)
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(comparison,indent=2))

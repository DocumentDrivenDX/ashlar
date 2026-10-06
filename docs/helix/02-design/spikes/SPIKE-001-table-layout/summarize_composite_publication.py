"""Own-ID reader telemetry, classified by caller overlap with atomic apply."""
import json,math
from pathlib import Path
B=Path(__file__).resolve().parent;p=B/'out/native/ashlar_composite_publication_20261005_p2'
r=json.loads((p/'all-statements.json').read_text());h={q['query_id']:q for q in json.loads((p/'query-history.json').read_text())}
a=next(x for x in r if x['label']=='atomic-apply');lo=a['start_epoch'];hi=lo+a['wall_ms']/1000
reads=[x for x in r if x['label'].startswith('reader-') and not x['label'].endswith('-0')]
for x in reads:
 m=h[x['statement_id']]['metrics'];assert all(k in m for k in ['execution_time_ms','read_remote_bytes','read_files_count','result_from_cache'])
 x['overlap']=x['start_epoch']<hi and x['start_epoch']+x['wall_ms']/1000>lo

def summary(xs):
 if not xs:return {'n':0}
 def p95(v):return sorted(v)[math.ceil(.95*len(v))-1]
 ms=[h[x['statement_id']]['metrics'] for x in xs]
 return dict(n=len(xs),caller_p95_ms=p95([x['wall_ms'] for x in xs]),engine_p95_ms=p95([m['execution_time_ms'] for m in ms]),remote_read_samples=sum(m['read_remote_bytes']>0 for m in ms),result_cache_hits=sum(m['result_from_cache'] for m in ms),file_range=[min(m['read_files_count'] for m in ms),max(m['read_files_count'] for m in ms)])
s=dict(all_reads=summary(reads),atomic_call_overlap=summary([x for x in reads if x['overlap']]),overlap_no_remote=summary([x for x in reads if x['overlap'] and h[x['statement_id']]['metrics']['read_remote_bytes']==0]),outside_overlap=summary([x for x in reads if not x['overlap']]),qualification='Rep zero excluded per reader; caller-interval overlap, not exact internal atomic execution or causal attribution; one synthetic publication, not population p95')
(p/'reader-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s))

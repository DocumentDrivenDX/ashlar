"""Audit exact saved publication IDs and fresh reads, without rerunning writes."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_isolation_r123'
s=json.loads((O/'summary.json').read_text());assert s['state'].startswith('completed one property publication')
all_records=[];hist={}
for out in (O,O/'journal-lane',O/'fresh'):
 c=Client(out);c.records=[json.loads(l) for l in (out/'statements.jsonl').read_text().splitlines()];all_records.extend(c.records);hist.update({q['query_id']:q for q in c.history()})
assert all(r['response']['status']['state']=='SUCCEEDED' and hist[r['statement_id']]['is_final'] for r in all_records),'Refresh same IDs only'
reads=[r for r in all_records if r['label'].startswith('fresh-post-')];assert len(reads)==30
assert all(not hist[r['statement_id']]['metrics'].get('result_from_cache') for r in reads)
def p95(v):return sorted(v)[math.ceil(.95*len(v))-1]
s['scope']='One serialized100k synthetic property publication on existing warehouse, parallel raw/journal appends and validation, no simultaneous readers. Uniform modeled arrivals, not an actual producer rate.'
s['reads']={'n':30,'caller_p95_ms':p95([r['wall_ms'] for r in reads]),'engine_p95_ms':p95([hist[r['statement_id']]['metrics']['execution_time_ms'] for r in reads]),'files_p95':p95([hist[r['statement_id']]['metrics']['read_files_count'] for r in reads]),'remote_queries':sum(hist[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in reads)}
s['phase_metrics']={r['label']:{'caller_ms':r['wall_ms'],'metrics':hist[r['statement_id']]['metrics']} for r in all_records if r['label'].startswith(('capture-','journal-r','journal-parity-','apply-','adjacency-reuse-','publish-'))}
s['qualification']='All20 affected carrier fields, exact token patch, raw bytes/digests/origins/UTC instant, exact property journal, global edge identity, full20M structural adjacency parity and explicit vector reuse. No untouched full-carrier EXCEPT ALL, real producer authority, fencing/acknowledgement, sustained-rate, p95 publication or billion admission.'
s['state']='Completed property publication and30 exact fresh singleton reads audited; all fresh histories final uncached'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'batches':s['batches'],'reads':s['reads'],'phase_metrics':s['phase_metrics']},indent=2))

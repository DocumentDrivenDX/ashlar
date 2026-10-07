"""Final saved-ID metrics for typed versus cast singleton parameters."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_typed_reads_r144'
c=Client(O);c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
def p95(values):return sorted(values)[math.ceil(.95*len(values))-1]
results={}
for mode in ('cast','typed'):
 rs=[r for r in c.records if r['label'].startswith(mode+'-')];assert len(rs)==12
 assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
 results[mode]={'n':len(rs),'caller_p95_ms':p95([r['wall_ms'] for r in rs]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs]),'compile_p95_ms':p95([h[r['statement_id']]['metrics']['compilation_time_ms'] for r in rs]),'files_p95':p95([h[r['statement_id']]['metrics']['read_files_count'] for r in rs]),'remote_queries':sum(h[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in rs)}
s={'state':'Partial24 exact pinned singleton reads audited; server-finished/client-stalled25th request retained','table':'client_dev.ashlar_entropy_20261006_r86.edge_current','version':23,'results':results,'qualification':'Only12 complete alternating pairs from intended30. Cast12 server finished239ms but client transport stalled for several minutes; process deliberately stopped without restart. Descriptive completed-query percentiles exclude the stalled request and must not be treated as latency admission or completed30-key comparison. All24 recorded carriers exact. No signed64 boundary, transport reliability, general p95, concurrency or billion admission.'}
s['termination']=json.loads((O/'termination.json').read_text())
assert s['termination']['completed_reads']==24 and s['termination']['process_exit_code']==143
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s,indent=2))

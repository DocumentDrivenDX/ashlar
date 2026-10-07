"""Final bounded-worker metrics and status recovery for the same lost query ID."""
import json,math,hashlib
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bounded_reads_r145'
process=json.loads((O/'process-bound.json').read_text());assert process['exit_code']==0 and process['worker_wall_s']<=60
c=Client(O);c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
rs=[r for r in c.records if r['label'].startswith('read-')];assert len(rs)==30
assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
def p95(values):return sorted(values)[math.ceil(.95*len(values))-1]
lost=json.loads((B/'out/native/ashlar_typed_reads_r144/termination.json').read_text())['pending_server_queries'][0]['query_id']
recovery=Client(O/'recovery');recovery.records=[{'statement_id':lost}]
observed={q['query_id']:q for q in recovery.history()};q=observed[lost]
assert q['status']=='FINISHED' and q['is_final'] and 'edge_current VERSION AS OF 23' in q['query_text']
(O/'recovery/summary.json').write_text(json.dumps({'state':'Same previously undelivered r144 query ID recovered as FINISHED/final via history GET only','query_id':lost,'query_start_time_ms':q['query_start_time_ms'],'query_end_time_ms':q['query_end_time_ms'],'qualification':'Status recovery only; no result recovery, native SQL submission/re-execution or undelivered carrier assertion. A request correlation tag can identify the owned pending request when no handle returned; ambiguity must fail closed.'},indent=2)+'\n')
root=Path('/private/tmp/ashlar-db-client/lib/python3.9/site-packages/databricks/sql')
sources={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [root/'client.py',root/'backend/thrift_backend.py',root/'auth/thrift_http_client.py']}
s={'state':'Thirty exact correlated full-carrier reads passed within60s worker bound; final result-uncached','process':process,'caller_p95_ms':p95([r['wall_ms'] for r in rs]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs]),'compile_p95_ms':p95([h[r['statement_id']]['metrics']['compilation_time_ms'] for r in rs]),'files_p95':p95([h[r['statement_id']]['metrics']['read_files_count'] for r in rs]),'remote_queries':sum(h[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in rs),'configured_controls':{'socket_seconds':10,'request_attempts':1,'retry_duration_seconds':10,'max_redirects':0,'sql_statement_seconds':15,'worker_wall_seconds':60},'installed_source_sha256':sources,'qualification':'Installed connector internal settings verified in source and accepted by this healthy run; not a fault-injected proof of every socket/auth timeout.60s bound covers the read-only worker including connect/auth, not this separate audit. Each request correlation persisted before submission; no generic replay on timeout. Existing canonical E23 and exact identities retained. Status recovery of r144 does not recover lost results. No production multiwriter, caller SLO, concurrency or billion admission.'}
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s,indent=2))

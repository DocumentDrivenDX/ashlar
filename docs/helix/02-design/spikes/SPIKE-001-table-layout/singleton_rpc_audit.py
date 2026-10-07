"""Audit saved final histories and method-only RPC traces; no query reruns."""
import json, math
from collections import Counter
from pathlib import Path
B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_singleton_rpc_r114'
s=json.loads((O/'summary.json').read_text())
h={q['query_id']:q for q in json.loads((O/'query-history.json').read_text())}
qs=s['queries']
assert len(qs)==30
for q in qs:
    v=h[q['statement_id']]
    assert v['is_final'] and v['status']=='FINISHED'
    assert not v['metrics'].get('result_from_cache')
    assert all(set(r)=={'method','wall_ms'} for r in q['rpc'])
def p95(v):return sorted(v)[math.ceil(.95*len(v))-1]
def metric(k):return p95([h[q['statement_id']]['metrics'][k] for q in qs])
s['state']='30 exact full-field singleton reads audited; all final uncached'
s['measurement']={
    'caller_p95_ms':p95([q['caller_ms'] for q in qs]),
    'engine_p95_ms':metric('execution_time_ms'),
    'compile_p95_ms':metric('compilation_time_ms'),
    'total_server_p95_ms':metric('total_time_ms'),
    'rpc_method_counts':dict(Counter(r['method'] for q in qs for r in q['rpc'])),
    'caller_outside_rpc_p95_ms':p95([q['caller_ms']-sum(r['wall_ms'] for r in q['rpc']) for q in qs]),
    'file_reads_p95':metric('read_files_count'),
    'remote_queries':sum(h[q['statement_id']]['metrics']['read_remote_bytes']>0 for q in qs),
}
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps(s['measurement'],indent=2))

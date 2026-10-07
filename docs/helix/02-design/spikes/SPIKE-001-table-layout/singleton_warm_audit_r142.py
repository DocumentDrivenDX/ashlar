"""Final saved-ID metrics for the post-maintenance warm repeat."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_singleton_warm_r142'
c=Client(O);c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
rs=[r for r in c.records if r['label'].startswith('warm-read-')];assert len(rs)==30
assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
def p95(values):return sorted(values)[math.ceil(.95*len(values))-1]
s={'state':'Thirty exact post-maintenance warm-repeat reads final uncached','caller_p95_ms':p95([r['wall_ms'] for r in rs]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs]),'files_p95':p95([h[r['statement_id']]['metrics']['read_files_count'] for r in rs]),'bytes_p95':p95([h[r['statement_id']]['metrics']['read_bytes'] for r in rs]),'remote_queries':sum(h[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in rs),'qualification':'Same30 SHA-ranked affected keys and optimized clone2 after full rewritten-row verification warmed files. Single finite repeat, no publication/concurrency/controlled-cold or billion admission.'}
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s,indent=2))

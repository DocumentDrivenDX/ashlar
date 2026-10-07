"""Saved-ID final metrics for eligible-key statistics screen."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_write_pruning_r130';s=json.loads((O/'summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
queries={}
for r in c.records:
 if r['label'].startswith('eligible-'):
  m=h[r['statement_id']]['metrics'];assert not m.get('result_from_cache')
  queries[r['label']]={'caller_ms':r['wall_ms'],'engine_ms':m['execution_time_ms'],'files':m['read_files_count'],'bytes':m['read_bytes'],'remote_bytes':m['read_remote_bytes']}
s.update(state='6 exact eligible-key counts and20M immutable row custody audited; all eligible queries final uncached',queries=queries,costs={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records if r['label'] in ('clone','stats-columns','analyze','immutable-custody')})
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(queries,indent=2))

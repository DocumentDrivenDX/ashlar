"""Final saved-ID serialization measurements, no replay."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_serialization_r135'
s=json.loads((O/'summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
measurements={}
for r in c.records:
 if r['label'].startswith(('encoded-','stored-')) or r['label']=='exact-wire':
  m=h[r['statement_id']]['metrics'];assert not m.get('result_from_cache')
  measurements[r['label']]={'caller_ms':r['wall_ms'],'engine_ms':m['execution_time_ms'],'read_files':m['read_files_count'],'read_bytes':m['read_bytes'],'remote_bytes':m['read_remote_bytes'],'metrics':m}
assert len(measurements)==7
s.update(state='Six forced-payload aggregates and independent exact wire comparison audited; all measurements final uncached',measurements=measurements)
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({k:{a:v for a,v in m.items() if a!='metrics'} for k,m in measurements.items()},indent=2))

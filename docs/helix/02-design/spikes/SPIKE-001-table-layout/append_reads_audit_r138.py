"""Final saved-ID read-pruning metrics for tracked scratch layouts."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_append_reads_r138'
s=json.loads((O/'summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
reads=[r for r in c.records if '-clustered-' in r['label'] or '-unclustered-' in r['label']]
assert len(reads)==20 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in reads)
s['reads']={r['label']:{'caller_ms':r['wall_ms'],'engine_ms':h[r['statement_id']]['metrics']['execution_time_ms'],'files':h[r['statement_id']]['metrics']['read_files_count'],'bytes':h[r['statement_id']]['metrics']['read_bytes'],'remote_bytes':h[r['statement_id']]['metrics']['read_remote_bytes']} for r in reads}
s['state']='20 exact full-row raw/history reads audited; all final uncached'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s['reads'],indent=2))

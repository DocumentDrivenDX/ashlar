"""Final metrics for r108; never replay native table writes or queries."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_adjacency_r108'
s=json.loads((O/'summary.json').read_text())
records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
c=Client(O);c.records=records
h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in records),'Refresh same histories only'
assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in records)
def p95(values):return sorted(values)[math.ceil(.95*len(values))-1]
summary={}
for source in ('pilot','other'):
 for kind in ('count','page'):
  rows=[r for r in records if r['label'].startswith(kind+'-'+source+'-')];assert len(rows)==5
  ms=[h[r['statement_id']]['metrics'] for r in rows]
  summary[kind+'-'+source]={'n':5,'caller_p95_ms':p95([r['wall_ms'] for r in rows]),'engine_p95_ms':p95([m['execution_time_ms'] for m in ms]),'read_files_p95':p95([m['read_files_count'] for m in ms]),'read_bytes_p95':p95([m['read_bytes'] for m in ms]),'remote_queries':sum(m.get('read_remote_bytes',0)>0 for m in ms)}
columns=records[-1]['response']['manifest']['schema']['columns'];detail=dict(zip([x['name'] for x in columns],s['detail'][0]))
s.update(state='audited 20M synthetic hub adjacency checks; all final uncached',query_comparison=summary,physical_detail={k:detail[k] for k in ('numFiles','sizeInBytes','clusteringColumns','minReaderVersion','minWriterVersion','tableFeatures')})
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'physical_detail':s['physical_detail'],'queries':summary},indent=2))

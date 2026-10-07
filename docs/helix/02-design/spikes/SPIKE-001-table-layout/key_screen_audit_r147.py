"""Final measurements from saved build/maintenance/read query IDs."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_key_screen_r147'
s=json.loads((O/'summary.json').read_text());records=[];h={}
for out in (O,O/'read-lane'):
 c=Client(out);c.records=[json.loads(l) for l in (out/'statements.jsonl').read_text().splitlines()];records+=c.records;h.update({q['query_id']:q for q in c.history()})
assert json.loads((O/'read-lane/process-bound.json').read_text())['exit_code']==0
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in records),'Refresh same IDs only'
def p95(values):return sorted(values)[math.ceil(.95*len(values))-1]
reads={}
for mode in ('hash','source_id'):
 rs=[r for r in records if r['label'].startswith(mode+'-read-')];assert len(rs)==30
 assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
 reads[mode]={'caller_p95_ms':p95([r['wall_ms'] for r in rs]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs]),'compile_p95_ms':p95([h[r['statement_id']]['metrics']['compilation_time_ms'] for r in rs]),'files_p95':p95([h[r['statement_id']]['metrics']['read_files_count'] for r in rs]),'bytes_p95':p95([h[r['statement_id']]['metrics']['read_bytes'] for r in rs]),'remote_queries':sum(h[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in rs)}
s.update(state='Both exact100k full-carrier key tables and60 exact reads audited; all read metrics final uncached',reads=reads,costs={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in records if r['label'].endswith(('-build','-optimize','-exact'))})
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'reads':reads,'tables':{k:{'version':v['version'],'files':v['detail']['numFiles'],'bytes':v['detail']['sizeInBytes']} for k,v in s['tables'].items()},'costs':{k:v['caller_ms'] for k,v in s['costs'].items()}},indent=2))

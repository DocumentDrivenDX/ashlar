"""Final metrics for stored-carrier append layout replay, saved query IDs only."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_append_layout_r137'
s=json.loads((O/'summary.json').read_text());assert s['state'].startswith('Both stored-carrier')
records=[];history={}
for out in (O,O/'journal-lane'):
 c=Client(out);c.records=[json.loads(l) for l in (out/'statements.jsonl').read_text().splitlines()]
 records+=c.records;history.update({q['query_id']:q for q in c.history()})
assert all(r['response']['status']['state']=='SUCCEEDED' and history[r['statement_id']]['is_final'] for r in records),'Refresh same IDs only'
measured=[r for r in records if r['label'].endswith(('-append','-exact'))]
assert len(measured)==8 and all(not history[r['statement_id']]['metrics'].get('result_from_cache') for r in measured)
s['measurements']={r['label']:{'caller_ms':r['wall_ms'],'metrics':history[r['statement_id']]['metrics']} for r in measured}
s['state']='Both stored-carrier append layouts and exact100k raw/journal comparisons audited; all measured queries final uncached'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({'pairs':[{k:r[k] for k in ('mode','append_pair_wall_s')} for r in s['runs']],'appends':{k:{'caller_ms':v['caller_ms'],**{a:b for a,b in v['metrics'].items() if a in ('execution_time_ms','write_remote_files','write_remote_bytes','read_bytes','read_files_count','spill_to_disk_bytes')}} for k,v in s['measurements'].items() if k.endswith('-append')}},indent=2))

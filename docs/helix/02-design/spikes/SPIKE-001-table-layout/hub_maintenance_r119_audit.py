"""Refresh saved query IDs only; audit maintenance and page costs."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_maintenance_r119'
s=json.loads((O/'summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' for r in c.records)
assert all(h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
groups={};writes={}
for r in c.records:
 m=h[r['statement_id']]['metrics']
 if r['label'] in ('clone','update','optimize'):writes[r['label']]={'caller_ms':r['wall_ms'],'metrics':m,'result':r['response']['result']['data_array']}
 if not r['label'].startswith('page-'):continue
 assert not m.get('result_from_cache')
 key=r['label'].rsplit('-',1)[0]
 groups.setdefault(key,[]).append({'caller_ms':r['wall_ms'],'engine_ms':m['execution_time_ms'],'files':m['read_files_count'],'bytes':m['read_bytes'],'remote_bytes':m.get('read_remote_bytes',0)})
assert sum(map(len,groups.values()))==54
s.update(state='Full20M parity, independent edge identity, unique pairs, typed target closure and54 exact pages audited; all page histories final uncached',writes=writes,pages={k:{'n':len(v),'max':{f:max(r[f] for r in v) for f in v[0]}} for k,v in groups.items()})
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({'changed_edges':s['changed_edges'],'versions':s['versions'],'hub_files':s['hub_files'],'pages':s['pages']},indent=2))

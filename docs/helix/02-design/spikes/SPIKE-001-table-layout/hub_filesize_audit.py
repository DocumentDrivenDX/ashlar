"""Audit same saved IDs; matched file-size maintenance and page metrics."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_filesize_r117'
s=json.loads((O/'summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
groups={};writes={}
for r in c.records:
 m=h[r['statement_id']]['metrics']
 if r['label'].startswith(('clone-','target-','optimize-')):writes[r['label']]={'caller_ms':r['wall_ms'],'metrics':m}
 if not r['label'].startswith('page-'):continue
 assert not m.get('result_from_cache')
 key=r['label'].rsplit('-',1)[0];groups.setdefault(key,[]).append({'caller_ms':r['wall_ms'],'engine_ms':m['execution_time_ms'],'files':m['read_files_count'],'bytes':m['read_bytes'],'remote_bytes':m.get('read_remote_bytes',0)})
assert sum(map(len,groups.values()))==36
s.update(state='Matched20M full parity and36 exact pages audited; all page histories final uncached',writes=writes,pages={k:{'n':len(v),'max':{f:max(r[f] for r in v) for f in v[0]}} for k,v in groups.items()})
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({'versions':s['versions'],'physical':{k:{f:v[f] for f in ('numFiles','sizeInBytes')} for k,v in s['details'].items()},'pages':s['pages']},indent=2))

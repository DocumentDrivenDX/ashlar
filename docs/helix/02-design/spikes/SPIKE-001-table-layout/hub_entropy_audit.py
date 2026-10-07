"""Same-ID final audit of higher-entropy adjacency pages and write costs."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_entropy_r116'
s=json.loads((O/'summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
groups={};writes={}
for r in c.records:
 m=h[r['statement_id']]['metrics']
 if r['label']=='optimize' or r['label'].startswith(('create','append-')):writes[r['label']]={'caller_ms':r['wall_ms'],'metrics':m}
 if not r['label'].startswith('page-'):continue
 assert not m.get('result_from_cache')
 key=r['label'].rsplit('-',1)[0];groups.setdefault(key,[]).append({'caller_ms':r['wall_ms'],'engine_ms':m['execution_time_ms'],'files':m['read_files_count'],'bytes':m['read_bytes'],'remote_bytes':m.get('read_remote_bytes',0)})
assert sum(map(len,groups.values()))==36
s.update(state='20M full parity, identity, typed closure and36 pages audited; all page histories final uncached',writes=writes,pages={k:{'n':len(v),'max':{f:max(r[f] for r in v) for f in v[0]}} for k,v in groups.items()})
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'versions':s['versions'],'details':s['details'],'pages':s['pages']},indent=2))

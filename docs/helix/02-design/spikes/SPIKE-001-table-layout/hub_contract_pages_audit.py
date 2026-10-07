"""Same-ID final audit for paired hub layouts; observe file count, never assume it."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_contract_pages_r112'
s=json.loads((O/'summary.json').read_text())
c=Client(O);c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
assert all(h[r['statement_id']]['is_final'] and not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records),'Refresh same IDs only'
groups={};writes={}
for r in c.records:
 m=h[r['statement_id']]['metrics']
 if r['label'].startswith(('create-','append-')):writes[r['label']]={'caller_ms':r['wall_ms'],'metrics':m}
 if not r['label'].startswith('page-'):continue
 key=r['label'].rsplit('-',1)[0];groups.setdefault(key,[]).append({'caller_ms':r['wall_ms'],'engine_ms':m['execution_time_ms'],'files':m['read_files_count'],'bytes':m['read_bytes'],'remote_bytes':m.get('read_remote_bytes',0)})
assert sum(map(len,groups.values()))==36
summary={k:{'n':len(v),'max':{field:max(r[field] for r in v) for field in v[0]}} for k,v in groups.items()}
s.update(state='audited 36 contract-order hub pages; all final uncached',query_comparison=summary,writes=writes)
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps(summary,indent=2))

"""Final native controls and separately scoped recorded lineage guard."""
import json,hashlib
from pathlib import Path
from persistent_sql import Client
from changed_structure_guard import guard
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_changed_structure_r125';s=json.loads((O/'summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records)
metrics={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records if r['label'].startswith(('stage-exact-','output-exact-','reject-'))}
p=B/'out/native/ashlar_isolation_r123';parent=json.loads((p/'audited-summary.json').read_text());r=parent['batches'][0]
records=[json.loads(l) for l in (p/'statements.jsonl').read_text().splitlines()];apply=next(r for r in records if r['label']=='apply-r123-b1')
assert guard(r['commits'],r['old_current_version'],r['versions']['client_dev.ashlar_entropy_20261006_r86.edge_current'],apply['statement_id'],100000)
ph={q['query_id']:q for q in json.loads((p/'query-history.json').read_text())};assert ph[apply['statement_id']]['query_text'].endswith(apply['sql'])
s.update(state='6 exact changed-set checks and9 corruptions audited; all final uncached; scoped recorded lineage guard passes',query_metrics=metrics,owned_apply={'query_id':apply['statement_id'],'sql_sha256':hashlib.sha256(apply['sql'].encode()).hexdigest(),'history_text_matches_recorded_sql':True},guard_limits='Continuous versions, exactly one recorded MERGE, exact query ID/readVersion/update counts and no inserts/deletes/copied rows. No generic SQL membership parser, unknown operationParameters parsing, concurrent fence or producer authority. Must separately bind trusted fixed membership, baseline adjacency and owned apply SQL; outside-membership changes are not covered by the differential alone.')
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print({k:round(v['caller_ms']/1000,3) for k,v in metrics.items() if not k.startswith('reject-')})

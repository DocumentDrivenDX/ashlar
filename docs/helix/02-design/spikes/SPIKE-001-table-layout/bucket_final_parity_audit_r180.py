"""Audit persisted native results; never rerun the expensive r178 SQL."""
import json
from pathlib import Path
B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_bucket_final_parity_r178'
s=json.loads((O/'summary.json').read_text())
records=[json.loads(line) for line in (O/'statements.jsonl').read_text().splitlines()]
h={r['query_id']:r for r in json.loads((O/'query-history.json').read_text())}
assert len(s['ranges'])==4
assert [int(r['rows']) for r in s['ranges']]==[4999762,5003588,4999471,4997179]
assert sum(int(r['rows']) for r in s['ranges'])==20000000
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['status']=='FINISHED' and h[r['statement_id']]['is_final'] for r in records)
metrics=[]
for r in s['ranges']:
 record=next(x for x in records if x['statement_id']==r['query_id'])
 assert record['response']['result']['data_array']==[[r['rows'],'0']]
 metrics.append({'range':r['range'],'query_id':r['query_id'],'caller_ms':record['wall_ms'],'metrics':h[r['query_id']]['metrics']})
assert s['version']==9 and s['source_version']==23 and s['stage_version']==0
assert s['costs']['read_bytes']==177557596584 and s['costs']['write_remote_bytes']==0
result=dict(s)
result.update(state='All four final 20M exact-value ranges passed; cost admission failed', original_state=s['state'], read_budget_bytes=140000000000, read_budget_pass=False, range_metrics=metrics, qualification='Persisted successful, final native query IDs establish all 20M logical rows across all 20 fields using the r178 uniqueness and expected-set checks. The original generic remaining-ranges wording is incorrect after four successful ranges and is preserved unchanged. The post-query cost guard failed; it was not an in-flight spending cap. No rerun, publication, latency, sustained ingest or billion-scale admission.')
(O/'audited-summary.json').write_text(json.dumps(result,indent=2)+'\n')
print(result['state'])

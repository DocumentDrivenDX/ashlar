"""Audit actual r99 commits; retain the terminal validator failure and recovery."""
import json
from pathlib import Path
from persistent_sql import Client

B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_filtered_publication_20261007_r99'
s=json.loads((O/'summary.json').read_text())
assert s['state']=='completed finite controlled schedule; correctness checks passed'
records=[json.loads(line) for line in (O/'statements.jsonl').read_text().splitlines()]
c=Client(O);c.records=records
h={q['query_id']:q for q in c.history()}
assert all(h.get(r['statement_id'],{}).get('is_final') for r in records), 'Refresh metrics only'
failed=[r for r in records if r['response']['status']['state']!='SUCCEEDED']
assert len(failed)==1 and failed[0]['label']=='raw-parity-r99-b1'
assert failed[0]['response']['status']['state']=='FAILED' and 'UNRESOLVED_COLUMN' in failed[0]['response']['status']['error']['message']
assert not any(h[r['statement_id']]['metrics'].get('result_from_cache') for r in records)
for label in ('prepare','capture','journal','apply','publish'):
    for i in range(1,4):
        matching=[r for r in records if r['label']==f'{label}-r99-b{i}']
        assert len(matching)==1 and matching[0]['response']['status']['state']=='SUCCEEDED', 'A write was repeated or failed'
for batch in s['batches']:
    commits=batch['commits']
    assert {int(v['version']) for v in commits}==set(range(batch['old_current_version']+1,batch['versions']['client_dev.ashlar_entropy_20261006_r86.edge_current']+1))
    batch['merge_metrics']=json.loads(next(v['operationMetrics'] for v in commits if v['operation']=='MERGE'))
    m=batch['merge_metrics']
    assert int(m['numTargetRowsUpdated'])==100000
    assert all(int(m[k])==0 for k in ('numTargetRowsCopied','numTargetRowsInserted','numTargetRowsDeleted'))
s.update(state='audited completed filtered publication; first-batch recovery timings qualified',
         clean_processing_samples_s=[b['processing_s'] for b in s['batches'][1:]],
         freshness_admission='Unproven: finite pre-staged hot set, no real producer or reader load; first batch interrupted and schedule offsets reconstructed. No clean p95 or sustained-rate claim.',
         phases=[{'label':r['label'],'state':r['response']['status']['state'],'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in records])
warehouse=c.w.api_client.do('GET','/api/2.0/sql/warehouses/2439e1f2e37ac563')
s['warehouse_observed_after_run']={k:warehouse.get(k) for k in ('id','name','cluster_size','warehouse_type','enable_serverless_compute','min_num_clusters','max_num_clusters','auto_stop_mins','state')}
s['cost_scope']='Existing shared warehouse, no resize/provision operation performed. Configuration observed after run; exact statement metrics retained. Attributable DBU/dollar cost not established.'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({'clean_processing_samples_s':s['clean_processing_samples_s'],'versions':[b['versions'] for b in s['batches']]},indent=2))

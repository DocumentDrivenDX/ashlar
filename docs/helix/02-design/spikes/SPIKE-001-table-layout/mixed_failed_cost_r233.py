"""Native GET-only complete failure cost audit; never retry SQL."""
import json,time
from pathlib import Path
from databricks.sdk import WorkspaceClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_apply_r231';records=[]
for path in [O/'statements.jsonl',O/'audit/statements.jsonl']:records += [json.loads(x) for x in path.read_text().splitlines()]
ids={r['statement_id'] for r in records};assert None not in ids;w=WorkspaceClient(profile='aidev-cus')
for n in range(12):
 response=w.api_client.do('GET','/api/2.0/sql/history/queries',query={'filter_by.query_start_time_range.start_time_ms':int(min(r['start_epoch'] for r in records)*1000)-1000,'max_results':1000,'include_metrics':True});found={x['query_id']:x for x in response['res'] if x['query_id'] in ids}
 if set(found)==ids and all(x.get('is_final') for x in found.values()):break
 if n==11:raise RuntimeError('Missing/final metrics pending; no SQL restart')
 time.sleep(2)
failed=[x for x in found.values() if x['status']=='FAILED'];assert len(failed)==1 and all(x['status'] in ['FINISHED','FAILED'] for x in found.values())
costs={k:sum(x['metrics'].get(k,0) for x in found.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};(O/'complete-failure-history.json').write_text(json.dumps(found,indent=2)+'\n');(O/'complete-failure-costs.json').write_text(json.dumps({'queries':len(ids),'costs':costs,'failed_query_id':failed[0]['query_id'],'qualification':'Includes original failed publisher and read-only post-failure status/table audit; all final native IDs, no SQL replay. Reported counters not full retained physical storage/billing.'},indent=2)+'\n');print(json.dumps(costs))

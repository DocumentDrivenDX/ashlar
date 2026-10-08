"""Recover the original interrupted source digest handle without SQL replay."""
import json
import hashlib
from append_edge_growth_r665 import BASE,OUT
from persistent_sql import Client
from publication_history import collect_history

c=Client(OUT)
c.records=[json.loads(x) for x in (OUT/'statements.jsonl').read_text().splitlines()]
live=json.loads((OUT/'live-statement.json').read_text())
sid=live['statement_id']
response=c.w.api_client.do('GET','/api/2.0/sql/statements/'+sid)
receipt={'statement_id':sid,'response':response,'original_sql':live['statement'],'qualification':'Same interrupted native handle; original client elapsed time unavailable, no SQL replay.'}
(OUT/'same-handle-recovery-r667.json').write_text(json.dumps(receipt,indent=2)+'\n')
assert response['status']['state']=='SUCCEEDED',response['status']
assert not response.get('manifest',{}).get('truncated',False)
oracle=json.loads((BASE/'out/append-edge-oracle-r656.json').read_text())
expected=[[str(i),str(x['roles']['source_record']['rows']),x['roles']['source_record']['digest']] for i,x in enumerate(oracle['chunks'])]
assert response['result']['data_array']==expected
assert sid not in {r['statement_id'] for r in c.records}
rec={'warehouse_id':c.warehouse_id,'label':live['label'],'sql':live['statement'],'statement_id':sid,'response':response,'parameters':None,'wall_ms':None,'start_epoch':None,'recovery':'Interrupted client timing unknown; same original handle'}
c.records.append(rec)
with (OUT/'statements.jsonl').open('a') as f:f.write(json.dumps(rec)+'\n')
h=collect_history(c.w,c.records,OUT/'shared-history.json')
for r in c.records:
 assert h[r['statement_id']]['query_text']==r['sql']
s=json.loads((OUT/'summary.json').read_text())
s['checks']['source_record']={'version':s['versions']['source_record'],'groups':expected,'rows':8000000,'all_fields':True,'invalid_membership':0,'same_handle_recovery':sid}
s['costs']={k:sum(q['metrics'][k] for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
s['state']='Interrupted controller stopped; carrier/raw parity complete; journal adjacency closure pending'
s['recovery_qualification']='Original controller process missing. Original source digest handle succeeded and final; no query replay. Full phase wall time unknown after interruption; prior wall_s is historical partial observation.'
(OUT/'summary.json').write_text(json.dumps(s,indent=2)+'\n')
(OUT/'live-statement.json').unlink()
print(json.dumps({'state':s['state'],'costs':s['costs'],'statements':len(c.records)},indent=2))

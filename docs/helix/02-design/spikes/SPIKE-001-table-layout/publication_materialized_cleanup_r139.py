"""Remove only audited materialized intermediates after durable publication."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_isolation_r139'
s=json.loads((O/'audited-summary.json').read_text());assert s['state'].startswith('Completed property publication')
batch=s['batches'][0];assert batch['batch']=='r139-b1'
c=DriverClient(O/'write-input-cleanup');removed=[];details={}
for role in ('raw','journal'):
 target=batch['write_inputs'][role]
 assert target==f'client_dev.ashlar_entropy_20261006_r86.write_{role}_r139'
 assert int(c.sql(role+'-version','DESCRIBE HISTORY '+target+' LIMIT 1')[0][0])==0
 assert c.sql(role+'-membership',f"SELECT count(*),count_if(apply_batch_id='r139-b1') FROM {target} VERSION AS OF 0")==[['100000','100000']]
 detail=c.sql(role+'-detail','DESCRIBE DETAIL '+target)
 names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 details[role]=dict(zip(names,detail[0]))
 assert target not in batch['versions'],'Published vectors must not reference temporary write inputs'
 c.sql(role+'-drop','DROP TABLE '+target);removed.append(target)
(O/'write-input-cleanup/summary.json').write_text(json.dumps({'state':'Two owned audited100k intermediates dropped after durable canonical publication','tables':removed,'details_before_drop':details,'qualification':'Platform retention applies; no immediate physical storage reclamation or VACUUM claim.'},indent=2)+'\n')
c.history();c.close();print('Materialized write inputs dropped')

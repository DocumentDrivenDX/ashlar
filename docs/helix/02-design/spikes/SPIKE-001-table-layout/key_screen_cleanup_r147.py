"""Drop audited key-screen tables after validating recorded table identities."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_key_screen_r147'
s=json.loads((O/'audited-summary.json').read_text());assert s['state'].startswith('Both exact100k')
c=DriverClient(O/'cleanup');removed=[]
for mode,entry in s['tables'].items():
 A=entry['table'];assert A=='client_dev.ashlar_entropy_20261006_r86.key_'+mode+'_r147'
 rows=c.sql(mode+'-identity','DESCRIBE DETAIL '+A);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 assert dict(zip(names,rows[0]))['id']==entry['detail']['id']
 assert int(c.sql(mode+'-version','DESCRIBE HISTORY '+A+' LIMIT 1')[0][0])==entry['version']
 c.sql(mode+'-drop','DROP TABLE '+A);removed.append(A)
(O/'cleanup/summary.json').write_text(json.dumps({'state':'Two identity-verified audited key-screen tables dropped; canonical data unchanged','tables':removed,'qualification':'UC retention applies; no VACUUM/immediate physical reclamation claim.'},indent=2)+'\n')
c.history();c.close();print('Owned key-screen tables dropped')

"""Drop only the audited owned shallow clone after preserving evidence."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_singleton_optimize_r140'
s=json.loads((O/'audited-summary.json').read_text());assert s['state'].startswith('60 exact singleton')
records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
record=next(r for r in records if r['label']=='after-detail');names=[x['name'] for x in record['response']['manifest']['schema']['columns']]
saved=dict(zip(names,record['response']['result']['data_array'][0]))
A=s['table'];assert A=='client_dev.ashlar_entropy_20261006_r86.edge_optimize_r140'
c=DriverClient(O/'cleanup');detail=c.sql('verify','DESCRIBE DETAIL '+A)
names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
assert dict(zip(names,detail[0]))['id']==saved['id']
assert int(c.sql('version','DESCRIBE HISTORY '+A+' LIMIT 1')[0][0])==s['versions']['after']
c.sql('drop','DROP TABLE '+A)
(O/'cleanup/summary.json').write_text(json.dumps({'state':'Audited owned shallow clone dropped; published source unchanged','table':A,'qualification':'No VACUUM. Managed storage retention applies; no immediate physical reclamation claim.'},indent=2)+'\n')
c.history();c.close();print('Owned maintenance clone dropped')

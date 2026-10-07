"""Drop only the four audited owned scratch tables; no VACUUM or canonical edits."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_append_layout_r134'
s=json.loads((O/'audited-summary.json').read_text());assert s['state'].startswith('Both stored-carrier')
c=DriverClient(O/'cleanup');removed=[]
for run in s['runs']:
 for role,target in run['targets'].items():
  assert target==f"client_dev.ashlar_entropy_20261006_r86.append_{role}_{run['mode']}_r134"
  detail=c.sql('verify-'+role+'-'+run['mode'],'DESCRIBE DETAIL '+target)
  names=[col['name'] for col in c.records[-1]['response']['manifest']['schema']['columns']]
  current=dict(zip(names,detail[0]));assert current['id']==run['checks'][role]['detail']['id']
  c.sql('drop-'+role+'-'+run['mode'],'DROP TABLE '+target);removed.append(target)
(O/'cleanup/summary.json').write_text(json.dumps({'state':'Four audited owned scratch tables dropped; canonical tables unchanged','tables':removed,'qualification':'UC managed-table drop is subject to platform retention; no immediate physical storage reclamation claim and no VACUUM.'},indent=2)+'\n')
c.history();c.close();print('Four owned scratch tables dropped')

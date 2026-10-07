"""Drop only audited owned distribution clones, no canonical edits/VACUUM."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_merge_pruning_r146'
s=json.loads((O/'audited-summary.json').read_text());assert s['state'].startswith('Both actual MERGEs')
c=DriverClient(O/'cleanup');details={}
for mode in ('control','candidate'):
 A=s['results'][mode]['table'];assert A=='client_dev.ashlar_entropy_20261006_r86.edge_merge_'+mode+'_r146'
 history=c.sql(mode+'-version','DESCRIBE HISTORY '+A+' LIMIT 1')
 names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 current=dict(zip(names,history[0]));saved=json.loads((O/(mode+'-delta-history.json')).read_text())[0]
 assert int(current['version'])==s['results'][mode]['new_version'] and current['queryHistoryStatementId']==saved['queryHistoryStatementId']
 rows=c.sql(mode+'-detail','DESCRIBE DETAIL '+A);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 details[mode]=dict(zip(names,rows[0]))
 c.sql(mode+'-drop','DROP TABLE '+A)
(O/'cleanup/summary.json').write_text(json.dumps({'state':'Both audited owned distribution clones dropped; canonical E23 unchanged','details_before_drop':details,'qualification':'No VACUUM or immediate physical storage reclamation; UC retention applies.'},indent=2)+'\n')
c.history();c.close();print('Both owned distribution clones dropped')

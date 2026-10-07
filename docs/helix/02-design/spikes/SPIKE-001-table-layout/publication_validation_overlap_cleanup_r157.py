"""Drop only finalized owned r157 tables after UUID/version checks."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_queue_r157'
s=json.loads((O/'audited-summary.json').read_text());assert s['state'].startswith('One isolated overlapped-write-and-validation publication finalized')
Q=O/'cleanup';assert not (Q/'statements.jsonl').exists(),'Inspect saved cleanup state; no blind repeat'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
for i,(table,expected) in enumerate(s['owned_identity'].items()):
 assert table.endswith('_queue_r157') or '.schedule_r157_' in table
 rows=c.sql('detail-'+str(i),'DESCRIBE DETAIL '+table)
 names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 assert dict(zip(names,rows[0]))['id']==expected['id']
 assert int(c.sql('version-'+str(i),'DESCRIBE HISTORY '+table+' LIMIT 1')[0][0])==expected['version']
 c.sql('drop-'+str(i),'DROP TABLE '+table)
c.history();c.close()
(Q/'summary.json').write_text(json.dumps({'state':'All five owned r157 tables UUID/version checked and dropped','tables':list(s['owned_identity']),'qualification':'Canonical E23/R17/J19/r139-b1 unchanged; no VACUUM, physical retention follows platform policy'},indent=2)+'\n')
print('All five owned queue experiment tables dropped')

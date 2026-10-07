"""Remove only five finalized failed-lane fixture tables; no repair/retry of mutation."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_failure_r156'
s=json.loads((O/'audited-summary.json').read_text());assert s['state'].startswith('Failed raw validation blocks')
Q=O/'cleanup';assert not (Q/'statements.jsonl').exists(),'Inspect cleanup state; no replay'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
for i,(table,expected) in enumerate(s['owned_identity'].items()):
 assert table.endswith('_failure_r156')
 rows=c.sql('detail-'+str(i),'DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 assert dict(zip(names,rows[0]))['id']==expected['id']
 assert int(c.sql('version-'+str(i),'DESCRIBE HISTORY '+table+' LIMIT 1')[0][0])==expected['version']
 c.sql('drop-'+str(i),'DROP TABLE '+table)
c.history();c.close();(Q/'summary.json').write_text(json.dumps({'state':'Five owned failed-lane tables UUID/version checked and dropped','tables':list(s['owned_identity']),'qualification':'Recorded unpublished versions inspected then dropped; no recovery/retry or VACUUM claim. Canonical E23/R17/J19/r139-b1 unchanged.'},indent=2)+'\n');print('Five owned failed-lane fixture tables dropped')

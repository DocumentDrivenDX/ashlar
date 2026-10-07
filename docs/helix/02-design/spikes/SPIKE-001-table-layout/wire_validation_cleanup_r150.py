"""Remove only the owned immutable wire witness after final native audit."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_wire_validation_r150'
s=json.loads((O/'audited-summary.json').read_text());assert s['state'].startswith('Balanced exact raw validation')
Q=O/'cleanup';assert not (Q/'statements.jsonl').exists(),'Inspect existing cleanup; no blind retry'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30');t=s['witness']
assert t=='client_dev.ashlar_entropy_20261006_r86.wire_witness_r150'
r=c.sql('detail','DESCRIBE DETAIL '+t);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
assert dict(zip(names,r[0]))['id']==s['witness_id']
assert c.sql('version','DESCRIBE HISTORY '+t+' LIMIT 1')[0][0]=='0'
c.sql('drop','DROP TABLE '+t);c.history();c.close()
(Q/'summary.json').write_text(json.dumps({'state':'Owned witness UUID/version checked and dropped','table':t,'qualification':'Canonical E23/R17/J19/r139-b1 unchanged; no VACUUM'},indent=2)+'\n')
print('Owned wire witness dropped')

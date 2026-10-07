"""Drop only UUID/version-verified owned bucket pilot tables after final audit."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_screen_r158'
s=json.loads((O/'audited-summary.json').read_text());assert s['state'].startswith('100k exact carrier layouts')
Q=O/'cleanup';assert not (Q/'statements.jsonl').exists(),'Inspect prior cleanup handle'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
for name,x in s['owned'].items():
 t=x['table'];assert t in ['client_dev.ashlar_entropy_20261006_r86.bucket_lc_r158','client_dev.ashlar_entropy_20261006_r86.bucket_part_r158']
 r=c.sql(name+'-detail','DESCRIBE DETAIL '+t);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']]
 assert dict(zip(names,r[0]))['id']==x['id']
 assert int(c.sql(name+'-version','DESCRIBE HISTORY '+t+' LIMIT 1')[0][0])==x['version']
 c.sql(name+'-drop','DROP TABLE '+t)
c.history();c.close();(Q/'summary.json').write_text(json.dumps({'state':'Two owned pilot tables UUID/version checked and dropped','qualification':'Canonical E23/R17/J19/r139-b1 unchanged; no VACUUM'},indent=2)+'\n');print('Owned bucket pilot tables dropped')

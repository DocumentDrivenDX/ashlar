"""Inspect terminal failed CTAS and remove only the UUID/version-owned clone."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_screen_r161'
h=json.loads((O/'query-history.json').read_text());failed=[q for q in h if q['status']=='FAILED'];assert len(failed)==1 and failed[0]['is_final'] and '180 seconds' in failed[0]['error_message']
owned=json.loads((O/'inspection/owned.json').read_text());assert len(owned)==1
Q=O/'cleanup';assert not (Q/'statements.jsonl').exists(),'Inspect existing cleanup handle'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
x=owned[0];t=x['table'];assert t=='client_dev.ashlar_entropy_20261006_r86.bucket_lc_r161' and int(x['version'])==1
r=c.sql('clone-detail','DESCRIBE DETAIL '+t);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,r[0]))['id']==x['id'];assert c.sql('clone-version','DESCRIBE HISTORY '+t+' LIMIT 1')[0][0]==x['version']
c.sql('clone-drop','DROP TABLE '+t)
assert c.sql('remaining',"SHOW TABLES IN client_dev.ashlar_entropy_20261006_r86 LIKE 'bucket*_r161'")==[]
pins=c.sql('publication',"SELECT table_versions_json FROM client_dev.ashlar_entropy_20261006_r86.publication_manifest_r89 WHERE publication_id='r139-b1'");assert json.loads(pins[0][0])['client_dev.ashlar_entropy_20261006_r86.edge_current']==23
c.history();c.close()
(O/'failed-summary.json').write_text(json.dumps({'state':'Full20M bucket CTAS terminal timeout; no bucket table registered; owned clone UUID/version checked and dropped','failed_query_id':failed[0]['query_id'],'metrics':failed[0]['metrics'],'owned':owned,'publication_vector':json.loads(pins[0][0]),'qualification':'No ZORDER, structural parity, update, singleton comparison or full-wide-copy proof ran. Remote write bytes are failed-operation work, not committed table size. No write replay or VACUUM; failed uncommitted storage cleanup not independently proven. UC Delta architecture unchanged.'},indent=2)+'\n');print('Failed run inspected; owned clone dropped')

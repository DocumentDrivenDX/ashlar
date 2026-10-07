import sys,json
from pathlib import Path
B=Path('/Users/erik/Projects/ashlar/docs/helix/02-design/spikes/SPIKE-001-table-layout');sys.path.insert(0,str(B))
from persistent_sql import Client
from bounded_reads_r145 import BoundedReads
O=B/'out/native/ashlar_bucket_screen_r161';c=Client(O);c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()];h=c.history();print(json.dumps([{k:q.get(k) for k in ('query_id','status','is_final','metrics','error_message')} for q in h],indent=2))
Q=O/'inspection';r=BoundedReads(Q);r.sql('timeout','SET STATEMENT_TIMEOUT=30');tables=r.sql('tables',"SHOW TABLES IN client_dev.ashlar_entropy_20261006_r86 LIKE 'bucket* r161'") if False else r.sql('tables',"SHOW TABLES IN client_dev.ashlar_entropy_20261006_r86 LIKE 'bucket*_r161'")
print(tables)
owned=[]
for row in tables:
 t='client_dev.ashlar_entropy_20261006_r86.'+row[1];d=r.sql(row[1]+'-detail','DESCRIBE DETAIL '+t);names=[x['name'] for x in r.records[-1]['response']['manifest']['schema']['columns']];detail=dict(zip(names,d[0]));v=r.sql(row[1]+'-version','DESCRIBE HISTORY '+t+' LIMIT 1')[0][0];owned.append({'table':t,'id':detail['id'],'version':v,'detail':detail})
(Q/'owned.json').write_text(json.dumps(owned,indent=2)+'\n');r.history();r.close()

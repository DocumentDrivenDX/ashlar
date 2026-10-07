"""Verify actual integrated writer hot-file ranges; no value/read-SLO inference."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_publication_shape_r199';assert not O.exists()
s=json.loads((B/'out/native/ashlar_queue_serial_r197/audited-summary.json').read_text());T=s['owned_tables'][0];x=s['owned_identity'][T];assert x['version']==1
c=BoundedReads(O);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
a=c.sql('detail','DESCRIBE DETAIL '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,a[0]))['id']==x['id'];assert c.sql('version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='1'
rows=c.sql('hot-ranges',f"SELECT _metadata.file_path,max(_metadata.file_size),count(*),min(lookup_hash),max(lookup_hash) FROM {T} VERSION AS OF 1 WHERE entity_version=16 AND apply_batch_id='r189-b1' GROUP BY _metadata.file_path");assert sum(int(r[2]) for r in rows)==100000
c.close()
for attempt in range(8):
 h={x['query_id']:x for x in c.history()}
 if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
 time.sleep(2)
assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records)
costs={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert costs['read_bytes']<=2000000000 and costs['write_remote_bytes']==0
spans=[(int(r[4],16)-int(r[3],16))/2**256 for r in rows]
(O/'summary.json').write_text(json.dumps({'state':'Integrated100k live hot-file range group passed','table':T,'id':x['id'],'version':1,'files':rows,'hash_domain_fractions':spans,'costs':costs,'qualification':'Actual integrated writer output, full-precision live extrema, not stored Delta statistics or point-read latency. Fresh property values on same synthetic100k hot identities; not a disjoint/skewed workload or production range count.'},indent=2)+'\n');print(json.dumps({'files':len(rows),'spans':spans,'costs':costs},indent=2))

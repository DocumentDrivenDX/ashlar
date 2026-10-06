"""Two paced clients reading pinned old/recovered full edge carriers."""
import json,time,hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_recovered_concurrent_reads_20261005_r9';s=json.loads((B/'out/native/ashlar_receipt_recovery_20261005_r8r1/summary.json').read_text());assert s['state']=='passed'
clients=[DriverClient(out/f'reader-{i}') for i in range(2)];hash=hashlib.sha256(json.dumps(dict(source_system='pilot:0',rel_type_id=7,id=1000001),separators=(',',':')).encode()).hexdigest()
queries=[f"SELECT * FROM {F}.edge_current VERSION AS OF {v} WHERE lookup_hash='{hash}' AND source_system='pilot:0' AND rel_type_id=7 AND id=1000001" for v in [4,5]]
expected=[]
for i,c in enumerate(clients):
 c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');r=c.sql('prime',queries[i]);assert len(r)==1 and r[0][9]==['{"201":"hub-fixture"}','{"201":"recovered-fixture"}'][i];expected.append(r)
def reader(i):
 c=clients[i]
 for rep in range(51):
  start=time.monotonic();assert c.sql(f'reader-{i}-{rep}',queries[i])==expected[i];time.sleep(max(0,.5-(time.monotonic()-start)))
 return 51
with ThreadPoolExecutor(max_workers=2) as pool:counts=list(pool.map(reader,range(2)))
records=[]
for c in clients:records.extend(c.records);c.close()
(out/'all-statements.json').write_text(json.dumps(records,indent=2));clients[0].records=records;clients[0].history()
(out/'summary.json').write_text(json.dumps(dict(state='passed',reads=counts,versions=[4,5],projection='all 17 columns',scope='two paced clients, one changed identity, no simultaneous writer or broad key distribution'),indent=2));print('Concurrent pinned carriers passed')

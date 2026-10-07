"""Read-only source-range probe before bounded resumable bucket-copy design."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_chunk_r162'
assert not (O/'statements.jsonl').exists(),'Inspect existing handle; do not repeat'
c=BoundedReads(O);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
E='client_dev.ashlar_entropy_20261006_r86.edge_current'
r=c.sql('range',f'SELECT min(id),max(id),count(*) FROM {E} VERSION AS OF 23');print(r,flush=True)
assert r[0][2]=='20000000'
lo,hi=map(int,r[0][:2]);width=(hi-lo+5)//5
ranges=[(lo+i*width,min(lo+(i+1)*width,hi+1)) for i in range(5)]
counts=[]
for i,(a,z) in enumerate(ranges):counts.append(c.sql('slice-'+str(i),f'SELECT count(*),min(id),max(id) FROM {E} VERSION AS OF 23 WHERE id>={a} AND id<{z}')[0])
assert sum(int(x[0]) for x in counts)==20000000
h=c.history();c.close();(O/'summary.json').write_text(json.dumps({'source':E,'version':23,'ranges':ranges,'counts':counts,'qualification':'Read-only source ranges; no copy, pruning, preservation, latency or ingest admission inferred; final metrics still require audit.'},indent=2)+'\n');print('Five disjoint complete source ranges measured')

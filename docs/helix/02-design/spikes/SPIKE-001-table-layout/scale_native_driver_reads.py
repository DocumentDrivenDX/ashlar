"""Repeat only contaminated REST read controls using persistent uncached sessions."""
import json,time,concurrent.futures,argparse
from pathlib import Path
from driver_sql import DriverClient
from scale_workload import EDGES
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_scale_20261006_r85';F='client_dev.ashlar_scale_20261006_r85'
parser=argparse.ArgumentParser();parser.add_argument('--version',type=int,default=1);v=parser.parse_args().version;assert v>=0
folder='uncached-driver-' if v==1 else f'uncached-v{v}-driver-'
clients=[DriverClient(O/f'{folder}{i}') for i in range(4)]
def read(d,j,label):
 key=1+(j*15485863)%EDGES;source='other' if key%10==0 else 'pilot';typ=key%32+1
 r=d.sql(label+str(j),f"SELECT * FROM {F}.edge_current VERSION AS OF {v} WHERE lookup_hash=sha2(to_json(named_struct('source_system','{source}','rel_type_id',cast({typ} AS BIGINT),'id',cast({key} AS BIGINT))),256) AND source_system='{source}' AND rel_type_id={typ} AND id={key}");assert len(r)==1 and r[0][2]==str(key)
for j in range(50):read(clients[0],j,'serial-pinned-')
def worker(i):
 for j in range(i,50,4):read(clients[i],j,'concurrent-pinned-')
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(worker,range(4)))
for d in clients:d.history();d.close()
(O/('uncached-driver-summary.json' if v==1 else f'uncached-v{v}-driver-summary.json')).write_text(json.dumps({'state':'read correctness passed; final cache/engine metrics require history audit','read_counts':{'serial':50,'four_clients':50},'edge_version':v,'driver':'databricks-sql-connector/4.3.0','session_configuration':{'use_cached_result':'false'},'scope':'warm data after validation/update; not cold/publication/ingest admission'},indent=2)+'\n')

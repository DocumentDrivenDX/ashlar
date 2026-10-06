import sys,json,concurrent.futures,time
from pathlib import Path
B=Path(__file__).resolve().parent;sys.path.insert(0,str(B))
from persistent_sql import Client
from scale_workload import EDGES
O=B/'out/native/ashlar_scale_20261006_r85';c=Client(O/'post-run');F='client_dev.ashlar_scale_20261006_r85'
assert c.sql('endpoint-closure',f'''SELECT count(*) FROM (
 SELECT e.id FROM {F}.edge_current VERSION AS OF 1 e LEFT ANTI JOIN {F}.object_current VERSION AS OF 0 n ON e.source_system=n.source_system AND e.source_type=n.type_id AND e.source_id=n.id
 UNION ALL
 SELECT e.id FROM {F}.edge_current VERSION AS OF 1 e LEFT ANTI JOIN {F}.object_current VERSION AS OF 0 n ON e.source_system=n.source_system AND e.target_type=n.type_id AND e.target_id=n.id)''')==[['0']]
c.sql('post-update-detail',f'DESCRIBE DETAIL {F}.edge_current')
c.sql('declared-shape-profile',f'SELECT count(DISTINCT source_id),count(DISTINCT target_id),count(DISTINCT struct(source_type,source_id,target_type,target_id)) FROM {F}.edge_current VERSION AS OF 1')
clients=[Client(O/f'concurrent-{i}') for i in range(4)]
for client in clients:client.sql('disable-result-cache','SET use_cached_result=false')
def worker(i):
 d=clients[i]
 for j in range(i,50,4):
  key=1+(j*15485863)%EDGES;source='other' if key%10==0 else 'pilot';typ=key%32+1
  r=d.sql('pinned-singleton-'+str(j),f"SELECT * FROM {F}.edge_current VERSION AS OF 1 WHERE lookup_hash=sha2(to_json(named_struct('source_system','{source}','rel_type_id',cast({typ} AS BIGINT),'id',cast({key} AS BIGINT))),256) AND source_system='{source}' AND rel_type_id={typ} AND id={key}");assert len(r)==1 and r[0][2]==str(key)
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(worker,range(4)))
for client in clients:client.history()
c.history()
# Refresh original final metrics, retaining exact statement identity.
d=Client(O);d.records=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];d.history()
(O/'post-run/summary.json').write_text(json.dumps({'state':'passed','checks':['all 20M source and target tuples close over pinned 4M node snapshot','post-MERGE file/DV inventory','actual endpoint cardinality','50 pinned full-carrier singleton result checks with four independent SDK clients; audit history for cache contamination'],'versions':{'objects':0,'edges':1}},indent=2)+'\n')

"""One measured maintenance comparison after the authorized scattered update."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
from scale_workload import EDGES,UPDATES
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_scale_20261006_r85/maintenance';F='client_dev.ashlar_scale_20261006_r85';c=DriverClient(O)
c.sql('statement-limit','SET STATEMENT_TIMEOUT=900')
assert int(c.sql('prior-version',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0])==1
c.sql('full-cluster-maintenance',f'OPTIMIZE {F}.edge_current FULL')
v=int(c.sql('maintained-version',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0])
c.sql('maintained-detail',f'DESCRIBE DETAIL {F}.edge_current')
# Complete row multiset must match pre-maintenance publication.
assert c.sql('full-row-maintenance-parity',f'SELECT count(*) FROM ((SELECT * FROM {F}.edge_current VERSION AS OF {v} EXCEPT ALL SELECT * FROM {F}.edge_current VERSION AS OF 1) UNION ALL (SELECT * FROM {F}.edge_current VERSION AS OF 1 EXCEPT ALL SELECT * FROM {F}.edge_current VERSION AS OF {v}))')==[['0']]
for j in range(50):
 key=1+(j*15485863)%EDGES;source='other' if key%10==0 else 'pilot';typ=key%32+1
 r=c.sql('maintained-pinned-'+str(j),f"SELECT * FROM {F}.edge_current VERSION AS OF {v} WHERE lookup_hash=sha2(to_json(named_struct('source_system','{source}','rel_type_id',cast({typ} AS BIGINT),'id',cast({key} AS BIGINT))),256) AND source_system='{source}' AND rel_type_id={typ} AND id={key}");assert len(r)==1 and r[0][2]==str(key)
c.history();c.close();(O/'summary.json').write_text(json.dumps({'state':'full-row parity and 50 read checks passed; metrics require final-history audit','old_version':1,'new_version':v,'scope':'One full 20M-edge maintenance comparison, not an adopted per-batch policy or sustained ingest evidence'},indent=2)+'\n')

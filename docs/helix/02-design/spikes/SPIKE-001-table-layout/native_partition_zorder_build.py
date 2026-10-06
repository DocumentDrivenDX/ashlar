"""Bounded four-bucket native layout comparison; never replaces canonical tables."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1'
N='client_dev.ashlar_partition_zorder_20261005_r29';out=B/'out/native/ashlar_partition_zorder_build_20261005_r29';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');c.sql('schema',f'CREATE SCHEMA {N}')
bucket="CAST(conv(substr(lookup_hash,1,1),16,10) AS INT) DIV 4"
t=time.perf_counter()
c.sql('create-copy',f"""CREATE TABLE {N}.edge_current USING DELTA PARTITIONED BY (lookup_bucket)
TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.enableRowTracking'='true','delta.enableDeletionVectors'='true','delta.targetFileSize'='16777216','delta.parquet.compression.codec'='zstd','delta.dataSkippingStatsColumns'='lookup_hash,source_system,rel_type_id,id')
AS SELECT {','.join(COLS)},{bucket} lookup_bucket FROM {F}.edge_current VERSION AS OF 19""")
copy_s=time.perf_counter()-t;c.sql('before-detail',f'DESCRIBE DETAIL {N}.edge_current')
t=time.perf_counter();c.sql('zorder',f'OPTIMIZE {N}.edge_current ZORDER BY (lookup_hash)');opt_s=time.perf_counter()-t
v=int(c.sql('version',f'DESCRIBE HISTORY {N}.edge_current LIMIT 1')[0][0]);c.sql('after-detail',f'DESCRIBE DETAIL {N}.edge_current');c.sql('history',f'DESCRIBE HISTORY {N}.edge_current')
assert c.sql('counts',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(lookup_bucket IS DISTINCT FROM ({bucket})) FROM {N}.edge_current VERSION AS OF {v}')==[['10019981','10019981','0']]
c.sql('bucket-distribution',f'SELECT lookup_bucket,count(*) FROM {N}.edge_current VERSION AS OF {v} GROUP BY lookup_bucket ORDER BY lookup_bucket')
checks=' OR '.join(f'a.{x} IS DISTINCT FROM b.{x}' for x in COLS)
assert c.sql('full-parity',f'SELECT count(*),count_if(a.id IS NULL),count_if(b.id IS NULL),count_if({checks}) FROM {F}.edge_current VERSION AS OF 19 a FULL OUTER JOIN {N}.edge_current VERSION AS OF {v} b ON a.source_system=b.source_system AND a.rel_type_id=b.rel_type_id AND a.id=b.id')==[['10019981','0','0','0']]
c.history();c.close()
(out/'summary.json').write_text(json.dumps({'state':'passed','source_version':19,'candidate':N+'.edge_current','version':v,'copy_wall_s':copy_s,'zorder_wall_s':opt_s,'rows':10019981,'buckets':4,'scope':'complete 17-field copy, exact derived bucket, unique native keys; new table row tracking lineage, no performance/ingest/full-scale admission'},indent=2))
print('Partition/Z-order candidate build and full parity passed',flush=True)

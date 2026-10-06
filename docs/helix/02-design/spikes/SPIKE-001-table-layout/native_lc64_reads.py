"""Wide canonical 64MiB copy: first-touch reads before exhaustive parity scans."""
import hashlib,json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_lc64_20261005_r42'
assert json.loads((B/'out/native/ashlar_maintained_final_verify_20261005_r41_lc/summary.json').read_text())['state']=='passed'
out=B/'out/native/ashlar_lc64_reads_20261005_r42';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
keys=[('pilot:'+str((i%10)//2),7+i%2,240001+(i*7919)%60000) for i in range(51)]
where=' OR '.join(f"(source_system='{s}' AND rel_type_id={r} AND id={i})" for s,r,i in keys)
oldrows=c.sql('source-sample',f"SELECT {','.join(COLS)} FROM {F}.edge_current VERSION AS OF 26 WHERE {where}")
expected={(r[0],int(r[1]),int(r[2])):[r] for r in oldrows};assert len(expected)==len(oldrows)==51
for source,rel,id in keys:
 row=expected[(source,rel,id)][0];batch=1 if id<=270000 else 2
 payload=''.join(hashlib.sha256(f'{source}:{rel}:{id}:r39:{batch}:{i}'.encode()).hexdigest() for i in range(32))
 assert row[9]==json.dumps({'201':payload},separators=(',',':')) and row[8]=='9' and row[12:15]==['fixture-maintained','e',str(batch)]
c.sql('schema',f'CREATE SCHEMA {N}');t=time.perf_counter()
c.sql('copy',f"""CREATE TABLE {N}.edge_current USING DELTA CLUSTER BY (lookup_hash)
TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.enableRowTracking'='true','delta.enableDeletionVectors'='true','delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd','delta.dataSkippingStatsColumns'='lookup_hash,source_system,rel_type_id,id')
AS SELECT {','.join(COLS)} FROM {F}.edge_current VERSION AS OF 26""")
copy_s=time.perf_counter()-t;v=int(c.sql('copy-version',f'DESCRIBE HISTORY {N}.edge_current LIMIT 1')[0][0]);c.sql('detail-64',f'DESCRIBE DETAIL {N}.edge_current');c.sql('detail-16',f'DESCRIBE DETAIL {F}.edge_current')
(out/'progress.json').write_text(json.dumps({'state':'reading','copy_wall_s':copy_s,'version':v},indent=2))
for phase_i,phase in enumerate(['prime','repeat','repeat2']):
 for rep,(source,rel,id) in enumerate(keys):
  h=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=rel,id=id),separators=(',',':')).encode()).hexdigest()
  for layout in (['64','16'] if (rep+phase_i)%2==0 else ['16','64']):
   ns,version=(N,v) if layout=='64' else (F,26)
   rows=c.sql(f'lc{layout}-{phase}-{rep}',f"SELECT {','.join(COLS)} FROM {ns}.edge_current VERSION AS OF {version} WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={rel} AND id={id}")
   assert rows==expected[(source,rel,id)] and rows[0][16]==h
 print(phase,'completed',flush=True)
checks=' OR '.join(f'a.{x} IS DISTINCT FROM b.{x}' for x in COLS)
assert c.sql('full-parity',f'SELECT count(*),count_if(a.id IS NULL),count_if(b.id IS NULL),count_if({checks}) FROM {F}.edge_current VERSION AS OF 26 a FULL OUTER JOIN {N}.edge_current VERSION AS OF {v} b ON a.source_system=b.source_system AND a.rel_type_id=b.rel_type_id AND a.id=b.id')==[['10019981','0','0','0']]
assert c.sql('unique',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {N}.edge_current VERSION AS OF {v}')==[['10019981','10019981']]
c.sql('history',f'DESCRIBE HISTORY {N}.edge_current');c.history();c.close()
(out/'scope.json').write_text(json.dumps({'state':'completed','source_version':26,'candidate_version':v,'copy_wall_s':copy_s,'scope':'same full 17 fields/entropy/51 updated native keys, alternating 16/64 target reads, new copy read before exhaustive parity; actual remote bytes govern first-touch claims; new row-tracking lineage; no ingest/scale/publication admission'},indent=2));print('LC64 read and full preservation comparison passed',flush=True)

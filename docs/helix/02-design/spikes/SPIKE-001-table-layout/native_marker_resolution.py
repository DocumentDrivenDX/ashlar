"""Resolve pinned versions from durable content markers across metadata-only commits."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_marker_resolution_20261005_r18';c=DriverClient(out)
for t in ['marker_current_r18','marker_journal_r18','marker_receipt_r18']:
 c.sql('ddl-'+t,f"CREATE TABLE {F}.{t} (id BIGINT,batch STRING,payload STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
for t in ['marker_current_r18','marker_journal_r18']:c.sql('seed-'+t,f"INSERT INTO {F}.{t} VALUES (1,'seed','original')")
base={t:int(c.sql('base-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in ['marker_current_r18','marker_journal_r18']}
# Deterministic metadata-only version increments, not undocumented OPTIMIZE behavior.
c.sql('metadata-current',f"ALTER TABLE {F}.marker_current_r18 SET TBLPROPERTIES ('delta.targetFileSize'='16777216')")
for size in [16777216,33554432]:c.sql('metadata-journal-'+str(size),f"ALTER TABLE {F}.marker_journal_r18 SET TBLPROPERTIES ('delta.targetFileSize'='{size}')")
c.sql('data-with-receipt',f"""BEGIN ATOMIC
 UPDATE {F}.marker_current_r18 SET batch='r18-1',payload='updated' WHERE id=1;
 INSERT INTO {F}.marker_journal_r18 VALUES (2,'r18-1','original -> updated');
 INSERT INTO {F}.marker_receipt_r18 VALUES (1,'r18-1','pending publication'); END""")
committed={t:int(c.sql('committed-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in base}
for t in base:c.sql('metadata-after-'+t,f"ALTER TABLE {F}.{t} SET TBLPROPERTIES ('delta.targetFileSize'='67108864')")
c.close();r=DriverClient(out/'resolver');assert r.sql('durable-receipt',f"SELECT count(*) FROM {F}.marker_receipt_r18 WHERE batch='r18-1'")==[['1']]
resolved={};latest={}
for t in base:
 rows=r.sql('history-'+t,f'DESCRIBE HISTORY {F}.{t}');versions=sorted(int(x[0]) for x in rows if int(x[0])>base[t]);latest[t]=max(versions)
 found=[]
 for v in versions:
  count=int(r.sql(f'marker-{t}-v{v}',f"SELECT count(*) FROM {F}.{t} VERSION AS OF {v} WHERE batch='r18-1'")[0][0]);assert count in [0,1]
  if count:found.append(v)
 assert found and found==versions[versions.index(found[0]):],found
 resolved[t]=found[0]
 assert resolved[t]==committed[t] and resolved[t]!=base[t]+1 and resolved[t]!=latest[t]
assert r.sql('resolved-current',f"SELECT * FROM {F}.marker_current_r18 VERSION AS OF {resolved['marker_current_r18']}")==[['1','r18-1','updated']]
assert r.sql('resolved-journal',f"SELECT * FROM {F}.marker_journal_r18 VERSION AS OF {resolved['marker_journal_r18']} ORDER BY id")==[['1','seed','original'],['2','r18-1','original -> updated']]
assert r.sql('old-current',f"SELECT * FROM {F}.marker_current_r18 VERSION AS OF {base['marker_current_r18']}")==[['1','seed','original']]
c.records.extend(r.records);c.history();r.close();(out/'summary.json').write_text(json.dumps({'state':'passed','base':base,'committed':committed,'resolved':resolved,'latest':latest,'scope':'small durable-content-marker prototype, deterministic metadata-only commits; no later logical writes, retention/history must be complete; not full-scale resolver or general monotonic current-state guarantee'},indent=2));print('Content marker resolution passed')

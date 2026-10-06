"""Prepare exact inherited history and a common scattered full-carrier stage."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_partition_zorder_20261005_r29'
out=B/'out/native/ashlar_partition_ingest_prepare_20261005_r31';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
t=time.perf_counter()
c.sql('copy-journal',f"CREATE TABLE {N}.property_journal USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.enableRowTracking'='true','delta.enableDeletionVectors'='true','delta.parquet.compression.codec'='zstd') AS SELECT * FROM {F}.property_journal VERSION AS OF 17")
jcopy=time.perf_counter()-t
payload='concat('+','.join(f"sha2(concat(source_system,':',cast(rel_type_id AS STRING),':',cast(id AS STRING),':r31:{i}'),256)" for i in range(32))+')'
t=time.perf_counter()
c.sql('stage',f"CREATE TABLE {N}.stage_r31 USING DELTA AS SELECT {','.join(COLS)},to_json(map('201',{payload})) new_props,row_number() OVER (ORDER BY source_system,rel_type_id,id) ordinal FROM {F}.edge_current VERSION AS OF 19 WHERE id BETWEEN 210001 AND 240000")
stage_s=time.perf_counter()-t
assert c.sql('stage-count',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count(DISTINCT ordinal),count_if(length(get_json_object(new_props,\'$.201\'))<>2048) FROM {N}.stage_r31')==[['300000','300000','300000','0']]
prior=' OR '.join(f's.{x} IS DISTINCT FROM e.{x}' for x in COLS)
for name,table,v in [('lc',F,19),('partition',N,1)]:
 assert c.sql('prior-'+name,f'SELECT count(*),count_if({prior}) FROM {N}.stage_r31 s JOIN {table}.edge_current VERSION AS OF {v} e ON s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id')==[['300000','0']]
assert c.sql('journal-count',f'SELECT count(*) FROM {N}.property_journal')==[['2820023']]
q=f"SELECT count(*) FROM ((SELECT * FROM {F}.property_journal VERSION AS OF 17 EXCEPT ALL SELECT * FROM {N}.property_journal) UNION ALL (SELECT * FROM {N}.property_journal EXCEPT ALL SELECT * FROM {F}.property_journal VERSION AS OF 17))"
assert c.sql('journal-full-parity',q)==[['0']]
c.history();c.close()
(out/'summary.json').write_text(json.dumps({'state':'passed','journal_copy_wall_s':jcopy,'stage_wall_s':stage_s,'stage':N+'.stage_r31','changed_rows':300000,'journal_rows':2820023,'scope':'common exact 17-field prior stage and inherited full journal multiset parity; setup outside future arrival clock; no ingest performance claim'},indent=2))
print('Paired ingest setup and preservation passed',flush=True)

"""Exact inherited journal and common wide stage for 16/64MiB guarded ingest."""
import hashlib,json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_lc64_20261005_r42'
assert json.loads((B/'out/native/ashlar_lc64_reads_20261005_r42/scope.json').read_text())['state']=='completed'
out=B/'out/native/ashlar_lc64_ingest_prepare_20261005_r44';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
t=time.perf_counter();c.sql('journal-copy',f"CREATE TABLE {N}.property_journal USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.enableRowTracking'='true','delta.enableDeletionVectors'='true','delta.parquet.compression.codec'='zstd') AS SELECT * FROM {F}.property_journal VERSION AS OF 20");jcopy=time.perf_counter()-t
payload='concat('+','.join(f"sha2(concat(source_system,':',cast(rel_type_id AS STRING),':',cast(id AS STRING),':r44:{i}'),256)" for i in range(32))+')'
t=time.perf_counter();c.sql('stage',f"CREATE TABLE {N}.stage_r44 USING DELTA AS SELECT {','.join(COLS)},to_json(map('201',{payload})) new_props,row_number() OVER (ORDER BY source_system,rel_type_id,id) ordinal FROM {F}.edge_current VERSION AS OF 26 WHERE id BETWEEN 300001 AND 330000");stage_s=time.perf_counter()-t
assert c.sql('stage-count',f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count(DISTINCT ordinal),count_if(length(get_json_object(new_props,'$.201'))<>2048) FROM {N}.stage_r44")==[['300000','300000','300000','0']]
prior=' OR '.join(f's.{x} IS DISTINCT FROM e.{x}' for x in COLS)
for name,table,v in [('16',F,26),('64',N,0)]:assert c.sql('prior-lc'+name,f'SELECT count(*),count_if({prior}) FROM {N}.stage_r44 s JOIN {table}.edge_current VERSION AS OF {v} e ON s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id')==[['300000','0']]
for domain in range(10):
 source='pilot:'+str(domain//2);rel=7+domain%2;id=300001+(domain*7919)%30000
 rows=c.sql('payload-domain'+str(domain),f"SELECT new_props FROM {N}.stage_r44 WHERE source_system='{source}' AND rel_type_id={rel} AND id={id}")
 want=''.join(hashlib.sha256(f'{source}:{rel}:{id}:r44:{i}'.encode()).hexdigest() for i in range(32));assert rows==[[json.dumps({'201':want},separators=(',',':'))]]
assert c.sql('journal-count',f'SELECT count(*) FROM {N}.property_journal')==[['3720023']]
old=f'SELECT * FROM {F}.property_journal VERSION AS OF 20';new=f'SELECT * FROM {N}.property_journal VERSION AS OF 0'
assert c.sql('journal-exact',f'SELECT count(*) FROM (({old} EXCEPT ALL {new}) UNION ALL ({new} EXCEPT ALL {old}))')==[['0']]
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'passed','journal_rows':3720023,'stage_rows':300000,'journal_copy_wall_s':jcopy,'stage_wall_s':stage_s,'source_edge_version':26,'source_journal_version':20,'candidate_edge_version':0,'candidate_journal_version':0,'scope':'exact inherited multiset and complete common prior carriers; setup outside future clock; independent new 2KB payloads in ten domains; no ingest performance admission'},indent=2));print('LC64 common ingest preparation passed',flush=True)

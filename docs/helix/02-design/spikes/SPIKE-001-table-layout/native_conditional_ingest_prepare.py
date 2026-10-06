"""Disjoint exact wide stage for conditional MERGE timing; no canonical mutation."""
import hashlib,json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_lc64_20261005_r42'
assert json.loads((B/'out/native/ashlar_publisher_marker_20261006_r50/summary.json').read_text())['state']=='passed'
out=B/'out/native/ashlar_conditional_prepare_20261006_r51';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
assert c.sql('initial-edge',f'DESCRIBE HISTORY {N}.edge_current LIMIT 1')[0][0]=='1'
assert c.sql('initial-journal',f'DESCRIBE HISTORY {N}.property_journal LIMIT 1')[0][0]=='1'
payload='concat('+','.join(f"sha2(concat(source_system,':',cast(rel_type_id AS STRING),':',cast(id AS STRING),':r51:{i}'),256)" for i in range(32))+')'
t=time.perf_counter();c.sql('stage',f"CREATE TABLE {N}.stage_r51 USING DELTA AS SELECT {','.join(COLS)},to_json(map('201',{payload})) new_props,row_number() OVER (ORDER BY source_system,rel_type_id,id) ordinal FROM {N}.edge_current VERSION AS OF 1 WHERE id BETWEEN 330001 AND 360000");stage_s=time.perf_counter()-t
assert c.sql('stage-count',f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count(DISTINCT ordinal),count_if(length(get_json_object(new_props,'$.201'))<>2048) FROM {N}.stage_r51")==[['300000','300000','300000','0']]
prior=' OR '.join(f's.{x} IS DISTINCT FROM e.{x}' for x in COLS)
for name,table,v in [('64',N,1)]:assert c.sql('prior-lc'+name,f'SELECT count(*),count_if({prior}) FROM {N}.stage_r51 s JOIN {table}.edge_current VERSION AS OF {v} e ON s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id')==[['300000','0']]
for domain in range(10):
 source='pilot:'+str(domain//2);rel=7+domain%2;id=330001+(domain*7919)%30000
 rows=c.sql('payload-domain'+str(domain),f"SELECT new_props FROM {N}.stage_r51 WHERE source_system='{source}' AND rel_type_id={rel} AND id={id}")
 want=''.join(hashlib.sha256(f'{source}:{rel}:{id}:r51:{i}'.encode()).hexdigest() for i in range(32));assert rows==[[json.dumps({'201':want},separators=(',',':'))]]
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'passed','stage_rows':300000,'stage_wall_s':stage_s,'source_edge_version':1,'source_journal_version':1,'scope':'disjoint IDs330001..360000; complete17-field prior carrier; unique typed keys and ordinals; independently checked2KB payloads in ten domains; setup outside timing; canonical/journal unchanged'},indent=2));print('Conditional MERGE stage preparation passed',flush=True)

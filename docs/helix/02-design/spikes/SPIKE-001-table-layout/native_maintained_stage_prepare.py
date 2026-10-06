"""Common disjoint full-carrier stages for a maintenance-inclusive schedule."""
import hashlib,json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_partition_zorder_20261005_r29'
pub=json.loads((B/'out/native/ashlar_partition_maintenance_publish_20261005_r38/summary.json').read_text());assert pub['state']=='passed'
out=B/'out/native/ashlar_maintained_stage_prepare_20261005_r39';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');stages=[]
for batch in [1,2]:
 lo=240001+(batch-1)*30000;hi=lo+29999;name=N+'.stage_r39_'+str(batch)
 payload='concat('+','.join(f"sha2(concat(source_system,':',cast(rel_type_id AS STRING),':',cast(id AS STRING),':r39:{batch}:{i}'),256)" for i in range(32))+')'
 t=time.perf_counter();c.sql('stage-'+str(batch),f"CREATE TABLE {name} USING DELTA AS SELECT {','.join(COLS)},to_json(map('201',{payload})) new_props,row_number() OVER (ORDER BY source_system,rel_type_id,id) ordinal FROM {F}.edge_current VERSION AS OF 21 WHERE id BETWEEN {lo} AND {hi}");wall=time.perf_counter()-t
 assert c.sql('count-'+str(batch),f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count(DISTINCT ordinal),count_if(length(get_json_object(new_props,'$.201'))<>2048) FROM {name}")==[['300000','300000','300000','0']]
 for domain in range(10):
  source='pilot:'+str(domain//2);rel=7+domain%2;id=lo+(domain*7919)%30000
  rows=c.sql('payload-'+str(batch)+'-domain'+str(domain),f"SELECT new_props FROM {name} WHERE source_system='{source}' AND rel_type_id={rel} AND id={id}")
  want=''.join(hashlib.sha256(f'{source}:{rel}:{id}:r39:{batch}:{i}'.encode()).hexdigest() for i in range(32));assert rows==[[json.dumps({'201':want},separators=(',',':'))]]
 stages.append({'batch':batch,'table':name,'lo':lo,'hi':hi,'prepare_wall_s':wall});print('stage',batch,'prepared',flush=True)
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'passed','stages':stages,'rows_per_batch':300000,'planned_interval_s':30,'scope':'two disjoint common full-carrier stages from LC21; preparation outside future clock, unique native keys and ordinals, independently checked 2KB payloads per domain; no ingest admission'},indent=2));print('Maintenance-inclusive stages prepared',flush=True)

"""Bounded actual canonical-edge layout comparison; synthetic IDs are not renumbered source IDs."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
BASE=Path(__file__).resolve().parent;schema='ashlar_edges_20261005_j1';F='client_dev.'+schema;out=BASE/'out/native'/schema;c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar synthetic canonical edge identity/adjacency comparison'")
c.sql('environment','SELECT current_version()')
ddl='\n'.join(x for x in (BASE/'sql/delta-candidate.sql').read_text().splitlines() if not x.lstrip().startswith('--')).split(';')[1].strip()
for table,keys in [('edge_endpoint','rel_type_id, source_id, target_id'),('edge_identity','source_system, rel_type_id, id')]:
 text=ddl.replace('CREATE TABLE edge_current','CREATE TABLE '+F+'.'+table).replace('CLUSTER BY (rel_type_id, source_id, target_id)','CLUSTER BY ('+keys+')')
 assert text.endswith(')');text=text[:-1]+",'delta.targetFileSize'='16777216')"
 c.sql('ddl-'+table,text)
payload='concat('+','.join(f"sha2(concat(cast(id AS STRING),':edge:{i}'),256)" for i in range(8))+')'
c.sql('populate',f"""INSERT INTO {F}.edge_endpoint SELECT 'pilot',7,id,1,src,1,pmod(src+CASE WHEN slot<2 THEN 17 ELSE slot*17 END-1,200000)+1,'r1',0,to_json(named_struct('201',{payload})), '{{"future":{{"preserve":true}}}}',cast(NULL AS STRING),'S','e',0,current_timestamp() FROM (SELECT id,pmod((id-1)*104729,200000)+1 src,cast(floor((id-1)/200000) AS BIGINT) slot FROM range(1,1000001))""")
c.sql('copy',f'INSERT INTO {F}.edge_identity SELECT * FROM {F}.edge_endpoint')
c.sql('adjacency',f"CREATE TABLE {F}.adjacency USING DELTA CLUSTER BY (rel_type_id,source_id,target_id) TBLPROPERTIES ('delta.dataSkippingStatsColumns'='source_system,rel_type_id,source_type,source_id,target_type,target_id,id','delta.targetFileSize'='16777216') AS SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id FROM {F}.edge_endpoint")
for table in ['edge_endpoint','edge_identity','adjacency']:
 c.sql('optimize-'+table,f'OPTIMIZE {F}.{table} FULL');c.sql('detail-'+table,f'DESCRIBE DETAIL {F}.{table}')
assert c.sql('identity-count',f'SELECT count(*),count(DISTINCT id) FROM {F}.edge_identity')==[['1000000','1000000']]
assert c.sql('carrier-parity',f'SELECT count(*) FROM {F}.edge_endpoint a FULL OUTER JOIN {F}.edge_identity b ON a.id=b.id WHERE a.id IS NULL OR b.id IS NULL OR NOT (struct(a.*) <=> struct(b.*))')==[['0']]
assert c.sql('endpoint-resolution',f"SELECT count(*) FROM {F}.edge_identity e LEFT JOIN client_dev.ashlar_scale_20261005_i1.object_current n ON n.source_system=e.source_system AND n.type_id=e.source_type AND n.id=e.source_id LEFT JOIN client_dev.ashlar_scale_20261005_i1.object_current t ON t.source_system=e.source_system AND t.type_id=e.target_type AND t.id=e.target_id WHERE n.id IS NULL OR t.id IS NULL")==[['0']]
assert c.sql('parallel-edges',f'SELECT count(*) FROM (SELECT source_id,target_id,count(*) n FROM {F}.adjacency GROUP BY source_id,target_id HAVING n=2)')==[['200000']]
for rep in range(31):
 key=1+(rep*104729)%1000000;src=1+(rep*7919)%200000
 baseline=None;adj=None
 for table in ['edge_endpoint','edge_identity','adjacency']:
  if table!='adjacency':
   rows=c.sql(f'{table}-lookup-{rep}',f"SELECT id,source_type,source_id,target_type,target_id,props_json,retained_json FROM {F}.{table} WHERE source_system='pilot' AND rel_type_id=7 AND id={key}")
   assert len(rows)==1 and rows[0][0]==str(key)
   if baseline is None:baseline=rows
   else:assert rows==baseline
  rows=c.sql(f'{table}-outgoing-{rep}',f"SELECT id,source_type,source_id,target_type,target_id FROM {F}.{table} WHERE source_system='pilot' AND rel_type_id=7 AND source_type=1 AND source_id={src} ORDER BY id")
  assert len(rows)==5
  if adj is None:adj=rows
  else:assert rows==adj
 if rep%10==0:print('round',rep,flush=True)
c.history();c.close();(out/'run.json').write_text(json.dumps({'state':'completed','edges':1000000,'endpoint_nodes':200000,'parallel_endpoint_pairs':200000,'scope':'one source/type/relationship; full canonical edge fields, exact copy parity, typed endpoint resolution, warm/mixed lookup and outgoing controls; no edge ingest or billion-scale admission'},indent=2)+'\n')

"""Mixed-domain canonical edge hash plus typed narrow adjacency baseline."""
import json,hashlib
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_composite_edges_20261005_q1';c=DriverClient(out)
N='client_dev.ashlar_composite_20261005_p1r1.object_current VERSION AS OF 3'
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');c.sql('environment','SELECT current_version()');c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar synthetic mixed-domain edge/adjacency baseline'")
expr="sha2(to_json(named_struct('source_system',source_system,'rel_type_id',rel_type_id,'id',id)),256)"
ddl='\n'.join(x for x in (B/'sql/delta-candidate.sql').read_text().splitlines() if not x.lstrip().startswith('--')).split(';')[1].strip()
ddl=ddl.replace('CREATE TABLE edge_current','CREATE TABLE '+F+'.edge_current').replace('published_at TIMESTAMP NOT NULL','published_at TIMESTAMP NOT NULL,lookup_hash STRING NOT NULL').replace('CLUSTER BY (rel_type_id, source_id, target_id)','CLUSTER BY (lookup_hash)').replace("'source_system,rel_type_id,source_type,source_id,target_type,target_id,id'","'lookup_hash,source_system,rel_type_id,id'")
assert ddl.endswith(')');c.sql('ddl',ddl[:-1]+",'delta.targetFileSize'='16777216','delta.feature.catalogManaged'='supported')")
c.sql('populate',f"""INSERT INTO {F}.edge_current SELECT *,{expr} FROM (SELECT concat('pilot:',cast(d.domain DIV 2 AS STRING)) source_system,cast(7+d.domain%2 AS BIGINT) rel_type_id,e.id,cast(1+d.domain%2 AS BIGINT) source_type,e.source_id,cast(1+d.domain%2 AS BIGINT) target_type,e.target_id,e.schema_revision,e.entity_version,e.props_json,e.retained_json,e.order_key,concat('synthetic-edge-domain-',cast(d.domain AS STRING)) source_feed,e.source_epoch,e.source_position,e.published_at FROM client_dev.ashlar_edges_20261005_j1.edge_identity VERSION AS OF 1 e CROSS JOIN range(10) d(domain))""")
c.sql('cluster',f'OPTIMIZE {F}.edge_current FULL');ev=int(c.sql('edge-version',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0]);c.sql('edge-detail',f'DESCRIBE DETAIL {F}.edge_current')
c.sql('adjacency',f"CREATE TABLE {F}.adjacency USING DELTA CLUSTER BY (source_system,rel_type_id,source_type,source_id) TBLPROPERTIES ('delta.dataSkippingStatsColumns'='source_system,rel_type_id,source_type,source_id,target_type,target_id,id','delta.targetFileSize'='16777216') AS SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id FROM {F}.edge_current VERSION AS OF {ev}")
c.sql('adjacency-cluster',f'OPTIMIZE {F}.adjacency FULL');av=int(c.sql('adjacency-version',f'DESCRIBE HISTORY {F}.adjacency LIMIT 1')[0][0]);c.sql('adjacency-detail',f'DESCRIBE DETAIL {F}.adjacency')
E=f'{F}.edge_current VERSION AS OF {ev}';A=f'{F}.adjacency VERSION AS OF {av}'
assert c.sql('identity-and-hash',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count(DISTINCT id),count_if(lookup_hash IS DISTINCT FROM {expr}) FROM {E}')==[['10000000','10000000','1000000','0']]
assert c.sql('carrier-parity',f'SELECT count(*),count_if(e.props_json IS DISTINCT FROM b.props_json OR e.retained_json IS DISTINCT FROM b.retained_json OR e.order_key IS DISTINCT FROM b.order_key OR e.source_id IS DISTINCT FROM b.source_id OR e.target_id IS DISTINCT FROM b.target_id) FROM {E} e JOIN client_dev.ashlar_edges_20261005_j1.edge_identity VERSION AS OF 1 b ON e.id=b.id')==[['10000000','0']]
for end in ['source','target']:
 assert c.sql('resolve-'+end,f'SELECT count(*) FROM {E} e LEFT ANTI JOIN {N} n ON e.source_system=n.source_system AND e.{end}_type=n.type_id AND e.{end}_id=n.id')==[['0']]
assert c.sql('adjacency-parity',f'SELECT count(*),count_if(e.source_type IS DISTINCT FROM a.source_type OR e.source_id IS DISTINCT FROM a.source_id OR e.target_type IS DISTINCT FROM a.target_type OR e.target_id IS DISTINCT FROM a.target_id) FROM {E} e JOIN {A} a ON e.source_system=a.source_system AND e.rel_type_id=a.rel_type_id AND e.id=a.id')==[['10000000','0']]
assert c.sql('parallel-pairs',f'SELECT count(*) FROM (SELECT source_system,rel_type_id,source_type,source_id,target_type,target_id,count(*) n FROM {A} GROUP BY source_system,rel_type_id,source_type,source_id,target_type,target_id HAVING n=2)')==[['2000000']]
expected={}
for phase in ['prime','repeat','repeat2']:
 for rep in range(51):
  domain=rep%10;source='pilot:'+str(domain//2);rel=7+domain%2;typ=1+domain%2;id=1+(rep*196613)%1000000;src=1+((id-1)*104729)%200000;slot=(id-1)//200000;target=1+(src+(17 if slot<2 else slot*17)-1)%200000
  hash=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=rel,id=id),separators=(',',':')).encode()).hexdigest()
  rows=c.sql(f'{phase}-{rep}',f"SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,props_json,retained_json,lookup_hash FROM {E} WHERE lookup_hash='{hash}' AND source_system='{source}' AND rel_type_id={rel} AND id={id}")
  assert len(rows)==1 and rows[0][:7]==[source,str(rel),str(id),str(typ),str(src),str(typ),str(target)] and rows[0][-1]==hash
  if phase=='prime':expected[rep]=rows
  else:assert rows==expected[rep]
 print(phase,'completed',flush=True)
for rep in range(21):
 domain=rep%10;source='pilot:'+str(domain//2);rel=7+domain%2;typ=1+domain%2;src=1+(rep*7919)%200000
 query=f"SELECT id,source_type,source_id,target_type,target_id FROM {{table}} WHERE source_system='{source}' AND rel_type_id={rel} AND source_type={typ} AND source_id={src} ORDER BY id"
 a=c.sql(f'adjacency-out-{rep}',query.format(table=A));e=c.sql(f'canonical-out-{rep}',query.format(table=E));assert len(a)==5 and a==e
c.history();c.close();(out/'scope.json').write_text(json.dumps(dict(state='completed',node_version=3,edge_version=ev,adjacency_version=av,nodes=10000000,edges=10000000,domains=10,distinct_native_edge_ids=1000000,parallel_endpoint_pairs=2000000,scope='static synthetic cloned domain fixture; full exact edge carriers, native independent IDs, typed endpoint resolution and narrow adjacency; no native relationship-catalog restriction, mutation/recovery/external-engine or billion-scale admission'),indent=2)+'\n');print('Composite edge/adjacency baseline completed',flush=True)

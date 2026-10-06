"""One 200k multi-domain edge property physical replacement with paced old-vector readers."""
import json,time,threading,hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_composite_edge_publication_20261005_q2';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');before=int(c.sql('before-version',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0]);c.sql('before-detail',f'DESCRIBE DETAIL {F}.edge_current')
ddl='\n'.join(x for x in (B/'sql/delta-candidate.sql').read_text().splitlines() if not x.lstrip().startswith('--')).split(';')
for name,index in [('property_journal',2),('publication_manifest',4)]:
 s=ddl[index].strip().replace('CREATE TABLE '+name,'CREATE TABLE '+F+'.'+name)
 if name=='property_journal':s=s[:-1]+",'delta.feature.catalogManaged'='supported','delta.targetFileSize'='134217728')"
 c.sql('ddl-'+name,s)
c.sql('receipt',f"CREATE TABLE {F}.receipt (batch BIGINT,applied_at TIMESTAMP) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
hash_expr="sha2(to_json(named_struct('source_system',s.source_system,'rel_type_id',s.rel_type_id,'id',s.id)),256)"
payload='concat('+','.join(f"sha2(concat(source_system,':',cast(rel_type_id AS STRING),':',cast(id AS STRING),':q2:{i}'),256)" for i in range(32))+')'
c.sql('producer',f"CREATE TABLE {F}.producer_q2 USING DELTA AS SELECT *,to_json(named_struct('201',{payload})) new_props,cast(substring(source_system,7) AS BIGINT)*2000000+(rel_type_id-7)*1000000+id event_ordinal FROM {F}.edge_current VERSION AS OF {before} WHERE pmod((id-1)*104729,1000000)<20000")
assert c.sql('producer-integrity',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count(DISTINCT event_ordinal),count(DISTINCT struct(source_system,rel_type_id)) FROM {F}.producer_q2')==[['200000','200000','200000','10']]
cols=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision','entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position','published_at','lookup_hash']
identity='s.source_system=o.source_system AND s.rel_type_id=o.rel_type_id AND s.id=o.id'
prior=' OR '.join(f'o.{x} IS DISTINCT FROM s.{x}' for x in cols)+f' OR s.lookup_hash IS DISTINCT FROM {hash_expr}'
keep=[x for x in cols if x not in ['entity_version','props_json','source_feed','source_position','published_at']]
post=' OR '.join([f'o.{x} IS DISTINCT FROM s.{x}' for x in keep]+["o.props_json IS DISTINCT FROM s.new_props","o.entity_version IS DISTINCT FROM 1","o.source_feed IS DISTINCT FROM 'fixture-multi-edge'","o.source_position IS DISTINCT FROM 1","o.published_at IS NULL",f'o.lookup_hash IS DISTINCT FROM {hash_expr}',"j.id IS NULL","j.property_id IS DISTINCT FROM 201","j.entity_version IS DISTINCT FROM 1","j.operation IS DISTINCT FROM 'update'","j.old_present IS DISTINCT FROM true","j.new_present IS DISTINCT FROM true","j.event_ordinal IS DISTINCT FROM s.event_ordinal","from_json(concat('[',j.old_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.props_json,'$.201')","from_json(concat('[',j.new_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.new_props,'$.201')"])
# Warm exactly five changed IDs per domain for old-publication controls.
ids=[id for id in range(1,1000001) if ((id-1)*104729)%1000000<20000][:5];keys=[];expected={}
for domain in range(10):
 for id in ids:
  source='pilot:'+str(domain//2);typ=7+domain%2;h=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=typ,id=id),separators=(',',':')).encode()).hexdigest();k=(source,typ,id,h);keys.append(k)
  expected[k]=c.sql(f'old-prime-{domain}-{id}',f"SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,props_json,retained_json,lookup_hash FROM {F}.edge_current VERSION AS OF {before} WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={typ} AND id={id}")
  assert len(expected[k])==1
readers=[DriverClient(out/f'reader-{i}') for i in range(2)]
for r in readers:r.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
stop=threading.Event();pool=ThreadPoolExecutor(max_workers=2)
def reader(i):
 r=readers[i];rep=0
 while not stop.is_set():
  start=time.monotonic();k=keys[(rep*2+i)%len(keys)];source,typ,id,h=k
  rows=r.sql(f'reader-{i}-{rep}',f"SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,props_json,retained_json,lookup_hash FROM {F}.edge_current VERSION AS OF {before} WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={typ} AND id={id}")
  assert rows==expected[k];rep+=1;stop.wait(max(0,.5-(time.monotonic()-start)))
 return rep
futures=[pool.submit(reader,i) for i in range(2)];t0=time.time();ready=t0+20
(out/'scope.json').write_text(json.dumps(dict(state='running',before_version=before,readers=2,max_qps_per_reader=2,entities=200000,accumulation_s=20,producer_generation='excluded',reader_priming='50 changed identities outside clock',origin='fixture-multi-edge/e/1; event ordinal domain*1M+native ID; no native Truss feed claim'),indent=2)+'\n')
try:
 while time.time()<ready:time.sleep(min(.5,ready-time.time()))
 begun=time.time();c.sql('stage',f'CREATE TABLE {F}.stage_q2 USING DELTA AS SELECT * FROM {F}.producer_q2')
 old="to_json(array(get_json_object(props_json,'$.201')))";new="to_json(array(get_json_object(new_props,'$.201')))"
 statement=f"""BEGIN ATOMIC
 IF (SELECT /*+ BROADCAST(s) */ count(*)<>200000 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>200000 OR count_if({prior})<>0 FROM {F}.stage_q2 s JOIN {F}.edge_current o ON {identity}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='composite prior/key mismatch'; END IF;
 DELETE FROM {F}.edge_current t WHERE EXISTS (SELECT 1 FROM {F}.stage_q2 s WHERE t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id);
 INSERT INTO {F}.edge_current SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,schema_revision,cast(1 AS BIGINT),new_props,retained_json,order_key,'fixture-multi-edge',source_epoch,cast(1 AS BIGINT),current_timestamp(),lookup_hash FROM {F}.stage_q2;
 INSERT INTO {F}.property_journal SELECT source_system,'edge',rel_type_id,id,201,1,'update',true,substring({old},2,length({old})-2),true,substring({new},2,length({new})-2),schema_revision,'fixture-multi-edge','e',1,event_ordinal,'synthetic-batch:1',current_timestamp() FROM {F}.stage_q2;
 IF (SELECT /*+ BROADCAST(s,j) */ count(*)<>200000 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>200000 OR count_if({post})<>0 FROM {F}.stage_q2 s JOIN {F}.edge_current o ON {identity} LEFT JOIN {F}.property_journal j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-multi-edge' AND j.source_epoch='e' AND j.source_position=1) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='composite post/journal mismatch'; END IF;
 IF (SELECT count(*) FROM {F}.property_journal WHERE source_feed='fixture-multi-edge' AND source_epoch='e' AND source_position=1)<>200000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='composite journal cardinality'; END IF;
 INSERT INTO {F}.receipt VALUES (1,current_timestamp()); END"""
 c.sql('atomic-apply',statement)
 versions={t:int(c.sql('version-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in ['edge_current','property_journal']}
 graph_versions={'client_dev.ashlar_composite_20261005_p1r1.object_current':3,F+'.edge_current':versions['edge_current'],F+'.adjacency':0,F+'.property_journal':versions['property_journal']}
 c.sql('publish',f"INSERT INTO {F}.publication_manifest VALUES ('q2-1','ashlar-delta/0.1-spike','{json.dumps(graph_versions,separators=(',',':'))}','{{\"fixture-multi\":{{\"epoch\":\"e\",\"position\":1}},\"fixture-multi-edge\":{{\"epoch\":\"e\",\"position\":1}}}}','{{\"pilot:0\":\"r1\",\"pilot:1\":\"r1\",\"pilot:2\":\"r1\",\"pilot:3\":\"r1\",\"pilot:4\":\"r1\"}}','{{\"transaction_checks\":\"passed\",\"changed_objects\":200000}}',current_timestamp())")
 ended=time.time();result=dict(state='published',before_version=before,versions=versions,graph_versions=graph_versions,processing_s=ended-begun,oldest_freshness_s=ended-t0,newest_freshness_s=ended-ready)
 (out/'summary.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
finally:
 stop.set();counts=[f.result() for f in futures];pool.shutdown(wait=True)
 for r in readers:r.close();c.records.extend(r.records)
 (out/'all-statements.json').write_text(json.dumps(c.records,indent=2)+'\n')
v=versions['edge_current'];jv=versions['property_journal']
assert c.sql('changed-exact',f"SELECT count(*),count_if({post}) FROM {F}.stage_q2 s LEFT JOIN {F}.edge_current VERSION AS OF {v} o ON {identity} LEFT JOIN {F}.property_journal VERSION AS OF {jv} j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-multi-edge' AND j.source_epoch='e' AND j.source_position=1")==[['200000','0']]
diff=' OR '.join(f'o.{x} IS DISTINCT FROM n.{x}' for x in cols)
assert c.sql('untouched-all-fields',f'SELECT count(*),count_if({diff}) FROM {F}.edge_current VERSION AS OF {v} o JOIN {F}.edge_current VERSION AS OF {before} n ON o.source_system=n.source_system AND o.rel_type_id=n.rel_type_id AND o.id=n.id WHERE pmod((o.id-1)*104729,1000000)>=20000')==[['9800000','0']]
assert c.sql('journal-origin-uniqueness',f'SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {F}.property_journal VERSION AS OF {jv}')==[['200000','200000']]
assert c.sql('canonical-keys',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {F}.edge_current VERSION AS OF {v}')==[['10000000','10000000']]
c.sql('after-detail',f'DESCRIBE DETAIL {F}.edge_current');c.sql('mutation-history',f'DESCRIBE HISTORY {F}.edge_current')
(out/'all-statements.json').write_text(json.dumps(c.records,indent=2)+'\n');c.history();c.close();result.update(state='completed',old_vector_reads=counts,preservation='all changed canonical/journal rows; 9.8M untouched all 17 fields; 10M typed edge identities; 200k unique journal origins; all old-vector reader carriers exact',scope='one 200k/20s multi-domain fixture batch; two paced readers; no sustained or billion-scale/production feed/recovery admission');(out/'summary.json').write_text(json.dumps(result,indent=2)+'\n');print('Composite publication/read controls completed',flush=True)

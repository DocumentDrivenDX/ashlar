"""Full canonical-edge property update; endpoints immutable in this scoped fixture."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
BASE=Path(__file__).resolve().parent;schema='ashlar_edge_ingest_20261005_l1';F='client_dev.'+schema;out=BASE/'out/native'/schema;c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar synthetic edge property publication; fixed typed endpoints'");c.sql('environment','SELECT current_version()')
ddl='\n'.join(x for x in (BASE/'sql/delta-candidate.sql').read_text().splitlines() if not x.lstrip().startswith('--')).split(';')
for name,index in [('edge_current',1),('property_journal',2),('publication_manifest',4)]:
 text=ddl[index].strip().replace('CREATE TABLE '+name,'CREATE TABLE '+F+'.'+name)
 if name=='edge_current':text=text.replace('CLUSTER BY (rel_type_id, source_id, target_id)','CLUSTER BY (source_system, rel_type_id, id)')
 if name!='publication_manifest':
  assert text.endswith(')');text=text[:-1]+",'delta.feature.catalogManaged'='supported','delta.targetFileSize'='"+('16777216' if name=='edge_current' else '134217728')+"')"
 c.sql('ddl-'+name,text)
c.sql('seed',f'INSERT INTO {F}.edge_current SELECT * FROM client_dev.ashlar_edges_20261005_j1.edge_identity')
c.sql('adjacency',f"CREATE TABLE {F}.adjacency USING DELTA CLUSTER BY (source_system,rel_type_id,source_type,source_id) AS SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id FROM {F}.edge_current")
c.sql('receipt',f"CREATE TABLE {F}.receipt (batch BIGINT,applied_at TIMESTAMP) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
payload='concat('+','.join(f"sha2(concat(cast(id AS STRING),':edge-update:{i}'),256)" for i in range(32))+')'
c.sql('producer',f"CREATE TABLE {F}.producer USING DELTA AS SELECT *,to_json(named_struct('201',{payload})) new_props FROM {F}.edge_current WHERE id<=200000")
adj_version=int(c.sql('adj-version',f'DESCRIBE HISTORY {F}.adjacency LIMIT 1')[0][0])
t0=time.time();ready=t0+20;time.sleep(max(0,ready-time.time()));begin=time.time()
c.sql('stage',f'CREATE TABLE {F}.stage USING DELTA AS SELECT * FROM {F}.producer')
old="to_json(array(get_json_object(props_json,'$.201')))";new="to_json(array(get_json_object(new_props,'$.201')))"
c.sql('atomic-apply',f"""BEGIN ATOMIC
 IF (SELECT count(*) FROM {F}.stage s JOIN {F}.edge_current e ON s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id WHERE e.id<=200000 AND (e.props_json IS DISTINCT FROM s.props_json OR e.entity_version<>s.entity_version))<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='edge prior mismatch'; END IF;
 MERGE INTO {F}.edge_current e USING {F}.stage s ON e.source_system=s.source_system AND e.rel_type_id=s.rel_type_id AND e.id=s.id AND e.id<=200000 WHEN MATCHED THEN UPDATE SET e.props_json=s.new_props,e.entity_version=1,e.source_position=1,e.published_at=current_timestamp();
 INSERT INTO {F}.property_journal SELECT source_system,'edge',rel_type_id,id,201,1,'update',true,substring({old},2,length({old})-2),true,substring({new},2,length({new})-2),schema_revision,source_feed,source_epoch,1,id,'synthetic-edge-batch:1',current_timestamp() FROM {F}.stage;
 IF (SELECT count(*) FROM {F}.stage s JOIN {F}.edge_current e ON s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id WHERE e.id<=200000)<>200000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='edge cardinality'; END IF;
 IF (SELECT count(*) FROM {F}.stage s JOIN {F}.edge_current e ON s.id=e.id WHERE e.id<=200000 AND (e.props_json IS DISTINCT FROM s.new_props OR e.retained_json IS DISTINCT FROM s.retained_json OR e.source_type<>s.source_type OR e.source_id<>s.source_id OR e.target_type<>s.target_type OR e.target_id<>s.target_id OR e.entity_version<>1))<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='edge carrier/endpoints'; END IF;
 IF (SELECT count(*) FROM {F}.property_journal)<>200000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='edge journal count'; END IF;
 IF (SELECT count(*) FROM {F}.property_journal j JOIN {F}.stage s ON j.id=s.id WHERE from_json(concat('[',j.old_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.props_json,'$.201') OR from_json(concat('[',j.new_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.new_props,'$.201'))<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='edge journal values'; END IF;
 INSERT INTO {F}.receipt VALUES (1,current_timestamp()); END""")
versions={'adjacency':adj_version}
for name in ['edge_current','property_journal']:versions[name]=int(c.sql('version-'+name,f'DESCRIBE HISTORY {F}.{name} LIMIT 1')[0][0])
v=json.dumps(versions,separators=(',',':'))
c.sql('publish',f"INSERT INTO {F}.publication_manifest VALUES ('edge-batch-1','ashlar-delta/0.1-spike','{v}','{{\"S\":{{\"epoch\":\"e\",\"position\":1}}}}','{{\"pilot\":\"r1\"}}','{{\"transaction_checks\":\"passed\",\"changed_edges\":200000,\"endpoints\":\"fixed\"}}',current_timestamp())")
end=time.time();summary={'state':'published','entities':200000,'accumulation_s':20,'modeled_entities_s':10000,'start_epoch':t0,'ready_epoch':ready,'processing_s':end-begin,'oldest_freshness_s':end-t0,'newest_freshness_s':end-ready,'versions':versions,'scope':'single synthetic edge property batch; no endpoint mutation, adjacency maintenance, steady stream, concurrent readers, replay/deletes, recovery or billion-edge admission'};(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
assert c.sql('hydrate',f"SELECT count(*),count_if(e.props_json IS DISTINCT FROM s.new_props OR e.retained_json IS DISTINCT FROM s.retained_json OR e.source_type<>a.source_type OR e.source_id<>a.source_id OR e.target_type<>a.target_type OR e.target_id<>a.target_id OR e.entity_version<>1) FROM {F}.edge_current VERSION AS OF {versions['edge_current']} e JOIN {F}.adjacency VERSION AS OF {adj_version} a ON e.source_system=a.source_system AND e.rel_type_id=a.rel_type_id AND e.id=a.id JOIN {F}.stage s ON e.id=s.id")==[['200000','0']]
assert c.sql('journal-unique',f"SELECT count(*),count(DISTINCT named_struct('f',source_feed,'e',source_epoch,'p',source_position,'o',event_ordinal)) FROM {F}.property_journal")==[['200000','200000']]
c.history();c.close();summary['state']='completed';(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('Edge publication and hydration completed',flush=True)

"""Small complete ordinary-manifest0.2 fixture; synthetic producer only."""
import json,hashlib
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;N='client_dev.ashlar_layout_v02_20261006_r65';out=B/'out/native/ashlar_layout_v02_fixture_20261006_r66';c=Client(out)
assert json.loads((B/'out/native/ashlar_layout_v02_ddl_20261006_r65_resume/summary.json').read_text())['state']=='passed'
def lit(x):return 'NULL' if x is None else "'"+str(x).replace("'","''")+"'"
def h(kind,t,i):return hashlib.sha256(json.dumps({'source_system':'pilot',('type_id' if kind=='node' else 'rel_type_id'):t,'id':i},separators=(',',':')).encode()).hexdigest()
tables=['object_current','edge_current','property_journal','tombstone','adjacency_forward','adjacency_reverse','degree_summary','publication_manifest','publisher_fence','apply_receipt']
for table in tables:assert c.sql('empty-'+table,f'SELECT count(*) FROM {N}.{table}')==[['0']]
props='{"101":null,"102":9007199254740993,"103":"verbatim"}';retained='{"future":{"unrecognized":[null,1,"x"]}}'
node_rows=[]
for t,i in [(1,1),(2,1),(1,-1)]:
 node_rows.append("('pilot',%d,%d,'%s','r1',1,%s,%s,NULL,'fixture-v02','e',1,current_timestamp(),'%s','r66:1')"%(t,i,'["logical",'+str(t)+','+str(i)+']',lit(props),lit(retained),h('node',t,i)))
c.sql('nodes',f'INSERT INTO {N}.object_current VALUES '+','.join(node_rows))
edges=[]
for i,tt in [(1,2),(2,2),(3,1)]:edges.append("('pilot',7,%d,1,1,%d,1,'r1',1,'{}',%s,NULL,'fixture-v02','e',1,current_timestamp(),'%s','r66:1')"%(i,tt,lit(retained),h('edge',7,i)))
c.sql('edges',f'INSERT INTO {N}.edge_current VALUES '+','.join(edges))
for table in ['adjacency_forward','adjacency_reverse']:c.sql('populate-'+table,f'INSERT INTO {N}.{table} SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,1 FROM {N}.edge_current')
c.sql('degrees',f"INSERT INTO {N}.degree_summary VALUES ('pilot',1,1,7,'out',3),('pilot',1,1,7,'in',1),('pilot',2,1,7,'in',2),('pilot',2,1,7,'out',0),('pilot',1,-1,7,'in',0),('pilot',1,-1,7,'out',0)")
# Lifecycle source evidence only: no inferred property-feed/entity-version semantics.
c.sql('node-history',f"INSERT INTO {N}.property_journal SELECT source_system,'node',type_id,id,NULL,1,'create',false,NULL,true,props_json,schema_revision,source_feed,source_epoch,source_position,row_number() OVER (ORDER BY type_id,id),'opaque-source-time',current_timestamp(),'r66:1' FROM {N}.object_current")
c.sql('edge-history',f"INSERT INTO {N}.property_journal SELECT source_system,'edge',rel_type_id,id,NULL,1,'create',false,NULL,true,props_json,schema_revision,source_feed,source_epoch,source_position,3+id,'opaque-source-time',current_timestamp(),'r66:1' FROM {N}.edge_current")
for table,typ in [('object_current','type_id'),('edge_current','rel_type_id')]:
 assert c.sql('unique-'+table,f'SELECT count(*),count(DISTINCT struct(source_system,{typ},id)),count_if(lookup_hash IS DISTINCT FROM sha2(to_json(named_struct(\'source_system\',source_system,\'{typ}\',{typ},\'id\',id)),256)) FROM {N}.{table}')==[['3','3','0']]
assert c.sql('endpoints',f'SELECT count(*) FROM {N}.edge_current e LEFT ANTI JOIN {N}.object_current n ON e.source_system=n.source_system AND e.target_type=n.type_id AND e.target_id=n.id')==[['0']]
assert c.sql('source-endpoints',f'SELECT count(*) FROM {N}.edge_current e LEFT ANTI JOIN {N}.object_current n ON e.source_system=n.source_system AND e.source_type=n.type_id AND e.source_id=n.id')==[['0']]
assert c.sql('parallel',f'SELECT count(*) FROM {N}.adjacency_forward WHERE target_type=2')==[['2']]
assert c.sql('journal-origins',f'SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {N}.property_journal')==[['6','6']]
for direction,typ,ident in [('out','source_type','source_id'),('in','target_type','target_id')]:
 assert c.sql('degree-'+direction,f"WITH actual AS (SELECT source_system,{typ} type_id,{ident} id,rel_type_id,count(*) cnt FROM {N}.adjacency_forward GROUP BY ALL) SELECT count_if(d.direction NOT IN ('out','in') OR d.edge_count<0 OR d.edge_count IS DISTINCT FROM coalesce(a.cnt,0)) FROM {N}.degree_summary d LEFT JOIN actual a ON d.source_system=a.source_system AND d.type_id=a.type_id AND d.id=a.id AND d.rel_type_id=a.rel_type_id WHERE d.direction='{direction}'")==[['0']]
# Publisher validation refusal controls do not mutate any table.
assert c.sql('bad-endpoint-refusal',f"SELECT count(*) FROM (SELECT 'pilot' source_system,2 type_id,999 id) e LEFT ANTI JOIN {N}.object_current n USING (source_system,type_id,id)")==[['1']]
assert c.sql('bad-degree-refusal',"SELECT count_if(direction NOT IN ('out','in') OR edge_count<0) FROM VALUES ('side',1),('out',-1) d(direction,edge_count)")==[['2']]
vector={N+'.'+t:int(c.sql('version-'+t,f'DESCRIBE HISTORY {N}.{t} LIMIT 1')[0][0]) for t in tables[:7]}
progress={'fixture-v02':{'epoch':'e','position':1}};revisions={'pilot':'r1'}
c.sql('fence',f"INSERT INTO {N}.publisher_fence VALUES ('fixture-v02',1,'fixture-publisher','r66:1',1)")
c.sql('receipt',f"INSERT INTO {N}.apply_receipt VALUES ('r66:1','fixture-v02',1,1,'inline-r66','synthetic-stage-digest',6,{lit(json.dumps(progress))},{lit(json.dumps(revisions))},current_timestamp())")
c.sql('manifest',f"INSERT INTO {N}.publication_manifest VALUES ('r66:publication1','ashlar-delta/0.2',{lit(json.dumps(vector))},{lit(json.dumps(progress))},{lit(json.dumps(revisions))},'{{\"native_small_fixture\":\"passed\",\"degree_coverage\":\"all fixture nodes/rel7/in+out\"}}',current_timestamp())")
c.sql('clear-fence',f"UPDATE {N}.publisher_fence SET pending_batch_id=NULL,sequence=2 WHERE stream='fixture-v02' AND pending_batch_id='r66:1'")
assert c.sql('fence-check',f'SELECT pending_batch_id,sequence FROM {N}.publisher_fence')==[[None,'2']]
old=c.sql('pinned-before',f"SELECT type_id,id,props_json,retained_json FROM {N}.object_current VERSION AS OF {vector[N+'.object_current']} ORDER BY type_id,id")
assert len(old)==3 and all(r[2:]==[props,retained] for r in old)
c.sql('unpublished-write',f"UPDATE {N}.object_current SET props_json='{{\"101\":\"unpublished\"}}' WHERE type_id=1 AND id=1")
assert c.sql('pinned-after',f"SELECT type_id,id,props_json,retained_json FROM {N}.object_current VERSION AS OF {vector[N+'.object_current']} ORDER BY type_id,id")==old
assert c.sql('manifest-readback',f'SELECT table_versions_json,source_progress_json,schema_revisions_json FROM {N}.publication_manifest')==[[json.dumps(vector),json.dumps(progress),json.dumps(revisions)]]
c.history();(out/'summary.json').write_text(json.dumps({'state':'passed','versions':vector,'nodes':3,'edges':3,'scope':'small synthetic whole-entity lifecycle evidence; exact JSON text, typed repeated IDs,parallel edges,self-loop,isolate,hash checks,degree coverage,6 unique origins,ordinary immutable manifest/pinned read surviving later unmanifested write; no cross-table atomicity,producer adapter,recovery race,performance,scale or external-engine execution claim'},indent=2));print('Complete0.2 native fixture passed',flush=True)

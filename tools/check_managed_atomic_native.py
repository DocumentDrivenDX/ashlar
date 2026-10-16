"""Actual bounded managed transaction prerequisites on the approved test warehouse.

Only freshly named fixture tables are written. This witness does not grant SDK
mutation support, action authority, replay custody or temporal qualification.
"""
from pathlib import Path
import argparse,json,subprocess,time,uuid
from databricks_transport import sql_result
from weft_native_profile import ENGINE_SQL,ANSI_SQL

parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--warehouse',required=True);parser.add_argument('--profile',required=True);args=parser.parse_args()
out=args.output.absolute();out.mkdir(parents=True,exist_ok=False)
def cli(*command):
 result=subprocess.run(['databricks',*command,'--profile',args.profile],capture_output=True,text=True,check=True)
 return json.loads(result.stdout)if result.stdout.strip()else None
sequence=0;checks=[];status='fail';schema='client_dev.umf_sdk_atomic_'+uuid.uuid4().hex[:12]
def sql(statement,*,allow_failure=False):
 global sequence
 sequence+=1
 request={'warehouse_id':args.warehouse,'statement':statement,'wait_timeout':'50s','on_wait_timeout':'CANCEL','row_limit':100}
 response=cli('api','post','/api/2.0/sql/statements','--json',json.dumps(request));until=time.monotonic()+120
 while response['status']['state']in('PENDING','RUNNING'):
  if time.monotonic()>=until:
   cli('api','post','/api/2.0/sql/statements/'+response['statement_id']+'/cancel');raise TimeoutError('native statement deadline')
  time.sleep(1);response=cli('api','get','/api/2.0/sql/statements/'+response['statement_id'])
 (out/f'statement-{sequence:03d}.json').write_text(json.dumps({'request':request,'response':response},indent=2)+'\n')
 return response if allow_failure else sql_result(response)
try:
 warehouse=cli('warehouses','get',args.warehouse,'-o','json')
 assert warehouse['name']=='umf-typed-sdk-qualification-20261016'and warehouse['cluster_size']=='2X-Small'and warehouse['min_num_clusters']==warehouse['max_num_clusters']==1 and warehouse['auto_stop_mins']==5
 (out/'warehouse.json').write_text(json.dumps(warehouse,indent=2)+'\n')
 engine=sql(ENGINE_SQL);ansi=sql(ANSI_SQL)
 assert json.loads(engine.rows[0]['__weft_warehouse'])=={'dbr_version':None,'dbsql_version':'2026.39','u_build_hash':'55233303686df68c7c3acb934c0a573947b3fae8','r_build_hash':'ec864f9e1ceaa3eb7afec091ca45053cf08a0afa'}
 assert list(ansi.rows)==[{'key':'ANSI_MODE','value':'true'}]
 sql('CREATE SCHEMA '+schema)
 data=schema+'.objects';receipt=schema+'.receipts'
 sql('CREATE TABLE '+data+" (id BIGINT, value BIGINT) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.isolationLevel'='Serializable')")
 sql('CREATE TABLE '+receipt+" (invocation STRING, changed BIGINT, observed BIGINT) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.isolationLevel'='Serializable')")
 for table in (data,receipt):
  detail=sql('DESCRIBE DETAIL '+table).rows[0]
  features=detail['tableFeatures'];features=json.loads(features)if isinstance(features,str)else features
  assert 'catalogManaged'in features
 checks.append('fresh UC-managed fixture tables expose native catalogManaged coordination')
 sql('INSERT INTO '+data+' VALUES (1,10),(2,20)')
 sql('BEGIN ATOMIC UPDATE '+data+' SET value=value+1 WHERE id=1; INSERT INTO '+receipt+" SELECT 'first',count(*),sum(value) FROM "+data+'; END;')
 assert list(sql('SELECT id,value FROM '+data+' ORDER BY id').rows)==[{'id':'1','value':'11'},{'id':'2','value':'20'}]
 assert list(sql('SELECT invocation,changed,observed FROM '+receipt).rows)==[{'invocation':'first','changed':'2','observed':'31'}]
 checks.append('one native atomic block commits data and a receipt observing its own writes across two tables')
 failure=sql('BEGIN ATOMIC UPDATE '+data+' SET value=999 WHERE id=1; INSERT INTO '+receipt+" VALUES ('rollback',1,999); SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='owned prerequisite rollback'; END;",allow_failure=True)
 assert failure['status']['state']=='FAILED'
 assert list(sql('SELECT id,value FROM '+data+' ORDER BY id').rows)==[{'id':'1','value':'11'},{'id':'2','value':'20'}]
 assert list(sql('SELECT invocation FROM '+receipt).rows)==[{'invocation':'first'}]
 checks.append('native signaled failure rolls back both changed data and receipt')
 sql('BEGIN ATOMIC UPDATE '+data+' SET value=value WHERE id=1; INSERT INTO '+receipt+" SELECT 'noop',0,sum(value) FROM "+data+'; END;')
 assert list(sql('SELECT invocation,changed,observed FROM '+receipt+" WHERE invocation='noop'").rows)==[{'invocation':'noop','changed':'0','observed':'31'}]
 checks.append('native semantic no-op can retain a distinct durable transactional receipt')
 for table in (data,receipt):sql('DESCRIBE HISTORY '+table+' LIMIT 10')
 status='pass'
finally:
 cleanup=[]
 try:
  cli('warehouses','stop',args.warehouse,'--no-wait')
  until=time.monotonic()+300
  while True:
   observed=cli('warehouses','get',args.warehouse,'-o','json')
   if observed['state']=='STOPPED':break
   if time.monotonic()>=until:raise TimeoutError('warehouse stop deadline')
   if observed['state']=='RUNNING':cli('warehouses','stop',args.warehouse,'--no-wait')
   time.sleep(2)
  (out/'warehouse-stopped.json').write_text(json.dumps(observed,indent=2)+'\n')
 except Exception as error:cleanup.append(type(error).__name__)
 (out/'verification.json').write_text(json.dumps({'status':status if not cleanup else 'cleanup-failed','checks':checks,'schema':schema,'temporal':'HELD','qualification':'Native managed transaction prerequisite only. SDK authority, original compiler program, exact commit receipt, conflict, response-loss and backend qualification gates remain required.','cleanupErrors':cleanup},indent=2)+'\n')
 if cleanup:raise RuntimeError('Native cleanup failed')
print(json.dumps({'status':status,'checks':len(checks),'output':str(out)}))

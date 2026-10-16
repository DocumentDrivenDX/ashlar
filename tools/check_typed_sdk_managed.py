"""Small actual UC/DBSQL publication -> native PostgreSQL pins -> typed read.

Explicit development carrier IDs are not accepted Truss IDs. The warehouse is
dedicated and approved; the schema is owned solely by this bounded test run.
All native SQL responses are retained. Historical qualification is unchanged.
"""
from pathlib import Path
from contextlib import contextmanager
from datetime import timedelta
import argparse,hashlib,json,subprocess,sys,time,uuid
import psycopg,weft
from ashlar.access import AshlarTarget
from ashlar.native import NativeBackend
from ashlar.pins import PinVector,PostgresPins
from ashlar.publication import ResolutionError
from ashlar_host.postgres import Session
from umf_access.engine_catalog import EngineRead
from umf_access.types import StrictModel
from databricks_transport import sql_result
from weft_native_profile import ENGINE_SQL,ANSI_SQL

parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--generated',type=Path,required=True);parser.add_argument('--model-request',type=Path,required=True);parser.add_argument('--warehouse',required=True);parser.add_argument('--profile',required=True);args=parser.parse_args()
out=args.output.absolute();out.mkdir(parents=True,exist_ok=False);root=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(args.generated.absolute()));from native_sales.models import Order
class Parameters(StrictModel):status:str
def cli(*command):
 p=subprocess.run(['databricks',*command,'--profile',args.profile],capture_output=True,text=True,check=True)
 return json.loads(p.stdout)if p.stdout.strip()else None
warehouse=cli('warehouses','get',args.warehouse,'-o','json')
assert warehouse['name']=='umf-typed-sdk-qualification-20261016'and warehouse['cluster_size']=='2X-Small'and warehouse['min_num_clusters']==warehouse['max_num_clusters']==1 and warehouse['auto_stop_mins']==5
(out/'warehouse.json').write_text(json.dumps(warehouse,indent=2))
sequence=0;operation_deadline={'until':None}
def sql(statement,parameters=None):
 global sequence
 sequence+=1
 remaining=50 if operation_deadline['until']is None else min(50,int(operation_deadline['until']-time.monotonic()))
 if remaining<5:raise TimeoutError('original managed whole-operation deadline')
 request={'warehouse_id':args.warehouse,'statement':statement,'wait_timeout':str(remaining)+'s','on_wait_timeout':'CANCEL','row_limit':20}
 if parameters:request['parameters']=[{'name':k,'type':'STRING','value':v}for k,v in parameters.items()]
 response=cli('api','post','/api/2.0/sql/statements','--json',json.dumps(request));until=time.monotonic()+120
 while response['status']['state']in('PENDING','RUNNING'):
  if time.monotonic()>=until:
   cli('api','post','/api/2.0/sql/statements/'+response['statement_id']+'/cancel');raise TimeoutError('native managed statement deadline')
  time.sleep(1);response=cli('api','get','/api/2.0/sql/statements/'+response['statement_id'])
 (out/f'statement-{sequence:03d}.json').write_text(json.dumps({'request':request,'response':response},indent=2)+'\n')
 return sql_result(response)
pgbin=Path('/opt/homebrew/opt/postgresql@18/bin');pg=out/'postgres';sock=out/'socket';sock.mkdir();started=False;checks=[];status='fail'
schema='client_dev.umf_typed_sdk_'+uuid.uuid4().hex[:12]
def local(*command):
 p=subprocess.run(list(map(str,command)),capture_output=True,text=True,check=True)
 with(out/'lifecycle.log').open('a')as f:f.write(p.stdout+p.stderr)
try:
 local(pgbin/'initdb','-D',pg,'-A','trust','-U','sdk_test','--no-sync');local(pgbin/'pg_ctl','-D',pg,'-l',out/'postgres.log','-o',f"-F -k {sock} -c listen_addresses='' -p 55496",'-w','start');started=True
 dsn=f'host={sock} port=55496 dbname=postgres user=sdk_test'
 with psycopg.connect(dsn,autocommit=True)as admin:
  for name in ('01-postgresql.sql','03-read-guard.sql','04-table-guard.sql','05-maintenance-quarantine.sql','06-availability-guard.sql'):admin.execute((root/'sql/ashlar-pins'/name).read_text())
  admin.execute('CREATE ROLE sdk_person LOGIN;GRANT ashlar_pin_reader,ashlar_pin_writer TO sdk_person')
 model=json.loads(args.model_request.read_bytes())['binding']['modelJson'];source_sha=hashlib.sha256(model.encode()).hexdigest();document=json.loads(model)
 engine=json.loads(sql(ENGINE_SQL).rows[0]['__weft_warehouse']);ansi=sql(ANSI_SQL)
 assert engine=={'dbr_version':None,'dbsql_version':'2026.39','u_build_hash':'55233303686df68c7c3acb934c0a573947b3fae8','r_build_hash':'ec864f9e1ceaa3eb7afec091ca45053cf08a0afa'}and list(ansi.rows)==[{'key':'ANSI_MODE','value':'true'}]
 sql('CREATE SCHEMA '+schema);table=schema+'.object_current';manifest_table=schema+'.manifest'
 sql('CREATE TABLE '+table+' (source_system STRING,type_id BIGINT,id BIGINT,schema_revision STRING,props_json STRING) USING DELTA')
 sql('INSERT INTO '+table+' VALUES (:source,17,1,:revision,:props)',{'source':'typed-sdk-development','revision':source_sha,'props':'{"1":"a","2":"draft"}'})
 detail=dict(sql('DESCRIBE DETAIL '+table).rows[0]);version=int(sql('DESCRIBE HISTORY '+table+' LIMIT 1').rows[0]['version'])
 publication_id=str(uuid.uuid4());row={'publication_id':publication_id,'profile_version':'ashlar-delta/0.3','table_versions_json':json.dumps({table:version}),'source_progress_json':'{"typed-sdk-development":{"offset":"1"}}','schema_revisions_json':json.dumps({'typed-sdk-development':source_sha}),'validation_report_json':json.dumps({'complete':True,'sourceSha256':source_sha,'scope':'explicit development carrier mapping; not accepted Truss identities'})}
 sql('CREATE TABLE '+manifest_table+' (publication_id STRING,profile_version STRING,table_versions_json STRING,source_progress_json STRING,schema_revisions_json STRING,validation_report_json STRING,recorded_at TIMESTAMP) USING DELTA')
 sql('INSERT INTO '+manifest_table+' VALUES (:publication_id,:profile_version,:table_versions_json,:source_progress_json,:schema_revisions_json,:validation_report_json,current_timestamp())',row)
 manifest_detail=dict(sql('DESCRIBE DETAIL '+manifest_table).rows[0]);raw_descriptor=dict(row,recorded_at=sql('SELECT cast(unix_micros(recorded_at) AS STRING) AS value FROM '+manifest_table).rows[0]['value'])
 custody=hashlib.sha256(json.dumps(raw_descriptor,sort_keys=True,separators=(',',':')).encode()).hexdigest();vector=PinVector('typed-sdk-owned-managed-development','manifest',publication_id,custody,{table:(detail['id'],version)})
 context=object();active={'pins':False,'deny':False}
 class PinExecutor:
  @contextmanager
  def transaction(self,supplied):
   assert supplied is context;params=psycopg.conninfo.conninfo_to_dict(dsn);params['user']='sdk_person'
   with psycopg.connect(**params)as connection:
    connection.execute("SET LOCAL statement_timeout='10000ms'");session=Session(connection)
    try:yield session
    finally:session.active=False
 class PinPolicy:
  def admit_registration(self,original,supplied):assert original==vector and supplied is context
  def authorize_read(self,original,supplied):
   assert original==vector and supplied is context
   if active['deny']:raise PermissionError('original development reader revoked')
 pins=PostgresPins(PinExecutor(),PinPolicy());pins.register(vector,context=context)
 class HeldPins:
  @contextmanager
  def hold(self,original,*,context):
   with pins.hold(original,context=context):
    active['pins']=True
    try:yield original
    finally:active['pins']=False
 class Executor:
  def query(self,statement,parameters):assert active['pins'];return sql(statement,parameters)
 class NativePolicy:
  def authorize(self,supplied,pid,tables):
   if supplied is not context or not active['pins']or active['deny']or pid!=publication_id or set(tables)!={table}:raise PermissionError('original managed publication refused')
  def validate_descriptor(self,descriptor,supplied):assert supplied is context and dict(descriptor.raw)==raw_descriptor
  def validate_snapshot(self,name,identity,retained,columns):assert active['pins']and(name,identity,retained)==(table,detail['id'],version)
 executor=Executor();backend=NativeBackend(executor,NativePolicy(),manifest_table,manifest_detail['id'],{table:(('source_system','STRING'),('type_id','BIGINT'),('id','BIGINT'),('schema_revision','STRING'),('props_json','STRING'))})
 pin={'documentId':document['id'],'revision':source_sha,'sha256':source_sha,'umfVersion':'0.8.0'}
 def logical(element):return {'documentId':document['id'],'revision':source_sha,'module':'sales','element':element}
 binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':'ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e','modelPins':[pin],'publication':{'id':publication_id,'manifestUuid':manifest_detail['id'],'tables':[{'name':table.split('.'),'uuid':detail['id'],'version':version}]},'records':[{'logical':logical('order'),'table':0,'kind':'object','sourceSystem':'typed-sdk-development','typeId':'17','schemaRevision':source_sha,'properties':[{'logical':logical(field),'home':{'kind':'props','propertyId':str(number)}}for number,field in((1,'id'),(2,'status'))]}]}
 binding_json=json.dumps(binding,sort_keys=True,separators=(',',':'));binding_sha=hashlib.sha256(binding_json.encode()).hexdigest()
 request={'interfaceVersion':'weft-compile/0.2.0','dialect':'weft-sql/0.2.0','sql':'SELECT "r"."id" AS "id", "r"."status" AS "status" FROM "sales"."order" AS "r" WHERE "r"."status" = :status LIMIT 10','parameters':{'status':{'family':'string','value':'draft'}},'modules':[{'documentJson':model,'pin':pin,'selectedModuleIds':['sales']}],'target':{'backendId':'ashlar.databricks','backendVersion':'0.1.0-managed-20261016-candidate','targetProfile':'dbsql2026.39-build-20261016-candidate','bindingJson':binding_json,'bindingSha256':binding_sha},'options':{'allowCandidate':True}}
 entry=EngineRead.build(weft,request,parameters=Parameters,result=Order,method='ashlar-dbsql-20261016');(out/'original-engine-request.json').write_text(json.dumps(request));(out/'engine-catalog.json').write_text(entry.artifact_json)
 class ReadPolicy:
  def bind_descriptor(self,descriptor,original,supplied):assert active['pins']and original==vector;NativePolicy().validate_descriptor(descriptor,supplied)
  def admit_artifact(self,current,artifact,descriptor,supplied):assert active['pins']and current['modules']==request['modules']and artifact['modelPins']==[pin]and artifact['bindingSha256']==binding_sha
  def verify_native_profile(self,profile,supplied):
   assert active['pins']and supplied is context and profile['targetProfile']=='dbsql2026.39-build-20261016-candidate'
   if json.loads(sql(ENGINE_SQL).rows[0]['__weft_warehouse'])!=engine or list(sql(ANSI_SQL).rows)!=[{'key':'ANSI_MODE','value':'true'}]:raise ResolutionError('original exact managed profile drift')
  def authorize_query(self,current,descriptor,supplied):NativePolicy().authorize(supplied,publication_id,(table,))
  def admit_record_integrity(self,check,artifact,supplied):assert check['record']==logical('order')and supplied is context
  def decode_result(self,column,value,current,supplied):
   assert active['pins']and supplied is context
   if column['representation']['kind']=='scalar'and type(value)is str:return value
   item=json.loads(value)
   if set(item)=={'state','value'}and item['state']=='value'and type(item['value'])is str:return item
   raise ResolutionError('exact selected managed string refused')
  def authorize_result(self,rows,descriptor,supplied):NativePolicy().authorize(supplied,publication_id,(table,))
 @contextmanager
 def deadline(budget):
  assert operation_deadline['until']is None
  operation_deadline['until']=time.monotonic()+budget.total_seconds()
  try:yield
  finally:operation_deadline['until']=None
 target=AshlarTarget(compiler=weft,compiler_method='ashlar-dbsql-20261016',executor=executor,backend=backend,pins=HeldPins(),vector=vector,policy=ReadPolicy(),context=context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={'typed-sdk-development':[source_sha]},binding_sha256=binding_sha,platform=(('dbsql','2026.39'),),default_timeout=timedelta(seconds=120),deadline=deadline,catalog_entries=(entry,))
 assert target.read(entry,Parameters(status='draft'))==[Order(id='a',status='draft')];checks.append('actual UC UUID/version publication and native PostgreSQL held pins release generated typed rows')
 (args.generated/'managed_declarations.py').write_text('from umf_access import query,Param\nfrom native_sales.models import Order\n@query\ndef selected(status:Param[str]):return Order.objects.filter(Order.c.status==status).limit(10).all()\n')
 config={'package':'native_sales','modules':['managed_declarations'],'targets':[{'id':'owned','kind':'ashlar','backendId':'ashlar.databricks','backendVersion':'0.1.0-managed-20261016-candidate','targetProfile':'dbsql2026.39-build-20261016-candidate','binding':json.loads(args.model_request.read_bytes())['binding'],'engineCompiler':'ashlar-dbsql-20261016','engineRequests':{'selected:0':request},'platform':[{'name':'dbsql','versions':['2026.39']}]}]}
 (args.generated/'managed-config.json').write_text(json.dumps(config))
 for extra in ([],['--check']):
  process=subprocess.run([sys.executable,'-B','-m','umf_access.build.catalog','catalog','--root',str(args.generated),'--config',str(args.generated/'managed-config.json'),*extra],capture_output=True,text=True)
  assert process.returncode==0,process.stdout+process.stderr
 from native_sales import queries
 assert queries.selected(target,status='draft')==[Order(id='a',status='draft')];checks.append('real CLI catalog/check emits generated query executing original managed engine program')
 assert target.read(entry,Parameters(status="draft' OR true--"))==[];checks.append('actual native named parameter binding')
 sql('UPDATE '+table+' SET props_json=:props WHERE id=1',{'props':'{"1":"a","2":"changed"}'})
 assert target.read(entry,Parameters(status='draft'))==[Order(id='a',status='draft')];checks.append('actual independent managed mutation preserves original immutable publication read')
 active['deny']=True
 try:target.read(entry,Parameters(status='draft'))
 except PermissionError:checks.append('native original publication policy revocation refuses release')
 else:raise AssertionError('revoked managed reader released data')
 status='pass'
finally:
 if started:local(pgbin/'pg_ctl','-D',pg,'-m','fast','-w','stop')
 cli('warehouses','stop',args.warehouse,'--no-wait')
 (out/'verification.json').write_text(json.dumps({'status':status,'checks':checks,'schema':schema,'warehouse':args.warehouse,'stopRequested':True,'ownedPostgresStopped':not(pg/'postmaster.pid').exists(),'temporal':'HELD','scope':'actual managed scalar STRING publication and typed reads with local native pins/development policy; no accepted Truss identities, independent UC actor enforcement, managed mutation or broader semantic qualification'},indent=2)+'\n')
print(json.dumps({'status':status,'checks':len(checks),'output':str(out)}))

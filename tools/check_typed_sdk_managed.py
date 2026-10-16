"""Small actual UC/DBSQL publication -> native PostgreSQL pins -> typed read.

Explicit development carrier IDs are not accepted Truss IDs. The warehouse is
dedicated and approved; the schema is owned solely by this bounded test run.
All native SQL responses are retained. Historical qualification is unchanged.
"""
from pathlib import Path
from contextlib import contextmanager
from datetime import timedelta
import argparse,hashlib,json,os,subprocess,sys,time,uuid
import psycopg,weft
from ashlar.access import AshlarTarget
from ashlar.native import NativeBackend
from ashlar.pins import PinVector,PostgresPins
from ashlar.publication import ResolutionError
from ashlar_host.postgres import Session
from umf_access.engine_catalog import EngineRead
from umf_access.pages import StaleContinuation
from umf_access.types import StrictModel
from databricks_transport import sql_result
from weft_native_profile import ENGINE_SQL,ANSI_SQL

parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--generated',type=Path,required=True);parser.add_argument('--model-request',type=Path,required=True);parser.add_argument('--warehouse',required=True);parser.add_argument('--profile',required=True);parser.add_argument('--reader-principal');parser.add_argument('--reader-app-id');args=parser.parse_args()
out=args.output.absolute();out.mkdir(parents=True,exist_ok=False);root=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(args.generated.absolute()));from native_sales.models import Order
class Parameters(StrictModel):status:str
def cli(*command):
 p=subprocess.run(['databricks',*command,'--profile',args.profile],capture_output=True,text=True,check=True)
 return json.loads(p.stdout)if p.stdout.strip()else None
warehouse=cli('warehouses','get',args.warehouse,'-o','json')
assert warehouse['name']=='umf-typed-sdk-qualification-20261016'and warehouse['cluster_size']=='2X-Small'and warehouse['min_num_clusters']==warehouse['max_num_clusters']==1 and warehouse['auto_stop_mins']==5
(out/'warehouse.json').write_text(json.dumps(warehouse,indent=2))
sequence=0;operation_deadline={'until':None};credential=None;reader_identity=None
def reader_cli(*command):
 if credential is None:raise ValueError('Original temporary reader credential required')
 env={k:v for k,v in os.environ.items()if not k.startswith('DATABRICKS_')}
 env.update(DATABRICKS_HOST='https://adb-7405607548213398.18.azuredatabricks.net',DATABRICKS_CLIENT_ID=args.reader_app_id,DATABRICKS_CLIENT_SECRET=credential['secret'],DATABRICKS_AUTH_TYPE='oauth-m2m',DATABRICKS_CONFIG_FILE='/dev/null')
 result=subprocess.run(['databricks',*command],env=env,capture_output=True,text=True)
 if result.returncode:
  diagnostic=result.stderr.replace(credential['secret'],'[redacted]')
  (out/'reader-client-failure.txt').write_text(diagnostic[:2000])
  raise RuntimeError('Constrained native client command failed with exit code '+str(result.returncode)+'; see redacted reader-client-failure.txt')
 return json.loads(result.stdout)if result.stdout.strip()else None
def sql(statement,parameters=None,*,reader=False,allow_failure=False):
 global sequence
 sequence+=1
 remaining=50 if operation_deadline['until']is None else min(50,int(operation_deadline['until']-time.monotonic()))
 if remaining<5:raise TimeoutError('original managed whole-operation deadline')
 request={'warehouse_id':args.warehouse,'statement':statement,'wait_timeout':str(remaining)+'s','on_wait_timeout':'CANCEL','row_limit':20}
 if parameters:request['parameters']=[{'name':k,'type':'STRING','value':v}for k,v in parameters.items()]
 execute=reader_cli if reader and credential is not None else cli
 response=execute('api','post','/api/2.0/sql/statements','--json',json.dumps(request));until=time.monotonic()+120
 while response['status']['state']in('PENDING','RUNNING'):
  if time.monotonic()>=until:
   execute('api','post','/api/2.0/sql/statements/'+response['statement_id']+'/cancel');raise TimeoutError('native managed statement deadline')
  time.sleep(1);response=execute('api','get','/api/2.0/sql/statements/'+response['statement_id'])
 (out/f'statement-{sequence:03d}.json').write_text(json.dumps({'request':request,'response':response,'readerPrincipal':args.reader_app_id if reader else None},indent=2)+'\n')
 if allow_failure:return response
 return sql_result(response)
pgbin=Path('/opt/homebrew/opt/postgresql@18/bin');pg=out/'postgres';sock=out/'socket';sock.mkdir();started=False;checks=[];status='fail'
schema='client_dev.umf_typed_sdk_'+uuid.uuid4().hex[:12]
def local(*command):
 p=subprocess.run(list(map(str,command)),capture_output=True,text=True,check=True)
 with(out/'lifecycle.log').open('a')as f:f.write(p.stdout+p.stderr)
try:
 if args.reader_principal is not None:
  principal=cli('service-principals','get',args.reader_principal,'-o','json')
  assert principal['applicationId']==args.reader_app_id and principal['displayName']=='umf-typed-sdk-qualification-20261016-reader'and not any(g.get('display')=='admins'for g in principal.get('groups',[]))
  cli('warehouses','update-permissions',args.warehouse,'--json',json.dumps({'access_control_list':[{'service_principal_name':args.reader_app_id,'permission_level':'CAN_USE'}]}))
  credential=cli('service-principal-secrets-proxy','create',args.reader_principal,'--lifetime','3600s','-o','json')
  assert type(credential.get('secret'))is str and credential['secret']
  native_actor=sql('SELECT current_user() AS actor',reader=True).rows[0]['actor']
  assert native_actor==args.reader_app_id
  reader_identity={'workspacePrincipal':principal,'originalNativeActor':native_actor}
  (out/'reader-identity.json').write_text(json.dumps(reader_identity,indent=2)+'\n')
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
 sql('INSERT INTO '+table+' VALUES (:source,17,2,:revision,:props)',{'source':'typed-sdk-development','revision':source_sha,'props':'{"1":"b","2":"draft"}'})
 detail=dict(sql('DESCRIBE DETAIL '+table).rows[0]);version=int(sql('DESCRIBE HISTORY '+table+' LIMIT 1').rows[0]['version'])
 publication_id=str(uuid.uuid4());row={'publication_id':publication_id,'profile_version':'ashlar-delta/0.3','table_versions_json':json.dumps({table:version}),'source_progress_json':'{"typed-sdk-development":{"offset":"1"}}','schema_revisions_json':json.dumps({'typed-sdk-development':source_sha}),'validation_report_json':json.dumps({'complete':True,'sourceSha256':source_sha,'scope':'explicit development carrier mapping; not accepted Truss identities'})}
 sql('CREATE TABLE '+manifest_table+' (publication_id STRING,profile_version STRING,table_versions_json STRING,source_progress_json STRING,schema_revisions_json STRING,validation_report_json STRING,recorded_at TIMESTAMP) USING DELTA')
 sql('INSERT INTO '+manifest_table+' VALUES (:publication_id,:profile_version,:table_versions_json,:source_progress_json,:schema_revisions_json,:validation_report_json,current_timestamp())',row)
 manifest_detail=dict(sql('DESCRIBE DETAIL '+manifest_table).rows[0]);raw_descriptor=dict(row,recorded_at=sql('SELECT cast(unix_micros(recorded_at) AS STRING) AS value FROM '+manifest_table).rows[0]['value'])
 if credential is not None:
  sql('GRANT USE CATALOG ON CATALOG client_dev TO `'+args.reader_app_id+'`')
  sql('GRANT USE SCHEMA ON SCHEMA '+schema+' TO `'+args.reader_app_id+'`')
  refused=sql('SELECT props_json FROM '+table,reader=True,allow_failure=True)
  assert refused['status']['state']=='FAILED'and 'PERMISSION'in json.dumps(refused['status']).upper()
  checks.append('independent non-admin native principal cannot read fixture before SELECT grant')
  for relation in (table,manifest_table):sql('GRANT SELECT ON TABLE '+relation+' TO `'+args.reader_app_id+'`')
  observed=sql('SELECT current_user() AS actor',reader=True).rows[0]['actor']
  assert observed==args.reader_app_id
  denied_write=sql('UPDATE '+table+" SET props_json=props_json WHERE id=1",reader=True,allow_failure=True)
  assert denied_write['status']['state']=='FAILED'and 'PERMISSION'in json.dumps(denied_write['status']).upper()
  checks.append('native authenticated reader is independent and SELECT grant does not allow UPDATE')
 custody=hashlib.sha256(json.dumps(raw_descriptor,sort_keys=True,separators=(',',':')).encode()).hexdigest();vector=PinVector('typed-sdk-owned-managed-development','manifest',publication_id,custody,{table:(detail['id'],version)})
 context=object();active={'pins':False,'deny':False,'policyRevision':1}
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
  @contextmanager
  def hold_page(self,original,*,context):
   with pins.hold_page(original,context=context):
    active['pins']=True
    try:yield original
    finally:active['pins']=False
 class Executor:
  def query(self,statement,parameters):assert active['pins'];return sql(statement,parameters,reader=True)
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
   if json.loads(sql(ENGINE_SQL,reader=True).rows[0]['__weft_warehouse'])!=engine or list(sql(ANSI_SQL,reader=True).rows)!=[{'key':'ANSI_MODE','value':'true'}]:raise ResolutionError('original exact managed profile drift')
  def authorize_query(self,current,descriptor,supplied):NativePolicy().authorize(supplied,publication_id,(table,))
  def admit_record_integrity(self,check,artifact,supplied):assert check['record']==logical('order')and supplied is context
  def admit_page_integrity(self,guard,artifact,supplied):
   assert active['pins'] and supplied is context and guard['key']==artifact['logicalPlan']['pageKey']
   record=next(e for e in document['modules'][0]['elements']if e['id']=='order')
   primary=[key for key in record['keys']if key.get('primary')is True]
   assert len(primary)==1 and guard['key']['id']==primary[0]['id']
   assert guard['key']['fields']==[logical(ref['element'])for ref in primary[0]['fields']]
   assert guard['key']['types']==[{'family':'string','facets':{},'nullable':False}for _ in primary[0]['fields']]
  def page_context(self,descriptor,original,supplied):
   self.bind_descriptor(descriptor,original,supplied)
   actor=sql('SELECT current_user() AS actor',reader=True).rows[0]['actor']
   assert actor==args.reader_app_id
   return json.dumps({'publication':original.scope_id,'custody':original.custody_digest,'nativeActor':actor,'policyRevision':active['policyRevision']},sort_keys=True,separators=(',',':')).encode()
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
 assert sorted(target.read(entry,Parameters(status='draft')),key=lambda row:row.id)==[Order(id='a',status='draft'),Order(id='b',status='draft')];checks.append('actual UC UUID/version publication and native PostgreSQL held pins release generated typed rows')
 (args.generated/'managed_declarations.py').write_text('from umf_access import query,Param\nfrom native_sales.models import Order\n@query\ndef selected(status:Param[str]):return Order.objects.filter(Order.c.status==status).limit(10).all()\n')
 config={'package':'native_sales','modules':['managed_declarations'],'targets':[{'id':'owned','kind':'ashlar','backendId':'ashlar.databricks','backendVersion':'0.1.0-managed-20261016-candidate','targetProfile':'dbsql2026.39-build-20261016-candidate','binding':json.loads(args.model_request.read_bytes())['binding'],'engineCompiler':'ashlar-dbsql-20261016','engineRequests':{'selected:0':request},'platform':[{'name':'dbsql','versions':['2026.39']}]}]}
 (args.generated/'managed-config.json').write_text(json.dumps(config))
 for extra in ([],['--check']):
  process=subprocess.run([sys.executable,'-B','-m','umf_access.build.catalog','catalog','--root',str(args.generated),'--config',str(args.generated/'managed-config.json'),*extra],capture_output=True,text=True)
  assert process.returncode==0,process.stdout+process.stderr
 from native_sales import queries
 assert sorted(queries.selected(target,status='draft'),key=lambda row:row.id)==[Order(id='a',status='draft'),Order(id='b',status='draft')];checks.append('real CLI catalog/check emits generated query executing original managed engine program')
 assert target.read(entry,Parameters(status="draft' OR true--"))==[];checks.append('actual native named parameter binding')
 page_request=json.loads(json.dumps(request));page_request['interfaceVersion']='weft-compile/0.2.2';page_request['target']['backendVersion']='0.1.0-managed-20261016-pages-candidate'
 page_request['sql']='SELECT "r"."id" AS "id", "r"."status" AS "status" FROM "sales"."order" AS "r" WHERE "r"."status" = :status'
 page_entry=EngineRead.build(weft,page_request,parameters=Parameters,result=Order,method='ashlar-dbsql-20261016-pages',page_size=1)
 page_target=AshlarTarget(compiler=weft,compiler_method='ashlar-dbsql-20261016-pages',executor=executor,backend=backend,pins=HeldPins(),vector=vector,policy=ReadPolicy(),context=context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={'typed-sdk-development':[source_sha]},binding_sha256=binding_sha,platform=target.platform,default_timeout=timedelta(seconds=120),deadline=deadline,catalog_entries=(page_entry,))
 (out/'original-engine-page-request.json').write_text(json.dumps(page_request));(out/'engine-page-catalog.json').write_text(page_entry.artifact_json)
 first=page_target.page(page_entry,Parameters(status='draft'));assert [r.id for r in first.items]==['a']and first.next_after is not None
 second=page_target.page(page_entry,Parameters(status='draft'),after=first.next_after);assert [r.id for r in second.items]==['b']and second.next_after is None
 checks.append('actual independent UC reader executes original Key-integrity page and native continuation')
 assert [r.id for r in page_target.stream(page_entry,Parameters(status='draft'),max_rows=2)]==['a','b']and not active['pins']
 checks.append('actual immutable managed publication stream releases each original native pin interval')
 active['policyRevision']=2
 try:page_target.page(page_entry,Parameters(status='draft'),after=first.next_after)
 except StaleContinuation:checks.append('changed owner policy context refuses managed continuation')
 else:raise AssertionError('changed original owner context accepted')
 active['policyRevision']=1

 sql('UPDATE '+table+' SET props_json=:props WHERE id=1',{'props':'{"1":"a","2":"changed"}'})
 assert sorted(target.read(entry,Parameters(status='draft')),key=lambda row:row.id)==[Order(id='a',status='draft'),Order(id='b',status='draft')];checks.append('actual independent managed mutation preserves original immutable publication read')
 if credential is not None:
  sql('REVOKE SELECT ON TABLE '+table+' FROM `'+args.reader_app_id+'`')
  try:target.read(entry,Parameters(status='draft'))
  except Exception as error:
   from durable_sql import SQLCustodyError
   if not isinstance(error,SQLCustodyError):raise
   checks.append('actual UC SELECT revocation refuses typed read through independent native principal')
  else:raise AssertionError('native UC revocation released typed rows')
  sql('GRANT SELECT ON TABLE '+table+' TO `'+args.reader_app_id+'`')
 active['deny']=True
 try:target.read(entry,Parameters(status='draft'))
 except PermissionError:checks.append('native original publication policy revocation refuses release')
 else:raise AssertionError('revoked managed reader released data')
 status='pass'
finally:
 cleanup=[];deleted=False;revoked=credential is None
 for label,action in [('postgres',lambda:local(pgbin/'pg_ctl','-D',pg,'-m','fast','-w','stop')if started else None),('warehouse',lambda:cli('warehouses','stop',args.warehouse,'--no-wait')),('credential',lambda:cli('service-principal-secrets-proxy','delete',args.reader_principal,credential['id'])if credential is not None else None),('principal',lambda:cli('service-principals','delete',args.reader_principal)if args.reader_principal is not None else None)]:
  try:
   action()
   if label=='credential':revoked=True
   if label=='principal':deleted=args.reader_principal is not None
  except BaseException as error:cleanup.append({'phase':label,'errorType':type(error).__name__})
 credential=None
 (out/'verification.json').write_text(json.dumps({'status':status,'checks':checks,'schema':schema,'warehouse':args.warehouse,'stopRequested':not any(c['phase']=='warehouse'for c in cleanup),'credentialRevoked':revoked,'temporaryPrincipalDeleted':deleted,'cleanupErrors':cleanup,'ownedPostgresStopped':not(pg/'postmaster.pid').exists(),'temporal':'HELD','scope':'actual managed scalar STRING publication and typed reads with local native pins; independent UC reader only when explicitly enrolled; no accepted Truss identities, managed action atomicity or broader semantic qualification'},indent=2)+'\n')
print(json.dumps({'status':status,'checks':len(checks),'output':str(out)}))

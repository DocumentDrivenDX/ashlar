"""Owned Spark4/Delta4 + PostgreSQL18 pins -> original Weft -> typed SDK.

Explicit local development carrier IDs are not accepted Truss IDs. Native
publication UUID/version, PostgreSQL roles/locks and retained files are real.
"""
from pathlib import Path
from contextlib import contextmanager
from datetime import timedelta
import argparse,hashlib,json,math,os,subprocess,sys,threading,uuid
from dataclasses import replace
from umf_access.pages import StaleContinuation
import psycopg,weft
from pyspark.sql import SparkSession
from ashlar.access import AshlarTarget
from ashlar.native import NativeBackend,SQLResult
from ashlar.pins import PinVector,PostgresPins
from ashlar.publication import ResolutionError
from ashlar_host.postgres import Session
from umf_access.engine_catalog import EngineRead
from umf_access.types import StrictModel
parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--generated',type=Path,required=True);parser.add_argument('--model-request',type=Path,required=True);parser.add_argument('--jars',type=Path,required=True);parser.add_argument('--conformance-root',type=Path);args=parser.parse_args()
if args.conformance_root is not None:
 sys.path.insert(0,str(args.conformance_root.absolute()))
 import conformance
 if Path(conformance.__file__).resolve().parent!=(args.conformance_root/'conformance').resolve():raise RuntimeError('Requested local shared conformance tooling was not loaded')
 from conformance import ReadWitness,run_read_suite
root=Path(__file__).resolve().parents[1];out=args.output.absolute();out.mkdir(parents=True,exist_ok=False)
sys.path.insert(0,str(args.generated.absolute()));from native_sales.models import Order
class Parameters(StrictModel):status:str
expected={'delta-spark_2.13-4.0.0.jar':'538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e','delta-storage-4.0.0.jar':'9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d'}
assert all(hashlib.sha256((args.jars/n).read_bytes()).hexdigest()==v for n,v in expected.items())
pgbin=Path('/opt/homebrew/opt/postgresql@18/bin');pg=out/'postgres';sock=out/'socket';sock.mkdir();started=False;spark=None;checks=[];status='fail'
def run(*command):
 p=subprocess.run(list(map(str,command)),capture_output=True,text=True,check=True)
 with(out/'lifecycle.log').open('a')as f:f.write(p.stdout+p.stderr)
try:
 run(pgbin/'initdb','-D',pg,'-A','trust','-U','sdk_test','--no-sync')
 run(pgbin/'pg_ctl','-D',pg,'-l',out/'postgres.log','-o',f"-F -k {sock} -c listen_addresses='' -p 55496",'-w','start');started=True
 dsn=f'host={sock} port=55496 dbname=postgres user=sdk_test'
 with psycopg.connect(dsn,autocommit=True)as admin:
  for name in ('01-postgresql.sql','03-read-guard.sql','04-table-guard.sql','05-maintenance-quarantine.sql','06-availability-guard.sql'):admin.execute((root/'sql/ashlar-pins'/name).read_text())
  admin.execute('CREATE ROLE sdk_person LOGIN;GRANT ashlar_pin_reader,ashlar_pin_writer TO sdk_person;CREATE ROLE sdk_maintenance LOGIN;GRANT ashlar_pin_maintenance TO sdk_maintenance')
 spark=(SparkSession.builder.master('local[1]').appName('Ashlar original typed SDK native E2E').config('spark.ui.enabled','false').config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.ansi.enabled','true').config('spark.sql.session.timeZone','UTC').config('spark.jars',','.join(str(args.jars/n)for n in expected)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir',str(out/'warehouse')).getOrCreate())
 spark.sparkContext.setLogLevel('ERROR')
 spark.sql(f"CREATE DATABASE sdk_ashlar LOCATION '{out/'database'}'")
 table='spark_catalog.sdk_ashlar.object_current';manifest_table='spark_catalog.sdk_ashlar.manifest'
 spark.sql(f"CREATE TABLE {table} (source_system STRING,type_id BIGINT,id BIGINT,schema_revision STRING,props_json STRING) USING DELTA LOCATION '{out/'objects'}'")
 model=json.loads(args.model_request.read_bytes())['binding']['modelJson'];source_sha=hashlib.sha256(model.encode()).hexdigest();document=json.loads(model)
 spark.sql(f'INSERT INTO {table} VALUES (:source,17,1,:revision,:a),(:source,17,2,:revision,:b),(:source,17,3,:revision,:c)',args={'source':'typed-sdk-local','revision':source_sha,**{key:json.dumps({'1':key,'2':'draft'})for key in ('a','b','c')}})
 detail=spark.sql('DESCRIBE DETAIL '+table).first().asDict();version=int(spark.sql('DESCRIBE HISTORY '+table).first()['version'])
 data_files=tuple((out/'objects').glob('*.parquet'));assert data_files
 originals={str(p):hashlib.sha256(p.read_bytes()).hexdigest()for p in data_files}
 publication_id=str(uuid.uuid4())
 row={'publication_id':publication_id,'profile_version':'ashlar-delta/0.3','table_versions_json':json.dumps({table:version}),'source_progress_json':json.dumps({'typed-sdk-local':{'offset':'1'}}),'schema_revisions_json':json.dumps({'typed-sdk-local':source_sha}),'validation_report_json':json.dumps({'complete':True,'sourceSha256':source_sha,'scope':'local explicit development carrier mapping; not accepted Truss identities'})}
 spark.sql(f"CREATE TABLE {manifest_table} (publication_id STRING,profile_version STRING,table_versions_json STRING,source_progress_json STRING,schema_revisions_json STRING,validation_report_json STRING,recorded_at TIMESTAMP) USING DELTA LOCATION '{out/'manifest'}'")
 spark.sql(f'INSERT INTO {manifest_table} VALUES (:publication_id,:profile_version,:table_versions_json,:source_progress_json,:schema_revisions_json,:validation_report_json,current_timestamp())',args=row)
 manifest_detail=spark.sql('DESCRIBE DETAIL '+manifest_table).first().asDict()
 raw_descriptor=dict(row,recorded_at=spark.sql(f'SELECT cast(unix_micros(recorded_at) AS STRING) AS value FROM {manifest_table}').first()['value'])
 custody=hashlib.sha256(json.dumps(raw_descriptor,sort_keys=True,separators=(',',':')).encode()).hexdigest();vector=PinVector('typed-sdk-owned-local','manifest',publication_id,custody,{table:(detail['id'],version)})
 context=object();active={'pins':False,'deny':False,'policyRevision':1,'pinActor':None,'faultSql':None,'fault':None};native_calls=[]
 class PinExecutor:
  @contextmanager
  def transaction(self,supplied):
   assert supplied is context
   params=psycopg.conninfo.conninfo_to_dict(dsn);params['user']='sdk_person'
   with psycopg.connect(**params)as connection:
    connection.execute("SET LOCAL statement_timeout='10000ms'")
    active['pinActor']=connection.execute('SELECT current_user,(SELECT oid::text FROM pg_catalog.pg_roles WHERE rolname=current_user),current_database()').fetchone()
    session=Session(connection)
    try:yield session
    finally:session.active=False
 class PinPolicy:
  def __init__(self,expected_vector=vector,expected_files=originals):self.expected_vector,self.expected_files=expected_vector,expected_files
  def admit_registration(self,original,supplied):
   assert supplied is context and original==self.expected_vector and all(Path(p).is_file()for p in self.expected_files)
  def authorize_read(self,original,supplied):
   assert supplied is context and original==self.expected_vector
   if active['deny']:raise PermissionError('local reader revoked')
 pins=PostgresPins(PinExecutor(),PinPolicy());pins.register(vector,context=context)
 class HeldPins:
  def __init__(self,original=pins):self.original=original
  @contextmanager
  def hold(self,original,*,context):
   with self.original.hold(original,context=context):
    active['pins']=True
    try:yield original
    finally:active['pins']=False
  @contextmanager
  def hold_page(self,original,*,context):
   with self.original.hold_page(original,context=context):
    active['pins']=True
    try:yield original
    finally:active['pins']=False
 class Executor:
  def query(self,sql,parameters):
   assert active['pins'];native_calls.append(sql)
   frame=spark.sql(sql,args=parameters)
   schema=tuple((f.name,f.dataType.simpleString().upper())for f in frame.schema)
   rows=[r.asDict(recursive=True)for r in frame.collect()]
   if active['fault']is not None and sql==active['faultSql']:
    fault=active['fault'];active['fault']=None;active['faultAt']=len(native_calls);fault()
   return SQLResult(rows,schema)
 class NativePolicy:
  def __init__(self,expected_vector=vector,expected_descriptor=raw_descriptor,expected_files=originals,expected_publication=publication_id):
   self.expected_vector,self.expected_descriptor,self.expected_files,self.expected_publication=expected_vector,expected_descriptor,expected_files,expected_publication
  def authorize(self,supplied,pid,tables):
   if supplied is not context or not active['pins'] or active['deny'] or pid!=self.expected_publication or set(tables)!=set(self.expected_vector.targets):raise PermissionError('original local publication authority refused')
  def validate_descriptor(self,descriptor,supplied):
   if supplied is not context or dict(descriptor.raw)!=self.expected_descriptor:raise PermissionError('original native descriptor differs')
  def validate_snapshot(self,name,identity,retained,columns):
   if not active['pins'] or name not in self.expected_vector.targets or (identity,retained)!=self.expected_vector.targets[name]:raise PermissionError('original held native snapshot differs')
   if any(not Path(p).is_file() or hashlib.sha256(Path(p).read_bytes()).hexdigest()!=sha for p,sha in self.expected_files.items()):raise PermissionError('original retained data files unavailable')
 executor=Executor();backend=NativeBackend(executor,NativePolicy(),manifest_table,manifest_detail['id'],{table:(('source_system','STRING'),('type_id','BIGINT'),('id','BIGINT'),('schema_revision','STRING'),('props_json','STRING'))})
 pin={'documentId':document['id'],'revision':source_sha,'sha256':source_sha,'umfVersion':'0.8.0'}
 def logical(element):return {'documentId':document['id'],'revision':source_sha,'module':'sales','element':element}
 binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':'ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e','modelPins':[pin],'publication':{'id':publication_id,'manifestUuid':manifest_detail['id'],'tables':[{'name':table.split('.'),'uuid':detail['id'],'version':version}]},'records':[{'logical':logical('order'),'table':0,'kind':'object','sourceSystem':'typed-sdk-local','typeId':'17','schemaRevision':source_sha,'properties':[{'logical':logical(field),'home':{'kind':'props','propertyId':str(number)}}for number,field in ((1,'id'),(2,'status'))]}]}
 binding_json=json.dumps(binding,sort_keys=True,separators=(',',':'));binding_sha=hashlib.sha256(binding_json.encode()).hexdigest()
 request={'interfaceVersion':'weft-compile/0.2.0','dialect':'weft-sql/0.2.0','sql':'SELECT r.id AS "id", r.status AS "status" FROM sales."order" r WHERE r.status = :status ORDER BY r.id ASC LIMIT 10','parameters':{'status':{'family':'string','value':'draft'}},'modules':[{'documentJson':model,'pin':pin,'selectedModuleIds':['sales']}],'target':{'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-candidate','targetProfile':'spark4.0.1-delta4.0.0-candidate','bindingJson':binding_json,'bindingSha256':binding_sha},'options':{'allowCandidate':True}}
 entry=EngineRead.build(weft,request,parameters=Parameters,result=Order,method='ashlar-spark4')
 (out/'original-engine-request.json').write_text(json.dumps(request));(out/'engine-catalog.json').write_text(entry.artifact_json)
 class ReadPolicy:
  def __init__(self,expected_vector=vector,expected_descriptor=raw_descriptor,expected_files=originals,expected_publication=publication_id,expected_binding=binding,expected_binding_sha=binding_sha):
   self.expected_vector,self.expected_binding,self.expected_binding_sha=expected_vector,expected_binding,expected_binding_sha
   self.native=NativePolicy(expected_vector,expected_descriptor,expected_files,expected_publication)
  def bind_descriptor(self,descriptor,original,supplied):
   assert active['pins'] and original==self.expected_vector;self.native.validate_descriptor(descriptor,supplied)
  def admit_artifact(self,current,artifact,descriptor,supplied):
   assert active['pins'] and current['modules']==request['modules'] and artifact['modelPins']==[pin]
   assert artifact['bindingSha256']==self.expected_binding_sha and json.loads(current['target']['bindingJson'])==self.expected_binding
  def verify_native_profile(self,profile,supplied):
   assert active['pins'] and supplied is context
   if profile['targetProfile']!='spark4.0.1-delta4.0.0-candidate' or profile['engineVersion']!=spark.version or profile['deltaVersion']!='4.0.0' or spark.conf.get('spark.sql.ansi.enabled')!='true' or spark.conf.get('spark.sql.session.timeZone')!='UTC':raise ResolutionError('original native Spark profile drift')
   assert all(hashlib.sha256((args.jars/n).read_bytes()).hexdigest()==v for n,v in expected.items())
  def authorize_query(self,current,descriptor,supplied):self.native.authorize(supplied,self.native.expected_publication,tuple(self.expected_vector.targets))
  def admit_record_integrity(self,check,artifact,supplied):
   if check.get('record')!=logical('order') or supplied is not context:raise ResolutionError('unadmitted original record integrity')
  def admit_page_integrity(self,guard,artifact,supplied):
   assert active['pins'] and supplied is context and guard['key']==artifact['logicalPlan']['pageKey']
   record=next(e for e in document['modules'][0]['elements']if e['id']=='order');primary=[key for key in record['keys']if key.get('primary')is True]
   assert len(primary)==1 and guard['key']['id']==primary[0]['id']
   assert guard['key']['fields']==[logical(ref['element'])for ref in primary[0]['fields']]and all(ref['module']=='sales'for ref in primary[0]['fields'])
   assert guard['key']['types']==[{'family':'string','facets':{},'nullable':False}for _ in primary[0]['fields']]
  def page_context(self,descriptor,original,supplied):
   self.bind_descriptor(descriptor,original,supplied);self.native.authorize(supplied,self.native.expected_publication,tuple(original.targets))
   assert active['pinActor']is not None
   return json.dumps({'publication':original.scope_id,'custody':original.custody_digest,'nativePinActor':active['pinActor'],'nativeSparkUser':spark.sparkContext.sparkUser(),'policyRevision':active['policyRevision']},sort_keys=True,separators=(',',':')).encode()
  def decode_result(self,column,value,current,supplied):
   assert active['pins'] and supplied is context
   representation=column['representation']
   if representation['kind']=='scalar' and representation['logicalType']=={'family':'string','facets':{},'nullable':False} and type(value)is str:return value
   if representation['kind']=='value':
    item=json.loads(value)
    if item.get('state')=='value' and set(item)=={'state','value'} and type(item['value'])is str:return item
   raise ResolutionError('original source exact string decoder refused')
  def authorize_result(self,rows,descriptor,supplied):
   assert active['pins'];self.native.authorize(supplied,self.native.expected_publication,tuple(self.expected_vector.targets))
 @contextmanager
 def deadline(budget):
  group='typed-sdk-'+str(uuid.uuid4());spark.sparkContext.setJobGroup(group,'bounded original Ashlar typed read',interruptOnCancel=True)
  timer=threading.Timer(budget.total_seconds(),lambda:spark.sparkContext.cancelJobGroup(group));timer.daemon=True;timer.start()
  try:yield
  finally:timer.cancel();spark.sparkContext.setLocalProperty('spark.jobGroup.id',None);spark.sparkContext.setLocalProperty('spark.job.description',None);spark.sparkContext.setLocalProperty('spark.job.interruptOnCancel',None)
 target=AshlarTarget(compiler=weft,compiler_method='ashlar-spark4',executor=executor,backend=backend,pins=HeldPins(),vector=vector,policy=ReadPolicy(),context=context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={'typed-sdk-local':[source_sha]},binding_sha256=binding_sha,platform=(('spark','4.0.1'),('delta','4.0.0'),('postgresql','18.6')),default_timeout=timedelta(seconds=60),deadline=deadline,catalog_entries=(entry,))
 assert target.read(entry,Parameters(status='draft'))==[Order(id=key,status='draft')for key in ('a','b','c')];checks.append('original published native typed read under real PostgreSQL pins')
 assert target.read(entry,Parameters(status="draft' OR true--"))==[];checks.append('native parameter binding')
 target.verify();checks.append('eager verification performs original held native read')
 page_request=json.loads(json.dumps(request));page_request['interfaceVersion']='weft-compile/0.2.2';page_request['target']['backendVersion']='0.1.0-local-spark4-pages-candidate'
 page_request['sql']='SELECT "r"."id" AS "id", "r"."status" AS "status" FROM "sales"."order" AS "r" WHERE "r"."status" = :status'
 page_entry=EngineRead.build(weft,page_request,parameters=Parameters,result=Order,method='ashlar-spark4-pages',page_size=1)
 page_target=AshlarTarget(compiler=weft,compiler_method='ashlar-spark4-pages',executor=executor,backend=backend,pins=HeldPins(),vector=vector,policy=ReadPolicy(),context=context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={'typed-sdk-local':[source_sha]},binding_sha256=binding_sha,platform=target.platform,default_timeout=timedelta(seconds=60),deadline=deadline,catalog_entries=(page_entry,))
 (out/'original-engine-page-request.json').write_text(json.dumps(page_request));(out/'engine-page-catalog.json').write_text(page_entry.artifact_json)
 first=page_target.page(page_entry,Parameters(status='draft'));assert [r.id for r in first.items]==['a']and first.next_after is not None
 second=page_target.page(page_entry,Parameters(status='draft'),after=first.next_after);assert [r.id for r in second.items]==['b']and second.next_after is not None
 third=page_target.page(page_entry,Parameters(status='draft'),after=second.next_after);assert [r.id for r in third.items]==['c']and third.next_after is None
 checks.append('original native page Key check and typed snapshot continuation return a/b/c without gaps')
 for changed in [Parameters(status='other')]:
  before=len(native_calls)
  try:page_target.page(page_entry,changed,after=first.next_after)
  except StaleContinuation:pass
  else:raise AssertionError('foreign page arguments accepted')
  assert len(native_calls)==before
 try:page_target.page(page_entry,Parameters(status='draft'),after=replace(first.next_after,values=(1,)))
 except StaleContinuation:pass
 else:raise AssertionError('typed page Key coercion accepted')
 active['policyRevision']+=1;before=len(native_calls)
 try:page_target.page(page_entry,Parameters(status='draft'),after=first.next_after)
 except StaleContinuation:pass
 else:raise AssertionError('changed original page policy context accepted')
 assert page_entry.artifact()['artifact']['sql']not in native_calls[before:]
 active['policyRevision']-=1
 checks.append('foreign parameters, wrong Key carrier and actual owner policy-context change refuse continuation')
 rows=list(page_target.stream(page_entry,Parameters(status='draft'),max_rows=2));assert [r.id for r in rows]==['a','b']and not active['pins']
 stream=page_target.stream(page_entry,Parameters(status='draft'),max_rows=100);assert next(stream).id=='a'and not active['pins'];stream.close();assert not active['pins']
 checks.append('bounded snapshot stream and early close leave no held pin transaction')
 (args.generated/'ashlar_page_declarations.py').write_text('from umf_access import query,Param\nfrom native_sales.models import Order\nfrom native_sales.keys import OrderKey\n@query\ndef selected_page(status:Param[str],after:Param[OrderKey|None]):return Order.objects.filter(Order.c.status==status).page(size=1,after=after)\n')
 packet=json.loads(args.model_request.read_bytes())
 config={'package':'native_sales','modules':['ashlar_page_declarations'],'targets':[{'id':'owned','kind':'ashlar','backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-pages-candidate','targetProfile':'spark4.0.1-delta4.0.0-candidate','binding':packet['binding'],'engineCompiler':'ashlar-spark4-pages','engineRequests':{'selected_page:0':page_request},'platform':[{'name':name,'versions':[version]}for name,version in target.platform]}]}
 config_path=args.generated/'ashlar-page-config.json';config_path.write_text(json.dumps(config))
 for extra in ([],['--check']):
  built=subprocess.run([sys.executable,'-B','-m','umf_access.build.catalog','catalog','--root',str(args.generated),'--config',str(config_path),*extra],capture_output=True,text=True)
  assert built.returncode==0,built.stdout+built.stderr
 from native_sales import queries
 generated_first=queries.selected_page(page_target,status='draft',after=None)
 generated_next=queries.selected_page(page_target,status='draft',after=generated_first.next_after)
 assert [r.id for r in generated_next.items]==['b']
 assert [r.id for r in queries.selected_page_stream(page_target,status='draft',after=None,max_rows=2)]==['a','b']
 checks.append('real CLI catalog/check and generated page/stream preserve native publication context')
 spark.sql(f"UPDATE {table} SET props_json='{{\"1\":\"a\",\"2\":\"changed\"}}' WHERE id=1")
 assert target.read(entry,Parameters(status='draft'))==[Order(id=key,status='draft')for key in ('a','b','c')];checks.append('original immutable publication remains at retained Delta version')
 def publish_page_case(native_version,files,progress):
  observed=spark.sql('DESCRIBE DETAIL '+table).first().asDict();assert observed['id']==detail['id']
  pid=str(uuid.uuid4());published=dict(row,publication_id=pid,table_versions_json=json.dumps({table:native_version}),source_progress_json=json.dumps({'typed-sdk-local':{'offset':str(progress)}}))
  spark.sql(f'INSERT INTO {manifest_table} VALUES (:publication_id,:profile_version,:table_versions_json,:source_progress_json,:schema_revisions_json,:validation_report_json,current_timestamp())',args=published)
  recorded=spark.sql(f'SELECT cast(unix_micros(recorded_at) AS STRING) AS value FROM {manifest_table} WHERE publication_id=:id',args={'id':pid}).first()['value']
  descriptor=dict(published,recorded_at=recorded);digest=hashlib.sha256(json.dumps(descriptor,sort_keys=True,separators=(',',':')).encode()).hexdigest()
  original=PinVector('typed-sdk-owned-local','manifest',pid,digest,{table:(observed['id'],native_version)})
  original_pins=PostgresPins(PinExecutor(),PinPolicy(original,files));original_pins.register(original,context=context)
  source_binding=json.loads(json.dumps(binding));source_binding['publication']['id']=pid;source_binding['publication']['tables'][0]['version']=native_version
  source_json=json.dumps(source_binding,sort_keys=True,separators=(',',':'));source_digest=hashlib.sha256(source_json.encode()).hexdigest()
  current=json.loads(json.dumps(page_request));current['target']['bindingJson']=source_json;current['target']['bindingSha256']=source_digest
  compiled=EngineRead.build(weft,current,parameters=Parameters,result=Order,method='ashlar-spark4-pages',page_size=1)
  native_backend=NativeBackend(executor,NativePolicy(original,descriptor,files,pid),manifest_table,manifest_detail['id'],backend.schemas)
  adopted=AshlarTarget(compiler=weft,compiler_method='ashlar-spark4-pages',executor=executor,backend=native_backend,pins=HeldPins(original_pins),vector=original,policy=ReadPolicy(original,descriptor,files,pid,source_binding,source_digest),context=context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={'typed-sdk-local':[source_sha]},binding_sha256=source_digest,platform=target.platform,default_timeout=timedelta(seconds=60),deadline=deadline,catalog_entries=(compiled,))
  (out/('publication-'+str(progress)+'.json')).write_text(json.dumps({'descriptor':descriptor,'tableUuid':observed['id'],'version':native_version,'program':json.loads(compiled.originals())},indent=2))
  return adopted,compiled,original
 empty_target,empty_entry,_=publish_page_case(0,{},'empty')
 empty=empty_target.page(empty_entry,Parameters(status='draft'));assert empty.items==()and empty.next_after is None
 checks.append('actual complete empty Delta publication returns an empty page without latest fallback')
 current_version=int(spark.sql('DESCRIBE HISTORY '+table).first()['version'])
 current_files={str(path):hashlib.sha256(path.read_bytes()).hexdigest()for path in(out/'objects').glob('*.parquet')}
 next_target,next_entry,_=publish_page_case(current_version,current_files,2)
 before=len(native_calls)
 try:next_target.page(next_entry,Parameters(status='draft'),after=first.next_after)
 except StaleContinuation:pass
 else:raise AssertionError('foreign publication continuation accepted')
 assert len(native_calls)==before
 fresh=next_target.page(next_entry,Parameters(status='draft'));assert [r.id for r in fresh.items]==['b']and fresh.next_after is not None
 assert [r.id for r in next_target.page(next_entry,Parameters(status='draft'),after=fresh.next_after).items]==['c']
 assert [r.id for r in page_target.page(page_entry,Parameters(status='draft')).items]==['a']
 checks.append('actual second publication has fresh b/c rows; P1 remains retained and foreign continuation refuses')
 original_file=Path(next(iter(originals)));saved=original_file.read_bytes();original_file.unlink()
 try:
  try:page_target.page(page_entry,Parameters(status='draft'),after=first.next_after)
  except StaleContinuation:checks.append('actual unavailable original retained file refuses continuation before data SQL')
  else:raise AssertionError('missing retained publication file accepted')
 finally:original_file.write_bytes(saved)
 # Optional repository-only shared suite. Every requested case is an actual
 # AshlarTarget/ReadPlan invocation; the oracle reads retained Delta directly.
 if args.conformance_root is not None:
  from umf_access.catalog import Argument,ReadPlan,ReadVariant,TargetPin
  from native_sales.keys import OrderKey
  from pyspark.errors.exceptions.captured import AnalysisException
  def native_reference(native_version,limit=None):
   physical=spark.read.format('delta').option('versionAsOf',native_version).load(str(out/'objects'))
   observed=[]
   for native_row in physical.collect():
    if native_row['source_system']=='typed-sdk-local' and native_row['type_id']==17 and native_row['schema_revision']==source_sha:
     props=json.loads(native_row['props_json'])
     if props['2']=='draft':observed.append(Order(id=props['1'],status=props['2']))
   observed.sort(key=lambda item:item.id)
   return tuple(observed if limit is None else observed[:limit])
  def read_plan(adopted,compiled,page=False):
   identity=adopted.identity()
   target_pin=TargetPin(identity.kind,identity.backend_id,identity.backend_version,identity.target_profile,identity.binding_sha256,tuple((name,(version,))for name,version in identity.platform))
   return ReadPlan('shared_native_page'if page else'shared_native_all','page'if page else'all',
    (Argument('status',str),)+((Argument('after',object),)if page else()),
    (ReadVariant((),target_pin,compiled,(('status','status',None),)),),
    cursor_argument='after'if page else None,cursor_key_model=OrderKey if page else None,key_components=(('id',0),)if page else())
  conformance_facts=[]
  class SharedNativeFixture:
   @contextmanager
   def open(self,case):
    chosen,compiled=target,entry;page=False;prepare=None;refusal=None
    reference=lambda:native_reference(version)
    complete=None;after_reference=None;restore_file=None;restore_alias=False;repointed=False
    original_calls=len(native_calls)
    renamed_shared='sdk_ashlar.shared_conformance_original'
    if case=='reads.empty_complete':
     chosen,compiled,page=empty_target,empty_entry,True
     reference=lambda:native_reference(0,1)
     def complete():
      native=spark.sql(f'SELECT validation_report_json FROM {manifest_table} WHERE publication_id=:id',args={'id':chosen.vector.scope_id}).collect()
      return len(native)==1 and json.loads(native[0]['validation_report_json'])['complete']is True
    elif case=='reads.refusal_not_empty':
     restore_file=(original_file,original_file.read_bytes())
     prepare=lambda:original_file.unlink()
     refusal=PermissionError
    elif case in('reads.alias_renamed','reads.alias_repointed'):
     restore_alias=True
     def prepare():
      nonlocal repointed
      spark.sql(f'ALTER TABLE {table} RENAME TO {renamed_shared}')
      if case=='reads.alias_repointed':
       spark.sql(f"CREATE TABLE {table} USING DELTA LOCATION '{out/'shared-repointed'}' AS SELECT * FROM {renamed_shared}")
       # Admit the requested version number on the replacement so refusal
       # exercises original UUID custody, not an absent-version side effect.
       spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('shared.conformance.fixture'='repointed')")
       replacement=spark.sql('DESCRIBE DETAIL '+table).first().asDict()
       assert replacement['id']!=detail['id'] and int(spark.sql('DESCRIBE HISTORY '+table).first()['version'])>=version
       repointed=True
     refusal=AnalysisException if case=='reads.alias_renamed'else PermissionError
    elif case=='reads.withheld_rows':
     chosen,compiled,page=page_target,page_entry,True
     reference=lambda:native_reference(version,1)
     restore_alias=True
     def prepare():
      active['faultSql']=compiled.artifact()['artifact']['sql']
      active['fault']=lambda:spark.sql(f'ALTER TABLE {table} RENAME TO {renamed_shared}')
     refusal=AnalysisException
    elif case=='reads.continuation':
     # The shared case uses a genuine native owner-policy revision transition.
     # P1/P2 and retention changes retain their separate existing witnesses.
     chosen,compiled,page=page_target,page_entry,True
     reference=lambda:native_reference(version,1)
     prior_policy=active['policyRevision']
     prepare=lambda:active.__setitem__('policyRevision',prior_policy+1)
     after_reference=lambda:native_reference(version,1)
    plan=read_plan(chosen,compiled,page)
    args_for_case={'status':'draft',**({'after':None}if page else{})}
    try:
     yield ReadWitness(chosen,plan,args_for_case,reference,prepare,refusal,complete,after_reference)
     if case=='reads.withheld_rows':
      assert active['fault']is None and spark.catalog.tableExists(renamed_shared)
      assert any(sql.startswith('DESCRIBE DETAIL')and table in sql.replace('`','')for sql in native_calls[active['faultAt']:])
     conformance_facts.append({'case':case,'nativeQueries':len(native_calls)-original_calls,'originalPublication':chosen.vector.scope_id,'sourceVersion':chosen.vector.targets[table][1],'faultAfterFetch':case=='reads.withheld_rows'})
    finally:
     active['fault']=None
     if restore_file is not None:restore_file[0].write_bytes(restore_file[1])
     if restore_alias and spark.catalog.tableExists(renamed_shared):
      if repointed and spark.catalog.tableExists(table):spark.sql(f'DROP TABLE {table}')
      spark.sql(f'ALTER TABLE {renamed_shared} RENAME TO {table.split(".",1)[1]}')
     if case=='reads.continuation':active['policyRevision']=prior_policy
  completed=run_read_suite(SharedNativeFixture())
  assert len(completed)==8 and len(conformance_facts)==8
  (out/'shared-conformance.json').write_text(json.dumps({'status':'pass','cases':list(completed),'facts':conformance_facts,'scope':'actual local Ashlar targets and retained Delta oracle; continuation is actual native policy revision change, publication/retention transitions retain separate existing harness witnesses','temporal':'HELD'},indent=2)+'\n')
  checks.append('eight shared non-temporal read cases pass against actual local native Ashlar targets and independent retained Delta oracle')
 spark.sql(f'INSERT INTO {table} VALUES (:source,17,4,:revision,:props)',args={'source':'typed-sdk-local','revision':source_sha,'props':'{"1":"b","2":"draft"}'})
 duplicate_version=int(spark.sql('DESCRIBE HISTORY '+table).first()['version'])
 duplicate_files={str(path):hashlib.sha256(path.read_bytes()).hexdigest()for path in(out/'objects').glob('*.parquet')}
 duplicate_target,duplicate_entry,_=publish_page_case(duplicate_version,duplicate_files,3)
 before=len(native_calls)
 try:duplicate_target.page(duplicate_entry,Parameters(status='draft'))
 except ResolutionError as error:
  assert str(error)=='Weft integrity check failed'
  assert duplicate_entry.artifact()['artifact']['sql']not in native_calls[before:]
  checks.append('actual duplicate authored Key in a new Delta publication refuses before page data SQL')
 else:raise AssertionError('duplicate authored Key publication accepted')
 # Change the actual alias after row fetch. No buffered row may escape a
 # closing original native identity failure; restoration belongs to this fixture.
 renamed='sdk_ashlar.object_renamed'
 active['faultSql']=page_entry.artifact()['artifact']['sql']
 active['fault']=lambda:spark.sql(f'ALTER TABLE {table} RENAME TO {renamed}')
 try:
  try:page_target.page(page_entry,Parameters(status='draft'))
  except Exception:
   if active['fault']is not None:raise
   assert spark.catalog.tableExists(renamed)
   assert any(sql.startswith('DESCRIBE DETAIL')and table in sql.replace('`','')for sql in native_calls[active['faultAt']:])
   checks.append('actual table alias change after native fetch withholds the complete buffered page')
  else:raise AssertionError('buffered page escaped after native table alias changed')
 finally:
  active['fault']=None
  if spark.catalog.tableExists(renamed):spark.sql(f'ALTER TABLE {renamed} RENAME TO {table.split(".",1)[1]}')
 spark.conf.set('spark.sql.ansi.enabled','false');before=len(native_calls)
 try:target.read(entry,Parameters(status='draft'))
 except ResolutionError:checks.append('native profile drift refuses before integrity and user SQL')
 else:raise AssertionError('native profile drift accepted')
 spark.conf.set('spark.sql.ansi.enabled','true')
 assert all('SELECT' not in sql or 'props_json' not in sql for sql in native_calls[before:])
 active['deny']=True
 try:target.read(entry,Parameters(status='draft'))
 except PermissionError:checks.append('native policy revocation refuses read')
 else:raise AssertionError('authority revocation accepted')
 active['deny']=False
 params=vector.parameters(table)
 with psycopg.connect(dsn,autocommit=True)as admin:admin.execute('SELECT ashlar_pins.release(%s,%s,decode(%s,\'hex\'))',(params['id'],params['authority'],params['digest']))
 try:target.read(entry,Parameters(status='draft'))
 except psycopg.Error:checks.append('actual released PostgreSQL pin refuses native read')
 else:raise AssertionError('released original pin accepted')
 try:page_target.page(page_entry,Parameters(status='draft'),after=first.next_after)
 except StaleContinuation:checks.append('actual released native pin refuses original continuation without latest fallback')
 else:raise AssertionError('released original page pin accepted')
 status='pass'
finally:
 if spark is not None:spark.stop()
 if started:run(pgbin/'pg_ctl','-D',pg,'-m','fast','-w','stop')
 (out/'verification.json').write_text(json.dumps({'status':status,'checks':checks,'ownedPostgresStopped':not(pg/'postmaster.pid').exists(),'sparkStopped':spark is None or spark.sparkContext._jsc is None,'temporal':'HELD','scope':'local native Spark4/Delta4 with real PostgreSQL18 pin custody; no managed Databricks or accepted Truss ID claim'},indent=2)+'\n')
print(json.dumps({'status':'pass','checks':len(checks),'output':str(out)}))

"""Owned Spark4/Delta4 + PostgreSQL18 pins -> original Weft -> typed SDK.

Explicit local development carrier IDs are not accepted Truss IDs. Native
publication UUID/version, PostgreSQL roles/locks and retained files are real.
"""
from pathlib import Path
from contextlib import contextmanager
from datetime import timedelta
import argparse,hashlib,json,math,os,subprocess,sys,threading,uuid
import psycopg,weft
from pyspark.sql import SparkSession
from ashlar.access import AshlarTarget
from ashlar.native import NativeBackend,SQLResult
from ashlar.pins import PinVector,PostgresPins
from ashlar.publication import ResolutionError
from ashlar_host.postgres import Session
from umf_access.engine_catalog import EngineRead
from umf_access.types import StrictModel
parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--generated',type=Path,required=True);parser.add_argument('--model-request',type=Path,required=True);parser.add_argument('--jars',type=Path,required=True);args=parser.parse_args()
root=Path(__file__).resolve().parents[1];out=args.output.absolute();out.mkdir(parents=True,exist_ok=False)
sys.path.insert(0,str(args.generated.absolute()));from native_sales.models import Order
class Parameters(StrictModel):status:str
expected={'delta-spark_2.13-4.0.0.jar':'538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e','delta-storage-4.0.0.jar':'9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d'}
assert all(hashlib.sha256((args.jars/n).read_bytes()).hexdigest()==v for n,v in expected.items())
pgbin=Path('/opt/homebrew/opt/postgresql@18/bin');pg=out/'postgres';sock=out/'socket';sock.mkdir();started=False;spark=None;checks=[]
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
 spark.sql(f'INSERT INTO {table} VALUES (:source,17,1,:revision,:props)',args={'source':'typed-sdk-local','revision':source_sha,'props':'{"1":"a","2":"draft"}'})
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
 context=object();active={'pins':False,'deny':False};native_calls=[]
 class PinExecutor:
  @contextmanager
  def transaction(self,supplied):
   assert supplied is context
   params=psycopg.conninfo.conninfo_to_dict(dsn);params['user']='sdk_person'
   with psycopg.connect(**params)as connection:
    connection.execute("SET LOCAL statement_timeout='10000ms'")
    session=Session(connection)
    try:yield session
    finally:session.active=False
 class PinPolicy:
  def admit_registration(self,original,supplied):
   assert supplied is context and original==vector and all(Path(p).is_file()for p in originals)
  def authorize_read(self,original,supplied):
   assert supplied is context and original==vector
   if active['deny']:raise PermissionError('local reader revoked')
 pins=PostgresPins(PinExecutor(),PinPolicy());pins.register(vector,context=context)
 class HeldPins:
  @contextmanager
  def hold(self,original,*,context):
   with pins.hold(original,context=context):
    active['pins']=True
    try:yield original
    finally:active['pins']=False
 class Executor:
  def query(self,sql,parameters):
   assert active['pins'];native_calls.append(sql)
   frame=spark.sql(sql,args=parameters)
   schema=tuple((f.name,f.dataType.simpleString().upper())for f in frame.schema)
   return SQLResult([r.asDict(recursive=True)for r in frame.collect()],schema)
 class NativePolicy:
  def authorize(self,supplied,pid,tables):
   if supplied is not context or not active['pins'] or active['deny'] or pid!=publication_id or set(tables)!={table}:raise PermissionError('original local publication authority refused')
  def validate_descriptor(self,descriptor,supplied):
   if supplied is not context or dict(descriptor.raw)!=raw_descriptor:raise PermissionError('original native descriptor differs')
  def validate_snapshot(self,name,identity,retained,columns):
   if not active['pins'] or (name,identity,retained)!=(table,detail['id'],version):raise PermissionError('original held native snapshot differs')
   if any(not Path(p).is_file() or hashlib.sha256(Path(p).read_bytes()).hexdigest()!=sha for p,sha in originals.items()):raise PermissionError('original retained data files unavailable')
 executor=Executor();backend=NativeBackend(executor,NativePolicy(),manifest_table,manifest_detail['id'],{table:(('source_system','STRING'),('type_id','BIGINT'),('id','BIGINT'),('schema_revision','STRING'),('props_json','STRING'))})
 pin={'documentId':document['id'],'revision':source_sha,'sha256':source_sha,'umfVersion':'0.8.0'}
 def logical(element):return {'documentId':document['id'],'revision':source_sha,'module':'sales','element':element}
 binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':'ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e','modelPins':[pin],'publication':{'id':publication_id,'manifestUuid':manifest_detail['id'],'tables':[{'name':table.split('.'),'uuid':detail['id'],'version':version}]},'records':[{'logical':logical('order'),'table':0,'kind':'object','sourceSystem':'typed-sdk-local','typeId':'17','schemaRevision':source_sha,'properties':[{'logical':logical(field),'home':{'kind':'props','propertyId':str(number)}}for number,field in ((1,'id'),(2,'status'))]}]}
 binding_json=json.dumps(binding,sort_keys=True,separators=(',',':'));binding_sha=hashlib.sha256(binding_json.encode()).hexdigest()
 request={'interfaceVersion':'weft-compile/0.2.0','dialect':'weft-sql/0.2.0','sql':'SELECT r.id AS "id", r.status AS "status" FROM sales."order" r WHERE r.status = :status','parameters':{'status':{'family':'string','value':'draft'}},'modules':[{'documentJson':model,'pin':pin,'selectedModuleIds':['sales']}],'target':{'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-candidate','targetProfile':'spark4.0.1-delta4.0.0-candidate','bindingJson':binding_json,'bindingSha256':binding_sha},'options':{'allowCandidate':True}}
 entry=EngineRead.build(weft,request,parameters=Parameters,result=Order,method='ashlar-spark4')
 (out/'original-engine-request.json').write_text(json.dumps(request));(out/'engine-catalog.json').write_text(entry.artifact_json)
 class ReadPolicy:
  def bind_descriptor(self,descriptor,original,supplied):
   assert active['pins'] and original==vector;NativePolicy().validate_descriptor(descriptor,supplied)
  def admit_artifact(self,current,artifact,descriptor,supplied):
   assert active['pins'] and current['modules']==request['modules'] and artifact['modelPins']==[pin]
   assert artifact['bindingSha256']==binding_sha and json.loads(current['target']['bindingJson'])==binding
  def verify_native_profile(self,profile,supplied):
   assert active['pins'] and supplied is context
   if profile['targetProfile']!='spark4.0.1-delta4.0.0-candidate' or profile['engineVersion']!=spark.version or profile['deltaVersion']!='4.0.0' or spark.conf.get('spark.sql.ansi.enabled')!='true' or spark.conf.get('spark.sql.session.timeZone')!='UTC':raise ResolutionError('original native Spark profile drift')
   assert all(hashlib.sha256((args.jars/n).read_bytes()).hexdigest()==v for n,v in expected.items())
  def authorize_query(self,current,descriptor,supplied):NativePolicy().authorize(supplied,publication_id,(table,))
  def admit_record_integrity(self,check,artifact,supplied):
   if check.get('record')!=logical('order') or supplied is not context:raise ResolutionError('unadmitted original record integrity')
  def decode_result(self,column,value,current,supplied):
   assert active['pins'] and supplied is context
   representation=column['representation']
   if representation['kind']=='scalar' and representation['logicalType']=={'family':'string','facets':{},'nullable':False} and type(value)is str:return value
   if representation['kind']=='value':
    item=json.loads(value)
    if item.get('state')=='value' and set(item)=={'state','value'} and type(item['value'])is str:return item
   raise ResolutionError('original source exact string decoder refused')
  def authorize_result(self,rows,descriptor,supplied):
   assert active['pins'];NativePolicy().authorize(supplied,publication_id,(table,))
 @contextmanager
 def deadline(budget):
  group='typed-sdk-'+str(uuid.uuid4());spark.sparkContext.setJobGroup(group,'bounded original Ashlar typed read',interruptOnCancel=True)
  timer=threading.Timer(budget.total_seconds(),lambda:spark.sparkContext.cancelJobGroup(group));timer.daemon=True;timer.start()
  try:yield
  finally:timer.cancel();spark.sparkContext.setLocalProperty('spark.jobGroup.id',None);spark.sparkContext.setLocalProperty('spark.job.description',None);spark.sparkContext.setLocalProperty('spark.job.interruptOnCancel',None)
 target=AshlarTarget(compiler=weft,compiler_method='ashlar-spark4',executor=executor,backend=backend,pins=HeldPins(),vector=vector,policy=ReadPolicy(),context=context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={'typed-sdk-local':[source_sha]},binding_sha256=binding_sha,platform=(('spark','4.0.1'),('delta','4.0.0'),('postgresql','18.6')),default_timeout=timedelta(seconds=60),deadline=deadline,catalog_entries=(entry,))
 assert target.read(entry,Parameters(status='draft'))==[Order(id='a',status='draft')];checks.append('original published native typed read under real PostgreSQL pins')
 assert target.read(entry,Parameters(status="draft' OR true--"))==[];checks.append('native parameter binding')
 target.verify();checks.append('eager verification performs original held native read')
 spark.sql(f"UPDATE {table} SET props_json='{{\"1\":\"a\",\"2\":\"changed\"}}' WHERE id=1")
 assert target.read(entry,Parameters(status='draft'))==[Order(id='a',status='draft')];checks.append('original immutable publication remains at retained Delta version')
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
finally:
 if spark is not None:spark.stop()
 if started:run(pgbin/'pg_ctl','-D',pg,'-m','fast','-w','stop')
 (out/'verification.json').write_text(json.dumps({'checks':checks,'ownedPostgresStopped':not(pg/'postmaster.pid').exists(),'sparkStopped':spark is None or spark.sparkContext._jsc is None,'temporal':'HELD','scope':'local native Spark4/Delta4 with real PostgreSQL18 pin custody; no managed Databricks or accepted Truss ID claim'},indent=2)+'\n')
print(json.dumps({'status':'pass','checks':len(checks),'output':str(out)}))

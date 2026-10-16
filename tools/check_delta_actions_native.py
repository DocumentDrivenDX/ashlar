"""Actual generated SDK actions -> native issuer -> one Delta atomic commit."""
from pathlib import Path
from datetime import timedelta
from contextlib import contextmanager
import argparse,copy,hashlib,json,subprocess,sys,uuid
import psycopg,weft
from pyspark.sql import SparkSession
from ashlar.access_actions import DeltaActionExecutor
from ashlar_host.delta_action_admission import LocalDeltaActionAdmission
from umf_access.native_program import NativeProgram
from umf_access.protocols import Committed,Rejected,Denied,ActionFailed,Conflict,Indeterminate
parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--generated',type=Path,required=True);parser.add_argument('--model-request',type=Path,required=True);parser.add_argument('--jars',type=Path,required=True);args=parser.parse_args();out=args.output.absolute();out.mkdir(parents=True,exist_ok=False);root=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(args.generated.absolute()))
from native_sales.actions import Approve,ApproveInput,Actions,Guarded,PostFail,CreateOrder,CreateOrderInput,FirstFalse
from native_sales.keys import OrderKey
expected={'delta-spark_2.13-4.0.0.jar':'538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e','delta-storage-4.0.0.jar':'9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d'}
assert all(hashlib.sha256((args.jars/n).read_bytes()).hexdigest()==sha for n,sha in expected.items())
pgbin=Path('/opt/homebrew/opt/postgresql@18/bin');pg=out/'postgres';sock=out/'socket';sock.mkdir();started=False;spark=None;checks=[]
def run(*command):
 p=subprocess.run(list(map(str,command)),capture_output=True,text=True,check=True)
 with(out/'lifecycle.log').open('a')as f:f.write(p.stdout+p.stderr)
try:
 run(pgbin/'initdb','-D',pg,'-A','trust','-U','sdk_test','--no-sync');run(pgbin/'pg_ctl','-D',pg,'-l',out/'postgres.log','-o',f"-F -k {sock} -c listen_addresses='' -p 55497",'-w','start');started=True
 dsn=f'host={sock} port=55497 dbname=postgres user=sdk_test'
 spark=(SparkSession.builder.master('local[1]').appName('Ashlar native SDK Delta action E2E').config('spark.ui.enabled','false').config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.ansi.enabled','true').config('spark.sql.session.timeZone','UTC').config('spark.jars',','.join(str(args.jars/n)for n in expected)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir',str(out/'warehouse')).getOrCreate());spark.sparkContext.setLogLevel('ERROR')
 path=out/'orders';epoch=str(uuid.uuid4());spark.sql(f"CREATE TABLE delta.`{path}` (id STRING NOT NULL,status STRING NOT NULL) USING DELTA TBLPROPERTIES ('delta.isolationLevel'='Serializable','umf.actions.epoch'='{epoch}')");spark.sql(f"INSERT INTO delta.`{path}` VALUES ('o-1','draft')")
 identity=spark.sql(f'DESCRIBE DETAIL delta.`{path}`').first()['id']
 request=json.loads(args.model_request.read_bytes());request['target']='spark-delta4'
 programs=[]
 for index,action in enumerate(json.loads(request['binding']['modelJson'])['modules'][0]['extensions']['umf.actions']['actions']):
  selected=copy.deepcopy(request);selected['source']['action']=action['id'];selected['source']['actionPointer']=f'/modules/0/extensions/umf.actions/actions/{index}'
  programs.append(NativeProgram.build(weft,selected,bundle_input_digest=ApproveInput.UMF_BUNDLE_INPUT_DIGEST))
 with psycopg.connect(dsn,autocommit=True)as admin:
  admin.execute((root/'sql/typed-sdk-actions/01-local-admission.sql').read_text());admin.execute('CREATE ROLE native_approver NOLOGIN;CREATE ROLE sdk_person LOGIN;CREATE ROLE sdk_denied LOGIN;CREATE ROLE sdk_service LOGIN;GRANT ashlar_sdk_action_caller,native_approver TO sdk_service;GRANT ashlar_sdk_action_caller,native_approver TO sdk_person;GRANT ashlar_sdk_action_caller TO sdk_denied')
  for native_role,kind in (('sdk_person','human'),('sdk_denied','human'),('sdk_service','service')):
   LocalDeltaActionAdmission.enroll_subject(admin,native_role=native_role,principal_identity=('local:'+native_role).encode(),subject_kind=kind)
  for program in programs:LocalDeltaActionAdmission.enroll(admin,weft,program,path=path,table_uuid=identity,epoch=epoch,native_executor=spark.sparkContext.sparkUser())
 def factory(user):
  def connect():
   params=psycopg.conninfo.conninfo_to_dict(dsn);params['user']=user
   return psycopg.connect(**params,autocommit=True)
  return connect
 admission=LocalDeltaActionAdmission(spark,factory('sdk_person'),weft,maximum_rows=10)
 executor=DeltaActionExecutor(tuple(programs),admission,default_timeout=timedelta(seconds=60));actions=Actions(executor)
 def rows():return [(r['id'],r['status'])for r in spark.read.format('delta').load(str(path)).orderBy('id').collect()]
 def version():return int(spark.sql(f'DESCRIBE HISTORY delta.`{path}`').first()['version'])
 before=version();validated=actions.approve.validate(ApproveInput(order=OrderKey(id='o-1')))
 assert validated.valid and validated.complete and version()==before and rows()==[('o-1','draft')];checks.append('generated validate runs original preconditions with no native commit')
 outcome=actions.approve(ApproveInput(order=OrderKey(id='o-1')))
 assert isinstance(outcome,Committed),outcome
 assert outcome.changed==(OrderKey(id='o-1'),)and outcome.created==()and rows()==[('o-1','approved')]
 assert outcome.receipt.store==identity and outcome.receipt.epoch==epoch and int(outcome.receipt.version)==version()
 events=[json.loads(line)for line in(path/'_delta_log'/f'{int(outcome.receipt.version):020d}.json').read_text().splitlines()]
 metadata=json.loads(next(e['commitInfo']['userMetadata']for e in events if 'commitInfo'in e));assert metadata['markers']['verification']==[] and metadata['markers']['changes']and any('txn'in e for e in events)and any('add'in e for e in events)
 checks.append('generated typed committed view, exact changed Key and original same-commit verification/effects/data/receipt')
 before=version();again=actions.approve(ApproveInput(order=OrderKey(id='o-1')))
 assert isinstance(again,Committed)and not again.changed and again.original.no_op is True and rows()==[('o-1','approved')]and version()==before+1
 checks.append('new unkeyed invocation preserves semantic no-op with an original native marker commit')
 before=version();rejected=executor.invoke(Guarded,{'order':OrderKey(id='o-1')});assert isinstance(rejected,Rejected)and version()==before and rows()==[('o-1','approved')];checks.append('first false original precondition rejects without native data or marker commit')
 first=executor.invoke(FirstFalse,{'order':OrderKey(id='o-1')});assert isinstance(first,Rejected) and first.code=='FIRST' and len(json.loads(first.original.verification_json))==1
 try:executor.validate(FirstFalse,{'order':OrderKey(id='o-1')})
 except ActionFailed as error:assert error.code=='RULE_EVALUATION' and len(json.loads(error.original.verification_json))==2
 else:raise AssertionError('later absent-frame evaluation error was collapsed into false')
 assert version()==before;checks.append('invoke stops at first false; validate reaches later missing-frame evaluation error without commit')
 try:executor.invoke(PostFail,{'order':OrderKey(id='o-1')})
 except ActionFailed as error:assert error.code.startswith('POSTCONDITION_')
 else:raise AssertionError('failed original postcondition committed')
 assert version()==before and rows()==[('o-1','approved')];checks.append('postcondition failure discards complete candidate before native commit')
 created=executor.invoke(CreateOrder,{'id':'new','status':'draft'});assert isinstance(created,Committed)and created.created==(OrderKey(id='new'),)and rows()==[('new','draft'),('o-1','approved')];checks.append('native CREATE exposes exact generated nominal Key')
 denied=DeltaActionExecutor(tuple(programs),LocalDeltaActionAdmission(spark,factory('sdk_denied'),weft,maximum_rows=10),default_timeout=timedelta(seconds=60));before=version();result=denied.invoke(Approve,{'order':OrderKey(id='o-1')});assert isinstance(result,Denied)and version()==before;checks.append('independent real native login without source role is denied before Delta transaction')
 service=DeltaActionExecutor(tuple(programs),LocalDeltaActionAdmission(spark,factory('sdk_service'),weft,maximum_rows=10),default_timeout=timedelta(seconds=60));before=version();result=service.invoke(Approve,{'order':OrderKey(id='o-1')});assert isinstance(result,Denied)and version()==before;checks.append('native service Subject remains denied despite native action role membership')
 with factory('sdk_person')()as person:
  try:person.execute('SELECT * FROM ashlar_sdk_actions.program')
  except psycopg.errors.InsufficientPrivilege:checks.append('ordinary actor cannot read or enroll native original program custody')
  else:raise AssertionError('private original program registry exposed')
 from ashlar.access_bulk import DeltaBulkTarget
 from umf_access.protocols import ActionRefused
 (args.generated/'delta_commands.py').write_text('from umf_access import command,Param\nfrom native_sales.models import Order\nfrom native_sales.actions import Approve\n@command(action=Approve)\ndef approve_drafts(status:Param[str]):return Order.objects.filter(Order.c.status==status).update(status="approved")\n')
 config={'package':'native_sales','modules':['delta_commands'],'targets':[{'id':'owned','kind':'spark-delta','backendId':'weft-databricks','backendVersion':'raw-read/0.1','targetProfile':'weft.raw-read/0.1','binding':request['binding'],'platform':[{'name':'spark','versions':['4.0.1']},{'name':'delta','versions':['4.0.0']}],'security':request['security'],'roleMappings':request['roleMappings'],'sessionPrincipal':'human','maxItems':2}]}
 (args.generated/'delta-command-config.json').write_text(json.dumps(config))
 for extra in ([],['--check']):
  process=subprocess.run([sys.executable,'-B','-m','umf_access.build.catalog','catalog','--root',str(args.generated),'--config',str(args.generated/'delta-command-config.json'),*extra],capture_output=True,text=True)
  assert process.returncode==0,process.stdout+process.stderr
 from native_sales import queries
 bulk_program=queries._C0.variants[0].program
 with psycopg.connect(dsn,autocommit=True)as admin:LocalDeltaActionAdmission.enroll(admin,weft,bulk_program,path=path,table_uuid=identity,epoch=epoch,native_executor=spark.sparkContext.sparkUser())
 bulk_target=DeltaBulkTarget(admission,binding=request['binding'],default_timeout=timedelta(seconds=60))
 plan=queries.approve_drafts(bulk_target,status='draft');before=version();preview=plan.preview()
 assert preview.selected==(OrderKey(id='new'),)and version()==before;checks.append('real catalog CLI/check generates original Delta bulk plan; preview is advisory and commits nothing')
 spark.sql(f"INSERT INTO delta.`{path}` VALUES ('later','draft')")
 result=plan.execute();assert result.selected==2 and result.changed==2 and rows()==[('later','approved'),('new','approved'),('o-1','approved')]
 events=[json.loads(line)for line in(path/'_delta_log'/f'{int(result.receipt.version):020d}.json').read_text().splitlines()];metadata=json.loads(next(e['commitInfo']['userMetadata']for e in events if 'commitInfo'in e))
 assert metadata['markers']['counts']==[{'selected':2,'changed':2}]and len(metadata['markers']['changedKeys'])==2 and any('txn'in e for e in events)
 checks.append('bulk execute reselects later committed row and verifies complete candidate; data/counts/Keys share original native commit')
 empty=queries.approve_drafts(bulk_target,status='missing').execute();assert empty.selected==0 and empty.changed==0 and empty.original.no_op is True;checks.append('zero-selection bulk preserves original semantic no-op and native receipt')
 spark.sql(f"INSERT INTO delta.`{path}` VALUES ('x1','draft'),('x2','draft'),('x3','draft')");before=version()
 try:queries.approve_drafts(bulk_target,status='draft').execute()
 except ActionRefused as error:assert error.code=='BULK_LIMIT'
 else:raise AssertionError('over-bound complete native selection was truncated')
 assert version()==before;checks.append('maxItems plus sentinel refuses complete over-bound selection before native write')
 class CommitBoundaryAdmission(LocalDeltaActionAdmission):
  boundary=None
  @contextmanager
  def hold(self,*values,**options):
   with super().hold(*values,**options)as scope:
    original=scope.atomic.commit
    def commit(candidate,*,markers_json):
     return self.boundary(original,candidate,markers_json)
    scope.atomic.commit=commit
    yield scope
 conflict_admission=CommitBoundaryAdmission(spark,factory('sdk_person'),weft,maximum_rows=10)
 def concurrent(original,candidate,markers):
  # A separate actual native write lands after this action's original read.
  spark.sql(f"UPDATE delta.`{path}` SET status='independent' WHERE id='o-1'")
  return original(candidate,markers_json=markers)
 conflict_admission.boundary=concurrent
 conflict_executor=DeltaActionExecutor(tuple(programs),conflict_admission,default_timeout=timedelta(seconds=60))
 before=version();result=conflict_executor.invoke(Approve,{'order':OrderKey(id='o-1')})
 assert isinstance(result,Conflict)and version()==before+1 and dict(rows())['o-1']=='independent'and not conflict_admission.quarantined
 checks.append('actual independent Delta commit produces SDK Conflict with no retry or quarantine')
 fenced=CommitBoundaryAdmission(spark,factory('sdk_person'),weft,maximum_rows=10)
 def held_fence(original,candidate,markers):
  with psycopg.connect(dsn,autocommit=True)as independent:
   independent.execute("SET lock_timeout='100ms'")
   try:independent.execute('REVOKE native_approver FROM sdk_person')
   except psycopg.errors.LockNotAvailable:pass
   else:raise AssertionError('native role closure changed inside held action')
  return original(candidate,markers_json=markers)
 fenced.boundary=held_fence
 result=DeltaActionExecutor(tuple(programs),fenced,default_timeout=timedelta(seconds=60)).invoke(Approve,{'order':OrderKey(id='o-1')})
 assert isinstance(result,Committed)
 checks.append('independent native role revocation is fenced through actual Delta commit')
 lost=CommitBoundaryAdmission(spark,factory('sdk_person'),weft,maximum_rows=10)
 def lose_ack(original,candidate,markers):
  original(candidate,markers_json=markers)
  raise ConnectionError('test transport loses response after actual native commit')
 lost.boundary=lose_ack
 uncertain=DeltaActionExecutor(tuple(programs),lost,default_timeout=timedelta(seconds=60));before=version()
 result=uncertain.invoke(Approve,{'order':OrderKey(id='o-1')})
 assert isinstance(result,Indeterminate)and version()==before+1 and lost.quarantined and uncertain.quarantined
 try:uncertain.invoke(Approve,{'order':OrderKey(id='o-1')})
 except ActionRefused:pass
 else:raise AssertionError('uncertain original submission was automatically reused')
 assert version()==before+1
 checks.append('lost response after actual commit yields Indeterminate and quarantines without resubmission')
 (out/'original-programs.json').write_text(json.dumps([json.loads(p.original_json)for p in programs]))
finally:
 if spark is not None:spark.stop()
 if started:run(pgbin/'pg_ctl','-D',pg,'-m','fast','-w','stop')
 (out/'verification.json').write_text(json.dumps({'checks':checks,'ownedPostgresStopped':not(pg/'postmaster.pid').exists(),'sparkStopped':spark is None or spark.sparkContext._jsc is None,'temporal':'HELD','scope':'local native PostgreSQL-issued actor/program admission and original Weft scalar single-table Delta actions; no Truss protected authority or managed Delta claim'},indent=2)+'\n')
print(json.dumps({'status':'pass','checks':len(checks),'output':str(out)}))

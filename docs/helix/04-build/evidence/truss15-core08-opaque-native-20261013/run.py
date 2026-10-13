import os,sys,json,tempfile,time
from pathlib import Path
import psycopg,psycopg.pq,jsonschema,referencing
root=Path(__file__).parent
(root/'ipc').mkdir();os.environ['TMPDIR']=str(root/'ipc');tempfile.tempdir=None
os.environ.update(JAVA_HOME='/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home',PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable)
for p in reversed(json.loads(Path('/private/tmp/ashlar-commerce-graphframes-installed-prep-j/command-j.json').read_bytes())['sdkRoots']['canonical']):sys.path.insert(0,p)
sys.path.insert(0,'/private/tmp/ashlar-truss15-parallel-example-20261013-a/ashlar-source/src');sys.path.insert(0,'/private/tmp/ashlar-truss15-parallel-example-20261013-a/truss-target')
import truss,ashlar,importlib.metadata
if importlib.metadata.version('truss-toolkit')!='0.0.15' or not Path(truss.__file__).resolve().is_relative_to(Path('/private/tmp/ashlar-truss15-parallel-example-20261013-a/truss-target')):raise ValueError('Actual isolated released Truss15 required')
if not Path(ashlar.__file__).resolve().is_relative_to(Path('/private/tmp/ashlar-truss15-parallel-example-20261013-a/ashlar-source/src')):raise ValueError('Immutable selected Ashlar source required')
from truss import LocalPostgres
from ashlar_host.lifecycle import owned_context
from ashlar_host.truss_example import private_file
from ashlar_host.truss_local_setup import run_setup_truss_local_command
from ashlar_host.truss_record_mutation import run_create_truss_record_command
from ashlar_host.truss_note_cli import run_publish_truss_graph_command
from ashlar_host.truss_resume_setup import run_configure_truss_resume_command
from ashlar_host.truss_record_read import run_read_truss_record_command
from ashlar_host.truss_note_publication import read_page
import base64,hashlib
start=time.monotonic();result=None
def save(name,value):return private_file(root/name,value)
def stage(name):print(json.dumps({'stage':name}),flush=True)
try:
 original=Path('/Users/erik/Projects/ashlar/examples/truss-native/core08-asset.umf.json').read_bytes()
 if hashlib.sha256(original).hexdigest()!='6ca475404a77ace5b963717b341cdea9934948a520f56eaea487441a11f85239' or json.loads(original)['umf']!='0.8.0':raise ValueError('Pinned original declared08 bytes required')
 model=save('model.umf.json',original);origin=save('origin.json',{'operator':'ashlar-core08-local-source'})
 selection={'bun':'/opt/homebrew/Cellar/bun/1.4.2/bin/bun','document_path':str(model),'document_id':'ashlar-core08-asset','document_revision':'r1','origin_path':str(origin)}
 stage('accept-original08')
 accepted=run_setup_truss_local_command(save('setup.json',{'data_directory':str(root/'data'),**selection,'request_key':'core08-accept'}),root/'accepted.json')
 report=json.loads(base64.b64decode(accepted['report_original_base64']));artifact=report['acceptedInput']['documents'][0]['artifact']
 if base64.b64decode(artifact['bytesBase64'])!=original or artifact['sha256']!=hashlib.sha256(original).hexdigest():raise ValueError('Original accepted model bytes differ')
 with owned_context(LocalPostgres(root/'data'))as runtime:
  parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri);administrator=psycopg.conninfo.make_conninfo(**parameters);parameters['user']=accepted['roles']['consumer'];ordinary=psycopg.conninfo.make_conninfo(**parameters)
  stage('create-original08')
  creation=save('create.json',{'connection_uri':ordinary,**selection,'module_id':'assets','record_id':'Asset','request_key':'core08-create','values':{'asset-key':{'kind':'string','text':'A1雪'},'units':{'kind':'integer','text':'9007199254740993'},'valuation':{'kind':'decimal','text':'1.2300'},'enabled':{'kind':'boolean','value':True}}})
  created=run_create_truss_record_command(creation,root/'created.json');obj=created['object']
  stage('publish-original08')
  publish=save('publish.json',{'connection_uri':ordinary,'administrator_uri':administrator,'reader_role':accepted['roles']['reader'],'consumer_id':'core08-delta','feed':'core08-source','jars':['/private/tmp/ashlar-supply-chain-finite-native-jars-f/delta-spark_2.13-4.0.0.jar','/private/tmp/ashlar-supply-chain-finite-native-jars-f/delta-storage-4.0.0.jar'],'maximum_pages':2,'maximum_page_bytes':2097152})
  published=run_publish_truss_graph_command(publish,root/'publication')
  if len(published['publications'])!=2:raise ValueError('Two complete original publications required')
  run_configure_truss_resume_command(publish,root/'publication/publication-intent-2.json',root/'reader.json')
  def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
  before=read_page(connection,'core08-delta',maximum_bytes=2097152).prior.content
  stage('read-original08')
  observed=run_read_truss_record_command(root/'reader.json',root/'record.json',type_id=obj['type_id'],entity_id=obj['id'])
  expected={'asset-key':('value','A1雪'),'units':('value',{'integer_token':'9007199254740993'}),'valuation':('value',{'decimal_token':'1.2300'}),'enabled':('value',True)}
  if observed['publication_id']!=published['publications'][-1]['manifest']['publication_id'] or observed['type_id']!=obj['type_id'] or observed['entity_id']!=obj['id'] or (observed['record']['document_id'],observed['record']['module_id'],observed['record']['record_id'])!=('ashlar-core08-asset','assets','Asset'):raise ValueError('Original publication and named record identity differ')
  fields=observed['record']['fields']
  if len(fields)!=4 or {f['field_id']:(f['state'],f['value'])for f in fields}!=expected:raise ValueError('Complete original typed fields differ')
  if before!=read_page(connection,'core08-delta',maximum_bytes=2097152).prior.content:raise ValueError('Named read changed source checkpoint')
 if model.read_bytes()!=original:raise ValueError('Original model changed')
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'umf_version':'0.8.0','model_sha256':hashlib.sha256(original).hexdigest(),'publications':2,'objects':1,'fields':4,'publication_id':observed['publication_id'],'unknown_extension_original_bytes_retained':True,'accepted_public_report':report,'unknown_extension_semantic_enforcement':False,'source_checkpoint_unchanged_by_read':True,'runtime':{'truss_version':'0.0.15','ashlar_source':'adf0ffcd7546bb04a5de1466241d002cc59c1a99','postgresql_major':'18','psycopg':psycopg.__version__,'spark':'4.0.1','delta':'4.0.0'},'scope':'Separately authored original declaredUMF0.8 fixture, actual public acceptance/create/committed readback/replay, two feed publications and exact complete named singleton. Unknown extension retained opaque with original preparation diagnostics; no unknown semantic enforcement. Caller-asserted Truss ACK, no protected PostgreSQL-outbox/F8/Weft/UC qualification.'}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:save('run-result.json',result);print(json.dumps(result),flush=True)

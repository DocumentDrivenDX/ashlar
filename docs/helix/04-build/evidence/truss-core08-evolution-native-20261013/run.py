import os,sys,json,tempfile,time
from pathlib import Path
import psycopg,psycopg.pq,jsonschema,referencing
root=Path(__file__).parent
(root/'ipc').mkdir();os.environ['TMPDIR']=str(root/'ipc');tempfile.tempdir=None
os.environ.update(JAVA_HOME='/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home',PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable)
for p in reversed(json.loads(Path('/private/tmp/ashlar-commerce-graphframes-installed-prep-j/command-j.json').read_bytes())['sdkRoots']['canonical']):sys.path.insert(0,p)
sys.path.insert(0,'/private/tmp/ashlar-truss-sdk-pinned-1f203768/packages/python/src');sys.path.insert(0,'/Users/erik/Projects/ashlar/src')
from truss import LocalPostgres
from ashlar_host.lifecycle import owned_context
from ashlar_host.truss_example import private_file
from ashlar_host.truss_evolve_graph import run_evolve_truss_graph_command
from ashlar_host.truss_resume_setup import run_configure_truss_resume_command
from ashlar_host.truss_record_read import run_read_truss_record_command
from ashlar_host.truss_note_publication import read_page
import base64,hashlib
start=time.monotonic();result=None
try:
 source=Path('/private/tmp/ashlar-truss-core08-simple-native-20261013-a');seed=Path('/private/tmp/ashlar-truss-core08-evolution-seed-20261013-a')
 accepted=json.loads((source/'accepted.json').read_bytes());created=json.loads((seed/'created.json').read_bytes())['object']
 original=(source/'model.umf.json').read_bytes();revised=Path('/Users/erik/Projects/ashlar/examples/truss-native/core08-simple-asset-r2.umf.json').read_bytes()
 model=private_file(root/'model-r2.umf.json',revised)
 with owned_context(LocalPostgres(source/'data'))as runtime:
  parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri);administrator=psycopg.conninfo.make_conninfo(**parameters);parameters['user']=accepted['roles']['consumer'];ordinary=psycopg.conninfo.make_conninfo(**parameters)
  selected=json.loads((source/'publish.json').read_bytes());selected.update(connection_uri=ordinary,administrator_uri=administrator);publish=private_file(root/'live-publish.json',selected)
  last=json.loads((seed/'continued.json').read_bytes())['publications'][-1]
  run_configure_truss_resume_command(publish,last['publication_intent']['path'],root/'original-reader.json')
  config={'connection_uri':ordinary,'bun':'/opt/homebrew/Cellar/bun/1.4.2/bin/bun','document_path':str(model),'document_revision':'r2','document_id':'ashlar-core08-simple-asset','module_id':'assets','record_id':'Asset','origin_path':str(source/'origin.json'),'object_id':created['id'],'accept_request_key':'core08-evolution-accept-r2','update_request_key':'core08-evolution-set-note','field_name':'note','field_value':'evolved雪'}
  answer=run_evolve_truss_graph_command(private_file(root/'configuration.json',config),root/'original-reader.json',root/'evolution')
  if answer['publications']!=2:raise ValueError('Two complete original evolution publications required')
  archive=json.loads((root/'evolution/evolution.json').read_bytes())
  if base64.b64decode(archive['previous_document']['content_original_base64'])!=original or base64.b64decode(archive['accepted_document']['content_original_base64'])!=revised:raise ValueError('Exact declared original/revised model bytes differ')
  if archive['before']['values']!=archive['evolved']['values'] or archive['before']['version']!=archive['evolved']['version'] or archive['after']['version']!=archive['before']['version']+1:raise ValueError('Catalog changed original values/version')
  original_reader=json.loads((root/'original-reader.json').read_bytes());original_reader['archived_acceptance_files']=[{'path':str(root/'evolution/evolution.json'),'sha256':hashlib.sha256((root/'evolution/evolution.json').read_bytes()).hexdigest()}]
  private_file(root/'original-archived-reader.json',original_reader)
  def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
  before=read_page(connection,'core08-delta',maximum_bytes=2097152).prior.content
  reads={}
  for name,path in [('original',root/'original-archived-reader.json'),('evolved',root/'evolution/reader-configuration.json')]:
   observed=run_read_truss_record_command(path,root/(name+'-record.json'),type_id=created['type_id'],entity_id=created['id'])
   expected={'asset-key':('value','A2雪'),'units':('value',{'integer_token':'9007199254740993'}),'valuation':('value',{'decimal_token':'1.2300'}),'enabled':('value',True)}
   if name=='evolved':expected['note']=('value','evolved雪')
   record=observed['record']
   if record is None or len(record['fields'])!=len(expected)or {f['field_id']:(f['state'],f['value'])for f in record['fields']}!=expected:raise ValueError('Complete original/evolved named fields differ')
   reads[name]={'publication_id':observed['publication_id'],'fields':len(expected),'entity_version':record['row']['entity_version'],'schema_revision':record['row']['schema_revision']}
  if before!=read_page(connection,'core08-delta',maximum_bytes=2097152).prior.content:raise ValueError('Historical reads changed source checkpoint')
 if (source/'model.umf.json').read_bytes()!=original or model.read_bytes()!=revised:raise ValueError('Original model custody changed')
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'umf_version':'0.8.0','original_sha256':hashlib.sha256(original).hexdigest(),'revised_sha256':hashlib.sha256(revised).hexdigest(),'publications':2,'reads':reads,'source_checkpoint_unchanged_by_reads':True,'scope':'Same-declared-version originalUMF0.8 additive optionalSTRING field: actual public catalog acceptance preserves original values/identity/version, expected-version field update and receipt replay, two complete local Delta publications, original/evolved named reads. Caller-asserted Truss ACK only; no protectedF8/unknown-extension/Weft/UC qualification.'}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:private_file(root/'result.json',result);print(json.dumps(result),flush=True)

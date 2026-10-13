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
from ashlar_host.truss_record_update import run_update_truss_record_command
from ashlar_host.truss_record_delete import run_delete_truss_record_command
from ashlar_host.truss_resume_setup import run_configure_truss_resume_command
from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
from ashlar_host.truss_record_read import run_read_truss_record_command
from ashlar_host.truss_note_publication import read_page
import hashlib
start=time.monotonic();result=None
def save(name,value):return private_file(root/name,value)
def stage(name):print(json.dumps({'stage':name}),flush=True)
try:
 source=Path('/private/tmp/ashlar-truss-core08-simple-native-20261013-a')
 accepted=json.loads((source/'accepted.json').read_bytes());created=json.loads((source/'created.json').read_bytes())['object']
 original=(source/'model.umf.json').read_bytes()
 with owned_context(LocalPostgres(source/'data'))as runtime:
  parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri);administrator=psycopg.conninfo.make_conninfo(**parameters);parameters['user']=accepted['roles']['consumer'];ordinary=psycopg.conninfo.make_conninfo(**parameters)
  selected=json.loads((source/'publish.json').read_bytes());selected.update(connection_uri=ordinary,administrator_uri=administrator)
  publish=save('live-publish.json',selected)
  run_configure_truss_resume_command(publish,source/'publication/publication-intent-2.json',root/'original-reader.json')
  common={'connection_uri':ordinary,'bun':'/opt/homebrew/Cellar/bun/1.4.2/bin/bun','document_path':str(source/'model.umf.json'),'document_revision':'r1','document_id':'ashlar-core08-simple-asset','module_id':'assets','record_id':'Asset','origin_path':str(source/'origin.json'),'object_id':created['id']}
  stage('reuse-original-completed-update')
  updated=json.loads(Path('/private/tmp/ashlar-truss-core08-lifecycle-native-20261013-a/updated.json').read_bytes())
  save('updated.json',updated)
  if updated['object']['version']!=2 or any(updated['object'][k]!=created[k]for k in ('id','type_id','definition_pin','document_id','module_id','record_id','catalog_revision','record_revision','retained')):raise ValueError('Exact original update identity/version differs')
  stage('delete-original08')
  deleted=run_delete_truss_record_command(save('delete.json',{**common,'request_key':'core08-lifecycle-delete-20261013-a','expected_version':2}),root/'deleted.json')
  stage('publish-two-pages')
  continued=run_continue_truss_graph_command(root/'original-reader.json',root/'continued.json',maximum_pages=2)
  if len(continued['publications'])!=2:raise ValueError('Two complete original continuation publications required')
  for name,item in zip(('updated','deleted'),continued['publications']):
   run_configure_truss_resume_command(publish,item['publication_intent']['path'],root/(name+'-reader.json'))
  def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
  before=read_page(connection,'core08-delta').prior.content
  reads={}
  for name,wanted,version in [('original',('9007199254740993','1.2300',True),1),('updated',('9007199254740994','2.3400',False),2),('deleted',None,None)]:
   stage(name+'-read')
   observed=run_read_truss_record_command(root/(name+'-reader.json'),root/(name+'-record.json'),type_id=created['type_id'],entity_id=created['id'])
   record=observed['record']
   if wanted is None:
    if record is not None:raise ValueError('Deleted singleton remains visible')
   else:
    expected={'asset-key':('value','A1雪'),'units':('value',{'integer_token':wanted[0]}),'valuation':('value',{'decimal_token':wanted[1]}),'enabled':('value',wanted[2])}
    if len(record['fields'])!=4 or {f['field_id']:(f['state'],f['value'])for f in record['fields']}!=expected or record['row']['entity_version']!=str(version):raise ValueError('Exact complete historical scalar fields differ')
   reads[name]={'publication_id':observed['publication_id'],'present':record is not None,'entity_version':version}
  if before!=read_page(connection,'core08-delta').prior.content:raise ValueError('Reads changed source checkpoint')
 if (source/'model.umf.json').read_bytes()!=original:raise ValueError('Original model changed')
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'umf_version':'0.8.0','model_sha256':hashlib.sha256(original).hexdigest(),'source_operations':2,'contiguous_publications':2,'reads':reads,'source_checkpoint_unchanged_by_reads':True,'scope':'Same original core-only0.8 model; actual public expected-version update/delete, exact source readback/receipt replay, two local Delta continuations and complete original/intermediate/deleted named singletons. Caller-asserted Truss ACK; no protected outbox/F8/unknown-extension/evolution/Weft/UC qualification.'}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:save('run-result.json',result);print(json.dumps(result),flush=True)

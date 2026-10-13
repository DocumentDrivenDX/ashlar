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
from ashlar_host.truss_local_setup import run_setup_truss_local_command
from ashlar_host.truss_record_mutation import run_create_truss_record_command
from ashlar_host.truss_typed_edge_command import run_create_truss_edge_command
from ashlar_host.truss_note_cli import run_publish_truss_graph_command
from ashlar_host.truss_resume_setup import run_configure_truss_resume_command
from ashlar_host.truss_record_read import run_read_truss_record_command
from ashlar_host.truss_typed_related_query_cli import run_query_truss_typed_related_keys_command
from ashlar_host.truss_note_publication import read_page

def save(name,value):return private_file(root/name,value)
def stage(name):print(json.dumps({'stage':name}),flush=True)
from ashlar_host.truss_graph_export_cli import run_export_truss_graph_command
from ashlar_host.graphframes_export_setup import run_configure_graphframes_export_command
import hashlib
start=time.monotonic();result=None
try:
 source=Path('/private/tmp/ashlar-truss-core08-simple-native-20261013-a');previous=Path('/private/tmp/ashlar-truss-core08-evolution-native-20261013-a')
 bootstrap=json.loads((source/'accepted.json').read_bytes())
 from ashlar_host.local_graph_export import LocalGraphExportSelection,open_local_graph_export
 from ashlar.schema import decode_retained_json
 with owned_context(LocalPostgres(source/'data'))as runtime:
  parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri);parameters['user']=bootstrap['roles']['consumer'];ordinary=psycopg.conninfo.make_conninfo(**parameters)
  def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
  publish=json.loads((source/'publish.json').read_bytes());consumer=publish['consumer_id']
  before=read_page(connection,consumer,maximum_bytes=2097152).prior.content
  exports=[];nodes=[]
  for label,path,record in (('seed',previous/'original-archived-reader.json',previous/'original-record.json'),('evolved',previous/'evolution/reader-configuration.json',previous/'evolved-record.json')):
   selected=json.loads(path.read_bytes());selected['connection_uri']=ordinary;configuration=save(label+'-reader.json',selected)
   export=run_export_truss_graph_command(configuration,root/(label+'-export.json'));exports.append(export)
   graph=json.loads(Path(export['release']['path']).read_bytes())
   expected=json.loads(record.read_bytes())['record']['row']
   if (export['nodes'],export['edges'])!=(1,0):raise ValueError('Complete single-node empty-edge graph required')
   node=graph['nodes'][0]
   if node['props_json']!=expected['props_json']or node['retained_json']!=expected['retained_json']or node['schema_revision']!=expected['schema_revision']or node['entity_version']!=expected['entity_version']:raise ValueError('Exact original field/version carrier differs')
   props=decode_retained_json(node['props_json'].encode())
   if len(props)!=(4 if label=='seed' else 5):raise ValueError('Exact complete field inventory differs')
   nodes.append(node)
  if nodes[0]['graph_id']!=nodes[1]['graph_id']or nodes[0]['id']!=nodes[1]['id']:raise ValueError('Evolution changed graph identity')
  after=read_page(connection,consumer,maximum_bytes=2097152).prior.content
  if before!=after:raise ValueError('Export changed source checkpoint')
 mixed=[]
 for left,right in ((exports[0],exports[1]),(exports[1],exports[0])):
  selected=LocalGraphExportSelection(Path(left['release']['path']),Path(right['custody']['path']),left['release']['sha256'],right['custody']['sha256'])
  try:
   with owned_context(open_local_graph_export(selected)):pass
  except (ValueError,PermissionError)as error:mixed.append(type(error).__name__)
  else:raise ValueError('Mixed release/custody pair admitted')
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'source_sdk_commit':'1f203768092178f356f267cd047ae149c08541f3','umf_version':'0.8.0','exports':exports,'stable_graph_id':nodes[0]['graph_id'],'field_counts':[4,5],'exact_original_numeric_and_presence_carriers':True,'mixed_pairs_refused':mixed,'source_checkpoint_unchanged':True,'qualification':'Actual private immutable historical/evolved exports under original source and full publication guards, followed by original local export mixed-pair admission refusal. Opaque props JSON remains exact. No mutations, ACK, ingest, external graph engine or numeric graph filtering qualification.'}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:save('run-result.json',result);print(json.dumps(result),flush=True)

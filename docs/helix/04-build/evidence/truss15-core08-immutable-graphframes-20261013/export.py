import os,sys,json,tempfile,time
from pathlib import Path
import psycopg,psycopg.pq,jsonschema,referencing
root=Path(__file__).parent
(root/'ipc').mkdir();os.environ['TMPDIR']=str(root/'ipc');tempfile.tempdir=None
os.environ.update(JAVA_HOME='/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home',PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable)
for p in reversed(json.loads(Path('/private/tmp/ashlar-commerce-graphframes-installed-prep-j/command-j.json').read_bytes())['sdkRoots']['canonical']):sys.path.insert(0,p)
sys.path.insert(0,'/private/tmp/ashlar-truss15-parallel-example-20261013-a/ashlar-source/src');sys.path.insert(0,'/private/tmp/ashlar-truss15-parallel-example-20261013-a/truss-target')
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
from ashlar_host.truss_example_reader import run_configure_truss_example_reader_command
import truss,ashlar,importlib.metadata
assert importlib.metadata.version('truss-toolkit')=='0.0.15'
assert Path(truss.__file__).resolve().is_relative_to(Path('/private/tmp/ashlar-truss15-parallel-example-20261013-a/truss-target'))
start=time.monotonic();result=None
try:
 source=Path('/private/tmp/ashlar-truss15-core08-native-20261013-b')
 protected=['model.umf.json','accepted.json','created.json','record.json','publication/source-page-1.json','publication/source-page-2.json','publication/publication-intent-2.json','publication/result.json']
 original_hashes={n:hashlib.sha256((source/n).read_bytes()).hexdigest()for n in protected}
 bootstrap=json.loads((source/'accepted.json').read_bytes())
 with owned_context(LocalPostgres(source/'data'))as runtime:
  live=save('live-connection.json',{'profile':'ashlar-local-truss-connection/0.1','data_directory':str(source/'data'),'connection_uri':runtime.info.connection_uri})
  run_configure_truss_example_reader_command(source,live,root/'reader.json',reader_configuration=source/'reader.json')
  ordinary=json.loads((root/'reader.json').read_bytes())['connection_uri']
  def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
  before=read_page(connection,'core08-delta',maximum_bytes=2097152).prior.content
  export=run_export_truss_graph_command(root/'reader.json',root/'export.json')
  if (export['nodes'],export['edges'],export['publication_id'])!=(1,0,'truss-graph-2')or before!=read_page(connection,'core08-delta',maximum_bytes=2097152).prior.content:raise ValueError('Complete graph or source checkpoint differs')
  release=json.loads(Path(export['release']['path']).read_bytes());node=release['nodes'][0]
  expected={f['property_id']:f['value'].get('integer_token',f['value'].get('decimal_token'))if type(f['value'])is dict else f['value']for f in json.loads((source/'record.json').read_bytes())['record']['fields']}
  if json.loads(node['props_json'],parse_int=str,parse_float=str)!=expected:raise ValueError('Original exact four scalar carriers differ')
  original_object=json.loads((source/'created.json').read_bytes())['object']
  if (str(node['id']),str(node['type_id']))!=(str(original_object['id']),str(original_object['type_id'])):raise ValueError('Actual source identity differs')
 if original_hashes!={n:hashlib.sha256((source/n).read_bytes()).hexdigest()for n in protected}:raise ValueError('Original opaque source evidence changed')
 jars=tuple('/private/tmp/ashlar-graphframes-installed-20261010-c/jars/'+n for n in ('io.graphframes_graphframes-spark3_2.12-0.12.3.jar','io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar'))
 run_configure_graphframes_export_command(str(root/'export.json'),hashlib.sha256((root/'export.json').read_bytes()).hexdigest(),root/'graphframes.json',jars=jars,java_home='/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home')
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'publication_id':export['publication_id'],'nodes':1,'edges':0,'source_checkpoint_unchanged':True,'original_source_file_hashes':original_hashes,'release_sha256':export['release']['sha256'],'custody_sha256':export['custody']['sha256'],'qualification':'Existing original declaredUMF08 source/publication immutable export only; exact four known scalar carrier tokens and source identity, opaque extension preservation; no unknown enforcement, numeric graph filtering, Weft, protectedACK/F8/UC.'}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:save('run-result.json',result);print(json.dumps(result),flush=True)

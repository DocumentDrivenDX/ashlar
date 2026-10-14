import os,sys,tempfile,time,json,base64,hashlib,importlib.metadata
from pathlib import Path
import psycopg,psycopg.pq,jsonschema,referencing
root=Path(__file__).parent;(root/'ipc').mkdir();os.environ['TMPDIR']=str(root/'ipc');tempfile.tempdir=None
os.environ.update(JAVA_HOME='/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home',PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable)
for p in reversed(json.loads(Path('/private/tmp/ashlar-commerce-graphframes-installed-prep-j/command-j.json').read_bytes())['sdkRoots']['canonical']):sys.path.insert(0,p)
sys.path.insert(0,str(root/'target'));sys.path.insert(0,'/private/tmp/ashlar-truss15-parallel-example-20261013-a/truss-target')
from contextlib import nullcontext
from ashlar_host.lifecycle import owned_context
from ashlar_host.truss_example import private_file
from ashlar_host.truss_example_reader import run_configure_truss_example_reader_command
from ashlar_host.truss_note_publication import read_page
from decimal import Decimal
from ashlar.cli import main
import ashlar,ashlar_host.truss_typed_query_batch as batch_module,truss
from decimal import Decimal
start=time.monotonic();result={'outcome':'failed'}
try:
 assert Path(batch_module.__file__).is_relative_to(root/'target')
 assert importlib.metadata.version('truss-toolkit')=='0.0.15'
 private_file(root/'origins.json',{'ashlar_module':ashlar.__file__,'batch_module':batch_module.__file__,'truss_module':truss.__file__,'truss_version':importlib.metadata.version('truss-toolkit'),'wheel_sha256':hashlib.sha256(next((root/'wheels').glob('*.whl')).read_bytes()).hexdigest()})
 example=Path('/private/tmp/ashlar-truss15-parallel-example-20261013-a/example')
 with nullcontext():
  live=root/'live.json'
  reader=root/'reader.json';run_configure_truss_example_reader_command(example,live,reader,reader_configuration=example/'reader-configuration.json');config=json.loads(reader.read_bytes())
  def connection():return psycopg.connect(config['connection_uri'],autocommit=False,connect_timeout=5)
  before=read_page(connection,'example-related-delta',maximum_bytes=2097152).prior.content;private_file(root/'checkpoint-before.json',{'cursor_base64':base64.b64encode(before).decode()})
  batch=json.loads((root/'batch-selection.json').read_bytes())
  private_file(root/'batch.json',batch)
  sys.argv=['ashlar','query-truss-typed-batch','--configuration',str(reader),'--batch',str(root/'batch.json'),'--output',str(root/'batch-result.json')]
  main()
  answer=json.loads((root/'batch-result.json').read_bytes());queries=answer['queries']
  after=read_page(connection,'example-related-delta',maximum_bytes=2097152).prior.content;private_file(root/'checkpoint-after.json',{'cursor_base64':base64.b64encode(after).decode()})
  assert before==after
  assert answer['publication_id']=='truss-graph-5' and len(queries)==1
  assert queries[0]['rows']==[{'root':'R1'}]*4
  assert queries[0]['response_original_json']==queries[0]['recompiled_original_json']
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'publication_id':answer['publication_id'],'rows':[q['rows']for q in queries],'source_checkpoint_unchanged':True,'qualification':'Fresh installed Ashlar wheel public typed batch CLI; original releasedTruss15 source pub5; one authored two-hop STRING root projection, unchanged original protocols/indexed recompile/fullguards and ownerclosure. No mutations, publication or ACK.'}
except BaseException as e:
 result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(e).__name__};raise
finally:private_file(root/'run-result.json',result);print(json.dumps(result),flush=True)

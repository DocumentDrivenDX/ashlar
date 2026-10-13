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
from ashlar_host.truss_resume_setup import run_configure_truss_resume_command
from ashlar_host.truss_resume_cli import run_resume_truss_graph_command
from ashlar_host.truss_feed_sessions import TrussFeedSessions
from ashlar_host.truss_note_publication import read_page
from unittest.mock import patch
import hashlib
start=time.monotonic();result=None
original_reconcile=TrussFeedSessions.reconcile;observed=[];ack_calls=[]
def reconcile(owner,*args,**kwargs):
 value=original_reconcile(owner,*args,**kwargs);observed.append(value);return value
def forbidden_ack(*args,**kwargs):ack_calls.append(True);raise AssertionError('Restart must reconcile, never acknowledge again')
def heads(publication):
 return {role:{p.name:hashlib.sha256(p.read_bytes()).hexdigest()for p in sorted((publication/role/'_delta_log').glob('*.json'))}for role in ('attempts','manifest','object_current','edge_current','tombstone','whole_source_history')}
try:
 source=Path('/private/tmp/ashlar-truss-typed-related-native-20261013-a');initial=Path('/private/tmp/ashlar-truss-receipt-commit-loss-native-20261013-a');publication=initial/'publication'
 original_publication=json.loads((publication/'result.json').read_bytes())['publications'][0]['manifest']
 inventory=heads(publication)
 bootstrap=json.loads((source/'accepted.json').read_bytes())
 with owned_context(LocalPostgres(source/'data'))as runtime:
  parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri);administrator=psycopg.conninfo.make_conninfo(**parameters);parameters['user']=bootstrap['roles']['consumer'];ordinary=psycopg.conninfo.make_conninfo(**parameters)
  config=json.loads((initial/'publication-configuration.json').read_bytes());config.update(connection_uri=ordinary,administrator_uri=administrator)
  publisher=private_file(root/'live-publication.json',config)
  run_configure_truss_resume_command(publisher,publication/'publication-intent-1.json',root/'resume.json')
  def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
  before=read_page(connection,config['consumer_id'],maximum_bytes=2097152).prior.content
  with patch.object(TrussFeedSessions,'acknowledge',forbidden_ack),patch.object(TrussFeedSessions,'reconcile',reconcile):
   resumed=run_resume_truss_graph_command(root/'resume.json',root/'resumed.json')
  if resumed['manifest']!=original_publication or len(observed)!=1 or not observed[0].get('receipt_observed')or not observed[0].get('source_receipt_retained')or ack_calls:raise ValueError('Original publication and fresh committed receipt differ')
  if before!=read_page(connection,config['consumer_id'],maximum_bytes=2097152).prior.content or inventory!=heads(publication):raise ValueError('Restart changed source checkpoint or native Delta commit inventory')
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'publication_id':resumed['manifest']['publication_id'],'request_digest':resumed['request_digest'],'original_manifest_equal':True,'source_checkpoint_unchanged':True,'delta_json_commit_inventory_unchanged':inventory,'receipt_reconciliation':observed,'source_ack_calls':len(ack_calls),'fresh_process_and_source_runtime':True,'qualification':'Actual public resume of the original receipt-backed catalog-only Delta publication after prior process/source-owner closure. Full original manifest and six-role Delta JSON commit files unchanged; source checkpoint unchanged and exact source-retained receipt freshly reconciled without another ACK. No new publication or ingest, full typed graph restart, missing-journal reconstruction, source history release, protected outbox or Unity Catalog qualification.'}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:private_file(root/'run-result.json',result);print(json.dumps(result),flush=True)

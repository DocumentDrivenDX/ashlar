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
from ashlar_host.truss_note_cli import run_publish_truss_graph_command
from ashlar_host.truss_note_publication import read_page
from ashlar_host.truss_feed_sessions import TrussFeedSessions
from ashlar_host.ack import AckOutcomeUncertain
from ashlar_host.driver import NativeDriver
import ashlar_host.truss_feed_sessions as ports
from unittest.mock import patch
from contextlib import contextmanager
import inspect,hashlib

def save(name,value):return private_file(root/name,value)
start=time.monotonic();result=None
original_commit=psycopg.Connection.commit;original_ack=TrussFeedSessions.acknowledge;original_reconcile=TrussFeedSessions.reconcile;original_assembly=ports._assembly
events=[];injected=[];uncertain=[];observed=[];pages=[];driver_ack=[]
original_driver_ack=NativeDriver.acknowledge
def capture_driver_ack(driver,*args,**kwargs):
 value=original_driver_ack(driver,*args,**kwargs);driver_ack.extend(driver.acks);return value
def lost_response(connection):
 caller=inspect.currentframe().f_back
 selected=not injected and caller.f_code.co_name=='_perform_receipt'and caller.f_globals.get('__name__')=='ashlar_host.truss_feed_sessions'and caller.f_locals.get('reconcile')is False
 original_commit(connection)
 if selected:
  injected.append({'backend_pid':connection.info.backend_pid,'exact_psycopg_connection':type(connection)is psycopg.Connection,'actual_commit_completed':True})
  raise OSError('Injected response loss after actual PostgreSQL COMMIT')
def acknowledge(owner,*args,**kwargs):
 events.append('acknowledge');pages.append({'prior':owner.page.prior.content,'next':owner.page.next.content,'sha256':owner.page.sha256})
 try:return original_ack(owner,*args,**kwargs)
 except AckOutcomeUncertain as error:
  uncertain.append({'commit_attempted':error.commit_attempted,'commit_observed':error.commit_observed,'request_sha256':hashlib.sha256(error.original_request).hexdigest(),'manifest_sha256':hashlib.sha256(error.original_manifest).hexdigest()});raise
def reconcile(owner,*args,**kwargs):
 events.append('reconcile');value=original_reconcile(owner,*args,**kwargs);observed.append(value);return value
@contextmanager
def assembly(connection,readonly=False):
 events.append({'backend_pid':connection.info.backend_pid,'readonly':readonly,'exact_psycopg_connection':type(connection)is psycopg.Connection})
 with original_assembly(connection,readonly)as value:yield value
try:
 source=Path('/private/tmp/ashlar-truss-typed-related-native-20261013-a');publication=Path('/private/tmp/ashlar-truss-typed-related-native-20261013-b')
 bootstrap=json.loads((source/'accepted.json').read_bytes())
 with owned_context(LocalPostgres(source/'data'))as runtime:
  parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri);administrator=psycopg.conninfo.make_conninfo(**parameters);parameters['user']=bootstrap['roles']['consumer'];ordinary=psycopg.conninfo.make_conninfo(**parameters)
  config=json.loads((publication/'publication-configuration.json').read_bytes());config.update(connection_uri=ordinary,administrator_uri=administrator,consumer_id='typed-receipt-loss-20261013-a',destination_id='typed-receipt-delta-20261013-a',maximum_pages=1)
  configuration=save('publication-configuration.json',config)
  with patch.object(psycopg.Connection,'commit',lost_response),patch.object(TrussFeedSessions,'acknowledge',acknowledge),patch.object(TrussFeedSessions,'reconcile',reconcile),patch.object(ports,'_assembly',assembly),patch.object(NativeDriver,'acknowledge',capture_driver_ack):
   published=run_publish_truss_graph_command(configuration,root/'publication')
  if len(published['publications'])!=1 or events.count('acknowledge')!=1 or events.count('reconcile')!=1 or len(injected)!=1 or len(uncertain)!=1 or uncertain[0]['commit_attempted']is not True or uncertain[0]['commit_observed']is not False:raise ValueError('Exact uncertain native commit/reconciliation counts differ')
  if len(observed)!=1 or observed[0].get('receipt_observed')is not True or observed[0].get('source_receipt_retained')is not True:raise ValueError('Original committed destination receipt not freshly observed')
  sessions=[event for event in events if type(event)is dict]
  if len(sessions)!=2 or [event['readonly']for event in sessions]!=[False,True]or sessions[0]['backend_pid']==sessions[1]['backend_pid']or not all(event['exact_psycopg_connection']for event in sessions):raise ValueError('Fresh exact readonly native reconciliation required')
  def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
  after=read_page(connection,config['consumer_id'],maximum_bytes=2097152)
  if after.prior.content!=pages[0]['next']or len(driver_ack)!=1 or driver_ack[0].get('uncertain_commit_fresh_reconciled')is not True:raise ValueError('Original cursor or driver uncertain reconciliation differs')
  # Actual receipt reconciliation binds the complete original prior/next feed page.
  checkpoint=json.loads(after.prior.content)
  if checkpoint['nextXid']=='0':raise ValueError('Original source checkpoint did not advance')
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'source_sdk_commit':'1f203768092178f356f267cd047ae149c08541f3','publications':1,'publication_id':published['publications'][0]['manifest']['publication_id'],'driver_ack':driver_ack,'original_page':{'prior':json.loads(pages[0]['prior']),'next':json.loads(pages[0]['next']),'sha256':pages[0]['sha256']},'events':events,'injected_commit':injected,'uncertain_outcome':uncertain,'fresh_receipt_reconciliation':observed,'source_checkpoint':checkpoint,'qualification':'Controlled client response-loss simulation after real PostgreSQL COMMIT, with exact native psycopg.Connection objects and public Truss receipt ACK/reconciliation. One catalog-only Delta publication from a new consumer on the retained original source; no second ACK call. No wire/network fault, protected outbox attestation, source history release, full typed graph ingestion, restart/missing-journal recovery, Unity Catalog or cloud qualification.'}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:save('run-result.json',result);print(json.dumps(result),flush=True)

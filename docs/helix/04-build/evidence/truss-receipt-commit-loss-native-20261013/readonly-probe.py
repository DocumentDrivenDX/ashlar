import os,sys,json,tempfile,time
from pathlib import Path
import psycopg,psycopg.pq,jsonschema,referencing
root=Path(__file__).parent
(root/'ipc').mkdir();os.environ['TMPDIR']=str(root/'ipc');tempfile.tempdir=None
os.environ.update(JAVA_HOME='/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home',PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable)
for p in reversed(json.loads(Path('/private/tmp/ashlar-commerce-graphframes-installed-prep-j/command-j.json').read_bytes())['sdkRoots']['canonical']):sys.path.insert(0,p)
sys.path.insert(0,'/private/tmp/ashlar-truss-sdk-pinned-1f203768/packages/python/src');sys.path.insert(0,'/Users/erik/Projects/ashlar/src')
from truss import LocalPostgres,FeedAckReconciled,FeedAckReceipt
from ashlar_host.lifecycle import owned_context
from ashlar_host.truss_example import private_file
from ashlar_host.truss_page_archive import decode_truss_page
from ashlar_host.truss_feed_sessions import _assembly,_value,encode_truss_destination_receipt
from ashlar_host.delta_custody import encoded
from types import SimpleNamespace
import sqlite3,hashlib
sha=lambda raw:hashlib.sha256(raw).hexdigest()
start=time.monotonic();result=None
try:
 source=Path('/private/tmp/ashlar-truss-typed-related-native-20261013-a');publication=Path('/private/tmp/ashlar-truss-receipt-commit-loss-native-20261013-a/publication')
 with sqlite3.connect('file:'+str(publication/'operations.sqlite')+'?mode=ro&immutable=1',uri=True)as db:rows=db.execute("SELECT original,digest FROM local_plan WHERE operation LIKE 'truss-ack:%'").fetchall()
 if len(rows)!=1 or sha(rows[0][0].encode())!=rows[0][1]:raise ValueError('Exact original ACK intent required')
 intent=json.loads(rows[0][0]);request=encoded(intent['request']).encode();manifest=encoded(intent['manifest']).encode();receipt=encode_truss_destination_receipt(request,manifest,SimpleNamespace(**intent['vector']))
 page_raw=(publication/'source-page-1.json').read_bytes();page=decode_truss_page(page_raw,sha(page_raw),intent['sessions']['feed'])
 original=json.loads(page.prior.content)
 with owned_context(LocalPostgres(source/'data'))as runtime:
  parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri);parameters['user']=json.loads((source/'accepted.json').read_bytes())['roles']['consumer'];ordinary=psycopg.conninfo.make_conninfo(**parameters)
  with psycopg.connect(ordinary,autocommit=False,connect_timeout=5)as connection:
   pid=connection.info.backend_pid
   with _assembly(connection,True)as (assembly,tx):
    answer=_value(assembly.reconcile_feed_ack_in_transaction(tx,page,destination_id=intent['sessions']['destination_id'],request_sha256=sha(request),publication_id=intent['manifest']['publication_id'],receipt_bytes=receipt))
    readonly=connection.execute('SHOW transaction_read_only').fetchone()[0]
    if type(answer)is not FeedAckReconciled or answer.outcome!='matched'or answer.checkpoint!=page.next or readonly!='on':raise ValueError('Exact fresh readonly committed receipt observation required')
    retained=answer.receipt
    expected={'installation_id':original['installationId'],'source_epoch':original['sourceEpoch'],'consumer_id':page.consumer_id,'registration_id':page.registration_id,'prior':page.prior,'next':page.next,'feed_sha256':page.sha256,'destination_id':intent['sessions']['destination_id'],'request_sha256':sha(request),'publication_id':intent['manifest']['publication_id'],'receipt_bytes':receipt,'receipt_sha256':sha(receipt)}
    if type(retained)is not FeedAckReceipt or any(getattr(retained,k)!=v for k,v in expected.items()):raise ValueError('Exact original opaque receipt binding differs')
   connection.rollback()
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'backend_pid':pid,'transaction_read_only':readonly,'reconciliation_outcome':answer.outcome,'request_sha256':sha(request),'manifest_sha256':sha(manifest),'receipt_sha256':sha(receipt),'receipt_bytes':len(receipt),'ack_intent_sha256':rows[0][1],'source_page_sha256':sha(page_raw),'publication_id':intent['manifest']['publication_id'],'destination_id':intent['sessions']['destination_id'],'consumer_id':page.consumer_id,'source_epoch':original['sourceEpoch'],'checkpoint':json.loads(answer.checkpoint.content),'exact_all_receipt_fields_verified':True,'original_next_checkpoint_matched':True,'source_ack_calls':0,'source_commit_calls':0,'qualification':'Fresh actual public Truss read-only reconciliation of the original source-retained receipt after the bounded commit-loss harness. Exact original page, request, manifest, vector and opaque receipt byte correspondence; rollback and owner closure. Reproduced vector grants no new publication authority. No publication or ACK repeated.'}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:private_file(root/'run-result.json',result);print(json.dumps(result),flush=True)

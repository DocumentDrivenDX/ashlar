"""Five-row native phase custody; synthetic artifacts confer no publication authority."""
from contextlib import contextmanager
import fcntl,hashlib,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.attempt_store import DeltaAttemptStore,AttemptStoreError
from ashlar.native import SQLResult
from ashlar.source import jsonl_batches
from ashlar.staging import batch_row
from persistent_sql import Client
OUT=B/'out/native/attempt_store_20261008';c=Client(OUT)
TABLE='client_dev.ashlar_graph_apply_20261008.publication_attempt_phase'
class Transport:
    def query(self,sql,parameters):
        rows=c.sql('attempt-custody',sql,parameters=[{'name':k,'type':'STRING','value':v} for k,v in parameters.items()] or None)
        names=[x['name'] for x in c.records[-1]['response'].get('manifest',{}).get('schema',{}).get('columns',[])]
        return SQLResult([dict(zip(names,row)) for row in rows])
class DevelopmentPolicy:
    @contextmanager
    def writer(self,table,uuid,context):
        if table!=TABLE or context!='storage-only-development':raise PermissionError('Wrong development scope')
        with open('/private/tmp/ashlar-attempt-store.lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            try:yield
            finally:fcntl.flock(lock,fcntl.LOCK_UN)
fields=['stream','batch_id','phase','request_digest','payload_json','payload_digest']
c.sql('create-phase-store','CREATE TABLE '+TABLE+' ('+','.join(k+' STRING NOT NULL' for k in fields)+') USING DELTA')
c.sql('phase-byte-check','ALTER TABLE '+TABLE+' ADD CONSTRAINT phase_byte_digest CHECK (payload_digest=sha2(payload_json,256))')
transport=Transport();uuid=transport.query('DESCRIBE DETAIL '+TABLE,{}).rows[0]['id']
batch=list(jsonl_batches((ROOT/'examples/end-to-end/source.jsonl').read_bytes().splitlines(keepends=True),feed='storage-fixture',epoch='epoch-1'))[0]
row=batch_row(batch)
request={'stream':'native-attempt-storage-fixture','batch_id':batch.batch_id,'predecessor':'synthetic-previous','schema_revisions_json':'{"storage-fixture":"fixture-only"}','source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest']}
request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
result='{"qualification":"storage-only; no completed graph effects"}'
descriptor='{"id":"fixture-only","qualification":"synthetic bytes; no actual publication"}'
store=DeltaAttemptStore(transport,DevelopmentPolicy(),TABLE,uuid)
with store.session('storage-only-development') as session:
    for phase in ['prepared','applying','applied','committing','committed']:
        kwargs={} if phase in ('prepared','applying') else {'result_json':result}
        if phase=='committed':kwargs['descriptor_json']=descriptor
        original=session.append(request,phase,**kwargs)
        assert session.append(request,phase,**kwargs)==original
    before=session.read(request['stream'],request['batch_id'])
    try:
        session.append(request,'applied',result_json='{"qualification":"changed-valid-artifact"}')
        raise AssertionError('Original phase was replaced')
    except AttemptStoreError:pass
    assert session.read(request['stream'],request['batch_id'])==before
# New object/session has no retained in-memory phase state.
reopened=DeltaAttemptStore(Transport(),DevelopmentPolicy(),TABLE,uuid)
with reopened.session('storage-only-development') as session:
    assert session.read(request['stream'],request['batch_id'])==before
    retained=json.loads(before[-1].payload_json)
    assert retained['request']==request and retained['result_json']==result and retained['descriptor_json']==descriptor
summary={'state':'passed','table':TABLE,'table_uuid':uuid,'phase_rows':5,'request_digest':request['request_digest'],'checks':['five original phase byte records persisted in UC Delta','phase predecessor and exact-request/result correspondence','identical phase replay preserves original bytes','changed valid result refuses and all originals remain','fresh object/session reloads all original custody from native state'],'qualification':'Native immutable-phase carrier and client-enforced serialized state-machine checks only. Administrative development authority plus local cooperating-process lock; no native/remote fencing or immutable grants. Result/descriptor are explicitly synthetic artifacts, not graph effect/commit proof or read publication. Existing graph remains unpublished; no acknowledgement. Five rows on existing warehouse; no scale test or resize.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))

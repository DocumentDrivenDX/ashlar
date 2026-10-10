"""Immutable Delta phase custody under mandatory injected writer authority."""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
from .native import _quoted
from .schema import _json, SchemaIntakeError

PHASES=('prepared','applying','applied','committing','committed')

class AttemptStoreError(ValueError):
    pass

@dataclass(frozen=True)
class PhaseRecord:
    phase: str
    request_digest: str
    payload_json: str
    payload_digest: str


def verify_request_digest(request: dict[str, str]) -> str:
    """Verify the complete original attempt-request strings and digest.

    A present checkpoint retains its existing source-specific validation. This
    verifies request correspondence only, never writer/source/ACK authority.
    """
    keys={'stream','batch_id','predecessor','schema_revisions_json','source_batch_json','source_batch_digest','request_digest'}
    if not isinstance(request,dict) or set(request) not in (keys,keys|{'source_checkpoint_json'}) or any(not isinstance(x,str) or not x for x in request.values()):
        raise AttemptStoreError('Complete original request strings required')
    content={k:v for k,v in request.items() if k!='request_digest'}
    digest=hashlib.sha256(json.dumps(content,separators=(',',':'),sort_keys=True).encode()).hexdigest()
    if request['request_digest']!=digest:raise AttemptStoreError('Original request digest mismatch')
    if 'source_checkpoint_json' in request:
        from .source_checkpoint import validate_checkpoint_request
        try:validate_checkpoint_request(request)
        except ValueError as exc:raise AttemptStoreError('Invalid original native source checkpoint') from exc
    return digest

# Retained compatibility name for existing internal consumers.
_request_digest = verify_request_digest

def _phase_records(stream,batch_id,rows):
    try:return _validate_phase_records(stream,batch_id,rows)
    except SchemaIntakeError as exc:raise AttemptStoreError('Malformed original phase JSON') from exc

def _validate_phase_records(stream,batch_id,rows):
    if len(rows)>5:raise AttemptStoreError('Ambiguous attempt inventory')
    result=[];original=None;applied=None
    for i,row in enumerate(rows):
        if set(row)!={'phase','request_digest','payload_json','payload_digest'} or row['phase']!=PHASES[i]:raise AttemptStoreError('Missing, duplicate or unknown phase')
        text=row['payload_json']
        if not isinstance(text,str) or len(text.encode())>4*1024*1024 or hashlib.sha256(text.encode()).hexdigest()!=row['payload_digest']:raise AttemptStoreError('Phase byte custody mismatch')
        value=_json(text.encode())
        if not isinstance(value,dict) or set(value)!={'request','result_json','descriptor_json'}:raise AttemptStoreError('Invalid original phase payload')
        digest=_request_digest(value['request'])
        if digest!=row['request_digest'] or value['request']['stream']!=stream or value['request']['batch_id']!=batch_id:raise AttemptStoreError('Phase/request correspondence mismatch')
        if original is not None and value['request']!=original:raise AttemptStoreError('Changed request across original phases')
        original=value['request'];r=value['result_json'];d=value['descriptor_json']
        if i<2:
            if r is not None or d is not None:raise AttemptStoreError('Premature effect/descriptor custody')
        else:
            if not isinstance(r,str) or not r:raise AttemptStoreError('Missing original applied artifact')
            _json(r.encode())
            if applied is not None and r!=applied:raise AttemptStoreError('Original effect result changed')
            applied=r
            if i==4:
                if not isinstance(d,str) or not d:raise AttemptStoreError('Missing original committed descriptor')
                _json(d.encode())
            elif d is not None:raise AttemptStoreError('Premature descriptor custody')
        result.append(PhaseRecord(row['phase'],digest,text,row['payload_digest']))
    return tuple(result)

class DeltaAttemptStore:
    def __init__(self,executor,policy,table,uuid,*,original_request=None):
        self.executor=executor;self.policy=policy;self.table=table;self.sql_table=_quoted(table);self.uuid=uuid
        if not isinstance(uuid,str) or not uuid:raise AttemptStoreError('Trusted attempt table UUID required')
        self.original_request = None
        self.source_scope = None
        if original_request is not None:
            if (type(original_request) is not dict or len(original_request)!=8
                    or any(type(k) is not str or type(v) is not str or len(v)>4*1024*1024 for k,v in original_request.items())):
                raise AttemptStoreError('Exact original scoped request strings required')
            original_bytes=json.dumps(original_request,sort_keys=True,separators=(',',':')).encode()
            if len(original_bytes)>4*1024*1024:raise AttemptStoreError('Bounded original scoped request required')
            _request_digest(original_request)
            checkpoint = _json(original_request.get('source_checkpoint_json','').encode())
            if (type(checkpoint) is not dict or checkpoint.get('profile') != 'ashlar-postgresql-outbox/0.1'
                    or any(type(checkpoint.get(k)) is not str or not checkpoint[k] for k in ('feed','epoch'))):
                raise AttemptStoreError('Original outbox feed/epoch scope required')
            self.original_request = json.dumps(original_request,sort_keys=True,separators=(',',':'))
            self.source_scope = (checkpoint['feed'],checkpoint['epoch'])
    def _identity(self):
        rows=self.executor.query('DESCRIBE DETAIL '+self.sql_table,{}).rows
        if len(rows)!=1 or rows[0].get('id')!=self.uuid:raise AttemptStoreError('Attempt table identity changed')
    @contextmanager
    def session(self,context):
        with self.policy.writer(self.table,self.uuid,context) as permit:
            if permit is not None:raise AttemptStoreError('Writer authority did not complete')
            self._identity();session=_Session(self)
            try:
                yield session
                self._identity()
            finally:session.closed=True

class _Session:
    def __init__(self,store):self.store=store;self.closed=False
    def _open(self):
        if self.closed:raise AttemptStoreError('Attempt session already released')
    def read(self,stream,batch_id):
        self._open()
        if any(not isinstance(x,str) or not x for x in [stream,batch_id]):raise AttemptStoreError('Explicit original attempt identity required')
        s=self.store
        order='CASE phase '+ ' '.join("WHEN '"+p+"' THEN "+str(i) for i,p in enumerate(PHASES))+' END'
        limit = '6' if s.original_request is None else '161'
        rows=s.executor.query('SELECT phase,request_digest,payload_json,payload_digest FROM '+s.sql_table+' WHERE stream=:stream AND batch_id=:batch ORDER BY '+order+' LIMIT '+limit,{'stream':stream,'batch':batch_id}).rows
        if s.original_request is None: return _phase_records(stream,batch_id,rows)
        original = _json(s.original_request.encode())
        if (stream,batch_id) != (original['stream'],original['batch_id']):
            raise AttemptStoreError('Original scoped stream/batch differs')
        if len(rows)>160: raise AttemptStoreError('Bounded complete source-qualified phase inventory required')
        groups={}
        for row in rows:
            try:
                if (type(row) is not dict or set(row)!={'phase','request_digest','payload_json','payload_digest'}
                        or any(type(value) is not str for value in row.values())
                        or row['phase'] not in PHASES or len(row['payload_json'])>4*1024*1024):
                    raise AttemptStoreError('Exact bounded original phase carriers required')
                raw=row['payload_json'].encode()
                if len(raw)>4*1024*1024 or hashlib.sha256(raw).hexdigest()!=row['payload_digest']:
                    raise AttemptStoreError('Original phase byte custody differs')
                payload=_json(raw);request=payload['request']
                if type(request) is not dict or any(type(k) is not str or type(v) is not str for k,v in request.items()):
                    raise AttemptStoreError('Exact existing original request strings required')
                _request_digest(request)
                checkpoint=_json(request['source_checkpoint_json'].encode())
                if (checkpoint.get('profile')!='ashlar-postgresql-outbox/0.1'
                        or any(type(checkpoint.get(k)) is not str or not checkpoint[k] for k in ('feed','epoch'))):
                    raise AttemptStoreError('Unknown original phase source scope')
                scope=(checkpoint['feed'],checkpoint['epoch'])
            except (KeyError,TypeError,AttributeError,ValueError) as exc:
                raise AttemptStoreError('Malformed or unknown original scoped custody') from exc
            groups.setdefault(scope,[]).append(row)
        result=()
        for scope,group in groups.items():
            records=_phase_records(stream,batch_id,group)
            if scope==s.source_scope:
                if any(record.request_digest!=original['request_digest'] for record in records):
                    raise AttemptStoreError('Changed original within source-qualified attempt')
                result=records
        return result
    def append(self,request,phase,*,result_json=None,descriptor_json=None):
        self._open();request=dict(request);digest=_request_digest(request)
        if self.store.original_request is not None and json.dumps(request,sort_keys=True,separators=(',',':')) != self.store.original_request:
            raise AttemptStoreError('Exact bound original scoped request required')
        if phase not in PHASES:raise AttemptStoreError('Unsupported phase')
        text=json.dumps({'request':request,'result_json':result_json,'descriptor_json':descriptor_json},sort_keys=True,separators=(',',':'))
        if len(text.encode())>4*1024*1024:raise AttemptStoreError('Phase byte budget exceeded')
        row={'stream':request['stream'],'batch_id':request['batch_id'],'phase':phase,'request_digest':digest,'payload_json':text,'payload_digest':hashlib.sha256(text.encode()).hexdigest()}
        existing=self.read(row['stream'],row['batch_id']);index=PHASES.index(phase)
        if index<len(existing):
            if existing[index]!=PhaseRecord(phase,digest,text,row['payload_digest']):raise AttemptStoreError('Immutable original phase conflict')
            return existing[index]
        if index!=len(existing):raise AttemptStoreError('Phase cannot skip original predecessor')
        # Validate candidate payload against the same reader before submission.
        candidate_rows=[{'phase':x.phase,'request_digest':x.request_digest,'payload_json':x.payload_json,'payload_digest':x.payload_digest} for x in existing]
        candidate_rows.append({k:row[k] for k in ['phase','request_digest','payload_json','payload_digest']})
        _phase_records(row['stream'],row['batch_id'],candidate_rows)
        columns=list(row);definition='STRUCT<'+','.join(k+':STRING' for k in columns)+'>'
        parity=' AND '.join('t.'+k+' IS NOT DISTINCT FROM s.'+k for k in columns)
        scope_key = ''
        if self.store.source_scope is not None:
            for key in ('feed','epoch'):
                scope_key += " AND get_json_object(get_json_object(t.payload_json,'$.request.source_checkpoint_json'),'$."+key+"')=get_json_object(get_json_object(s.payload_json,'$.request.source_checkpoint_json'),'$."+key+"')"
        sql='MERGE INTO '+self.store.sql_table+" t USING (SELECT r.* FROM (SELECT from_json(:payload,'"+definition+"') r)) s ON t.stream=s.stream AND t.batch_id=s.batch_id AND t.phase=s.phase"+scope_key+" WHEN MATCHED AND NOT ("+parity+") THEN UPDATE SET payload_json=cast(raise_error('ATTEMPT_PHASE_CONFLICT') AS STRING) WHEN NOT MATCHED THEN INSERT *"
        self.store.executor.query(sql,{'payload':json.dumps(row,separators=(',',':'))})
        actual=self.read(row['stream'],row['batch_id'])
        if len(actual)!=index+1 or actual[-1]!=PhaseRecord(phase,digest,text,row['payload_digest']):raise AttemptStoreError('Native phase readback mismatch')
        return actual[-1]

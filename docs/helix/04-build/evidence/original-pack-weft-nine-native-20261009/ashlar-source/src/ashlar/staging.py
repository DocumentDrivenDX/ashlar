"""Serialized Delta raw-batch custody. Publication and checkpoints are separate."""
import base64
from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Protocol
from .source import SourceBatch, SourceRecord, SourceError, jsonl_batches
from .native import Executor, _quoted
from .schema import _json

class StagingError(ValueError):
    pass

class StagePolicy(Protocol):
    def writer(self, table: str, uuid: str, context: Any):
        """Authorized context manager holding exclusive writer custody through readback.

        Must fence every admitted write path and retain recovery custody on unknown
        native outcome. An ordinary caller flag is not writer authority.
        """

@dataclass(frozen=True)
class StagedBatch:
    table: str
    table_uuid: str
    feed: str
    epoch: str
    batch_id: str
    batch_digest: str
    cursor_after: str
    # This receipt never authorizes source acknowledgement or data publication.


def batch_row(batch: SourceBatch):
    if not isinstance(batch,SourceBatch) or not isinstance(batch.records,tuple):raise StagingError('Immutable source batch required')
    if len(batch.records)>1000 or any(not isinstance(x,SourceRecord) or not isinstance(x.raw,bytes) for x in batch.records) or not isinstance(batch.begin,bytes) or not isinstance(batch.commit,bytes):raise StagingError('Invalid bounded raw batch')
    if len(batch.begin)+sum(len(x.raw) for x in batch.records)+len(batch.commit)>1024*1024:raise StagingError('Stage transaction byte limit exceeded')
    if batch.profile!='ashlar-jsonl-transactions/0.1':raise StagingError('Unsupported source profile')
    raw=batch.begin+b''.join(x.raw for x in batch.records)+batch.commit
    try:
        recovered=list(jsonl_batches(raw.splitlines(keepends=True),feed=batch.feed,epoch=batch.epoch,cursor_before=batch.cursor_before))
    except SourceError as exc:raise StagingError('Invalid batch custody') from exc
    if recovered != [batch]:raise StagingError('Batch differs from original source transaction')
    payload=asdict(batch)
    payload['begin_base64']=base64.b64encode(payload.pop('begin')).decode('ascii')
    payload['commit_base64']=base64.b64encode(payload.pop('commit')).decode('ascii')
    for event in payload['records']:event['raw_base64']=base64.b64encode(event.pop('raw')).decode('ascii')
    text=json.dumps(payload,separators=(',',':'),sort_keys=True,ensure_ascii=True)
    return {'source_profile':batch.profile,'feed':batch.feed,'epoch':batch.epoch,'batch_id':batch.batch_id,
            'cursor_before':batch.cursor_before,'cursor_after':batch.cursor_after,'records_digest':batch.records_sha256,
            'batch_json':text,'batch_digest':hashlib.sha256(text.encode('utf-8')).hexdigest()}


def batch_from_row(row):
    """Recover one bounded original staged transaction, checking all custody fields.

    The caller must independently establish table UUID, snapshot, authorization
    and batch ordering. A recovered batch grants no publication or source ACK.
    """
    fields = {'source_profile', 'feed', 'epoch', 'batch_id', 'cursor_before',
              'cursor_after', 'records_digest', 'batch_json', 'batch_digest'}
    if not isinstance(row, dict) or set(row) != fields:
        raise StagingError('Complete exact staged row required')
    if any(not isinstance(value, str) for value in row.values()):
        raise StagingError('Staged custody fields must be strings')
    try:
        encoded = row['batch_json'].encode('utf-8')
        if len(encoded) > 4 * 1024 * 1024:
            raise StagingError('Retained batch artifact byte limit exceeded')
        if hashlib.sha256(encoded).hexdigest() != row['batch_digest']:
            raise StagingError('Retained batch artifact digest differs')
        value = _json(encoded)
        if not isinstance(value, dict) or not isinstance(value.get('records'), list):
            raise StagingError('Complete retained batch artifact required')
        if len(value['records']) > 1000:
            raise StagingError('Retained record count exceeded')
        raw = []
        remaining = 1024 * 1024
        def original(text):
            nonlocal remaining
            if not isinstance(text, str) or len(text) > ((remaining + 2) // 3) * 4:
                raise StagingError('Retained original byte budget exceeded')
            decoded = base64.b64decode(text, validate=True)
            if base64.b64encode(decoded).decode('ascii') != text or len(decoded) > remaining:
                raise StagingError('Noncanonical or oversized original bytes')
            remaining -= len(decoded)
            return decoded
        raw.append(original(value['begin_base64']))
        for record in value['records']:
            raw.append(original(record['raw_base64']))
        raw.append(original(value['commit_base64']))
        recovered = list(jsonl_batches(b''.join(raw).splitlines(keepends=True),
                         feed=row['feed'], epoch=row['epoch'],
                         cursor_before=row['cursor_before']))
        if len(recovered) != 1 or batch_row(recovered[0]) != row:
            raise StagingError('Retained metadata differs from original transaction')
        return recovered[0]
    except StagingError:
        raise
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise StagingError('Malformed retained original transaction') from exc

class DeltaBatchStage:
    def __init__(self,executor:Executor,policy:StagePolicy,table:str,uuid:str):
        self.executor=executor;self.policy=policy;self.table=table;self.sql_table=_quoted(table)
        if not isinstance(uuid,str) or not uuid:raise StagingError('Trusted stage UUID required')
        self.uuid=uuid

    def _identity(self):
        rows=self.executor.query('DESCRIBE DETAIL '+self.sql_table,{}).rows
        if len(rows)!=1 or rows[0].get('id')!=self.uuid:raise StagingError('Stage table identity changed')

    def stage(self,batch:SourceBatch,*,context:Any)->StagedBatch:
        row=batch_row(batch)
        with self.policy.writer(self.table,self.uuid,context) as permit:
            if permit is not None:raise StagingError('Writer policy did not complete its contract')
            self._identity()
            columns=list(row)
            definition='STRUCT<'+','.join(k+':STRING' for k in columns)+'>'
            same=' AND '.join('t.'+k+' IS NOT DISTINCT FROM s.'+k for k in columns)
            sql='MERGE INTO '+self.sql_table+" t USING (SELECT r.* FROM (SELECT from_json(:payload,'"+definition+"') r)) s ON t.feed=s.feed AND t.epoch=s.epoch AND t.batch_id=s.batch_id WHEN MATCHED AND NOT ("+same+") THEN UPDATE SET batch_json=cast(raise_error('SOURCE_BATCH_CONFLICT') AS STRING) WHEN NOT MATCHED THEN INSERT *"
            self.executor.query(sql,{'payload':json.dumps(row,separators=(',',':'))})
            actual=self.executor.query('SELECT '+','.join(columns)+' FROM '+self.sql_table+' WHERE feed=:feed AND epoch=:epoch AND batch_id=:batch',{'feed':batch.feed,'epoch':batch.epoch,'batch':batch.batch_id}).rows
            if len(actual)!=1 or dict(actual[0])!=row:raise StagingError('Missing, ambiguous or mismatched staged batch')
            self._identity()
        return StagedBatch(self.table,self.uuid,batch.feed,batch.epoch,batch.batch_id,row['batch_digest'],batch.cursor_after)

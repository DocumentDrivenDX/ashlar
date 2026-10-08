"""Committed PostgreSQL outbox reader; native cursor stays separate from JSONL offsets."""
from dataclasses import dataclass
import hashlib
import re
from .source import SourceBatch,SourceError,jsonl_batches

class OutboxError(ValueError):
    pass

@dataclass(frozen=True)
class OutboxTransaction:
    profile: str
    feed: str
    epoch: str
    previous: str
    position: str
    payload_digest: str
    batch: SourceBatch


def _position(value):
    if not isinstance(value,str) or not value.isascii() or not value.isdecimal() or str(int(value))!=value or not 0<=int(value)<2**63:
        raise OutboxError('Canonical nonnegative signed64 position required')
    return int(value)

class PostgresOutbox:
    def __init__(self,executor,*,feed,epoch,schema='ashlar_outbox'):
        if any(not isinstance(x,str) or not x or '\x00' in x for x in [feed,epoch]) or not re.fullmatch('[A-Za-z_][A-Za-z_0-9]*',schema):
            raise OutboxError('Explicit trusted source namespace required')
        self.executor=executor;self.feed=feed;self.epoch=epoch;self.schema='"'+schema+'"'
    def read(self,after='0',*,limit=10):
        last=_position(after)
        if type(limit) is not int or not 1<=limit<=32:raise OutboxError('Bounded page limit required')
        rows=self.executor.query('SELECT position::text AS position FROM '+self.schema+'.head WHERE id=1',{}).rows
        if len(rows)!=1:raise OutboxError('Missing or ambiguous committed head')
        head=_position(rows[0]['position'])
        if last>head:raise OutboxError('Checkpoint exceeds committed source head')
        rows=self.executor.query('SELECT position::text AS position,batch_id,payload,encode(digest,\'hex\') AS digest FROM '+self.schema+'.batch WHERE position>CAST(:after AS bigint) AND position<=CAST(:head AS bigint) ORDER BY position LIMIT CAST(:limit AS integer)',{'after':after,'head':str(head),'limit':str(limit)}).rows
        if len(rows)>limit:raise OutboxError('Unbounded source page')
        result=[]
        for row in rows:
            current=_position(row['position'])
            if current!=last+1 or current>head:raise OutboxError('Missing or unordered original committed group')
            if not isinstance(row.get('payload'),str):raise OutboxError('Exact original UTF8 payload required')
            raw=row['payload'].encode('utf-8')
            if len(raw)>1048576 or hashlib.sha256(raw).hexdigest()!=row['digest']:raise OutboxError('Original outbox byte custody mismatch')
            try:batches=list(jsonl_batches(raw.splitlines(keepends=True),feed=self.feed,epoch=self.epoch))
            except SourceError as exc:raise OutboxError('Unsupported or incomplete original source batch') from exc
            if len(batches)!=1 or batches[0].batch_id!=row['batch_id']:raise OutboxError('Outbox group differs from exact source batch')
            result.append(OutboxTransaction('ashlar-postgresql-outbox/0.1',self.feed,self.epoch,str(last),str(current),row['digest'],batches[0]));last=current
        if len(rows)<limit and last<head:raise OutboxError('Committed source custody unavailable')
        return tuple(result)
    # No acknowledgement: only the publisher can admit descriptor-bound progress.

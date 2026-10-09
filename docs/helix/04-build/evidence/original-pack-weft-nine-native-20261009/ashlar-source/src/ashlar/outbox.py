"""Committed PostgreSQL outbox reader; native cursor stays separate from JSONL offsets."""
from dataclasses import dataclass
import hashlib
import re
from .source import SourceBatch,SourceError,jsonl_batches
from .staging import batch_row
from .apply import plan_apply
from .whole_entity import changes_from_batch

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

@dataclass(frozen=True)
class OutboxApplyResult:
    state: object
    position: str
    transactions: int


def publish_outbox_transaction(backend,stream,transaction,*,predecessor,schema_revisions_json,context):
    """Retain the original native checkpoint in every publication phase.

    The backend must bind descriptor progress and ACK to this exact group under
    current native source authority; this wrapper supplies no permissive backend.
    """
    if not isinstance(transaction,OutboxTransaction):raise OutboxError('Original committed outbox group required')
    from .source_checkpoint import outbox_checkpoint
    from .publisher import publish_batch
    return publish_batch(backend,stream,transaction.batch,predecessor=predecessor,
        schema_revisions_json=schema_revisions_json,context=context,
        source_checkpoint_json=outbox_checkpoint(transaction))


def apply_outbox_transactions(transactions, *, prior, feed, epoch, after,
                              expected_position, schema_policy, schema_transition_policy=None):
    """Apply an admitted committed page using native group positions, never byte offsets.

    The caller owns original reader/table/epoch/checkpoint admission and complete
    prior state. This pure result grants neither publication nor source ACK.
    """
    position=_position(after);_position(expected_position)
    if any(not isinstance(value,str) or not value or '\x00' in value for value in (feed,epoch)):
        raise OutboxError('Explicit admitted outbox scope required')
    state=prior;count=0;seen=set();size=0
    for transaction in transactions:
        count+=1
        if count>32 or not isinstance(transaction,OutboxTransaction):
            raise OutboxError('Bounded original outbox page required')
        if (transaction.profile,transaction.feed,transaction.epoch,transaction.previous) != (
                'ashlar-postgresql-outbox/0.1',feed,epoch,str(position)):
            raise OutboxError('Mixed, reordered or overlapping outbox page')
        next_position=_position(transaction.position)
        if next_position!=position+1:raise OutboxError('Missing committed outbox group')
        batch=transaction.batch;batch_row(batch)
        if (batch.feed,batch.epoch,batch.cursor_before)!=(feed,epoch,'0'):
            raise OutboxError('Outbox-contained transaction has independent zero-based byte cursor')
        if batch.batch_id in seen:raise OutboxError('Repeated original outbox batch identity')
        seen.add(batch.batch_id)
        raw=batch.begin+b''.join(record.raw for record in batch.records)+batch.commit
        size+=len(raw)
        if size>16*1024*1024:raise OutboxError('Outbox page original byte budget exceeded')
        if hashlib.sha256(raw).hexdigest()!=transaction.payload_digest:
            raise OutboxError('Original outbox payload differs')
        state=plan_apply(state,changes_from_batch(batch),schema_policy=schema_policy,
                         schema_transition_policy=schema_transition_policy)
        position=next_position
    if str(position)!=expected_position:raise OutboxError('Outbox page does not reach admitted checkpoint')
    return OutboxApplyResult(state,str(position),count)


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

"""Exact native outbox checkpoint binding for durable publication intent."""
import hashlib
import json
from .schema import _json
from .staging import batch_row, batch_from_row

class CheckpointError(ValueError):
    pass


def validate_outbox_checkpoint(text, batch):
    if not isinstance(text,str) or len(text)>16384:
        raise CheckpointError('Bounded original checkpoint text required')
    value=_json(text.encode('utf-8'))
    fields={'profile','feed','epoch','previous','position','payload_digest','batch_id'}
    if not isinstance(value,dict) or set(value)!=fields or any(not isinstance(item,str) for item in value.values()):
        raise CheckpointError('Complete native checkpoint binding required')
    for key in ('previous','position'):
        token=value[key]
        if not token.isascii() or not token.isdecimal() or len(token)>19 or str(int(token))!=token or not 0<=int(token)<2**63:
            raise CheckpointError('Canonical native checkpoint position required')
    if int(value['position'])!=int(value['previous'])+1:
        raise CheckpointError('Complete contiguous committed group required')
    batch_row(batch)
    raw=batch.begin+b''.join(record.raw for record in batch.records)+batch.commit
    expected={'profile':'ashlar-postgresql-outbox/0.1','feed':batch.feed,'epoch':batch.epoch,
              'previous':value['previous'],'position':value['position'],
              'payload_digest':hashlib.sha256(raw).hexdigest(),'batch_id':batch.batch_id}
    if batch.cursor_before!='0' or value!=expected:
        raise CheckpointError('Native checkpoint differs from original contained transaction')
    return value


def outbox_checkpoint(transaction):
    value={key:getattr(transaction,key) for key in
           ['profile','feed','epoch','previous','position','payload_digest']}
    value['batch_id']=transaction.batch.batch_id
    text=json.dumps(value,sort_keys=True,separators=(',',':'))
    validate_outbox_checkpoint(text,transaction.batch)
    return text


def validate_checkpoint_request(request):
    """Validate added checkpoint against full original source bytes during phase reads."""
    value=_json(request['source_batch_json'].encode('utf-8'))
    if not isinstance(value,dict):raise CheckpointError('Original source batch artifact required')
    try:
        row={'source_profile':value['profile'],'feed':value['feed'],'epoch':value['epoch'],
             'batch_id':value['batch_id'],'cursor_before':value['cursor_before'],
             'cursor_after':value['cursor_after'],'records_digest':value['records_sha256'],
             'batch_json':request['source_batch_json'],'batch_digest':request['source_batch_digest']}
    except KeyError as exc:raise CheckpointError('Incomplete original source artifact') from exc
    validate_outbox_checkpoint(request['source_checkpoint_json'],batch_from_row(row))

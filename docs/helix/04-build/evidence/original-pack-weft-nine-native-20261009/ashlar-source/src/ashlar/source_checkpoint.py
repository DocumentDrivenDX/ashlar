"""Exact explicit source checkpoint binding for durable publication intent."""
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
    validate_source_checkpoint(request['source_checkpoint_json'],batch_from_row(row))


def bind_source_descriptor(request,descriptor,*,expected_publication_id):
    """Require exact original source progress before a backend may consider ACK.

    This checks correspondence only. The caller must resolve authoritative native
    manifest/pin/effect custody and current source/checkpoint permission separately.
    Other feeds remain in the descriptor and are not discarded or normalized.
    """
    from collections.abc import Mapping
    from .attempt_store import _request_digest
    from .publication import Descriptor
    if not isinstance(request,Mapping) or 'source_checkpoint_json' not in request:
        raise CheckpointError('Original native source request required')
    request=dict(request)
    _request_digest(request)  # Revalidates full original inner and outer custody.
    if not isinstance(descriptor,Descriptor) or not isinstance(expected_publication_id,str) or not expected_publication_id or descriptor.publication_id!=expected_publication_id:
        raise CheckpointError('Independently admitted original publication identity required')
    checkpoint=_json(request['source_checkpoint_json'].encode('utf-8'))
    progress=descriptor.source_progress
    if not isinstance(progress,Mapping) or progress.get(checkpoint['feed'])!=checkpoint:
        raise CheckpointError('Publication progress differs from original native source group')
    revisions=_json(request['schema_revisions_json'].encode('utf-8'))
    if descriptor.revisions!=revisions:
        raise CheckpointError('Publication schema inventory differs from original request')
    report=descriptor.validation_report
    if not isinstance(report,Mapping) or report.get('request_digest')!=request['request_digest']:
        raise CheckpointError('Publication validation does not bind original request')
    # No source mutation/acknowledgement is issued by correspondence validation.


def csv_checkpoint(batch):
    """Bind one original CSV row ordinal separately from its inner JSONL cursor.

    This does not prove immutable file identity, source permissions or semantic
    mapping admission. Those remain required independent host policy checks.
    """
    import base64
    from .source import records_digest
    batch_row(batch)
    if batch.cursor_before!='0' or len(batch.records)!=1:raise CheckpointError('One original CSV singleton transaction required')
    value=_json(batch.records[0].delivery_id.encode('utf-8'))
    fields={'profile','header_base64','row_base64','row_ordinal'}
    if not isinstance(value,dict) or set(value)!=fields or value.get('profile')!='ashlar-single-line-csv/0.1':raise CheckpointError('Original CSV delivery custody required')
    token=value['row_ordinal']
    if not isinstance(token,str) or not token.isascii() or not token.isdecimal() or len(token)>4 or str(int(token))!=token or not 1<=int(token)<=1000 or batch.batch_id!='csv-row-'+token:raise CheckpointError('Canonical bounded CSV row ordinal required')
    originals=[]
    for field in ('header_base64','row_base64'):
        text=value[field]
        if not isinstance(text,str) or len(text)>87384:raise CheckpointError('Bounded original CSV bytes required')
        try:raw=base64.b64decode(text,validate=True)
        except (ValueError,TypeError) as exc:raise CheckpointError('Invalid CSV original encoding') from exc
        if base64.b64encode(raw).decode('ascii')!=text or not raw.endswith(b'\n') or b'\n' in raw[:-1] or len(raw)>65536:raise CheckpointError('Original CSV single-line custody differs')
        originals.append(raw)
    checkpoint={'profile':'ashlar-single-line-csv/0.1','feed':batch.feed,'epoch':batch.epoch,
                'previous':str(int(token)-1),'position':token,'payload_digest':records_digest(originals),'batch_id':batch.batch_id}
    return json.dumps(checkpoint,sort_keys=True,separators=(',',':'))


def jsonl_checkpoint(batch):
    """Bind the complete original transaction and its actual byte-offset cursors."""
    batch_row(batch)
    if batch.profile!='ashlar-jsonl-transactions/0.1':raise CheckpointError('Original JSONL transaction profile required')
    for token in (batch.cursor_before,batch.cursor_after):
        if not isinstance(token,str) or len(token)>19 or not token.isascii() or not token.isdecimal() or str(int(token))!=token or not 0<=int(token)<2**63:
            raise CheckpointError('Canonical signed64 JSONL byte cursor required')
    if int(batch.cursor_after)<=int(batch.cursor_before):raise CheckpointError('Complete forward JSONL transaction required')
    value={'profile':'ashlar-immutable-jsonl/0.1','feed':batch.feed,'epoch':batch.epoch,
           'previous':batch.cursor_before,'position':batch.cursor_after,'batch_id':batch.batch_id,
           'payload_digest':hashlib.sha256(batch.begin+b''.join(r.raw for r in batch.records)+batch.commit).hexdigest()}
    return json.dumps(value,sort_keys=True,separators=(',',':'))


def validate_source_checkpoint(text,batch):
    if not isinstance(text,str) or len(text)>16384:raise CheckpointError('Bounded original checkpoint text required')
    value=_json(text.encode('utf-8'))
    if not isinstance(value,dict):raise CheckpointError('Explicit source checkpoint profile required')
    if value.get('profile')=='ashlar-immutable-jsonl/0.1':
        expected=_json(jsonl_checkpoint(batch).encode('utf-8'))
        if value!=expected:raise CheckpointError('JSONL checkpoint differs from original transaction bytes/cursors')
        return value
    if value.get('profile')=='ashlar-postgresql-outbox/0.1':return validate_outbox_checkpoint(text,batch)
    if value.get('profile')=='ashlar-single-line-csv/0.1':
        expected=_json(csv_checkpoint(batch).encode('utf-8'))
        if value!=expected:raise CheckpointError('CSV checkpoint differs from original row custody')
        return value
    raise CheckpointError('Unsupported source checkpoint profile')


def bind_outbox_descriptor(request,descriptor,*,expected_publication_id):
    # Preserve the previously qualified outbox-only entrypoint, never silently
    # admit another source profile through that explicit API.
    value=_json(request['source_checkpoint_json'].encode('utf-8'))
    if not isinstance(value,dict) or value.get('profile')!='ashlar-postgresql-outbox/0.1':raise CheckpointError('Original outbox checkpoint required')
    return bind_source_descriptor(request,descriptor,expected_publication_id=expected_publication_id)

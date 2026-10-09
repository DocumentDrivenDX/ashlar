"""Explicit whole-entity JSONL adapter. Never reconstructs Truss property feeds."""
from .apply import ApplyError,Change,EntityKey,EntityState
from .schema import _json
from .staging import batch_row


def _integer(value):
    if not isinstance(value,str):raise ApplyError('Exact decimal integer text required')
    try:n=int(value)
    except ValueError as exc:raise ApplyError('Invalid integer text') from exc
    if str(n)!=value or not -(2**63)<=n<2**63:raise ApplyError('Noncanonical or out-of-range signed64 integer')
    return n


def changes_from_batch(batch):
    batch_row(batch)  # Original complete transaction bytes and metadata must agree.
    changes=[]
    for record in batch.records:
        value=_json(record.raw)
        if value.get('source_profile')!='ashlar-whole-entity/0.1':raise ApplyError('Explicit whole-entity source profile required')
        needed={'kind','delivery_id','source_profile','source_system','entity_kind','type_id','id','entity_version','schema_revision','operation','props_json','retained_json'}
        extras={'endpoints'} if value.get('entity_kind')=='edge' else set()
        if set(value)!=needed|extras:raise ApplyError('Unsupported/missing executable source assertion')
        key=EntityKey(value['source_system'],value['entity_kind'],_integer(value['type_id']),_integer(value['id']))
        endpoints=None
        if key.kind=='edge':
            refs=value['endpoints']
            if not isinstance(refs,list) or len(refs)!=2 or any(not isinstance(r,dict) or set(r)!={'type_id','id'} for r in refs):raise ApplyError('Complete typed endpoint references required')
            endpoints=tuple(EntityKey(key.source,'object',_integer(r['type_id']),_integer(r['id'])) for r in refs)
        state=EntityState(key,_integer(value['entity_version']),value['schema_revision'],value['props_json'],value['retained_json'],endpoints)
        changes.append(Change(batch.feed,batch.epoch,record.delivery_id,record.sha256,value['operation'],state))
    return tuple(changes)

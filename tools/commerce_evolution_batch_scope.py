"""Explicit source/epoch-qualified envelopes; original event/value bytes unchanged.

This development profile changes only declared transaction identity and its byte
cursor witnesses, so different sources can share one publication stream safely.
It grants no source, writer, migration, publication or ACK authority.
"""
from dataclasses import replace
import json
from ashlar.source import jsonl_batches,records_digest
from ashlar.whole_entity import changes_from_batch
from commerce_evolution_admission import CommerceEvolutionAdmission
from commerce_evolution_transactions import prepare,independent_oracle,text

PROFILE='source-epoch-qualified/0.1'

def qualify(prepared):
    batches=[]
    for original in prepared.batches:
        identity=text([prepared.source_system,prepared.epoch,original.batch_id])
        records=tuple(r.raw for r in original.records)
        begin=(text({'kind':'begin','batch_id':identity})+'\n').encode()
        commit=(text({'kind':'commit','batch_id':identity,'record_count':len(records),'records_sha256':records_digest(records)})+'\n').encode()
        batch,=jsonl_batches((begin,*records,commit),feed=original.feed,epoch=original.epoch)
        if tuple((r.delivery_id,r.raw,r.sha256)for r in batch.records)!=tuple((r.delivery_id,r.raw,r.sha256)for r in original.records):raise ValueError('Qualified envelope changed original records')
        batches.append(batch)
    return replace(prepared,batches=tuple(batches))

class ScopedCommerceEvolutionAdmission(CommerceEvolutionAdmission):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.prepared=qualify(self.prepared)
        if tuple(c for b in self.prepared.batches for c in changes_from_batch(b))!=self.changes:raise PermissionError('Original admitted exact changes changed under envelope binding')
        self.facts['profile']='ashlar-commerce-evolution-private-source-admission/0.2'
        self.facts['batch_identity_profile']=PROFILE
        self.facts['batch_identities']=[b.batch_id for b in self.prepared.batches]

def scoped_oracle(candidate_bytes,proof_bytes,model_bytes,scoped,columns,*,prefix,materialized_at):
    """Independent logical originals plus exact named envelope custody witnesses.

    Only metadata cells are derived from the new declared envelope; all entity
    values, presence, endpoints, retained payloads and histories use the original
    independent snapshot oracle, never native query results or emitted properties.
    """
    original=prepare(candidate_bytes,proof_bytes,model_bytes,source_system=scoped.source_system,epoch=scoped.epoch)
    if scoped.registry_json!=original.registry_json or scoped.schema_revisions!=original.schema_revisions or len(scoped.batches)!=len(original.batches):raise ValueError('Original development/source revision custody differs')
    witnesses={}
    for source_batch,qualified in zip(original.batches,scoped.batches):
        # Independently derive exact envelope identity/serialization/cursor from
        # original byte records, without invoking qualify() or reading SQL rows.
        identity=json.dumps([original.source_system,original.epoch,source_batch.batch_id],ensure_ascii=False,separators=(',',':'))
        begin=(json.dumps({'kind':'begin','batch_id':identity},ensure_ascii=False,separators=(',',':'))+'\n').encode()
        commit=(json.dumps({'kind':'commit','batch_id':identity,'record_count':len(source_batch.records),'records_sha256':source_batch.records_sha256},ensure_ascii=False,separators=(',',':'))+'\n').encode()
        if (qualified.profile,qualified.feed,qualified.epoch,qualified.batch_id,qualified.begin,qualified.commit,qualified.records_sha256)!=(source_batch.profile,source_batch.feed,source_batch.epoch,identity,begin,commit,source_batch.records_sha256):raise ValueError('Exact named source/epoch envelope differs')
        offset=len(begin)
        final_cursor=str(len(begin)+sum(len(r.raw)for r in source_batch.records)+len(commit))
        for before,after in zip(source_batch.records,qualified.records):
            offset+=len(before.raw)
            if (before.delivery_id,before.raw,before.sha256)!=(after.delivery_id,after.raw,after.sha256)or after.cursor!=str(offset):raise ValueError('Original record bytes/cursor correspondence differs')
            witnesses[before.delivery_id]={'batch_id':identity,'cursor':json.dumps({'profile':qualified.profile,'offset':final_cursor},separators=(',',':'))}
        if len(qualified.records)!=len(source_batch.records)or qualified.cursor_before!='0'or qualified.cursor_after!=str(offset+len(commit)):raise ValueError('Exact original envelope offsets differ')
    result=independent_oracle(candidate_bytes,proof_bytes,model_bytes,original,columns,prefix=prefix,materialized_at=materialized_at)
    for role in ('object_current','edge_current','tombstone'):
        for row in result[role]:
            witness=witnesses[row['source_delivery_id']];row['source_cursor_json']=witness['cursor']
            if 'apply_batch_id'in row:row['apply_batch_id']=witness['batch_id']
    return result

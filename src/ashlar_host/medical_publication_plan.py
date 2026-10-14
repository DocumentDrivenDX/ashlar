"""Construction-bound original medical intent; no native, phase or ACK authority."""
from dataclasses import asdict
import datetime
import hashlib
import json
from types import MappingProxyType
from ashlar.apply import empty_state, Change
from ashlar.whole_entity import changes_from_batch
from ashlar.staging import batch_row
from ashlar.source_checkpoint import outbox_checkpoint
from ashlar.outbox import OutboxTransaction
from ashlar.source import SourceBatch
from ashlar.medical_source import SOURCE_SHA
from .delta_custody import encoded
from .lifecycle import finish
from .graph_sql import graph_sql_plan
from .local_physical_carriers import original_local_physical_carriers, original_local_materialization_columns, ORIGINAL_LOCAL_PHYSICAL_PROVENANCE
from .original_medical_publication import MedicalAdmission, original_medical_oracle
from .publication_planning import request_for, local_effect_plan

FEED='private-original-medical-fixture'
EPOCH='original-medical-v1'
STREAM='private-original-medical'
PREDECESSOR='explicit-original-medical-origin'
PUBLICATION_ID='original-medical-publication-1'
CLOCK='2026-10-09T12:00:00+00:00'
ROLES=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
MAXIMUM_BYTES=8*1024*1024
_REQUEST_OMITTED=object()


class MedicalPublicationPlan:
    """Exact original source → independent oracle and generated effect intent.

    Returned snapshots are detached complete views. Renew before owner activation;
    current source/native/lease admission remains with the publication owners.
    """
    __slots__=('_admission','_original_admission','_transaction','_original_transaction','_tables','_original')

    def __init__(self,admission,transaction,tables):
        if type(admission)is not MedicalAdmission or type(transaction)is not OutboxTransaction:
            raise ValueError('Exact original medical admission and outbox transaction required')
        if (type(tables)not in (dict,MappingProxyType)or set(tables)!=set(ROLES)
                or any(type(k)is not str or type(v)is not str or v!='local.original_medical.'+k for k,v in tables.items())):
            raise ValueError('Exact six original medical target names required')
        object.__setattr__(self,'_admission',admission);object.__setattr__(self,'_original_admission',admission)
        object.__setattr__(self,'_transaction',transaction);object.__setattr__(self,'_original_transaction',transaction);object.__setattr__(self,'_tables',encoded(dict(tables)))
        object.__setattr__(self,'_original',encoded(self._build()))
        if len(self._original.encode())>MAXIMUM_BYTES:raise ValueError('Bounded complete medical plan required')

    def __setattr__(self,name,value):raise AttributeError('Original medical plan is immutable')

    @property
    def admission(self):return self._admission

    @property
    def transaction(self):return self._transaction

    def _build(self):
        admission=self._admission;transaction=self._transaction
        if admission is not self._original_admission or type(admission)is not MedicalAdmission or type(transaction)is not OutboxTransaction or transaction is not self._original_transaction:
            raise PermissionError('Original construction-bound medical owners changed')
        metadata=admission.metadata();original_batch=encoded(batch_row(admission.batch))
        original_transaction=self._transaction;original_transaction_view=None
        result=None;primary=None
        def closing_admission():
            if encoded(admission.metadata())!=encoded(metadata):raise PermissionError('Original medical admission changed during planning')
        def closing_batch():
            if encoded(batch_row(admission.batch))!=original_batch:raise PermissionError('Original medical batch changed during planning')
        def closing_transaction():
            if self._transaction is not original_transaction or original_transaction is not self._original_transaction or encoded(self._transaction_view(original_transaction))!=original_transaction_view:raise PermissionError('Original medical transaction changed during planning')
        try:
            original_transaction_view=encoded(self._transaction_view(original_transaction))
            result=self._assemble(metadata)
        except BaseException as error:primary=error
        finish(primary,[closing_admission,closing_batch,closing_transaction]);return result

    def _transaction_view(self,transaction):
        if (type(transaction)is not OutboxTransaction or type(transaction.batch)is not SourceBatch
                or any(type(getattr(transaction,name))is not str for name in ('profile','feed','epoch','previous','position','payload_digest'))):
            raise PermissionError('Exact original complete transaction carriers required')
        return {'checkpoint':outbox_checkpoint(transaction),'batch':batch_row(transaction.batch)}

    def _assemble(self,metadata):
        admission=self._admission;transaction=self._transaction;batch=admission.batch
        raw=batch.begin+b''.join(record.raw for record in batch.records)+batch.commit
        if (type(transaction.batch)is not SourceBatch or type(transaction.profile)is not str or type(transaction.feed)is not str or type(transaction.epoch)is not str
                or type(transaction.previous)is not str or type(transaction.position)is not str or type(transaction.payload_digest)is not str
                or (transaction.profile,transaction.feed,transaction.epoch,transaction.previous,transaction.position)!=('ashlar-postgresql-outbox/0.1',FEED,EPOCH,'0','1')
                or (batch.feed,batch.epoch,batch.batch_id)!=(FEED,EPOCH,EPOCH)
                or transaction.batch!=batch or len(raw)>1048576
                or transaction.payload_digest!=hashlib.sha256(raw).hexdigest()
                or transaction.payload_digest!=metadata['source_transaction_sha256']):
            raise PermissionError('Original complete medical checkpoint/transaction differs')
        tables=json.loads(self._tables);columns=original_local_materialization_columns();carriers=original_local_physical_carriers()
        graph_tables={role:tables[role]for role in ROLES[:4]}
        expected=original_medical_oracle(*admission.originals[:2],json.loads(admission.bindings_bytes),feed=FEED,epoch=EPOCH,batch_id=EPOCH,materialized_at=CLOCK,columns=columns)
        if tuple(len(expected[r])for r in ROLES[:4])!=(51,62,0,113):raise PermissionError('Complete original medical oracle required')
        original_changes=changes_from_batch(batch)
        original_change_views=frozenset(encoded(asdict(change))for change in original_changes)
        def admitted(change):
            if type(change)is not Change or encoded(asdict(change))not in original_change_views:raise PermissionError('Exact original medical change required for pure planning')
        _,generated=graph_sql_plan(empty_state(),batch,graph_tables,materialized_at=CLOCK,schema_policy=admitted)
        empty={role:[]for role in ROLES[:4]};selected,elisions=local_effect_plan(generated,empty,graph_tables)
        request=request_for(STREAM,transaction,PREDECESSOR,{FEED:SOURCE_SHA});checkpoint=json.loads(request['source_checkpoint_json'])
        stamp=datetime.datetime.fromisoformat(CLOCK)-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)
        return {'profile':'ashlar-original-medical-publication-plan/0.1','request':request,'publication_id':PUBLICATION_ID,
            'materialized_at':CLOCK,'recorded_at':str((stamp.days*86400+stamp.seconds)*1000000+stamp.microseconds),
            'tables':tables,'columns':columns,'schemas':{role:carriers['carriers'][role]['schemaJson']for role in ROLES[:4]},
            'generated_provenance_sha256':ORIGINAL_LOCAL_PHYSICAL_PROVENANCE,'source_admission':metadata,
            'generated_steps':generated,'selected_steps':selected,'zero_match_elisions':elisions,
            'previous_expected':empty,'expected':expected,'previous_progress':{},'progress':{FEED:checkpoint}}

    def renew(self):
        if encoded(self._build())!=self._original:raise PermissionError('Original complete medical plan correspondence changed')

    def snapshot(self):
        self.renew();return json.loads(self._original)

    def validated_snapshot(self,admission,*,request=_REQUEST_OMITTED):
        """One full renewal for jointly checked original admission/request pins."""
        def owner():
            if type(admission)is not MedicalAdmission or admission is not self._original_admission or self._admission is not admission:
                raise PermissionError('Actual construction-bound medical admission required')
        owner();self.renew();owner()
        value=json.loads(self._original)
        if request is not _REQUEST_OMITTED and (type(request)not in (dict,MappingProxyType)
                or any(type(k)is not str or type(v)is not str for k,v in request.items())
                or encoded(dict(request))!=encoded(value['request'])):
            raise PermissionError('Exact original medical request required')
        return value

    def require_request(self,request):
        self.renew()
        if (type(request)not in (dict,MappingProxyType)or any(type(k)is not str or type(v)is not str for k,v in request.items())
                or encoded(dict(request))!=encoded(json.loads(self._original)['request'])):
            raise PermissionError('Exact original medical request required')

    def require_admission(self,admission):
        if type(admission)is not MedicalAdmission or admission is not self._original_admission:raise PermissionError('Actual construction-bound medical admission required')
        self.renew()

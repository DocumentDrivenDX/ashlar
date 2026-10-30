# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original configured immutable-file lifecycle specifications, not Delta authority.

The source owns portable prefix states and exact publisher requests. Native
creation/effects/manifest resolution and local progress remain separate owners.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib,json
from pathlib import Path
from ashlar.apply import empty_state,plan_apply
from ashlar.attempt_store import verify_request_digest
from ashlar.source import jsonl_batches
from ashlar.source_checkpoint import jsonl_checkpoint
from ashlar.staging import batch_row,batch_from_row
from ashlar.whole_entity import changes_from_batch
from ashlar.schema import decode_retained_json
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .configured_source_admission import ConfiguredSourceAdmissionConfig,validate_configured_source
from .lifecycle import finish


def _require(value):
    if not value:raise ValueError('Original configured local source refused')

def _encoded(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))


@dataclass(frozen=True)
class _Sdk058TypedGraphConfiguration:
    """Explicit release policy view over an unchanged historical source config."""
    base:object
    receipt:bytes
    policy:object
    policy_revision:str
    @classmethod
    def admitted(cls,base,admission,receipt=None):
        from .typed_graph_sdk058_admission import admit_typed_graph_sdk058,SDK058_PRODUCER_REVISION
        from ashlar.typed_graph_source import TypedGraphSourcePolicy
        raw=receipt if receipt is not None else admit_typed_graph_sdk058(admission,base,base.model_source)
        return cls(base,raw,
                   TypedGraphSourcePolicy(base,raw,producer_revision=SDK058_PRODUCER_REVISION),
                   SDK058_PRODUCER_REVISION)
    def __getattr__(self,name):return getattr(self.base,name)
    def verify(self):
        self.base.verify()
        from ashlar.typed_graph_source import TypedGraphSourcePolicy
        expected=TypedGraphSourcePolicy(self.base,self.receipt,producer_revision=self.policy_revision)
        _require(vars(expected)==vars(self.policy))

@dataclass(frozen=True)
class ConfiguredLocalBatchSpec:
    ordinal:int
    request_json:str
    publication_id:str
    def request(self):return decode_retained_json(self.request_json.encode())


class ConfiguredLocalSource:
    """Minted immutable source; original strings/unknown content remain unchanged."""
    def __setattr__(self,name,value):
        if getattr(self,'_sealed',False):raise AttributeError('Configured local source is immutable')
        object.__setattr__(self,name,value)
    def __init__(self,admission,stream,context,*,sdk058_admission=None,_mint=None):
        _require(_mint is _MINT and type(admission)is ConfiguredSourceAdmissionConfig)
        _require(type(stream)is str and 0<len(stream.encode())<=1024 and '\0'not in stream and context is not None)
        admission.__post_init__();self.context=context;self.stream=stream;self._active=True
        self.configuration_file=InvocationFile.read(admission.configuration,262144)
        document=decode_retained_json(self.configuration_file.raw);files=[]
        self.graph=document.get('profile')=='ashlar-jsonl-typed-graph-source/0.1'
        self.typed=self.graph or document.get('profile')=='ashlar-jsonl-typed-source/0.1'
        for name in (('source','schema','bindings')if self.typed else('source','schema','intake','interpretation')):
            ref=document[name];_require(type(ref)is dict and set(ref)=={'path','sha256'}and type(ref['path'])is str)
            file=InvocationFile.read((self.configuration_file.path.parent/ref['path']).resolve(),262144)
            _require(hashlib.sha256(file.raw).hexdigest()==ref['sha256']);files.append(file)
        self.files=tuple(files)
        if self.graph:
            from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
            self.configuration=load_typed_graph_jsonl_configuration(self.configuration_file.path)
        elif self.typed:
            from ashlar.typed_source_config import load_typed_jsonl_configuration
            self.configuration=load_typed_jsonl_configuration(self.configuration_file.path)
        else:
            from ashlar.source_config import load_jsonl_configuration
            self.configuration=load_jsonl_configuration(self.configuration_file.path)
        self.original_source=self.files[0].raw
        self.sdk058_admission=sdk058_admission
        self.sdk058_receipt=None
        if sdk058_admission is not None:
            _require(self.graph)
            from .typed_graph_sdk058_admission import admit_typed_graph_sdk058
            self.sdk058_receipt=admit_typed_graph_sdk058(
                sdk058_admission,self.configuration,self.configuration.model_source)
        self.batches=tuple(jsonl_batches(self.original_source.splitlines(keepends=True),feed=self.configuration.feed,epoch=self.configuration.epoch))
        _require(1<=len(self.batches)<=4)
        if not self.typed:self._specs=self._prepare()
        report=validate_configured_source(admission);self.admission_json=_encoded(report)
        self.receipt_files=tuple((admission.output/item['file'],snapshot(admission.output/item['file'],admission.maximum_receipt_bytes))for item in report['receipts'])
        _require(all(hashlib.sha256(file[1]).hexdigest()==item['sha256']and len(file[1])==item['bytes']for file,item in zip(self.receipt_files,report['receipts'])))
        self.maximum_receipt_bytes=admission.maximum_receipt_bytes
        if self.typed:
            _require(len(self.receipt_files)==1)
            if self.graph:
                if self.sdk058_admission is None:
                    self.configuration=load_typed_graph_jsonl_configuration(self.configuration_file.path,receipt=self.receipt_files[0][1])
                else:
                    base=load_typed_graph_jsonl_configuration(self.configuration_file.path)
                    self.configuration=_Sdk058TypedGraphConfiguration.admitted(
                        base,self.sdk058_admission,self.sdk058_receipt)
            else:self.configuration=load_typed_jsonl_configuration(self.configuration_file.path,receipt=self.receipt_files[0][1])
            self._specs=self._prepare()
        self.type_ids=self.configuration.type_ids
        _require(decode_retained_json(self.admission_json.encode())['source_sha256']==hashlib.sha256(self.original_source).hexdigest())
        self.renew(context);self._sealed=True
    def _prepare(self):
        state=empty_state();specs=[];previous='empty-local-configured-jsonl-stream'
        revisions=_encoded({self.configuration.schema_alias:self.configuration.document_revision})
        for ordinal,batch in enumerate(self.batches,1):
            staged=batch_row(batch);_require(batch_from_row(staged)==batch)
            state=plan_apply(state,changes_from_batch(batch),schema_policy=self.configuration.policy)
            request={'stream':self.stream,'batch_id':batch.batch_id,'predecessor':previous,'schema_revisions_json':revisions,
                     'source_batch_json':staged['batch_json'],'source_batch_digest':staged['batch_digest'],'source_checkpoint_json':jsonl_checkpoint(batch)}
            request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
            verify_request_digest(request)
            publication='local-configured-jsonl-'+request['request_digest'];specs.append(ConfiguredLocalBatchSpec(ordinal,_encoded(request),publication));previous=publication
        replay=state
        for batch in self.batches:replay=plan_apply(replay,changes_from_batch(batch),schema_policy=self.configuration.policy)
        _require(replay==state)
        return tuple(specs)
    def renew(self,context):
        _require(self._active and context is self.context)
        finish(None,[lambda:self.configuration_file.renew(262144),*(lambda f=f:f.renew(262144)for f in self.files),self.configuration.verify,*(lambda f=f:_require(snapshot(f[0],self.maximum_receipt_bytes)==f[1])for f in self.receipt_files)])
        if self.graph:
            from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
            if self.sdk058_admission is None:
                fresh=load_typed_graph_jsonl_configuration(self.configuration_file.path,receipt=self.receipt_files[0][1])
            else:
                from .typed_graph_sdk058_admission import admit_typed_graph_sdk058
                unadmitted=load_typed_graph_jsonl_configuration(self.configuration_file.path)
                receipt=admit_typed_graph_sdk058(
                    self.sdk058_admission,unadmitted,unadmitted.model_source)
                _require(receipt==self.sdk058_receipt)
                fresh=_Sdk058TypedGraphConfiguration.admitted(
                    unadmitted,self.sdk058_admission,receipt)
        elif self.typed:
            from ashlar.typed_source_config import load_typed_jsonl_configuration
            fresh=load_typed_jsonl_configuration(self.configuration_file.path,receipt=self.receipt_files[0][1])
        else:
            from ashlar.source_config import load_jsonl_configuration
            fresh=load_jsonl_configuration(self.configuration_file.path)
        _require(vars(fresh.policy)==vars(self.configuration.policy))
        _require(self.type_ids==fresh.type_ids)
        _require(tuple(jsonl_batches(self.files[0].raw.splitlines(keepends=True),feed=self.configuration.feed,epoch=self.configuration.epoch))==self.batches)
        _require(self._prepare()==self._specs)
    def metadata(self,context):
        self.renew(context)
        result={'profile':'ashlar-configured-local-typed-graph-source/0.1'if self.graph else'ashlar-configured-local-typed-source/0.1'if self.typed else'ashlar-configured-local-source/0.1','configuration_sha256':hashlib.sha256(self.configuration_file.raw).hexdigest(),
                'source_sha256':hashlib.sha256(self.original_source).hexdigest(),'model_sha256':self.configuration.model_sha256,
                'admission_report_sha256':hashlib.sha256(self.admission_json.encode()).hexdigest(),'batches':len(self.batches),
                'checkpoint_profile':'ashlar-immutable-jsonl/0.1',
                'qualification':('Original authored core0.8 joint supplied-dataset and carried Record checks with local immutable-file progress only; no protected source ACK, native/catalog or query authority.'if self.graph else'Original authored core0.8 scalar carried Record values, including deletes, and local immutable-file progress only; no keys/relationships, protected source ACK, native or query authority.'if self.typed else'Original configured STRING objects and local immutable-file progress only; no protected source ACK or native authority.')}
        if self.sdk058_admission is not None:
            result['typed_graph_policy_revision']=self.configuration.policy_revision
            result['typed_graph_sdk058_receipt_sha256']=hashlib.sha256(self.sdk058_receipt).hexdigest()
        return result
    def spec(self,ordinal,context):
        self.renew(context);_require(type(ordinal)is int and 1<=ordinal<=len(self.batches));return self._specs[ordinal-1]
    def state_at(self,completed_ordinal,context):
        """Fresh detached portable prefix state; never an observed Delta snapshot."""
        self.renew(context);_require(type(completed_ordinal)is int and 0<=completed_ordinal<=len(self.batches))
        state=empty_state()
        for batch in self.batches[:completed_ordinal]:state=plan_apply(state,changes_from_batch(batch),schema_policy=self.configuration.policy)
        return state
    def admit_request(self,request,context):
        self.renew(context);_require(type(request)is dict);verify_request_digest(request)
        _require(any(_encoded(request)==spec.request_json for spec in self._specs))
    def close(self):
        try:self.renew(self.context)
        finally:object.__setattr__(self,'_active',False)

_MINT=object()

@contextmanager
def open_configured_local_source(admission,*,stream,context,sdk058_admission=None):
    owner=ConfiguredLocalSource(admission,stream,context,sdk058_admission=sdk058_admission,_mint=_MINT);primary=None
    try:yield owner
    except BaseException as exc:primary=exc
    finish(primary,[owner.close])

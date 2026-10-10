"""Held original immutable-file source; no outbox or protected ACK semantics."""
import hashlib,json
from pathlib import Path
from ashlar.supply_chain_source import build_transaction,SOURCE_SHA,GRAPH_SHA
from ashlar.whole_entity import changes_from_batch
from ashlar.staging import batch_row
from .source import read_bounded
from .supply_chain_request import SOURCE_SYSTEM,source_snapshot
from .delta_custody import encoded
from .evolution_admission import original_equal

UMF_REVISION='fc78a6d48f08b3640748ccac0fc4a9ee06fc61e3'
PROFILE='ashlar-original-supply-chain-immutable-file/0.1'

class FiniteSupplyChainSource:
    """The supplied producer must actually execute public finite-dataset validation.

    The callback receives original bytes and returns its complete emitted receipt.
    Its identity is a composition boundary, never evidence that it ran correctly.
    Qualification requires actual producer/runtime evidence independently of this
    source owner. Cooperating original files remain held and byte-exact.
    """
    def __init__(self,model,graph,*,current_dataset):
        if not callable(current_dataset):raise ValueError('Actual selected public dataset producer required')
        paths=[Path(p)for p in (model,graph)]
        if any(not p.is_absolute()or any(q.is_symlink()for q in (p,*p.parents))for p in paths):
            raise ValueError('Explicit immutable original source files required')
        self.originals=tuple((p,read_bounded(p,1024*1024))for p in paths)
        self.identities=tuple((p.stat().st_dev,p.stat().st_ino)for p,_ in self.originals)
        self.model,self.graph=[raw for _,raw in self.originals]
        if hashlib.sha256(self.model).hexdigest()!=SOURCE_SHA or hashlib.sha256(self.graph).hexdigest()!=GRAPH_SHA:
            raise ValueError('Exact original finite source required')
        self.batch,self.bindings=build_transaction(self.model,self.graph,source_system=SOURCE_SYSTEM)
        self.changes=changes_from_batch(self.batch);self.current_dataset=current_dataset
        self.receipt=self._dataset();self.raw=self.batch.begin+b''.join(r.raw for r in self.batch.records)+self.batch.commit
        self.receipt_facts={'profile':PROFILE,'qualification':'Original finite immutable-file supply-chain source; no PostgreSQL/outbox/protected ACK authority',
            'source_system':SOURCE_SYSTEM,'declared_umf':'0.8.0','source_sha256':SOURCE_SHA,'graph_sha256':GRAPH_SHA,
            'source_transaction_sha256':hashlib.sha256(self.raw).hexdigest(),'public_dataset_receipt_sha256':hashlib.sha256(self.receipt).hexdigest(),
            'umf_revision':UMF_REVISION,'records':20,'relationships':23}
    def _dataset(self):
        raw=self.current_dataset(self.model,self.graph)
        if type(raw)is not bytes or not 0<len(raw)<=8*1024*1024:raise ValueError('Complete bounded actual dataset receipt required')
        def pairs(items):
            result={}
            for key,value in items:
                if key in result:raise ValueError('Duplicate public dataset member')
                result[key]=value
            return result
        def integer(token):
            if len(token)>21:raise ValueError('Public dataset integer bound')
            return int(token)
        def refuse(token):raise ValueError('Exact public dataset values required')
        value=json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refuse,parse_constant=refuse);receipt=value['receipt'];graph=json.loads(self.graph)
        if (value.get('profile')!='ashlar-supply-chain-public-dataset/0.1' or value.get('umfRevision')!=UMF_REVISION
            or value.get('sourceSha256')!=SOURCE_SHA or value.get('graphSha256')!=GRAPH_SHA
            or receipt.get('scope')!='supplied-dataset-only' or receipt.get('input',{}).get('scope')!={'id':'ashlar-original-supply-chain-fixture','closure':'supplied-dataset-only'}
            or receipt.get('datasetValidation',{}).get('valid')is not True
            or receipt.get('datasetValidation',{}).get('complete')is not True
            or receipt.get('datasetValidation',{}).get('diagnostics')!=[]):
            raise ValueError('Original complete public finite dataset semantics required')
        records=receipt['records'];keys=receipt['keys'];edges=receipt['relationships']
        if len(records)!=20 or {r['instanceId']for r in records}!={r['key']for r in graph['objects']} or len(keys)!=20:
            raise ValueError('Original complete Record/key inventory differs')
        if any(r['result']['validation']['valid']is not True for r in records):raise ValueError('Original Record validation refused')
        if len(edges)!=23 or {r['instanceId']:(r['sourceInstanceId'],r['targetInstanceId'])for r in edges}!={r['key']:(r['source'],r['target'])for r in graph['edges']}:
            raise ValueError('Original complete relationship occurrences differ')
        return raw
    def metadata(self):
        if tuple((p.stat().st_dev,p.stat().st_ino)for p,_ in self.originals)!=self.identities or any(read_bounded(p,1024*1024)!=raw for p,raw in self.originals):raise ValueError('Original immutable source file changed')
        return source_snapshot(self.receipt_facts)
    def renew(self):
        self.metadata()
        if self._dataset()!=self.receipt:raise ValueError('Closing actual public producer receipt differs')
        self.metadata()
    def admit(self,change):
        self.metadata()
        if not any(original_equal(change,owned)for owned in self.changes):raise ValueError('No admitted original finite change')
    def admit_request(self,request):
        request=source_snapshot(request)
        self.metadata();row=batch_row(self.batch)
        if (type(request)is not dict or set(request)!={'stream','batch_id','predecessor','schema_revisions_json','source_batch_json','source_batch_digest','request_digest'}
            or request['batch_id']!=self.batch.batch_id or request['source_batch_json']!=row['batch_json']or request['source_batch_digest']!=row['batch_digest']
            or json.loads(request['schema_revisions_json'])!={SOURCE_SYSTEM:SOURCE_SHA}):
            raise ValueError('Exact original finite request without outbox checkpoint required')
        original={k:v for k,v in request.items()if k!='request_digest'}
        if hashlib.sha256(encoded(original).encode()).hexdigest()!=request['request_digest']:raise ValueError('Original request digest differs')

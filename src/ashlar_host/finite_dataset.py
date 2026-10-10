"""Owned original finite files and actual public UMF dataset custody.

Dataset validation never grants source, writer, publication or ACK authority.
The independently supplied current source policy remains mandatory at every gate.
"""
from dataclasses import dataclass
from pathlib import Path
import hashlib,json
from typing import Protocol,Mapping
from ashlar.staging import batch_row
from ashlar.whole_entity import changes_from_batch
from .finite_pack import FinitePackDefinition,UMF_REVISION
from .evolution_producer import capture,snapshot
from .evolution_admission import original_equal
from .resources import RESOURCE_ROOT
from .supply_chain_request import source_snapshot
from .delta_custody import encoded

class FiniteSourcePolicy(Protocol):
    def admit_source(self,facts:Mapping[str,object],context:object)->None:...

def _path(path):
    if type(path)is not type(Path('/'))or not path.is_absolute()or any(p.is_symlink()for p in (path,*path.parents)):raise ValueError('Exact absolute original finite path required')
    return path

@dataclass(frozen=True)
class FiniteDatasetConfig:
    definition:FinitePackDefinition
    source:Path
    bun:Path
    git:Path
    timeout_seconds:int
    maximum_output_bytes:int
    maximum_receipt_bytes:int
    def __post_init__(self):
        if type(self.definition)is not FinitePackDefinition:raise ValueError('Owned closed finite pack required')
        self.definition.__post_init__()
        for p in (self.source,self.bun,self.git):_path(p)
        for v,limit in ((self.timeout_seconds,60),(self.maximum_output_bytes,1048576),(self.maximum_receipt_bytes,33554432)):
            if type(v)is not int or not 0<v<=limit:raise ValueError('Explicit bounded public dataset operation required')

class HeldFiniteDataset:
    """Execute public validation once, renew clean pinned producer and whole receipt."""
    def __init__(self,config,model,graph,output):
        if type(config)is not FiniteDatasetConfig:raise ValueError('Typed public producer configuration required')
        config.__post_init__();self.config=config
        self.model,self.graph,self.output=map(_path,(model,graph,output))
        if output.exists():raise ValueError('Fresh public receipt output required')
        self.originals=(snapshot(model,1048576),snapshot(graph,1048576));config.definition.inputs(*self.originals)
        self.script=RESOURCE_ROOT/'check_finite_pack_dataset.ts';self.script_bytes=snapshot(self.script,1048576)
        self.environment={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','TZ':'UTC','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_CONFIG_SYSTEM':'/dev/null','GIT_TERMINAL_PROMPT':'0'}
        self._pin()
        self.stdout,self.stderr=capture([str(config.bun),str(self.script),str(config.source),str(output),str(model),str(graph),str(config.git),config.definition.name],cwd=output.parent,environment=self.environment,timeout_seconds=config.timeout_seconds,maximum_output_bytes=config.maximum_output_bytes)
        self.raw=snapshot(output,config.maximum_receipt_bytes);self.renew()
    def _pin(self):
        for args,expected in ((['rev-parse','HEAD'],UMF_REVISION),(['status','--porcelain'],'')):
            out,err=capture([str(self.config.git),'-C',str(self.config.source),*args],cwd=self.output.parent,environment=self.environment,timeout_seconds=self.config.timeout_seconds,maximum_output_bytes=self.config.maximum_output_bytes)
            if out.decode().strip()!=expected or err:raise ValueError('Clean exact public UMF source required')
    def renew(self):
        self.config.__post_init__();self._pin()
        if (snapshot(self.model,1048576),snapshot(self.graph,1048576))!=self.originals or snapshot(self.script,1048576)!=self.script_bytes or snapshot(self.output,self.config.maximum_receipt_bytes)!=self.raw:raise ValueError('Original public producer custody changed')
    def receipt(self,model,graph):
        if type(model)is not bytes or type(graph)is not bytes or (model,graph)!=self.originals:raise ValueError('Original public producer inputs differ')
        self.renew();return self.raw

def _receipt(raw):
    def pairs(items):
        value={}
        for k,v in items:
            if k in value:raise ValueError('Duplicate dataset receipt member')
            value[k]=v
        return value
    def integer(token):
        if len(token)>21:raise ValueError('Dataset integer bound')
        return int(token)
    def refuse(token):raise ValueError('Exact dataset receipt values required')
    return json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refuse,parse_constant=refuse)

class FinitePackSource:
    """Typed original bytes plus public semantics, under separate current authority."""
    def __init__(self,definition,model,graph,producer,*,policy,context):
        if type(definition)is not FinitePackDefinition or type(producer)is not HeldFiniteDataset or not callable(getattr(policy,'admit_source',None)):raise ValueError('Owned producer and independent current source authority required')
        definition.__post_init__()
        if not original_equal(definition,producer.config.definition):raise ValueError('Selected producer pack differs')
        self.definition=FinitePackDefinition(definition.name);self.policy=policy;self.context=context
        self.paths=tuple(map(_path,(model,graph)));self.identities=tuple((p.stat().st_dev,p.stat().st_ino)for p in self.paths)
        self.originals=tuple(snapshot(p,1048576)for p in self.paths)
        if tuple((p.stat().st_dev,p.stat().st_ino)for p in self.paths)!=self.identities:raise ValueError('Original finite files changed during opening')
        self.model,self.graph=self.originals;facts=self.definition.inputs(*self.originals);self._facts=encoded(facts)
        self._authority();self.producer=producer;self.raw=producer.receipt(*self.originals)
        if type(self.raw)is not bytes or not 0<len(self.raw)<=producer.config.maximum_receipt_bytes:raise ValueError('Exact bounded original public receipt required')
        value=_receipt(self.raw);receipt=value['receipt'];authored=json.loads(self.graph)
        if value.get('profile')!='ashlar-finite-pack-public-dataset/0.1' or value.get('pack')!=definition.name or value.get('umfRevision')!=UMF_REVISION or value.get('sourceSha256')!=facts['model_sha256']or value.get('graphSha256')!=facts['graph_sha256']or receipt.get('scope')!='supplied-dataset-only'or receipt.get('input',{}).get('scope')!={'id':'ashlar-original-'+definition.name+'-fixture','closure':'supplied-dataset-only'}or receipt.get('datasetValidation',{}).get('valid')is not True or receipt.get('datasetValidation',{}).get('complete')is not True or receipt.get('datasetValidation',{}).get('diagnostics')!=[]:raise ValueError('Complete original public dataset admission required')
        records=receipt['records'];edges=receipt['relationships']
        if any(r.get('result',{}).get('validation',{}).get('valid')is not True for r in records):raise ValueError('Original public Record validation refused')
        if len(records)!=facts['objects']or len(receipt['keys'])!=facts['objects']or {r['instanceId']for r in records}!={o['key']for o in authored['objects']}or len(edges)!=facts['edges']or {r['instanceId']:(r['sourceInstanceId'],r['targetInstanceId'])for r in edges}!={e['key']:(e['source'],e['target'])for e in authored['edges']}:raise ValueError('Complete original dataset occurrences/endpoints differ')
        self.batch,self.bindings=self.definition.build(*self.originals);self.changes=changes_from_batch(self.batch)
        self.metadata()
    def _authority(self):
        # A copied read-only fact inventory is correspondence, never the authority.
        from types import MappingProxyType
        if self.policy.admit_source(MappingProxyType(json.loads(self._facts)),self.context)is not None:raise PermissionError('Current source policy must admit explicitly')
    def metadata(self):
        self._authority()
        for p,raw,identity in zip(self.paths,self.originals,self.identities):
            _path(p)
            if (p.stat().st_dev,p.stat().st_ino)!=identity or snapshot(p,1048576)!=raw:raise ValueError('Original finite source file changed')
        receipt=self.producer.receipt(*self.originals)
        if type(receipt)is not bytes or receipt!=self.raw:raise ValueError('Original admitted public dataset receipt changed')
        self._authority()
        return {**json.loads(self._facts),'public_dataset_receipt_sha256':hashlib.sha256(self.raw).hexdigest(),'qualification':'Original finite-file semantics only; no catalog/writer/publication/outbox/ACK grant'}
    def admit(self,change):
        if not any(original_equal(change,owned)for owned in self.changes):raise ValueError('No exact original finite change')
        self.metadata()
    def admit_request(self,request):
        request=source_snapshot(request)
        if type(request)is not dict or set(request)!={'stream','batch_id','predecessor','schema_revisions_json','source_batch_json','source_batch_digest','request_digest'}or any(type(v)is not str or not v for v in request.values()):raise ValueError('Exact finite request carriers required')
        row=batch_row(self.batch);facts=json.loads(self._facts)
        if request['batch_id']!=self.batch.batch_id or request['source_batch_json']!=row['batch_json']or request['source_batch_digest']!=row['batch_digest']or request['schema_revisions_json']!=encoded({facts['source_system']:facts['model_sha256']})or request['request_digest']!=hashlib.sha256(encoded({k:v for k,v in request.items()if k!='request_digest'}).encode()).hexdigest():raise ValueError('Original finite request differs')
        self.metadata()

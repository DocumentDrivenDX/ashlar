"""Closed immutable original17 selections; no file, SDK or authority acquisition."""
from __future__ import annotations
from dataclasses import dataclass
import json,re
from pathlib import Path
from .config import ProducerConfig
from .local_graph_export import LocalGraphExportSelection

PROFILE='ashlar-original17-puppygraph-invocation/0.1'
MAP_PUPPY='ashlar-puppygraph-1.13-flat-valuemap/0.1'

def _text(value,maximum):
    if type(value)is not str or not value or len(value.encode())>maximum or any(c in value for c in ('\0','\n','\r')):raise ValueError('Explicit bounded scenario text required')
    return value
def _path(value):
    if type(value)is not type(Path('/')):raise ValueError('Typed absolute scenario path required')
    text=_text(str(value),4096)
    if not value.is_absolute()or '..'in value.parts or ','in text:raise ValueError('Explicit lexical absolute scenario path required')
    return value
def _decode_path(value):
    text=_text(value,4096);path=Path(text)
    if str(path)!=text:raise ValueError('Unchanged lexical scenario path required')
    return _path(path)
def _bound(value,maximum):
    if type(value)is not int or not 1<=value<=maximum:raise ValueError('Exact finite positive scenario bound required')
def _closed(value,keys):
    if type(value)is not dict or set(value)!=set(keys):raise ValueError('Closed original17 selection required')
    return value

@dataclass(frozen=True)
class PuppyScenarioPackFiles:
    name:str
    model:Path
    graph:Path
    pack:Path
    export:LocalGraphExportSelection
    def __post_init__(self):
        if self.name not in ('archaeology','ecology')or type(self.name)is not str or type(self.export)is not LocalGraphExportSelection:raise ValueError('Named original scenario pack/export required')
        paths=(self.model,self.graph,self.pack,self.export.release_path,self.export.custody_path)
        for p in paths:_path(p)
        if len(set(paths))!=5:raise ValueError('Distinct original pack and pair paths required')
        for digest in (self.export.release_sha256,self.export.custody_sha256):
            if type(digest)is not str or re.fullmatch('[0-9a-f]{64}',digest)is None:raise ValueError('Separately selected original pair SHA256 required')

@dataclass(frozen=True)
class PuppyScenarioCredentials:
    username:ScenarioSecretText
    password:ScenarioSecretText
    def __post_init__(self):
        if type(self.username)is not ScenarioSecretText or type(self.password)is not ScenarioSecretText:raise ValueError('Typed local secrets required')

class ScenarioSecretText:
    """Original17 secret; this module owns the explicit transport reveal port."""
    __slots__=('__value',)
    def __init__(self,value):object.__setattr__(self,'_ScenarioSecretText__value',_text(value,256))
    def __setattr__(self,name,value):raise AttributeError('Immutable scenario secret')
    def __delattr__(self,name):raise AttributeError('Immutable scenario secret')
    def __repr__(self):return 'ScenarioSecretText([redacted])'
    def __str__(self):return '[redacted]'
    def __reduce__(self):raise TypeError('Scenario secret serialization refused')
    def __deepcopy__(self,memo):return self
    def _read(self):return self.__value

def with_puppy_scenario_credentials(credentials,use):
    """Pass explicit secrets only to the caller's owned runtime transport action."""
    if type(credentials)is not PuppyScenarioCredentials or not callable(use):raise ValueError('Exact credentials and explicit transport action required')
    credentials.__post_init__()
    return use(credentials.username._read(),credentials.password._read())

@dataclass(frozen=True)
class PuppyScenarioRuntimeConfig:
    docker:Path
    docker_host:str
    image:str
    name:str
    protocol:str
    http_port:int
    bolt_port:int
    gremlin_port:int|None=None
    native_map_profile:str|None=None
    def __post_init__(self):
        _path(self.docker)
        text=_text(self.docker_host,4103)
        if not text.startswith('unix://')or '?'in text or '#'in text:raise ValueError('Explicit local Docker socket required')
        _decode_path(text[7:])
        if type(self.image)is not str or re.fullmatch('sha256:[0-9a-f]{64}',self.image)is None:raise ValueError('Explicit cached image digest required')
        if type(self.name)is not str or re.fullmatch('ashlar-[a-z0-9-]{1,80}',self.name)is None:raise ValueError('Explicit fresh owned name required')
        if type(self.protocol)is not str or self.protocol not in ('Cypher','Gremlin'):raise ValueError('One explicit native protocol required')
        ports=[self.http_port,self.bolt_port]
        if self.protocol=='Gremlin':
            if type(self.native_map_profile)is not str or self.native_map_profile!=MAP_PUPPY:raise ValueError('Explicit observed flat native map profile required')
            ports.append(self.gremlin_port)
        elif self.gremlin_port is not None or self.native_map_profile is not None:raise ValueError('Cypher rejects Gremlin selections')
        if any(type(p)is not int or not 1024<=p<=65535 for p in ports)or len(set(ports))!=len(ports):raise ValueError('Distinct explicit loopback ports required')

@dataclass(frozen=True)
class PuppyScenarioInvocationBounds:
    native_seconds:int
    whole_seconds:int
    stdout_bytes:int
    stderr_bytes:int
    maximum_rows:int
    maximum_bytes:int
    def __post_init__(self):
        for value,maximum in ((self.native_seconds,180),(self.whole_seconds,300),(self.stdout_bytes,1048576),(self.stderr_bytes,1048576),(self.maximum_rows,1000),(self.maximum_bytes,1048576)):_bound(value,maximum)
        if self.whole_seconds<self.native_seconds:raise ValueError('Whole invocation must include native interval')

@dataclass(frozen=True)
class PuppyScenarioInvocationConfig:
    packs:tuple
    producer:ProducerConfig
    runtime:PuppyScenarioRuntimeConfig
    credentials:PuppyScenarioCredentials
    output:Path
    bounds:PuppyScenarioInvocationBounds
    def __post_init__(self):
        if type(self.packs)is not tuple or len(self.packs)!=2 or any(type(p)is not PuppyScenarioPackFiles for p in self.packs)or tuple(p.name for p in self.packs)!=('archaeology','ecology'):raise ValueError('Exact ordered two original packs required')
        if type(self.producer)is not ProducerConfig or type(self.runtime)is not PuppyScenarioRuntimeConfig or type(self.credentials)is not PuppyScenarioCredentials or type(self.bounds)is not PuppyScenarioInvocationBounds:raise ValueError('Exact immutable scenario owners required')
        for value in (*self.packs,self.producer,self.runtime,self.credentials,self.bounds):value.__post_init__()
        _path(self.output)
        inputs=[self.producer.source,self.producer.bun,self.producer.git,self.runtime.docker]+[p for f in self.packs for p in (f.model,f.graph,f.pack,f.export.release_path,f.export.custody_path)]
        if any(self.output==p or self.output in p.parents or p in self.output.parents for p in inputs):raise ValueError('Separate selected fresh output required')

def parse_puppy_scenario_configuration(raw):
    if type(raw)is not bytes or not 0<len(raw)<=262144:raise ValueError('Bounded original17 configuration required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate original17 configuration member')
            result[key]=value
        return result
    def constant(_):raise ValueError('Nonfinite original17 configuration refused')
    value=json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)
    _closed(value,('profile','packs','producer','runtime','credentials','output','bounds'))
    if value['profile']!=PROFILE or type(value['packs'])is not list or len(value['packs'])!=2:raise ValueError('Exact original17 invocation required')
    packs=[]
    for pack in value['packs']:
        _closed(pack,('name','model','graph','pack','export'));e=_closed(pack['export'],('release_path','custody_path','release_sha256','custody_sha256'))
        selection=LocalGraphExportSelection(_decode_path(e['release_path']),_decode_path(e['custody_path']),e['release_sha256'],e['custody_sha256'])
        packs.append(PuppyScenarioPackFiles(pack['name'],*(_decode_path(pack[k])for k in ('model','graph','pack')),selection))
    p=_closed(value['producer'],ProducerConfig.__dataclass_fields__)
    producer=ProducerConfig(*(_decode_path(p[k])for k in ('source','bun','git')),*(p[k]for k in ('timeout_seconds','maximum_output_bytes','maximum_receipt_bytes')))
    r=value['runtime'];keys={'docker','docker_host','image','name','protocol','http_port','bolt_port'}
    if type(r)is dict and r.get('protocol')=='Gremlin':keys|={'gremlin_port','native_map_profile'}
    _closed(r,keys);runtime=PuppyScenarioRuntimeConfig(**{**r,'docker':_decode_path(r['docker'])})
    c=_closed(value['credentials'],('username','password'));credentials=PuppyScenarioCredentials(ScenarioSecretText(c['username']),ScenarioSecretText(c['password']))
    b=_closed(value['bounds'],PuppyScenarioInvocationBounds.__dataclass_fields__)
    return PuppyScenarioInvocationConfig(tuple(packs),producer,runtime,credentials,_decode_path(value['output']),PuppyScenarioInvocationBounds(**b))

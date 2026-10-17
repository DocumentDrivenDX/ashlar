"""Closed original core0.8 graph source metadata; public UMF owns semantics."""
import hashlib,os,re,stat
from dataclasses import dataclass,replace
from pathlib import Path
from types import MappingProxyType
from .schema import decode_retained_json

from .typed_graph_source import GRAPH_CHECK_PIN
def require(value):
    if not value:raise ValueError('Original typed graph configuration refused')
def text(value):return type(value)is str and 0<len(value.encode())<=1024 and '\0'not in value
def identifier(value):return type(value)is str and re.fullmatch('[1-9][0-9]{0,9}',value)is not None and int(value)<2**31
def freeze(value):
    if type(value)is dict:return MappingProxyType({k:freeze(v)for k,v in value.items()})
    if type(value)is list:return tuple(freeze(v)for v in value)
    return value

def read_graph_input(path,maximum):
    """Bounded no-follow exact regular-file observation, without source authority."""
    require(path.is_absolute()and path.parent.resolve()==path.parent)
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);primary=None;raw=None
    try:
        before=os.fstat(fd);require(stat.S_ISREG(before.st_mode)and 0<before.st_size<=maximum)
        chunks=[];size=0
        while True:
            part=os.read(fd,min(65536,maximum+1-size))
            if not part:break
            chunks.append(part);size+=len(part);require(size<=maximum)
        after=os.fstat(fd);named=path.lstat()
        fields=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        require(fields(before)==fields(after)==fields(named)and stat.S_ISREG(named.st_mode));raw=b''.join(chunks)
    except BaseException as error:primary=error
    try:os.close(fd)
    except BaseException as error:
        if primary is None or isinstance(primary,Exception)and not isinstance(error,Exception):primary=error
    if primary is not None:raise primary
    return raw

def graph_bindings(raw,model,source_system,revision):
    """Check declared identity/family correspondence only, never constraints."""
    value=decode_retained_json(raw)
    require(type(value)is dict and set(value)=={'profile','document_revision','source_system','records','relationships','qualification'}
        and value['profile']=='ashlar-authored-typed-graph-source/0.1'and value['document_revision']==revision and value['source_system']==source_system and text(value['qualification']))
    elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    records=value['records'];relations=value['relationships'];require(type(records)is dict and 1<=len(records)<=128 and type(relations)is dict and 1<=len(relations)<=128)
    seen=set()
    for tid,b in records.items():
        require(identifier(tid)and type(b)is dict and set(b)=={'record','fields'}and type(b['record'])is list and len(b['record'])==2 and all(text(v)for v in b['record']))
        identity=tuple(b['record']);record=elements.get(identity);require(identity not in seen and type(record)is dict and record.get('kind')=='record');seen.add(identity)
        members=[(m['module'],m['element'])for m in record['members']];fields=b['fields'];require(type(fields)is dict and 1<=len(fields)==len(members)<=128 and len(set(members))==len(members))
        bound=set()
        for pid,f in fields.items():
            require(identifier(pid)and type(f)is list and len(f)==3 and all(text(v)for v in f)and f[2]in ('string','integer','decimal','boolean'))
            key=tuple(f[:2]);field=elements.get(key);require(key in members and key not in bound and type(field)is dict and field.get('kind')=='field'and field.get('scalarType')==f[2]);bound.add(key)
    require(seen=={key for key,e in elements.items()if e.get('kind')=='record'})
    declared={(m['id'],r['id']):r for m in model['modules']for r in m.get('relationships',[])};seen=set()
    for tid,b in relations.items():
        require(identifier(tid)and type(b)is dict and set(b)=={'identity','source_type_id','target_type_id','target_key'})
        require(type(b['identity'])is list and len(b['identity'])==2 and all(text(v)for v in b['identity'])and type(b['target_key'])is list and len(b['target_key'])==3 and all(text(v)for v in b['target_key']))
        identity=tuple(b['identity']);r=declared.get(identity);require(identity not in seen and type(r)is dict);seen.add(identity)
        require(all(type(b[k])is int and str(b[k])in records for k in ('source_type_id','target_type_id')))
        start=records[str(b['source_type_id'])]['record'];end=records[str(b['target_type_id'])]['record'];key=b['target_key']
        require(r.get('directed')is True and r.get('targetLifecycle')=='independent'and 'associationRecord'not in r
            and r.get('source')==[{'module':start[0],'element':start[1]}]and r.get('target')==[{'module':end[0],'element':end[1],'key':key[2]}]and key[:2]==end)
        require(any(k['id']==key[2]for k in elements[tuple(end)].get('keys',[])))
    require(seen==set(declared))
    return freeze(records),freeze(relations),value['qualification']

@dataclass(frozen=True)
class TypedGraphJsonlConfiguration:
    original:bytes
    path:Path
    source:Path
    schema:Path
    bindingpath:Path
    references:tuple
    feed:str
    epoch:str
    schema_alias:str
    source_system:str
    document_revision:str
    producer_revision:str
    model_source:bytes
    model_sha256:str
    source_bytes:bytes
    binding_source:bytes
    bindings:object
    relationships:object
    qualification:str
    policy:object=None
    original_receipt:bytes=None
    @property
    def refs(self):return self.references
    @property
    def objecttype_ids(self):return tuple(sorted(int(t)for t in self.bindings))
    @property
    def type_ids(self):return self.objecttype_ids
    def verify(self):
        rebuilt=load_typed_graph_jsonl_configuration(self.path,_verify=False)
        require(replace(self,policy=None,original_receipt=None)==rebuilt)
        if self.policy is None:require(self.original_receipt is None)
        else:
            from .typed_graph_source import TypedGraphSourcePolicy
            expected=TypedGraphSourcePolicy(self,self.original_receipt)
            require(type(self.policy)is TypedGraphSourcePolicy and vars(self.policy)==vars(expected))
    def with_receipt(self,raw):
        self.verify()
        from .typed_graph_source import TypedGraphSourcePolicy
        result=replace(self,policy=TypedGraphSourcePolicy(self,raw),original_receipt=raw);result.verify();return result

def load_typed_graph_jsonl_configuration(path,*,receipt=None,_verify=True):
    path=Path(path).resolve();original=read_graph_input(path,262144);v=decode_retained_json(original)
    require(type(v)is dict and set(v)=={'profile','feed','epoch','sourceSystem','schemaAlias','documentRevision','producerRevision','source','schema','bindings'}and v['profile']=='ashlar-jsonl-typed-graph-source/0.1')
    require(all(text(v[k])for k in ('feed','epoch','sourceSystem','schemaAlias','documentRevision'))and v['producerRevision']==GRAPH_CHECK_PIN)
    refs=[];raws=[]
    for key,maximum in (('source',1048576),('schema',65536),('bindings',262144)):
        ref=v[key];require(type(ref)is dict and set(ref)=={'path','sha256'}and text(ref['path'])and type(ref['sha256'])is str and re.fullmatch('[0-9a-f]{64}',ref['sha256'])is not None)
        target=(path.parent/ref['path']).resolve();raw=read_graph_input(target,maximum);require(hashlib.sha256(raw).hexdigest()==ref['sha256']);refs.append((target,ref['sha256']));raws.append(raw)
    model=decode_retained_json(raws[1]);require(type(model)is dict and model.get('umf')=='0.8.0')
    bindings,relations,qualification=graph_bindings(raws[2],model,v['sourceSystem'],v['documentRevision'])
    result=TypedGraphJsonlConfiguration(original,path,refs[0][0],refs[1][0],refs[2][0],tuple(refs),v['feed'],v['epoch'],v['schemaAlias'],v['sourceSystem'],v['documentRevision'],v['producerRevision'],raws[1],hashlib.sha256(raws[1]).hexdigest(),raws[0],raws[2],bindings,relations,qualification)
    if _verify:result.verify()
    return result if receipt is None else result.with_receipt(receipt)

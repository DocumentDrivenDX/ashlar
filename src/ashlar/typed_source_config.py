"""Explicit typed JSONL byte/binding configuration, without semantic admission.

The public UMF producer owns all value, presence and constraint validation.
Only an original pinned receipt can attach TypedSourcePolicy; IDs remain explicit
operator development mappings, not native catalog or source authority.
"""
import base64,hashlib,os,re,stat
from pathlib import Path
from dataclasses import dataclass,replace
from types import MappingProxyType
from .schema import decode_retained_json
from .typed_source_policy import TypedSourcePolicy,RECORD_CHECK_PIN

class TypedConfigurationError(ValueError):pass

def _require(value):
    if not value:raise TypedConfigurationError('Original typed JSONL configuration refused')
def _text(value,maximum=1024):return type(value)is str and 0<len(value.encode())<=maximum and '\0'not in value
def _read(path,maximum):
    _require(type(path)is type(Path('/'))and path.is_absolute()and path.parent.resolve()==path.parent)
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);primary=None;result=None
    try:
        before=os.fstat(fd);_require(stat.S_ISREG(before.st_mode)and 0<before.st_size<=maximum)
        chunks=[];count=0
        while True:
            part=os.read(fd,min(65536,maximum+1-count))
            if not part:break
            chunks.append(part);count+=len(part);_require(count<=maximum)
        after=os.fstat(fd);named=path.lstat()
        _require((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)and(before.st_dev,before.st_ino)==(named.st_dev,named.st_ino)and stat.S_ISREG(named.st_mode))
        result=b''.join(chunks)
    except BaseException as error:primary=error
    try:os.close(fd)
    except BaseException as error:
        if primary is None or isinstance(primary,Exception)and not isinstance(error,Exception):primary=error
    if primary is not None:raise primary
    return result

def _bindings(raw,model,source_system,revision):
    value=decode_retained_json(raw)
    _require(type(value)is dict and set(value)=={'profile','document_revision','source_system','type_id','binding','qualification'}and value['profile']=='ashlar-authored-typed-source/0.1')
    _require(value['document_revision']==revision and value['source_system']==source_system and _text(value['qualification'],4096))
    tid=value['type_id'];_require(type(tid)is int and 1<=tid<2**31)
    bound=value['binding'];_require(type(bound)is dict and set(bound)=={'record','fields'})
    record=bound['record'];_require(type(record)is list and len(record)==2 and all(_text(v)for v in record))
    elements={};modules=set()
    _require(type(model.get('modules'))is list and len(model['modules'])<=128)
    for module in model['modules']:
        _require(type(module)is dict and _text(module.get('id'))and type(module.get('elements'))is list and len(module['elements'])<=4096)
        _require(module['id']not in modules);modules.add(module['id'])
        for element in module['elements']:
            _require(type(element)is dict and _text(element.get('id'))and(module['id'],element['id'])not in elements)
            elements[module['id'],element['id']]=element
    owner=elements.get(tuple(record));_require(type(owner)is dict and owner.get('kind')=='record'and type(owner.get('members'))is list)
    members=owner['members'];_require(1<=len(members)<=128 and all(type(m)is dict and set(m)=={'module','element'}and all(_text(m[k])for k in ('module','element'))for m in members))
    identities={(m['module'],m['element'])for m in members};_require(len(identities)==len(members))
    fields=bound['fields'];_require(type(fields)is dict and len(fields)==len(members));mapped={};seen=set()
    for pid,identity in fields.items():
        _require(type(pid)is str and re.fullmatch('[1-9][0-9]{0,9}',pid)is not None and int(pid)<2**31)
        _require(type(identity)is list and len(identity)==3 and all(_text(v)for v in identity)and identity[2]in ('integer','decimal','boolean','string'))
        key=tuple(identity[:2]);field=elements.get(key)
        _require(key in identities and key not in seen and type(field)is dict and field.get('kind')=='field'and field.get('scalarType')==identity[2]);seen.add(key);mapped[pid]=tuple(identity)
    _require(seen==identities)
    return MappingProxyType({tid:MappingProxyType({'record':tuple(record),'fields':MappingProxyType(mapped)})}),value['qualification']

@dataclass(frozen=True)
class TypedJsonlConfiguration:
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
    qualification:str
    policy:object=None
    original_receipt:bytes=None
    @property
    def refs(self):return self.references
    @property
    def type_ids(self):return tuple(sorted(self.bindings))
    def verify(self):
        _require(_read(self.path,262144)==self.original)
        declared=decode_retained_json(self.original)
        _require(declared.get('profile')=='ashlar-jsonl-typed-source/0.1'and self.producer_revision==RECORD_CHECK_PIN
            and all(declared.get(k)==v for k,v in (('feed',self.feed),('epoch',self.epoch),('sourceSystem',self.source_system),('schemaAlias',self.schema_alias),('documentRevision',self.document_revision),('producerRevision',self.producer_revision))))
        _require(type(self.bindings)is MappingProxyType and all(type(b)is MappingProxyType and type(b['fields'])is MappingProxyType for b in self.bindings.values()))
        _require(self.references==tuple(((self.path.parent/declared[n]['path']).resolve(),declared[n]['sha256'])for n in ('source','schema','bindings')))
        _require((self.source,self.schema,self.bindingpath)==tuple(p for p,_ in self.references))
        _require(_read(self.source,2097152)==self.source_bytes and _read(self.schema,1048576)==self.model_source and _read(self.bindingpath,262144)==self.binding_source)
        for path,digest in self.references:_require(hashlib.sha256(_read(path,2097152)).hexdigest()==digest)
        _require(hashlib.sha256(self.model_source).hexdigest()==self.model_sha256)
        rebuilt,qualification=_bindings(self.binding_source,decode_retained_json(self.model_source),self.source_system,self.document_revision)
        _require(rebuilt==self.bindings and qualification==self.qualification)
        if self.policy is None:
            _require(self.original_receipt is None)
        else:
            rebuilt=self._receipt_policy(self.original_receipt)
            # Compare the complete nominal policy state, including its delivery
            # inventory, against a fresh reconstruction from retained bytes.
            _require(type(self.policy)is TypedSourcePolicy and vars(self.policy)==vars(rebuilt))
    def _receipt_policy(self,raw):
        _require(type(raw)is bytes and 0<len(raw)<=4194304)
        bindings={tid:{'record':b['record'],'fields':dict(b['fields'])}for tid,b in self.bindings.items()}
        policy=TypedSourcePolicy(self.model_source,raw,bindings,source_system=self.source_system,schema_revision=self.document_revision,trusted_producer_revision=self.producer_revision)
        from .source import jsonl_batches
        from .whole_entity import changes_from_batch
        batches=tuple(jsonl_batches(self.source_bytes.splitlines(keepends=True),feed=self.feed,epoch=self.epoch))
        changes=[change for batch in batches for change in changes_from_batch(batch)]
        report=decode_retained_json(raw);request=decode_retained_json(base64.b64decode(report['requestBase64'],validate=True))
        _require(request=={'records':[policy.request(change)for change in changes]})
        for change in changes:policy(change)
        return policy
    def with_receipt(self,raw):
        self.verify()
        result=replace(self,policy=self._receipt_policy(raw),original_receipt=raw)
        result.verify();return result


def load_typed_jsonl_configuration(path,*,receipt=None):
    path=Path(path).resolve();original=_read(path,262144);value=decode_retained_json(original)
    _require(type(value)is dict and set(value)=={'profile','feed','epoch','sourceSystem','schemaAlias','documentRevision','producerRevision','source','schema','bindings'}and value['profile']=='ashlar-jsonl-typed-source/0.1')
    _require(all(_text(value[k])for k in ('feed','epoch','sourceSystem','schemaAlias','documentRevision'))and value['producerRevision']==RECORD_CHECK_PIN)
    refs=[];selected={};total=0
    for name,maximum in (('source',2097152),('schema',1048576),('bindings',262144)):
        ref=value[name];_require(type(ref)is dict and set(ref)=={'path','sha256'}and _text(ref['path'],4096)and type(ref['sha256'])is str and re.fullmatch('[0-9a-f]{64}',ref['sha256'])is not None)
        target=(path.parent/ref['path']).resolve();raw=_read(target,maximum);_require(hashlib.sha256(raw).hexdigest()==ref['sha256']);refs.append((target,ref['sha256']));selected[name]=(target,raw);total+=len(raw)
    _require(total<=4194304)
    model=decode_retained_json(selected['schema'][1]);_require(type(model)is dict and model.get('umf')=='0.8.0'and _text(model.get('id')))
    bindings,qualification=_bindings(selected['bindings'][1],model,value['sourceSystem'],value['documentRevision'])
    result=TypedJsonlConfiguration(original,path,selected['source'][0],selected['schema'][0],selected['bindings'][0],tuple(refs),value['feed'],value['epoch'],value['schemaAlias'],value['sourceSystem'],value['documentRevision'],value['producerRevision'],selected['schema'][1],hashlib.sha256(selected['schema'][1]).hexdigest(),selected['source'][1],selected['bindings'][1],bindings,qualification)
    result.verify();return result if receipt is None else result.with_receipt(receipt)

"""Explicit immutable-file/model/binding configuration; no native catalog authority."""
from pathlib import Path
from dataclasses import dataclass
import hashlib
from ashlar.schema import SchemaIntake,_json
from ashlar.catalog import Identity,MappingEntry
from ashlar.semantic_policy import StringRecordPolicy

@dataclass(frozen=True)
class JsonlConfiguration:
    original:bytes
    path:Path
    intake:SchemaIntake
    policy:StringRecordPolicy
    source:Path
    schema:Path
    feed:str
    epoch:str
    schema_alias:str
    references:tuple
    def verify(self):
        if self.path.read_bytes()!=self.original:raise ValueError('Original source configuration changed')
        for path,digest in self.references:
            if hashlib.sha256(path.read_bytes()).hexdigest()!=digest:raise ValueError('Original configured input changed: '+str(path))


def load_jsonl_configuration(path):
    path=Path(path).resolve();original=path.read_bytes();value=_json(original)
    required={'profile','feed','epoch','sourceSystem','schemaAlias','documentRevision','validatorRevision','source','schema','intake','interpretation','bindings'}
    if not isinstance(value,dict) or set(value)!=required or value['profile']!='ashlar-jsonl-source/0.1':raise ValueError('Exact JSONL source configuration required')
    for name in ('feed','epoch','sourceSystem','schemaAlias','documentRevision','validatorRevision'):
        text=value[name]
        if type(text) is not str or not text or '\x00' in text or len(text.encode('utf-8'))>1024:raise ValueError('Explicit bounded source/model identity required')
    references=[];files={}
    for name in ('source','schema','intake','interpretation'):
        ref=value[name]
        if not isinstance(ref,dict) or set(ref)!={'path','sha256'} or type(ref['path']) is not str or not ref['path']:
            raise ValueError('Exact input reference required')
        digest=ref['sha256']
        if type(digest) is not str or len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest):raise ValueError('Original input SHA256 required')
        target=(path.parent/ref['path']).resolve();raw=target.read_bytes()
        if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('Configured input digest differs')
        references.append((target,digest));files[name]=(target,raw)
    intake=SchemaIntake.read(files['intake'][1],value['documentRevision'],trusted_validator_revision=value['validatorRevision'])
    if files['schema'][1]!=intake.source:raise ValueError('Exact original schema bytes required')
    if not isinstance(value['bindings'],list) or not value['bindings']:raise ValueError('Explicit complete ID bindings required')
    entries=[]
    for entry in value['bindings']:
        if not isinstance(entry,dict) or set(entry)!={'family','parts','catalogId','active'} or not isinstance(entry['parts'],list):raise ValueError('Exact qualified binding entry required')
        entries.append(MappingEntry(Identity(entry['family'],tuple(entry['parts'])),entry['catalogId'],entry['active']))
    policy=StringRecordPolicy.from_intake(intake,files['interpretation'][1],entries,source_system=value['sourceSystem'])
    result=JsonlConfiguration(original,path,intake,policy,files['source'][0],files['schema'][0],value['feed'],value['epoch'],value['schemaAlias'],tuple(references))
    result.verify();return result

"""Fixed independently indexed f36/042 product; no native/source authority.

Original regular Git bytes are admitted; only owned installation copies are made
readonly. File/JSON/tree readers reuse the existing bounded public mechanics.
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import hashlib,json,re
from . import _weft_installation_mechanics as mechanics
from .weft_paths_package import (read_snapshot, verify_exact_tree, decode_document, encode_document)

INDEX_REVISION='cdd8fa698683cd87bb126916a75200e80e19d831'
INDEX_SHA256='6aa24c395dc721ef6cfef281f34628051a041530ba426d8dc31e9126e9510399'
REALIZATION_ID='weft-f36f206-implicit-arithmetic-aarch64-apple-darwin-candidate'
SOURCE='f36f206eef70f69f2386a2acb466814d8302b016'
MANIFEST_SHA256='8ef7b41c11e9d1d86750406a06f346dda991af26aa49f0a193022fe8826f58f3'
CUSTODY_SHA256='81ece3c4e9aede42ff6f5453088bd6cfeb18c9c60e149745cf3f935ea0784f66'
BINARY_SHA256='8e1e43aa81b312a124e88698477e1392f6c538451d5cb8f582a38934c776e55a'
BACKEND_SHA256='33de51e60a1f2eb46813abe8601d90dc7cbe67d6a6b248890dcc70842bfcbe6e'
VERSION='0.4.2-implicit-arithmetic-labels-candidate'
JSON_LIMIT=mechanics.JSON_LIMIT
FILE_LIMIT=mechanics.FILE_LIMIT
PROTOCOL_LIMIT=mechanics.PROTOCOL_LIMIT
SCHEMA_BASE='source-subset/docs/helix/02-design/contracts/'
INSTALLED_SCHEMAS=mechanics.installation_layout('implicit-arithmetic').schemas

class ImplicitArithmeticInstallationError(ValueError):
    """Payload-free exact selected-product refusal; no fallback."""
def refuse():raise ImplicitArithmeticInstallationError('ASHLAR-WEFT-IMPLICIT-ARITHMETIC-REFUSED')
def sha(raw):return hashlib.sha256(raw).hexdigest()
def descriptor(value):
    if type(value)is not dict or set(value)!={'path','sha256','bytes'}:refuse()
    name=value['path'];digest=value['sha256'];size=value['bytes']
    if type(name)is not str or not name or len(name)>4096 or ':'in name or '\\'in name or name.startswith('/')or any(p in ('','.', '..')for p in name.split('/'))or any(ord(c)<32 or ord(c)==127 for c in name):refuse()
    if type(digest)is not str or re.fullmatch('[0-9a-f]{64}',digest)is None or type(size)is not int or not 0<=size<=FILE_LIMIT:refuse()


@dataclass(frozen=True)
class ImplicitArithmeticInstallationConfig:
    index_path:Path
    index_revision:str
    index_sha256:str
    package:Optional[Path]
    realization_id:str
    output:Path
    observed_target:str
    observed_os:str
    def __post_init__(self):
        if any(type(getattr(self,n))is not str for n in ('index_revision','index_sha256','realization_id','observed_target','observed_os')):refuse()
        if (self.index_revision,self.index_sha256,self.realization_id,self.observed_target,self.observed_os)!=(INDEX_REVISION,INDEX_SHA256,REALIZATION_ID,'aarch64-apple-darwin','27.0.1'):refuse()
        if any(not isinstance(p,Path)or not p.is_absolute()for p in (self.index_path,self.output)):refuse()
        if self.package is not None and (not isinstance(self.package,Path)or not self.package.is_absolute()):refuse()


def verify_trusted_index(config):
    if type(config)is not ImplicitArithmeticInstallationConfig:refuse()
    config.__post_init__()
    raw=read_snapshot(config.index_path,JSON_LIMIT)
    if sha(raw)!=INDEX_SHA256:refuse()
    index=decode_document(raw)
    if type(index)is not dict or set(index)!={'format','entries'}or index['format']!='weft-distribution-index/0.1'or type(index['entries'])is not list or len(index['entries'])!=5:refuse()
    identities=[];selected=None
    for entry in index['entries']:
        if type(entry)is not dict or set(entry)!={'realizationId','manifest','executable','assemblyCustody','target'}or type(entry['realizationId'])is not str or type(entry['target'])is not str:refuse()
        for name in ('manifest','executable','assemblyCustody'):descriptor(entry[name])
        identities.append(entry['realizationId'])
        if entry['realizationId']==REALIZATION_ID:selected=entry
    expected={'realizationId':REALIZATION_ID,'target':'aarch64-apple-darwin',
        'manifest':{'path':'manifest.json','sha256':MANIFEST_SHA256,'bytes':9902},
        'assemblyCustody':{'path':'assembly-custody.json','sha256':CUSTODY_SHA256,'bytes':17079},
        'executable':{'path':'bin/weft-implicit-arithmetic','sha256':BINARY_SHA256,'bytes':8688144}}
    if len(set(identities))!=5 or selected!=expected:refuse()
    return raw,selected


def validate_manifest(manifest):
    if sha(json.dumps(manifest,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()+b'\n')!=MANIFEST_SHA256:refuse()

@dataclass(frozen=True)
class VerifiedImplicitArithmeticPackage:
    manifest_bytes:bytes
    custody_bytes:bytes
    executable_bytes:bytes
    provenance_bytes:bytes
    resources:tuple[tuple[str,bytes],...]


def inspect_package(config):
    """Verify all ninety exact product files twice before returning owned bytes."""
    try:
        index_raw,entry=verify_trusted_index(config)
        if config.package is None:refuse()
        records={};total=0
        def take(item):
            nonlocal total
            descriptor(item);raw=read_snapshot(config.package/item['path'])
            if len(raw)!=item['bytes']or sha(raw)!=item['sha256']:refuse()
            if item['path']not in records:total+=len(raw)
            if total>mechanics.TOTAL_LIMIT:refuse()
            records[item['path']]=raw;return raw
        manifest_raw=take(entry['manifest']);manifest=decode_document(manifest_raw);validate_manifest(manifest)
        custody_raw=take(entry['assemblyCustody']);custody=decode_document(custody_raw)
        if custody['format']!='weft-distribution-assembly-custody/0.1'or custody['sourceCommit']!=SOURCE or type(custody['artifacts'])is not list or len(custody['artifacts'])!=89:refuse()
        names=[]
        for item in custody['artifacts']:
            descriptor(item)
            if item['path']in names or item['path']=='assembly-custody.json':refuse()
            names.append(item['path']);take(item)
        if names!=sorted(names):refuse()
        verify_exact_tree(config.package,tuple(records))
        layout=mechanics.installation_layout('implicit-arithmetic')
        resources=tuple((name,records[manifest['backendManifests'][0]['path']if name=='backend-manifest.json'else SCHEMA_BASE+name[len('schemas/'):]])for name in layout.resources)
        if sha(resources[0][1])!=BACKEND_SHA256:refuse()
        provenance={'format':'ashlar-weft-implicit-arithmetic-provenance/0.1','indexRevision':INDEX_REVISION,'indexSha256':INDEX_SHA256,'realizationId':REALIZATION_ID,
            'manifestHex':manifest_raw.hex(),'custodyHex':custody_raw.hex(),'resources':[{'path':name,'sha256':sha(raw),'bytes':len(raw)}for name,raw in resources],
            'qualification':'Exact independently indexed f36/042 compiler product only. No source/publication/native/ACK authority granted.'}
        for name,raw in records.items():
            if read_snapshot(config.package/name,max(1,len(raw)))!=raw:refuse()
        verify_exact_tree(config.package,tuple(records))
        if read_snapshot(config.index_path,JSON_LIMIT)!=index_raw:refuse()
        return VerifiedImplicitArithmeticPackage(manifest_raw,custody_raw,records[entry['executable']['path']],encode_document(provenance)+b'\n',resources)
    except (OSError,ValueError,KeyError,TypeError,RecursionError):refuse()

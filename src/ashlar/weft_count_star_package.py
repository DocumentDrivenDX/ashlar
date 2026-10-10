"""Fixed independently indexed 5dd/041 package; no native or publication authority.

Git source snapshots may be writable regular files. Every indexed content byte is
checked before and after inspection; only owned installed copies become readonly.
"""
from __future__ import annotations
from dataclasses import dataclass
import hashlib,json,os,re,stat
from pathlib import Path
from typing import Optional
from . import _weft_installation_mechanics as mechanics
from .weft_paths_package import decode_document,encode_document

INDEX_REVISION='bd490abc877a1992666c7b45412b14a0ca6c2cac'
INDEX_SHA256='7eddc39661f6cb400afae897aa0a7d8cd5dee0aec026ed45a4e197fa7c9558a6'
REALIZATION_ID='weft-5ddcebd-count-star-aarch64-apple-darwin-candidate'
SOURCE='5ddcebd6941c572ddd62b78925fdb1fe78b1c688'
MANIFEST_SHA256='5ad944f609969e1183e8147b56141f0769a9cfd79b5dc65701fefe858713f743'
CUSTODY_SHA256='a0b25554c859265b1442f26a62381ea25e1f69f16b5e6147ea2f0ebc926c0cca'
BINARY_SHA256='0e8199a8953f46d7151c87ec001e6c8edff414c0af9cecc6c13e0ea8d23ede80'
BACKEND_SHA256='075dc92c7ca08b37abd1ed003fcd80674f816353742b79fa3e3a98021826b273'
VERSION='0.4.1-count-star-having-candidate'
JSON_LIMIT=4*1024*1024
FILE_LIMIT=32*1024*1024
TOTAL_LIMIT=96*1024*1024
PROTOCOL_LIMIT=16*1024*1024
SCHEMA_BASE='source-subset/docs/helix/02-design/contracts/'
INSTALLED_SCHEMAS=mechanics.installation_layout('count-star').schemas

class CountStarInstallationError(ValueError):
    """Payload-free selected-profile refusal; no fallback."""

def refuse():raise CountStarInstallationError('ASHLAR-WEFT-COUNT-STAR-REFUSED')
def sha(raw):return hashlib.sha256(raw).hexdigest()

def descriptor(value):
    if type(value)is not dict or set(value)!={'path','sha256','bytes'}:refuse()
    name=value['path'];digest=value['sha256'];size=value['bytes']
    if type(name)is not str or not name or len(name)>4096 or ':'in name or '\\'in name or name.startswith('/') or any(x in ('','.', '..')for x in name.split('/')):refuse()
    if any(ord(c)<32 or ord(c)==127 for c in name):refuse()
    if type(digest)is not str or re.fullmatch('[0-9a-f]{64}',digest)is None or type(size)is not int or not 0<=size<=FILE_LIMIT:refuse()
    return value

def read_snapshot(path,limit=FILE_LIMIT):
    if type(limit)is not int or not 1<=limit<=FILE_LIMIT:refuse()
    path=Path(path)
    if not path.is_absolute() or any(p.is_symlink()for p in (path,*path.parents)):refuse()
    fd=None;primary=None;raw=None
    try:
        fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW)
        if not stat.S_ISREG(os.fstat(fd).st_mode):refuse()
        chunks=[];remaining=limit+1
        while remaining:
            chunk=os.read(fd,min(remaining,65536))
            if not chunk:break
            chunks.append(chunk);remaining-=len(chunk)
        raw=b''.join(chunks)
        if len(raw)>limit:refuse()
    except BaseException as error:primary=error
    mechanics.finish(primary,(lambda:os.close(fd),)if fd is not None else ())
    return raw

def verify_exact_tree(root,paths):
    """Bounded admitted trie; owned iterator close keeps cancellation priority."""
    trie={};nodes=0
    for name in paths:
        descriptor({'path':name,'sha256':'0'*64,'bytes':0})
        parts=name.split('/')
        if len(parts)>64:refuse()
        branch=trie
        for part in parts[:-1]:
            if part in branch and branch[part]is None:refuse()
            if part not in branch:branch[part]={};nodes+=1
            if nodes>8192:refuse()
            branch=branch[part]
        if parts[-1]in branch:refuse()
        branch[parts[-1]]=None;nodes+=1
        if nodes>8192:refuse()
    pending=[(root,trie)]
    while pending:
        directory,expected=pending.pop()
        if any(p.is_symlink()for p in (directory,*directory.parents)):refuse()
        entries=None;primary=None;seen=set()
        try:
            entries=os.scandir(directory)
            for entry in entries:
                if entry.name not in expected or entry.name in seen:refuse()
                seen.add(entry.name);mode=entry.stat(follow_symlinks=False).st_mode;child=expected[entry.name]
                if child is None:
                    if not stat.S_ISREG(mode):refuse()
                else:
                    if not stat.S_ISDIR(mode):refuse()
                    pending.append((directory/entry.name,child))
            if seen!=set(expected):refuse()
        except BaseException as error:primary=error
        mechanics.finish(primary,(entries.close,)if entries is not None else ())

@dataclass(frozen=True)
class CountStarInstallationConfig:
    index_path:Path
    index_revision:str
    index_sha256:str
    package:Optional[Path]
    realization_id:str
    output:Path
    observed_target:str
    observed_os:str
    def __post_init__(self):
        if any(type(getattr(self,name))is not str for name in ('index_revision','index_sha256','realization_id','observed_target','observed_os')):refuse()
        if self.index_revision!=INDEX_REVISION or self.index_sha256!=INDEX_SHA256 or self.realization_id!=REALIZATION_ID:refuse()
        if self.observed_target!='aarch64-apple-darwin' or self.observed_os!='27.0.1':refuse()
        if any(not isinstance(p,Path) or not p.is_absolute()for p in (self.index_path,self.output)):refuse()
        if self.package is not None and (not isinstance(self.package,Path)or not self.package.is_absolute()):refuse()

def verify_trusted_index(config):
    if type(config)is not CountStarInstallationConfig:refuse()
    raw=read_snapshot(config.index_path,JSON_LIMIT)
    if sha(raw)!=INDEX_SHA256:refuse()
    value=decode_document(raw)
    if set(value)!={'format','entries'} or value['format']!='weft-distribution-index/0.1' or type(value['entries'])is not list or len(value['entries'])!=4:refuse()
    selected=None;identities=[]
    for entry in value['entries']:
        if type(entry)is not dict or set(entry)!={'realizationId','manifest','executable','assemblyCustody','target'}:refuse()
        for key in ('manifest','executable','assemblyCustody'):descriptor(entry[key])
        identities.append(entry['realizationId'])
        if entry['realizationId']==REALIZATION_ID:selected=entry
    if len(set(identities))!=4 or selected is None:refuse()
    expected={'realizationId':REALIZATION_ID,'target':'aarch64-apple-darwin',
              'manifest':{'path':'manifest.json','sha256':MANIFEST_SHA256,'bytes':7733},
              'assemblyCustody':{'path':'assembly-custody.json','sha256':CUSTODY_SHA256,'bytes':12344},
              'executable':{'path':'bin/weft-paths-keys','sha256':BINARY_SHA256,'bytes':8532672}}
    if selected!=expected:refuse()
    return raw,selected

def validate_manifest(manifest):
    # This fixed profile admits the exact independently reviewed immutable record,
    # not arbitrary manifests that happen to satisfy a permissive shape schema.
    raw=json.dumps(manifest,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()+b'\n'
    if sha(raw)!=MANIFEST_SHA256:refuse()

@dataclass(frozen=True)
class VerifiedCountStarPackage:
    manifest_bytes:bytes
    custody_bytes:bytes
    executable_bytes:bytes
    provenance_bytes:bytes
    resources:tuple[tuple[str,bytes],...]

def inspect_package(config):
    """Index first, full regular byte closure second; never execute source files."""
    try:
        index_raw,entry=verify_trusted_index(config)
        if config.package is None:refuse()
        records={};total=0
        def take(d):
            nonlocal total
            descriptor(d);name=d['path'];raw=read_snapshot(config.package/name)
            if len(raw)!=d['bytes'] or sha(raw)!=d['sha256']:refuse()
            if name not in records:
                total+=len(raw)
                if total>TOTAL_LIMIT:refuse()
            elif records[name]!=raw:refuse()
            records[name]=raw;return raw
        manifest_raw=take(entry['manifest']);manifest=decode_document(manifest_raw);validate_manifest(manifest)
        custody_raw=take(entry['assemblyCustody']);custody=decode_document(custody_raw)
        if custody['format']!='weft-distribution-assembly-custody/0.1' or custody['sourceCommit']!=SOURCE or len(custody['artifacts'])!=66:refuse()
        names=[];descriptors={}
        for item in custody['artifacts']:
            descriptor(item);name=item['path']
            if name in descriptors or name=='assembly-custody.json':refuse()
            names.append(name);descriptors[name]=item;take(item)
        if names!=sorted(names) or descriptors['manifest.json']!=entry['manifest'] or descriptors['bin/weft-paths-keys']!=entry['executable']:refuse()
        verify_exact_tree(config.package,tuple(records))
        resources=[('backend-manifest.json',records[manifest['backendManifests'][0]['path']])]
        if sha(resources[0][1])!=BACKEND_SHA256:refuse()
        for name in INSTALLED_SCHEMAS:resources.append(('schemas/'+name,records[SCHEMA_BASE+name]))
        provenance={'format':'ashlar-weft-count-star-provenance/0.1','indexRevision':INDEX_REVISION,'indexSha256':INDEX_SHA256,
                    'realizationId':REALIZATION_ID,'manifestHex':manifest_raw.hex(),'custodyHex':custody_raw.hex(),
                    'resources':[{'path':name,'sha256':sha(raw),'bytes':len(raw)}for name,raw in resources],
                    'qualification':'Independently indexed exact5dd/041 compiler bytes and retained component/boundary proofs only. Source Git modes need not be readonly; owned installation is readonly. No native/source/publication/ACK obligations discharged.'}
        provenance_raw=encode_document(provenance)+b'\n'
        if len(provenance_raw)>JSON_LIMIT:refuse()
        for name,raw in records.items():
            if read_snapshot(config.package/name,max(1,len(raw)))!=raw:refuse()
        verify_exact_tree(config.package,tuple(records))
        if read_snapshot(config.index_path,JSON_LIMIT)!=index_raw:refuse()
        return VerifiedCountStarPackage(manifest_raw,custody_raw,records['bin/weft-paths-keys'],provenance_raw,tuple(resources))
    except (OSError,ValueError,KeyError,TypeError,RecursionError):raise CountStarInstallationError('ASHLAR-WEFT-COUNT-STAR-REFUSED')from None

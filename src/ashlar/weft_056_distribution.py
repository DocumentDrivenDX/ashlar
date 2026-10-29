# SPDX-License-Identifier: MIT OR Apache-2.0
"""Separately pinned public Weft 0.4.56 distribution and API boundary.

The runtime index is created from one platform's official wheel and records
every installed file. Historical 0.4.53 packet paths remain independent.
"""
from __future__ import annotations
from dataclasses import dataclass
import hashlib, importlib, importlib.metadata, json, os, platform, stat
from pathlib import Path
import sys

VERSION = '0.4.56'
SOURCE_COMMIT = '2f62e1148d1b2030558550e4f6f2ffe518bac62d'
PROFILE = 'ashlar-weft-056-runtime-index/0.1'
WHEEL_SHA256 = {
    ('Darwin', 'arm64'): '68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d',
    ('Linux', 'x86_64'): '32f4c38d9a614dd4c3a367f8b24d5cd4ba41ec80c2a4a2e3423e2082a294dbdd',
}
SCHEMA_SHA256 = {
    'compile-request-v0.4.1.schema.json': 'eadea16ecb850add4dd1a276e417e5659a553d37171ff6ba455668acec7ab13b',
    'compile-response-v0.4.1.schema.json': 'e6ba09a90a08e7065c134f6ca0958c70da02cedbd843eeab5d608b59ba335be3',
    'logical-plan-v0.4.1.schema.json': '7e48576b69a688c41894ce033af7bcb9a0792e1346010b4f5b19887ad536112f',
}
MAX_INDEX=1_048_576; MAX_FILE=32*1024*1024; MAX_REQUEST=1_048_576; MAX_RESPONSE=1_048_576

class Weft056Refused(ValueError):
    """Payload-free refusal of the pinned public Weft 0.4.56 runtime."""

def _require(value):
    if not value: raise Weft056Refused('Pinned Weft 0.4.56 runtime refused')

def _read(path: Path, limit: int) -> bytes:
    _require(type(path) is type(Path()) and path.is_absolute() and not any(p.is_symlink() for p in (path,*path.parents)))
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        _require(stat.S_ISREG(os.fstat(fd).st_mode)); chunks=[]; remaining=limit+1
        while remaining:
            block=os.read(fd,min(remaining,65536))
            if not block: break
            chunks.append(block); remaining-=len(block)
        raw=b''.join(chunks); _require(len(raw)<=limit); return raw
    finally: os.close(fd)

def _json(raw):
    def pairs(items):
        out={}
        for key,value in items:
            _require(key not in out); out[key]=value
        return out
    try: return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs)
    except Exception: raise Weft056Refused('Pinned Weft 0.4.56 runtime refused') from None

@dataclass(frozen=True)
class Weft056Paths:
    index: Path
    index_sha256: str
    def __post_init__(self):
        _require(type(self.index)is type(Path()) and self.index.is_absolute() and type(self.index_sha256)is str and len(self.index_sha256)==64 and all(c in '0123456789abcdef' for c in self.index_sha256))

class Weft056Distribution:
    """Recheck the platform's exact release wheel installation around APIs."""
    def __init__(self,paths: Weft056Paths):
        _require(type(paths)is Weft056Paths); paths.__post_init__(); self.paths=paths
        self.index_bytes=_read(paths.index,MAX_INDEX); _require(hashlib.sha256(self.index_bytes).hexdigest()==paths.index_sha256)
        self.index=_json(self.index_bytes)
        _require(set(self.index)=={'profile','sourceCommit','version','wheel','root','module','files','schemas'}
            and self.index['profile']==PROFILE and self.index['sourceCommit']==SOURCE_COMMIT and self.index['version']==VERSION)
        # Python package bytecode can execute independently of the source file
        # we inventory. Refuse any preexisting cache before import, then prevent
        # this verifier from creating an unindexed cache during import.
        self.module=None; self._verify()
        self._refuse_bytecode_caches()
        old_dont_write_bytecode=sys.dont_write_bytecode
        try:
            sys.dont_write_bytecode=True
            self.module=importlib.import_module('weft')
        finally:
            sys.dont_write_bytecode=old_dont_write_bytecode
        _require(str(Path(self.module.__file__).resolve())==self.index['module']); self._verify()
    def _refuse_bytecode_caches(self):
        root=Path(self.index['root'])
        for package in (root/'weft',root/'umf_weft-0.4.56.dist-info'):
            for parent,dirs,names in os.walk(package,followlinks=False):
                folder=Path(parent)
                _require(not any(name=='__pycache__' for name in dirs))
                _require(not any(name.endswith(('.pyc','.pyo')) for name in names))
    def _verify(self):
        _require(hashlib.sha256(_read(self.paths.index,MAX_INDEX)).hexdigest()==self.paths.index_sha256)
        wheel=self.index['wheel']; key=(platform.system(),platform.machine()); expected=WHEEL_SHA256.get(key)
        _require(expected is not None and set(wheel)=={'path','readPath','sha256'} and wheel['sha256']==expected)
        _require(hashlib.sha256(_read(Path(wheel['readPath']),MAX_FILE)).hexdigest()==expected)
        dist=importlib.metadata.distribution('umf-weft'); _require(dist.version==VERSION)
        direct=dist.read_text('direct_url.json'); _require(type(direct)is str)
        direct_value=_json(direct.encode()); recorded_hash=direct_value.get('archive_info',{}).get('hashes',{}).get('sha256')
        _require(direct_value.get('url')=='file://'+wheel['path'] and (recorded_hash is None or recorded_hash==expected))
        root=Path(self.index['root']); _require(root.is_absolute() and root.resolve()==root and not root.is_symlink())
        files=self.index['files']; _require(type(files)is list and 1<=len(files)<=128); expected_paths=set()
        for item in files:
            _require(type(item)is dict and set(item)=={'path','bytes','sha256'} and type(item['path'])is str and not item['path'].startswith('/')
                and all(p not in ('','.','..') for p in item['path'].split('/')) and type(item['bytes'])is int and 0<=item['bytes']<=MAX_FILE)
            rel=item['path']; _require(rel not in expected_paths); expected_paths.add(rel)
            raw=_read(root/rel,MAX_FILE); _require(len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256'])
        actual=set()
        for base in (root/'weft',root/'umf_weft-0.4.56.dist-info'):
            _require(base.is_dir() and not base.is_symlink())
            for parent,dirs,names in os.walk(base,followlinks=False):
                folder=Path(parent); _require(not any((folder/name).is_symlink() for name in dirs+names))
                actual.update((folder/name).relative_to(root).as_posix() for name in names)
        _require(actual==expected_paths)
        module_path=Path(self.index['module']); _require(module_path.is_absolute() and module_path.resolve()==module_path and str(module_path).startswith(str(root)+os.sep))
        if self.module is not None:
            _require(str(Path(self.module.__file__).resolve())==self.index['module'] and self.module.__version__==VERSION)
            _require(all(callable(getattr(self.module,n,None)) for n in ('compile_json','compile_paths_keys_json','compile_engine_page_json','check_engine_page_json','check_engine_read_json')))
        schemas=self.index['schemas']; _require(type(schemas)is dict and set(schemas)==set(SCHEMA_SHA256))
        for name,digest in SCHEMA_SHA256.items():
            _require(schemas[name]['sha256']==digest and hashlib.sha256(_read(Path(schemas[name]['path']),MAX_FILE)).hexdigest()==digest)
        return True
    def _call(self,name,*args):
        self._verify(); _require(name in {'compile_json','compile_paths_keys_json','compile_engine_page_json','check_engine_page_json','check_engine_read_json'})
        if name=='check_engine_page_json':
            _require(len(args)==3 and all(type(x)is str and 0<len(x.encode())<=MAX_REQUEST for x in args[:2]) and (args[2]is None or type(args[2])is str and 0<len(args[2].encode())<=MAX_REQUEST))
        else: _require(all(type(x)is str and 0<len(x.encode())<=MAX_REQUEST for x in args))
        out=getattr(self.module,name)(*args); _require(type(out)is str and 0<len(out.encode())<=MAX_RESPONSE); self._verify(); return out
    def compile_json(self,request): return self._call('compile_json',request)
    def compile_paths_keys_json(self,request): return self._call('compile_paths_keys_json',request)
    def compile_engine_page_json(self,request): return self._call('compile_engine_page_json',request)
    def check_engine_page_json(self,original,request,after=None): return self._call('check_engine_page_json',original,request,after)
    def check_engine_read_json(self,original,request): return self._call('check_engine_read_json',original,request)
    def schema_bundle(self):
        self._verify(); result={name:_read(Path(record['path']),MAX_FILE) for name,record in self.index['schemas'].items()}
        _require(all(hashlib.sha256(result[name]).hexdigest()==SCHEMA_SHA256[name] for name in SCHEMA_SHA256)); self._verify(); return result

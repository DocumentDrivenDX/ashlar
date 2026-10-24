# SPDX-License-Identifier: MIT OR Apache-2.0
"""Separately pinned public Weft 0.4.53 distribution and API boundary.

This is intentionally independent of the task-private 0.4.48 candidate verifier.
The index is packet-created from the selected installed cohort and is hash-bound
by its caller. Runtime files, wheel origin and selected release/schema identities
are rechecked before and after every API call.
"""
from __future__ import annotations
from dataclasses import dataclass
import hashlib, importlib, importlib.metadata, json, os, stat
from pathlib import Path

VERSION = '0.4.53'
SOURCE_COMMIT = '726a840e956e25217d6aeea2674fdfcf3f342a24'
WHEEL_SHA256 = '4c8281945b852af02cae8602747fb7b1b652ab3a1369d7650efce62ae6d35c6c'
# Read from the exact v0.4.53 source commit. These happen to retain the same
# schema bytes as the .48 candidate; the versioned map makes that fact explicit.
SCHEMA_SHA256 = {
    'compile-request-v0.4.1.schema.json': 'eadea16ecb850add4dd1a276e417e5659a553d37171ff6ba455668acec7ab13b',
    'compile-response-v0.4.1.schema.json': 'e6ba09a90a08e7065c134f6ca0958c70da02cedbd843eeab5d608b59ba335be3',
    'logical-plan-v0.4.1.schema.json': '7e48576b69a688c41894ce033af7bcb9a0792e1346010b4f5b19887ad536112f',
}
MAX_INDEX = 1_048_576
MAX_FILE = 32 * 1024 * 1024
MAX_REQUEST = 1_048_576
MAX_RESPONSE = 1_048_576

class Weft053Refused(ValueError):
    """Payload-free refusal of the pinned public Weft 0.4.53 runtime."""

def _require(value):
    if not value: raise Weft053Refused('Pinned Weft 0.4.53 runtime refused')

def _read(path: Path, limit: int) -> bytes:
    _require(type(path) is type(Path()) and path.is_absolute() and not any(p.is_symlink() for p in (path, *path.parents)))
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        _require(stat.S_ISREG(os.fstat(fd).st_mode))
        chunks=[]; remaining=limit+1
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
    except Exception: raise Weft053Refused('Pinned Weft 0.4.53 runtime refused') from None

def _hex(value): return type(value) is str and len(value)==64 and all(c in '0123456789abcdef' for c in value)

@dataclass(frozen=True)
class Weft053Paths:
    index: Path
    index_sha256: str
    def __post_init__(self):
        _require(type(self.index) is type(Path()) and self.index.is_absolute() and _hex(self.index_sha256))

class Weft053Distribution:
    """Recheck an exact selected wheel installation and expose bounded APIs."""
    def __init__(self, paths: Weft053Paths):
        _require(type(paths) is Weft053Paths); paths.__post_init__(); self.paths=paths
        self.index_bytes=_read(paths.index,MAX_INDEX)
        _require(hashlib.sha256(self.index_bytes).hexdigest()==paths.index_sha256)
        self.index=_json(self.index_bytes)
        _require(set(self.index)=={'profile','sourceCommit','version','wheel','root','module','files','schemas'}
            and self.index['profile']=='ashlar-weft-053-runtime-index/0.1'
            and self.index['sourceCommit']==SOURCE_COMMIT and self.index['version']==VERSION)
        self.module=importlib.import_module('weft'); self._verify()

    def _verify(self):
        _require(hashlib.sha256(_read(self.paths.index,MAX_INDEX)).hexdigest()==self.paths.index_sha256)
        wheel=self.index['wheel']; _require(set(wheel)=={'path','readPath','sha256'} and wheel['sha256']==WHEEL_SHA256)
        _require(hashlib.sha256(_read(Path(wheel['readPath']),MAX_FILE)).hexdigest()==WHEEL_SHA256)
        dist=importlib.metadata.distribution('weft-sql')
        _require(dist.version==VERSION and self.module.__version__==VERSION)
        direct=dist.read_text('direct_url.json'); _require(type(direct)is str)
        direct_value=_json(direct.encode())
        _require(direct_value.get('url')=='file://'+wheel['path'] and direct_value.get('archive_info',{}).get('hashes',{}).get('sha256')==WHEEL_SHA256)
        root=Path(self.index['root']); _require(root.is_absolute() and root.resolve()==root and not root.is_symlink())
        files=self.index['files']; _require(type(files)is list and 1<=len(files)<=128)
        expected=set()
        for item in files:
            _require(type(item)is dict and set(item)=={'path','bytes','sha256'} and type(item['path'])is str
                and not item['path'].startswith('/') and all(p not in ('','.','..') for p in item['path'].split('/'))
                and type(item['bytes'])is int and 0<=item['bytes']<=MAX_FILE and _hex(item['sha256']))
            rel=item['path']; _require(rel not in expected); expected.add(rel)
            actual=_read(root/rel,MAX_FILE); _require(len(actual)==item['bytes'] and hashlib.sha256(actual).hexdigest()==item['sha256'])
        actual_paths=set()
        for base in (root/'weft',root/'weft_sql-0.4.53.dist-info'):
            _require(base.is_dir() and not base.is_symlink())
            for parent,dirs,names in os.walk(base,followlinks=False):
                d=Path(parent); _require(not any((d/n).is_symlink() for n in dirs+names))
                actual_paths.update((d/n).relative_to(root).as_posix() for n in names)
        _require(actual_paths==expected)
        module=Path(self.module.__file__).resolve(); _require(str(module)==self.index['module'])
        _require(callable(getattr(self.module,'compile_paths_keys_json',None))
            and callable(getattr(self.module,'compile_engine_page_json',None))
            and callable(getattr(self.module,'check_engine_page_json',None))
            and callable(getattr(self.module,'check_engine_read_json',None)))
        schemas=self.index['schemas']; _require(type(schemas)is dict and set(schemas)==set(SCHEMA_SHA256))
        for name,digest in SCHEMA_SHA256.items():
            _require(schemas[name]['sha256']==digest and hashlib.sha256(_read(Path(schemas[name]['path']),MAX_FILE)).hexdigest()==digest)
        return True

    def _call(self,name,*args):
        self._verify(); _require(name in {'compile_paths_keys_json','compile_engine_page_json','check_engine_page_json','check_engine_read_json'})
        if name=='check_engine_page_json':
            _require(len(args)==3 and all(type(x)is str and 0<len(x.encode())<=MAX_REQUEST for x in args[:2])
                and (args[2]is None or type(args[2])is str and 0<len(args[2].encode())<=MAX_REQUEST))
        else:
            _require(all(type(x)is str and 0<len(x.encode())<=MAX_REQUEST for x in args))
        out=getattr(self.module,name)(*args)
        _require(type(out)is str and 0<len(out.encode())<=MAX_RESPONSE); self._verify(); return out
    def compile_paths_keys_json(self,request): return self._call('compile_paths_keys_json',request)
    def compile_engine_page_json(self,request): return self._call('compile_engine_page_json',request)
    def check_engine_page_json(self,original,request,after=None): return self._call('check_engine_page_json',original,request,after)
    def check_engine_read_json(self,original,request): return self._call('check_engine_read_json',original,request)
    def schema_bundle(self):
        self._verify()
        result={name:_read(Path(record['path']),MAX_FILE) for name,record in self.index['schemas'].items()}
        _require(all(hashlib.sha256(result[name]).hexdigest()==SCHEMA_SHA256[name]for name in SCHEMA_SHA256))
        self._verify(); return result

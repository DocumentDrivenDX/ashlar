"""Closed installed preparation selections; no Truss import or source authority."""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from .lifecycle import finish, owned_context

PROFILE = 'ashlar-truss-preparation/0.1'
MAX_CONFIG = 1048576


def closed(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        raise ValueError('Closed preparation configuration required')
    return value


def absolute(value):
    if type(value) is not str or not 0 < len(value.encode()) <= 4096 or '\x00' in value:
        raise ValueError('Canonical absolute path required')
    p = Path(value)
    if not p.is_absolute() or p.parent.resolve() != p.parent or str(p) != value:
        raise ValueError('Canonical absolute path required')
    return p


def text(value):
    if type(value) is not str or not 0 < len(value.encode()) <= 4096 or '\x00' in value:
        raise ValueError('Exact bounded text required')
    return value


def digest(value):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise ValueError('Explicit SHA256 required')
    return value


def positive(value, ceiling):
    if type(value) is not int or not 0 < value <= ceiling:
        raise ValueError('Explicit bounded integer required')
    return value


def decode(raw, maximum=MAX_CONFIG):
    if type(raw) is not bytes or not 0 < len(raw) <= maximum:
        raise ValueError('Bounded preparation JSON required')
    # Bound nesting before the JSON decoder constructs containers.
    depth=0;quoted=False;escaped=False
    for char in raw:
        if quoted:
            if escaped: escaped=False
            elif char==92: escaped=True
            elif char==34: quoted=False
        elif char==34: quoted=True
        elif char in (91,123):
            depth+=1
            if depth>12: raise ValueError('Preparation JSON depth exceeded')
        elif char in (93,125): depth-=1
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise ValueError('Duplicate preparation member')
            result[key]=value
        return result
    def integer(value):
        if len(value)>10: raise ValueError('Preparation integer token exceeded')
        return int(value)
    def refuse(value): raise ValueError('Exact preparation integer required')
    return json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refuse,parse_constant=refuse)


@dataclass(frozen=True)
class PinnedFile:
    path: Path
    bytes: int
    sha256: str

    def __post_init__(self):
        if type(self.path) is not type(Path('/')): raise ValueError('Exact pinned path required')
        absolute(str(self.path))
        if type(self.bytes)is not int or not 0<=self.bytes<=67108864:raise ValueError('Bounded resource length required')
        digest(self.sha256)
        initial=self.path.lstat()
        if not stat.S_ISREG(initial.st_mode) or initial.st_nlink!=1:raise ValueError('Pinned regular file required')
        object.__setattr__(self,'_identity',(initial.st_dev,initial.st_ino,initial.st_uid,initial.st_mode))

    def read(self, maximum):
        if self.bytes>maximum: raise ValueError('Pinned file exceeds selected bound')
        fd=None;primary=None;raw=None
        try:
            fd=os.open(self.path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            before=os.fstat(fd)
            if (before.st_dev,before.st_ino,before.st_uid,before.st_mode)!=self._identity or not stat.S_ISREG(before.st_mode) or before.st_size!=self.bytes or before.st_nlink!=1:
                raise ValueError('Original pinned regular file required')
            chunks=[];size=0;h=hashlib.sha256()
            while True:
                chunk=os.read(fd,min(65536,self.bytes-size+1))
                if not chunk:break
                size+=len(chunk)
                if size>self.bytes:raise ValueError('Pinned file grew')
                chunks.append(chunk);h.update(chunk)
            named=self.path.lstat();after=os.fstat(fd)
            if after.st_nlink!=1 or named.st_nlink!=1 or size!=self.bytes or h.hexdigest()!=self.sha256 or (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) or (named.st_dev,named.st_ino)!=(before.st_dev,before.st_ino):
                raise ValueError('Original pinned bytes changed')
            raw=b''.join(chunks)
        except BaseException as error:primary=error
        finish(primary,[lambda:os.close(fd)] if fd is not None else [])
        return raw

    def renew(self):
        # Stream hash without retaining an executable/package payload in memory.
        fd=None;primary=None
        try:
            fd=os.open(self.path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            before=os.fstat(fd);h=hashlib.sha256();size=0
            if (before.st_dev,before.st_ino,before.st_uid,before.st_mode)!=self._identity or not stat.S_ISREG(before.st_mode) or before.st_size!=self.bytes or before.st_nlink!=1:raise ValueError('Pinned file identity refused')
            while True:
                data=os.read(fd,min(65536,self.bytes-size+1))
                if not data:break
                size+=len(data)
                if size>self.bytes:raise ValueError('Pinned file grew')
                h.update(data)
            after=os.fstat(fd);named=self.path.lstat()
            if after.st_nlink!=1 or named.st_nlink!=1 or size!=self.bytes or h.hexdigest()!=self.sha256 or (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) or (named.st_dev,named.st_ino)!=(before.st_dev,before.st_ino):raise ValueError('Original pinned bytes changed')
        except BaseException as error:primary=error
        finish(primary,[lambda:os.close(fd)]if fd is not None else [])


@dataclass(frozen=True)
class ImportRoot:
    path: Path
    files: tuple
    directories: tuple

    def __post_init__(self):
        if type(self.path)is not type(Path('/')) or self.path.resolve()!=self.path:raise ValueError('Canonical import root required')
        if type(self.files)is not tuple or not self.files or len(self.files)>4096 or any(type(f)is not PinnedFile or not f.path.is_relative_to(self.path) for f in self.files):raise ValueError('Complete bounded import files required')
        if type(self.directories)is not tuple or len(self.directories)>4096 or any(type(d)is not str or not d or Path(d).is_absolute() or '..'in Path(d).parts for d in self.directories):raise ValueError('Complete import directories required')
        initial=self.path.lstat()
        if not stat.S_ISDIR(initial.st_mode):raise ValueError('Import directory required')
        object.__setattr__(self,'_identity',(initial.st_dev,initial.st_ino,initial.st_uid,initial.st_mode))
        names=[str(f.path.relative_to(self.path))for f in self.files]
        if len(set(names))!=len(names) or len(set(self.directories))!=len(self.directories) or set(names)&set(self.directories) or sum(f.bytes for f in self.files)>134217728:raise ValueError('Distinct bounded import namespace required')

    def renew(self):
        current=self.path.lstat()
        if self.path.resolve()!=self.path or (current.st_dev,current.st_ino,current.st_uid,current.st_mode)!=self._identity:raise ValueError('Import root changed')
        found_files=[];found_dirs=[];stack=[self.path]
        while stack:
            directory=stack.pop()
            with owned_context(os.scandir(directory)) as items:
                for item in items:
                    relative=str(Path(item.path).relative_to(self.path))
                    if item.is_symlink():raise ValueError('Import namespace symlink refused')
                    if item.is_dir(follow_symlinks=False):found_dirs.append(relative);stack.append(Path(item.path))
                    elif item.is_file(follow_symlinks=False):found_files.append(relative)
                    else:raise ValueError('Import namespace entry refused')
                    if len(found_dirs)+len(found_files)>8192:raise ValueError('Import namespace capacity exceeded')
        if set(found_files)!={str(f.path.relative_to(self.path))for f in self.files} or set(found_dirs)!=set(self.directories):raise ValueError('Complete import namespace changed')
        finish(None,[f.renew for f in self.files])


@dataclass(frozen=True)
class TrussPreparationLimits:
    input_bytes: int
    capture_bytes: int
    output_bytes: int
    receipt_bytes: int
    deadline_seconds: int

    def __post_init__(self):
        for v,c in ((self.input_bytes,1048576),(self.capture_bytes,16777216),(self.output_bytes,67108864),(self.receipt_bytes,1048576),(self.deadline_seconds,60)):positive(v,c)
        if self.receipt_bytes>self.output_bytes:raise ValueError('Receipt exceeds aggregate output')


@dataclass(frozen=True)
class PreparationDocument:
    document_id: str
    document_revision: str
    artifact_identity: object
    source: PinnedFile

    def __post_init__(self):
        text(self.document_id);text(self.document_revision)
        if self.artifact_identity is not None:text(self.artifact_identity)
        if type(self.source)is not PinnedFile or self.source.bytes==0:raise ValueError('Exact original document file required')


@dataclass(frozen=True)
class TrussPreparationConfig:
    documents: tuple
    configuration: PinnedFile
    python: PinnedFile
    bun: PinnedFile
    python_version: str
    bun_version: str
    import_roots: tuple
    distribution: str
    version: str
    source_revision: str
    wheel: PinnedFile
    module_origin: Path
    output: Path
    limits: TrussPreparationLimits

    def __post_init__(self):
        if type(self.documents)is not tuple or not 1<=len(self.documents)<=512 or any(type(d)is not PreparationDocument for d in self.documents):raise ValueError('Original ordered documents required')
        if len({d.document_id for d in self.documents})!=len(self.documents):raise ValueError('Duplicate document identity')
        if any(type(f)is not PinnedFile for f in (self.configuration,self.python,self.bun,self.wheel))or type(self.limits)is not TrussPreparationLimits:raise ValueError('Exact file/limit carriers required')
        if type(self.python_version)is not str or re.fullmatch(r'3\.(?:1[1-9]|[2-9][0-9])\.[0-9]+',self.python_version)is None:raise ValueError('Selected Python at least3.11 required')
        text(self.bun_version);text(self.version)
        if self.distribution!='truss-toolkit' or re.fullmatch('[0-9a-f]{40}',self.source_revision or '')is None:raise ValueError('Explicit Truss origin required')
        if type(self.import_roots)is not tuple or not 1<=len(self.import_roots)<=8 or any(type(r)is not ImportRoot for r in self.import_roots):raise ValueError('Explicit import roots required')
        roots=[r.path for r in self.import_roots]
        if any(a==b or a.is_relative_to(b)or b.is_relative_to(a)for i,a in enumerate(roots)for b in roots[i+1:]):raise ValueError('Distinct import roots required')
        if type(self.module_origin)is not type(Path('/'))or not any(self.module_origin==r.path/'truss/__init__.py'for r in self.import_roots):raise ValueError('Exact public Truss module origin required')
        if not any(self.module_origin==f.path for r in self.import_roots for f in r.files):raise ValueError('Truss origin missing from closure')
        if type(self.output)is not type(Path('/')):raise ValueError('Exact output required')
        absolute(str(self.output))
        if any(f.bytes==0 for f in (self.configuration,self.python,self.bun,self.wheel)):raise ValueError('Nonempty input/runtime/wheel files required')
        inputs=[d.source.path for d in self.documents]+[self.configuration.path,self.python.path,self.bun.path,self.wheel.path]
        if len(set(inputs))!=len(inputs)or any(p==self.output or p.is_relative_to(self.output)for p in inputs+roots):raise ValueError('Distinct input/output custody required')
        if sum(d.source.bytes for d in self.documents)+self.configuration.bytes>self.limits.input_bytes:raise ValueError('Aggregate input exceeded')

    def renew(self):
        finish(None,[d.source.renew for d in self.documents]+[f.renew for f in (self.configuration,self.python,self.bun,self.wheel)]+[r.renew for r in self.import_roots])


def parse_truss_preparation_configuration(raw):
    value=closed(decode(raw),('profile','documents','configuration','runtime','package','output','limits'))
    if value['profile']!=PROFILE:raise ValueError('Exact preparation profile required')
    def pin(v):
        v=closed(v,('path','bytes','sha256'));return PinnedFile(absolute(v['path']),v['bytes'],v['sha256'])
    runtime=closed(value['runtime'],('python','bun','python_version','bun_version','import_roots'))
    package=closed(value['package'],('distribution','version','source_revision','wheel','module_origin'))
    if type(value['documents'])is not list or type(runtime['import_roots'])is not list:raise ValueError('Explicit arrays required')
    docs=[]
    for v in value['documents']:
        v=closed(v,('document_id','document_revision','artifact_identity','source'));docs.append(PreparationDocument(v['document_id'],v['document_revision'],v['artifact_identity'],pin(v['source'])))
    roots=[]
    for v in runtime['import_roots']:
        v=closed(v,('path','files','directories'))
        if type(v['files'])is not list or type(v['directories'])is not list:raise ValueError('Complete import inventories required')
        roots.append(ImportRoot(absolute(v['path']),tuple(pin(f)for f in v['files']),tuple(v['directories'])))
    limits=TrussPreparationLimits(**closed(value['limits'],('input_bytes','capture_bytes','output_bytes','receipt_bytes','deadline_seconds')))
    return TrussPreparationConfig(tuple(docs),pin(value['configuration']),pin(runtime['python']),pin(runtime['bun']),runtime['python_version'],runtime['bun_version'],tuple(roots),package['distribution'],package['version'],package['source_revision'],pin(package['wheel']),absolute(package['module_origin']),absolute(value['output']),limits)

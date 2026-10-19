# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit local-development use of two operator-selected private export files.

This owner grants no live source, canonical writer, storage or Fabric authority.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import json,os,stat,threading
from ashlar.graph_release import GraphRelease
from .lifecycle import finish
from .private_graph_custody import load_private_graph_release

PROFILE='ashlar-local-development-private-export-use/0.1'

@dataclass(frozen=True)
class LocalGraphExportSelection:
    release_path:Path
    custody_path:Path
    release_sha256:str
    custody_sha256:str


def _read(path):
    if type(path)is not type(Path('/'))or not path.is_absolute()or path.resolve()!=path:
        raise ValueError('Canonical selected export file required')
    fd=None;primary=None;answer=None
    try:
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);before=os.fstat(fd)
        facts=lambda info:(info.st_dev,info.st_ino,info.st_uid,info.st_mode,info.st_nlink,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
        if (not stat.S_ISREG(before.st_mode)or before.st_uid!=os.geteuid()or before.st_nlink!=1
                or not before.st_mode&stat.S_IRUSR or not 0<before.st_size<=16777216
                or facts(path.lstat())!=facts(before)):
            raise PermissionError('Current ordinary read access to owned export file required')
        chunks=[];count=0
        while True:
            chunk=os.read(fd,min(65536,before.st_size-count+1))
            if not chunk:break
            count+=len(chunk)
            if count>before.st_size:raise ValueError('Original export file grew')
            chunks.append(chunk)
        if count!=before.st_size or path.resolve()!=path or facts(os.fstat(fd))!=facts(before)or facts(path.lstat())!=facts(before):
            raise ValueError('Original export file changed during read')
        answer=b''.join(chunks),facts(before)
    except BaseException as error:primary=error
    finish(primary,[lambda:os.close(fd)]if fd is not None else[])
    return answer


class _HistoricalSource:
    def __init__(self,value):self._raw=json.dumps(value,sort_keys=True,separators=(',',':'))
    def metadata(self):return json.loads(self._raw)


class LocalGraphExportUse:
    """Live file-use owner; construction belongs to open_local_graph_export."""
    def __init__(self):raise TypeError('Use open_local_graph_export')
    @property
    def release(self):return self._release
    @property
    def custody_bytes(self):return self._custody_bytes
    @property
    def custody_sha256(self):return self._custody_sha256
    @property
    def source(self):return self._source
    @property
    def context(self):return self._context
    @property
    def profile(self):return PROFILE
    @property
    def policy(self):return self
    def renew(self):
        if self._closed or os.geteuid()!=self._uid:raise PermissionError('Original local export-use owner required')
        for path,raw,identity in self._files:
            current,current_identity=_read(path)
            if current!=raw or current_identity!=identity:raise PermissionError('Selected original export file changed')
    @contextmanager
    def writer(self,context):
        if context is not self.context or not self._use_lock.acquire(blocking=False):
            raise PermissionError('Nonnested original local use interval required')
        primary=None
        try:self.renew();self._active=True;yield
        except BaseException as error:primary=error
        try:finish(primary,[self.renew])
        finally:self._active=False;self._use_lock.release()
    def admit_original_export(self,release,custody_bytes,source,context):
        if (context is not self.context or not self._active or type(release)is not GraphRelease or release!=self.release
                or custody_bytes!=self.custody_bytes or source is not self.source):
            raise PermissionError('Exact held local-development export use required')
        self.renew()
        return None


@contextmanager
def open_local_graph_export(selection):
    """Select trusted digests explicitly; do not compute authority from input files."""
    if type(selection)is not LocalGraphExportSelection or selection.release_path==selection.custody_path:
        raise ValueError('Two separately selected original export files required')
    release_raw,release_identity=_read(selection.release_path)
    custody_raw,custody_identity=_read(selection.custody_path)
    value=load_private_graph_release(release_raw,selection.release_sha256,custody_raw,selection.custody_sha256)
    owner=object.__new__(LocalGraphExportUse);owner._release=GraphRelease(release_raw,selection.release_sha256)
    owner._custody_bytes=custody_raw;owner._custody_sha256=selection.custody_sha256
    owner._source=_HistoricalSource(json.loads(value['publication']['validation_report_json'])['source_admission'])
    owner._context=object();owner._closed=False;owner._active=False;owner._uid=os.geteuid();owner._use_lock=threading.Lock()
    owner._files=((selection.release_path,release_raw,release_identity),(selection.custody_path,custody_raw,custody_identity))
    primary=None
    try:owner.renew();yield owner
    except BaseException as error:primary=error
    try:finish(primary,[owner.renew])
    finally:owner._closed=True

# SPDX-License-Identifier: MIT OR Apache-2.0
"""Configure a selected saved export pair without an engine or live source grant."""
import hashlib,json,os,stat
from dataclasses import dataclass
from pathlib import Path
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes,require_custody_digest
from .local_graph_export import LocalGraphExportSelection,open_local_graph_export
from .lifecycle import owned_context,finish


@dataclass(frozen=True)
class GraphExportReportFile:
    path:Path
    raw:bytes
    identity:tuple

    @staticmethod
    def _identity(path):
        if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical report required')
        value=path.stat()
        if not stat.S_ISREG(value.st_mode)or value.st_size>1048576:raise ValueError('Bounded regular report required')
        return (value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns)

    @classmethod
    def read(cls,path):
        path=Path(path);before=cls._identity(path);raw=snapshot(path,1048576)
        if cls._identity(path)!=before:raise ValueError('Original report changed during capture')
        return cls(path,raw,before)

    def renew(self):
        if self._identity(self.path)!=self.identity:raise ValueError('Original report identity changed')
        raw=snapshot(self.path,1048576)
        if raw!=self.raw or self._identity(self.path)!=self.identity:raise ValueError('Original report bytes changed')


def graph_export_selection_from_report(raw,expected_sha256):
    """Select original report digests unchanged; hashes confer no authority."""
    require_custody_digest(expected_sha256)
    if type(raw)is not bytes or not 0<len(raw)<=1048576 or hashlib.sha256(raw).hexdigest()!=expected_sha256:raise ValueError('Selected original export report digest differs')
    report=decode_custody_bytes(raw)
    if type(report)is not dict:raise ValueError('Original export report required')
    if type(report.get('release'))is dict and type(report.get('custody'))is dict:
        left,right=report['release'],report['custody']
        if set(left)!={'path','sha256'}or set(right)!={'path','sha256'}:raise ValueError('Exact pair selectors required')
        paths=(left['path'],right['path']);digests=(left['sha256'],right['sha256'])
    elif {'release','release_sha256','custody','custody_sha256'}<=set(report):
        paths=(report['release'],report['custody']);digests=(report['release_sha256'],report['custody_sha256'])
    else:raise ValueError('Standalone Truss or finite export report required')
    selected=[]
    for text in paths:
        if type(text)is not str:raise ValueError('Canonical existing export path required')
        path=Path(text)
        if not path.is_absolute()or path.resolve()!=path or not path.is_file()or any(c in text for c in ('\0','\n','\r',',')):raise ValueError('Canonical existing export path required')
        selected.append(path)
    for digest in digests:require_custody_digest(digest)
    return LocalGraphExportSelection(*selected,*digests)


def run_configure_graph_export_command(export_report,export_report_sha256,output):
    report=GraphExportReportFile.read(export_report);selection=graph_export_selection_from_report(report.raw,export_report_sha256)
    destination=Path(output)
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or not destination.parent.is_dir()or destination.exists()or destination.is_symlink():raise ValueError('Fresh canonical output required')
    if any(destination==p or destination in p.parents or p in destination.parents for p in (report.path,selection.release_path,selection.custody_path)):raise ValueError('Separate output required')
    primary=None
    try:
        report.renew()
        with owned_context(open_local_graph_export(selection))as owner:
            with owned_context(owner.writer(owner.context)):
                owner.admit_original_export(owner.release,owner.custody_bytes,owner.source,owner.context)
    except BaseException as error:primary=error
    finish(primary,[lambda:report.renew()])
    value={'release_path':str(selection.release_path),'custody_path':str(selection.custody_path),'release_sha256':selection.release_sha256,'custody_sha256':selection.custody_sha256}
    payload=(json.dumps(value,separators=(',',':'))+'\n').encode();fd=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(payload)!=len(payload):raise OSError('Complete private configuration write required')
        stream.flush();os.fsync(stream.fileno())
    return {'output':str(destination)}

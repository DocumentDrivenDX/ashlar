"""Owned local file/process mechanics; no package proof, trust or SDK ownership.

Ready publication is a no-clobber availability commit under cooperating directory
writers. It is not a crash-durability guarantee. Profile layouts are closed here;
verification callbacks remain responsible for the owning package proof.
"""
from __future__ import annotations
from dataclasses import dataclass, replace
import hashlib
import os
from pathlib import Path
import selectors
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from types import MappingProxyType
from typing import Callable, Protocol, TypeVar

FILE_LIMIT=32*1024*1024
JSON_LIMIT=4*1024*1024
TOTAL_LIMIT=96*1024*1024
PROTOCOL_LIMIT=16*1024*1024
_SCHEMA_NAMES=('compile-request-v0.4.schema.json','compile-response-v0.4.schema.json',
               'logical-plan-v0.4.schema.json','application-result-v0.4.schema.json',
               'backend-manifest-v0.3.schema.json')
_LAYOUT_VALUES=MappingProxyType({
    'paths':('weft-paths','ashlar-weft-paths-ready/0.1','.ashlar-paths-',_SCHEMA_NAMES),
    'paths-keys':('weft-paths-keys','ashlar-weft-paths-keys-ready/0.1','.ashlar-paths-keys-',
                  _SCHEMA_NAMES+('application-result-v0.2.schema.json',)),
})

@dataclass(frozen=True)
class InstallationLayout:
    """Internally selected closed layout; no caller-defined file or proof policy."""
    profile: str
    executable: str
    ready_format: str
    staging_prefix: str
    schemas: tuple[str,...]

    def __post_init__(self):
        if type(self.profile) is not str or self.profile not in _LAYOUT_VALUES or (
            self.executable,self.ready_format,self.staging_prefix,self.schemas)!=_LAYOUT_VALUES[self.profile]:
            raise ValueError('ASHLAR-INSTALLATION-LAYOUT-REFUSED')

    @property
    def resources(self) -> tuple[str,...]:
        return ('backend-manifest.json',)+tuple('schemas/'+name for name in self.schemas)

_LAYOUTS=MappingProxyType({name:InstallationLayout(name,*values) for name,values in _LAYOUT_VALUES.items()})

def installation_layout(profile: str) -> InstallationLayout:
    """Select a fixed layout; configuration cannot widen its names or bounds."""
    if type(profile) is not str or profile not in _LAYOUTS:
        raise ValueError('ASHLAR-INSTALLATION-LAYOUT-REFUSED')
    return _LAYOUTS[profile]

def mark_cleanup(primary: BaseException) -> None:
    """Best-effort safe marker; annotation failure cannot replace the primary."""
    try:setattr(primary,'cleanup_failed',True)
    except BaseException:pass

def finish(primary: BaseException | None, callbacks: tuple[Callable[[],None],...]) -> None:
    """Attempt every owned cleanup; retain first failure by exact identity."""
    for callback in callbacks:
        try:callback()
        except BaseException as exc:
            if primary is None:primary=exc
            else:mark_cleanup(primary)
    if primary is not None:raise primary

def write_owned(path: Path, raw: bytes, *, refuse: Callable[[],None]) -> None:
    """Exclusive no-follow write; caller owns destination and finite bytes."""
    if type(raw) is not bytes or len(raw)>FILE_LIMIT:refuse()
    fd=None;primary=None
    try:
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        offset=0
        while offset<len(raw):
            count=os.write(fd,raw[offset:offset+65536])
            if count<=0:refuse()
            offset+=count
    except BaseException as exc:primary=exc
    finish(primary,(lambda:os.close(fd),) if fd is not None else ())

class InstallationSettings(Protocol):
    """Mechanical locations/labels; proof and authority belong to the caller."""
    output: Path
    index_revision: str
    index_sha256: str
    realization_id: str
    observed_target: str
    observed_os: str

class InstallationBytes(Protocol):
    """Owned immutable byte snapshots, not a package admission decision."""
    executable_bytes: bytes
    provenance_bytes: bytes
    resources: tuple[tuple[str, bytes], ...]

Result = TypeVar('Result')

def publish_installation(config: InstallationSettings, verified: InstallationBytes, *,
                         layout: InstallationLayout,
                         write: Callable[[Path, bytes], None], read: Callable[..., bytes],
                         encode: Callable[[dict], bytes], verify: Callable[..., Result],
                         refuse: Callable[[], None]) -> Result:
    """Stage/copy only owned closed files, then commit the fully verified ready link.

    verify constructs the owning immutable result before commit. Cleanup-only
    OSError after commit is maintenance success; cancellation propagates while
    committed availability remains intact. No rollback or durability is promised.
    """
    if type(layout) is not InstallationLayout or layout is not _LAYOUTS.get(layout.profile):refuse()
    if config.output.exists() or config.output.is_symlink() or any(p.is_symlink() for p in config.output.parents):refuse()
    resources=tuple(verified.resources)
    if tuple(name for name,_ in resources)!=layout.resources:refuse()
    files=((layout.executable,verified.executable_bytes),('provenance.json',verified.provenance_bytes))+resources
    if any(type(raw) is not bytes or len(raw)>(FILE_LIMIT if name==layout.executable else JSON_LIMIT) for name,raw in files) or sum(len(raw)for _,raw in files)>TOTAL_LIMIT:refuse()
    staging=Path(tempfile.mkdtemp(prefix=layout.staging_prefix,dir=config.output.parent))
    primary=None;owned=None;committed=False;result=None;maintenance_result=None
    link_started=False;ready_identity=None;rollback_allowed=True
    def put(root,name,raw):
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);write(path,raw)
        path.chmod(0o555 if name==layout.executable else 0o444)
        if read(path)!=raw:refuse()
    def descriptor(name,raw):return {'path':name,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
    try:
        for name,raw in files:put(staging,name,raw)
        ready={'format':layout.ready_format,'indexRevision':config.index_revision,'indexSha256':config.index_sha256,
               'realizationId':config.realization_id,'target':config.observed_target,'observedOS':config.observed_os,
               'executable':descriptor(layout.executable,verified.executable_bytes),
               'provenance':descriptor('provenance.json',verified.provenance_bytes),
               'resources':[descriptor(name,raw)for name,raw in resources]}
        ready_raw=encode(ready)+b'\n'
        if type(ready_raw)is not bytes or len(ready_raw)>JSON_LIMIT:refuse()
        write(staging/'ready.json',ready_raw);(staging/'ready.json').chmod(0o444)
        config.output.mkdir(exist_ok=False);info=config.output.stat();owned=(info.st_dev,info.st_ino)
        for name,raw in files:put(config.output,name,raw)
        if read(staging/'ready.json',JSON_LIMIT)!=ready_raw:refuse()
        result=verify(config,ready_raw,ready_present=False)
        maintenance_result=replace(result,cleanup_pending=True)
        result=replace(result,cleanup_pending=False)
        ready_info=(staging/'ready.json').lstat()
        ready_identity=(ready_info.st_dev,ready_info.st_ino)
        link_started=True;rollback_allowed=False
        os.link(staging/'ready.json',config.output/'ready.json');committed=True
    except BaseException as exc:primary=exc
    if link_started and not committed:
        # A successful syscall may be followed by interruption before assignment.
        # Only our exact regular-file hardlink establishes that availability commit.
        try:
            directory=config.output.lstat()
            final_ready=(config.output/'ready.json').lstat()
            committed=(stat.S_ISDIR(directory.st_mode) and
                       (directory.st_dev,directory.st_ino)==owned and
                       stat.S_ISREG(final_ready.st_mode) and
                       (final_ready.st_dev,final_ready.st_ino)==ready_identity)
        except FileNotFoundError:rollback_allowed=True
        except BaseException as exc:
            if primary is None:primary=exc
            else:mark_cleanup(primary)
        # A foreign ready inode or uncertain observation is never an owned commit
        # and is never removed by rollback. The original failure still propagates.
    if not committed and rollback_allowed and owned is not None:
        try:
            info=config.output.stat()
            if not config.output.is_symlink() and (info.st_dev,info.st_ino)==owned:shutil.rmtree(config.output)
        except BaseException as exc:
            if primary is None:primary=exc
            else:mark_cleanup(primary)
    cleanup_pending=False
    try:shutil.rmtree(staging)
    except BaseException as exc:
        if committed and isinstance(exc,OSError):
            cleanup_pending=True
            if primary is not None:mark_cleanup(primary)
        elif primary is None:primary=exc
        else:mark_cleanup(primary)
    if primary is not None:raise primary
    return maintenance_result if cleanup_pending else result

def compile_transport(executable: Path, request: bytes, *, refuse: Callable[[], None],
                      validate_response: Callable[[bytes],None],
                      closing_verify: Callable[[],None]) -> bytes:
    """One bounded original-byte process; process-group cleanup on every outcome.

    Package/opening proof belongs to the caller. Response validation and closing
    proof happen before bytes can return. Descendant cleanup assumes inherited
    process-group membership; this is not an adversarial process sandbox.
    """
    if type(request)is not bytes or len(request)>PROTOCOL_LIMIT:refuse()
    process=None;selector=None;primary=None;streams=[];stdout=bytearray();stderr=bytearray()
    deadline=time.monotonic()+30
    try:
        selector=selectors.DefaultSelector()
        process=subprocess.Popen([str(executable)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,start_new_session=True,env={'PATH':'/usr/bin:/bin'})
        streams=[process.stdout,process.stderr,process.stdin]
        for stream,label in ((process.stdout,'out'),(process.stderr,'err')):
            os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,label)
        os.set_blocking(process.stdin.fileno(),False);offset=0
        if request:selector.register(process.stdin,selectors.EVENT_WRITE,'in')
        else:process.stdin.close()
        while selector.get_map():
            if time.monotonic()>=deadline:refuse()
            for key,_ in selector.select(min(0.1,max(0,deadline-time.monotonic()))):
                stream=key.fileobj;label=key.data
                if label=='in':
                    try:count=os.write(stream.fileno(),request[offset:offset+65536])
                    except BlockingIOError:continue
                    if count<=0:refuse()
                    offset+=count
                    if offset==len(request):selector.unregister(stream);stream.close()
                else:
                    target=stdout if label=='out' else stderr;limit=PROTOCOL_LIMIT if label=='out' else 4096
                    try:chunk=os.read(stream.fileno(),min(65536,limit+1-len(target)))
                    except BlockingIOError:continue
                    if not chunk:selector.unregister(stream);stream.close()
                    else:
                        target.extend(chunk)
                        if len(target)>limit:refuse()
        code=process.wait(timeout=max(0.001,deadline-time.monotonic()))
        if code!=0 or stderr or not stdout.endswith(b'\n') or stdout.count(b'\n')!=1:refuse()
        validate_response(bytes(stdout));closing_verify()
    except BaseException as exc:primary=exc
    callbacks=[]
    if process is not None:
        def terminate():
            failure=None
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            except BaseException as exc:failure=exc
            finish(failure,(lambda:process.wait(timeout=2),))
        callbacks.append(terminate)
    if selector is not None:callbacks.append(selector.close)
    callbacks.extend(stream.close for stream in streams if not stream.closed)
    finish(primary,tuple(callbacks))
    return bytes(stdout)

"""Separate indexed Paths530 installation and bounded string transport.

Availability commits at the final no-clobber ready link. This is cooperating
local-writer scope, not crash durability, native query or publication proof.
"""
from __future__ import annotations
from dataclasses import dataclass, replace
import hashlib
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import tempfile
import time

from .weft_paths_package import (
    PathsInstallationConfig, PathsInstallationError, VerifiedPathsPackage,
    inspect_package, read_snapshot, encode_document, decode_document,
    verify_trusted_index, verify_exact_tree, validate_manifest,
    JSON_LIMIT, FILE_LIMIT, PROTOCOL_LIMIT, INSTALLED_SCHEMAS, SCHEMA_BASE,
)


def _refuse():
    raise PathsInstallationError('ASHLAR-WEFT-PATHS-REFUSED')


def _sha(raw): return hashlib.sha256(raw).hexdigest()


def _marker(primary):
    try: setattr(primary, 'cleanup_failed', True)
    except BaseException: pass


@dataclass(frozen=True)
class PathsInstallation:
    config: PathsInstallationConfig
    ready_bytes: bytes
    cleanup_pending: bool = False


def _write_owned(path, raw):
    fd=None;primary=None
    try:
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        offset=0
        while offset<len(raw):
            count=os.write(fd,raw[offset:offset+65536])
            if count<=0:_refuse()
            offset+=count
    except BaseException as exc:primary=exc
    finally:
        if fd is not None:
            try:os.close(fd)
            except BaseException as exc:
                if primary is None:primary=exc
                else:_marker(primary)
    if primary is not None:raise primary


def _resource_paths():
    return ('backend-manifest.json',) + tuple('schemas/' + name for name in INSTALLED_SCHEMAS)


def _verify(config, ready_raw, *, ready_present):
    index_raw, entry = verify_trusted_index(config)
    ready = decode_document(ready_raw)
    if type(ready) is not dict or set(ready) != {'format','indexRevision','indexSha256','realizationId','target','observedOS','executable','provenance','resources'}: _refuse()
    if ready['format'] != 'ashlar-weft-paths-ready/0.1' or ready['indexRevision'] != config.index_revision or ready['indexSha256'] != config.index_sha256 or ready['realizationId'] != config.realization_id or ready['target'] != config.observed_target or ready['observedOS'] != config.observed_os: _refuse()
    expected_exe = {**entry['executable'], 'path':'weft-paths'}
    if ready['executable'] != expected_exe: _refuse()
    binary = read_snapshot(config.output/'weft-paths')
    if len(binary) != expected_exe['bytes'] or _sha(binary) != expected_exe['sha256'] or (config.output/'weft-paths').stat().st_mode & 0o777 != 0o555: _refuse()
    provenance_raw = read_snapshot(config.output/'provenance.json', JSON_LIMIT)
    if ready['provenance'] != {'path':'provenance.json','sha256':_sha(provenance_raw),'bytes':len(provenance_raw)}: _refuse()
    provenance = decode_document(provenance_raw)
    if type(provenance) is not dict or set(provenance) != {'format','indexRevision','indexSha256','realizationId','manifestHex','custodyHex','resources','qualification'}: _refuse()
    if provenance['format'] != 'ashlar-weft-paths-provenance/0.1' or provenance['indexRevision'] != config.index_revision or provenance['indexSha256'] != config.index_sha256 or provenance['realizationId'] != config.realization_id: _refuse()
    manifest_raw = bytes.fromhex(provenance['manifestHex']); custody_raw = bytes.fromhex(provenance['custodyHex'])
    for raw, desc in ((manifest_raw,entry['manifest']),(custody_raw,entry['assemblyCustody'])):
        if len(raw) != desc['bytes'] or _sha(raw) != desc['sha256']: _refuse()
    manifest = decode_document(manifest_raw); validate_manifest(manifest)
    if manifest['realizationId'] != config.realization_id or manifest['build']['platform']['observedOS'] != config.observed_os or manifest['executable'] != entry['executable']: _refuse()
    custody = decode_document(custody_raw)
    artifacts = {d['path']:d for d in custody['artifacts']}
    expected_resources = []
    for name in _resource_paths():
        original = manifest['backendManifests'][0]['path'] if name == 'backend-manifest.json' else SCHEMA_BASE + name[len('schemas/'):]
        expected_resources.append({**artifacts[original], 'path':name})
    if ready['resources'] != expected_resources or provenance['resources'] != expected_resources: _refuse()
    for desc in expected_resources:
        raw = read_snapshot(config.output/desc['path'], JSON_LIMIT)
        if len(raw) != desc['bytes'] or _sha(raw) != desc['sha256']: _refuse()
    verify_exact_tree(config.output, ('weft-paths','provenance.json') + _resource_paths() + (('ready.json',) if ready_present else ()))
    if read_snapshot(config.index_path, JSON_LIMIT) != index_raw: _refuse()
    return PathsInstallation(config, ready_raw)


def open_installation(config: PathsInstallationConfig) -> PathsInstallation:
    """Trust gate precedes caller-controlled installed files; package may be None."""
    try:
        verify_trusted_index(config)
        return _verify(config, read_snapshot(config.output/'ready.json', JSON_LIMIT), ready_present=True)
    except (OSError, ValueError, TypeError, KeyError, RecursionError): _refuse()


def install(config: PathsInstallationConfig) -> PathsInstallation:
    """Reverify all package bytes; final ready publication commits availability."""
    verified = inspect_package(config)
    if config.output.exists() or config.output.is_symlink() or any(p.is_symlink() for p in config.output.parents): _refuse()
    staging = Path(tempfile.mkdtemp(prefix='.ashlar-paths-', dir=config.output.parent))
    primary = None; owned = None; committed = False; result = None
    try:
        resources = tuple(verified.resources)
        files = (('weft-paths',verified.executable_bytes),('provenance.json',verified.provenance_bytes)) + resources
        for name,raw in files:
            path = staging/name; path.parent.mkdir(parents=True,exist_ok=True)
            _write_owned(path,raw)
            path.chmod(0o555 if name == 'weft-paths' else 0o444)
            if read_snapshot(path) != raw: _refuse()
        ready = {'format':'ashlar-weft-paths-ready/0.1','indexRevision':config.index_revision,'indexSha256':config.index_sha256,'realizationId':config.realization_id,'target':config.observed_target,'observedOS':config.observed_os,'executable':{'path':'weft-paths','sha256':_sha(verified.executable_bytes),'bytes':len(verified.executable_bytes)},'provenance':{'path':'provenance.json','sha256':_sha(verified.provenance_bytes),'bytes':len(verified.provenance_bytes)},'resources':[{'path':name,'sha256':_sha(raw),'bytes':len(raw)} for name,raw in resources]}
        ready_raw = encode_document(ready)+b'\n'
        _write_owned(staging/'ready.json',ready_raw)
        (staging/'ready.json').chmod(0o444)
        config.output.mkdir(exist_ok=False);info=config.output.stat();owned=(info.st_dev,info.st_ino)
        for name,raw in files:
            path=config.output/name;path.parent.mkdir(parents=True,exist_ok=True)
            _write_owned(path,raw)
            path.chmod(0o555 if name=='weft-paths' else 0o444)
            if read_snapshot(path)!=raw:_refuse()
        if read_snapshot(staging/'ready.json',JSON_LIMIT)!=ready_raw:_refuse()
        result=_verify(config,ready_raw,ready_present=False)
        os.link(staging/'ready.json',config.output/'ready.json');committed=True
    except BaseException as exc:primary=exc
    if not committed and owned is not None:
        try:
            info=config.output.stat()
            if not config.output.is_symlink() and (info.st_dev,info.st_ino)==owned:shutil.rmtree(config.output)
        except BaseException:
            if primary is not None:_marker(primary)
    cleanup_pending=False
    try:shutil.rmtree(staging)
    except BaseException as exc:
        if committed and isinstance(exc,OSError):cleanup_pending=True
        elif committed:primary=exc
        elif primary is None:primary=exc
        else:_marker(primary)
    if primary is not None:raise primary
    return replace(result,cleanup_pending=cleanup_pending)


def installed_schema_bundle(installation: PathsInstallation) -> tuple[tuple[str, bytes], ...]:
    """Owned exact public schemas; caller still owns actual offline validation."""
    opening=open_installation(installation.config)
    if opening.ready_bytes!=installation.ready_bytes:_refuse()
    result=tuple((name,read_snapshot(installation.config.output/'schemas'/name,JSON_LIMIT)) for name in INSTALLED_SCHEMAS)
    if open_installation(installation.config).ready_bytes!=opening.ready_bytes:_refuse()
    return result


def compile_request(installation: PathsInstallation, request: bytes) -> bytes:
    """Bounded original string transport, no compiler result/native authority."""
    if not isinstance(installation,PathsInstallation) or type(request)is not bytes or len(request)>PROTOCOL_LIMIT:_refuse()
    opening=open_installation(installation.config)
    if opening.ready_bytes!=installation.ready_bytes:_refuse()
    process=None;selector=None;primary=None;streams=[];stdout=bytearray();stderr=bytearray();deadline=time.monotonic()+30
    try:
        selector=selectors.DefaultSelector()
        process=subprocess.Popen([str(installation.config.output/'weft-paths')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True,env={'PATH':'/usr/bin:/bin'})
        streams=[process.stdout,process.stderr,process.stdin]
        for stream,label in ((process.stdout,'out'),(process.stderr,'err')):
            os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,label)
        os.set_blocking(process.stdin.fileno(),False)
        offset=0
        if request:selector.register(process.stdin,selectors.EVENT_WRITE,'in')
        else:process.stdin.close()
        while selector.get_map():
            if time.monotonic()>=deadline:_refuse()
            for key,_ in selector.select(min(0.1,max(0,deadline-time.monotonic()))):
                stream=key.fileobj;label=key.data
                if label=='in':
                    try:count=os.write(stream.fileno(),request[offset:offset+65536])
                    except BlockingIOError:continue
                    offset+=count
                    if offset==len(request):selector.unregister(stream);stream.close()
                else:
                    target=stdout if label=='out' else stderr;limit=PROTOCOL_LIMIT if label=='out' else 4096
                    try:chunk=os.read(stream.fileno(),min(65536,limit+1-len(target)))
                    except BlockingIOError:continue
                    if not chunk:selector.unregister(stream);stream.close()
                    else:
                        target.extend(chunk)
                        if len(target)>limit:_refuse()
        code=process.wait(timeout=max(0.001,deadline-time.monotonic()))
        if code!=0 or stderr or not stdout.endswith(b'\n') or stdout.count(b'\n')!=1:_refuse()
        response=decode_document(bytes(stdout))
        if type(response)is not dict or response.get('interfaceVersion')!='weft-compile/0.4.0' or response.get('status')not in ('compiled','blocked'):_refuse()
        if open_installation(installation.config).ready_bytes!=opening.ready_bytes:_refuse()
    except BaseException as exc:primary=exc
    finally:
        callbacks=[]
        if process is not None:
            def terminate():
                try:os.killpg(process.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                process.wait(timeout=2)
            callbacks.append(terminate)
        if selector is not None:callbacks.append(selector.close)
        callbacks.extend(stream.close for stream in streams if not stream.closed)
        for callback in callbacks:
            try:callback()
            except BaseException as exc:
                if primary is None:primary=exc
                else:_marker(primary)
    if primary is not None:raise primary
    return bytes(stdout)

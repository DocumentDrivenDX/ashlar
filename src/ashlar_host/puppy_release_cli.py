"""Explicit installed activation/read handles under trusted operator policy.

Operator code is explicitly SHA-selected, not sandboxed. Durable original intent
precedes its execution; the provider owns current native/source authority.
Receipts select exact original handles, never a latest alias or repair grant.
"""
from contextlib import AbstractContextManager
from dataclasses import dataclass, asdict
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from types import ModuleType
from typing import Protocol
from ashlar.graph_release import GraphRelease
from .evolution_cli import InvocationFile, MAX_CONFIG, MAX_PROVIDER
from .lifecycle import finish, owned_context
from .puppy_release import PuppyReleaseHandle, PuppyReleaseActivation, PuppyReleaseOwner, PuppyReleaseError
from .puppy_native import PuppyNativeReleaseAdapter, decode, encoded

PROFILE = 'ashlar-puppy-invocation/0.1'
SELECTED = 'ashlar-puppy-selected-release/0.1'
MAX_RELEASE = 16 * 1024 * 1024


def digest(value):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise PuppyReleaseError('Exact externally trusted SHA256 required')
    return value


def absolute(value):
    if type(value) is not str or not 0 < len(value) <= 4096 or not Path(value).is_absolute():
        raise PuppyReleaseError('Explicit absolute original file path required')
    return Path(value)


@dataclass(frozen=True)
class PuppyInvocation:
    mode: str
    release_path: Path
    release_sha256: str
    output_directory: Path
    activation_path: Path = None
    activation_sha256: str = None

    def __post_init__(self):
        if type(self.mode) is not str or self.mode not in ('activate','read'):
            raise PuppyReleaseError('Explicit original operation required')
        for path in (self.release_path, self.output_directory):
            if type(path) is not type(Path('/')) or not path.is_absolute():
                raise PuppyReleaseError('Explicit original operation paths required')
        digest(self.release_sha256)
        if self.mode == 'read':
            if type(self.activation_path) is not type(Path('/')) or not self.activation_path.is_absolute():
                raise PuppyReleaseError('Explicit selected activation required')
            digest(self.activation_sha256)
        elif self.activation_path is not None or self.activation_sha256 is not None:
            raise PuppyReleaseError('Fresh activation cannot substitute a selected handle')


@dataclass(frozen=True)
class PuppySession:
    adapter: PuppyNativeReleaseAdapter
    policy: object
    context: object

    def __post_init__(self):
        if (type(self.adapter) is not PuppyNativeReleaseAdapter
                or not all(callable(getattr(self.policy, name, None)) for name in ('writer','admit'))):
            raise PuppyReleaseError('Actual public adapter and independent policy required')


class PuppyOperatorProvider(Protocol):
    def open_puppy_release(self, invocation: PuppyInvocation, release: GraphRelease) -> AbstractContextManager:
        """Supply ordinary transports/current policy; close owned resources."""


def parse_invocation(raw, mode):
    value = decode(raw)
    required = {'profile','release_path','release_sha256','output_directory'}
    if mode == 'read': required |= {'activation_path','activation_sha256'}
    if type(value) is not dict or set(value) != required or value['profile'] != PROFILE:
        raise PuppyReleaseError('Closed original invocation required')
    return PuppyInvocation(mode, absolute(value['release_path']), digest(value['release_sha256']),
        absolute(value['output_directory']),
        absolute(value['activation_path']) if mode == 'read' else None,
        digest(value['activation_sha256']) if mode == 'read' else None)


def original_file(path, maximum):
    """Bounded release/receipt snapshot with original inode and named custody."""
    if type(path) is not type(Path('/')) or path.parent.resolve() != path.parent:
        raise PuppyReleaseError('Canonical original file required')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    primary = None; answer = None
    try:
        start = os.fstat(fd)
        if not stat.S_ISREG(start.st_mode) or not 0 < start.st_size <= maximum:
            raise PuppyReleaseError('Bounded original regular file required')
        chunks = []; count = 0
        while True:
            block = os.read(fd, min(65536, maximum + 1 - count))
            if not block: break
            count += len(block)
            if count > maximum: raise PuppyReleaseError('Original file exceeded bound')
            chunks.append(block)
        closing = os.fstat(fd); named = path.lstat()
        identity = (start.st_dev,start.st_ino,start.st_size,start.st_mtime_ns)
        if identity != (closing.st_dev,closing.st_ino,closing.st_size,closing.st_mtime_ns) or (
                start.st_dev,start.st_ino) != (named.st_dev,named.st_ino) or stat.S_ISLNK(named.st_mode):
            raise PuppyReleaseError('Original file custody changed')
        answer = (b''.join(chunks), identity)
    except BaseException as error: primary = error
    finish(primary, [lambda:os.close(fd)])
    return answer


def directory_identity(directory):
    info = directory.lstat()
    if (directory.parent.resolve() != directory.parent or stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022):
        raise PuppyReleaseError('Original private output directory required')
    return info.st_dev, info.st_ino


def retain(directory, name, raw, expected_identity):
    if directory_identity(directory) != expected_identity:
        raise PuppyReleaseError('Original output directory replaced')
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    stream = None; target = None; primary = None
    try:
        info = os.fstat(fd)
        if (info.st_dev,info.st_ino) != expected_identity:
            raise PuppyReleaseError('Original output directory replaced')
        target = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
        stream = os.fdopen(target, 'wb')
        if stream.write(raw) != len(raw): raise PuppyReleaseError('Incomplete owned receipt write')
        stream.flush(); os.fsync(stream.fileno()); os.fchmod(stream.fileno(), 0o444)
        os.fsync(fd)
        if directory_identity(directory) != expected_identity:
            raise PuppyReleaseError('Original output directory changed during retention')
    except BaseException as error: primary = error
    finish(primary, [lambda:stream.close() if stream is not None else os.close(target) if target is not None else None,lambda:os.close(fd)])


def load_provider(snapshot, expected_sha256):
    if type(snapshot) is not InvocationFile or hashlib.sha256(snapshot.raw).hexdigest() != digest(expected_sha256):
        raise PuppyReleaseError('Externally selected original provider changed')
    snapshot.__post_init__()
    module = ModuleType('ashlar_operator_puppy_' + expected_sha256)
    module.__file__ = str(snapshot.path)
    exec(compile(snapshot.raw, str(snapshot.path), 'exec'), module.__dict__)
    if not callable(getattr(module, 'open_puppy_release', None)):
        raise PuppyReleaseError('Explicit ordinary Puppy provider required')
    return module


def run_puppy_command(configuration, provider_path, provider_sha256, mode):
    config = InvocationFile.read(configuration, MAX_CONFIG)
    invocation = parse_invocation(config.raw, mode)
    provider_file = InvocationFile.read(provider_path, MAX_PROVIDER)
    if hashlib.sha256(provider_file.raw).hexdigest() != digest(provider_sha256):
        raise PuppyReleaseError('Externally selected provider changed')
    original = original_file(invocation.release_path, MAX_RELEASE)
    if hashlib.sha256(original[0]).hexdigest() != invocation.release_sha256:
        raise PuppyReleaseError('Original release bytes changed')
    release = GraphRelease(original[0], invocation.release_sha256)
    PuppyReleaseHandle(release)
    selected = None; selected_original = None
    if mode == 'read':
        selected_original = original_file(invocation.activation_path, 65536)
        if hashlib.sha256(selected_original[0]).hexdigest() != invocation.activation_sha256:
            raise PuppyReleaseError('Externally selected activation changed')
        receipt = decode(selected_original[0])
        if (type(receipt) is not dict or set(receipt) != {'profile','release_sha256','activation'}
                or receipt['profile'] != SELECTED or receipt['release_sha256'] != release.sha256
                or type(receipt['activation']) is not dict or set(receipt['activation']) != {
                    'release_sha256','engine_id','schema_sha256','carrier_sha256','catalog_visibility'}):
            raise PuppyReleaseError('Exact original selected handle required')
        selected = PuppyReleaseHandle(release, PuppyReleaseActivation(**receipt['activation']))
    directory = invocation.output_directory
    parent = directory.parent.stat()
    if (directory.parent.resolve() != directory.parent or parent.st_uid != os.getuid()
            or parent.st_mode & 0o022): raise PuppyReleaseError('Owned private output parent required')
    directory.mkdir(mode=0o700, exist_ok=False)
    output_identity = directory_identity(directory)
    intent = encoded({'profile':PROFILE,'mode':mode,'original_configuration_sha256':hashlib.sha256(config.raw).hexdigest(),
        'provider_sha256':provider_sha256,'release_sha256':release.sha256,
        'activation_sha256':invocation.activation_sha256})
    retain(directory, 'original-intent.json', intent, output_identity)
    primary = None; answer = None; observation = None
    try:
        provider = load_provider(provider_file, provider_sha256)
        with owned_context(provider.open_puppy_release(invocation, release)) as session:
            if type(session) is not PuppySession: raise PuppyReleaseError('Explicit original native session required')
            session.__post_init__()
            owner = PuppyReleaseOwner(session.adapter, session.policy)
            answer = owner.refresh(release, context=session.context) if mode == 'activate' else selected
            observation = owner.read(answer, context=session.context)
    except BaseException as error: primary = error
    callbacks = [lambda:provider_file.renew(MAX_PROVIDER),lambda:config.renew(MAX_CONFIG)]
    def renew_release():
        if original_file(invocation.release_path,MAX_RELEASE) != original:
            raise PuppyReleaseError('Original release replaced during invocation')
    callbacks.append(renew_release)
    def renew_output():
        if directory_identity(directory) != output_identity:
            raise PuppyReleaseError('Original output directory replaced during invocation')
    callbacks.append(renew_output)
    if selected_original is not None:
        def renew_selected():
            if original_file(invocation.activation_path,65536) != selected_original:
                raise PuppyReleaseError('Original selected handle replaced during read')
        callbacks.append(renew_selected)
    finish(primary, callbacks)
    if type(answer) is not PuppyReleaseHandle or type(observation) is not bytes:
        raise PuppyReleaseError('Closed native observation incomplete')
    receipt = {'profile':SELECTED,'release_sha256':release.sha256,'activation':asdict(answer.activation)}
    raw = encoded(receipt)
    retain(directory,'observation.json',observation,output_identity)
    retain(directory,'activation.json',raw,output_identity)
    return {'profile':SELECTED,'release_sha256':release.sha256,'activation_path':str(directory/'activation.json'),
        'activation_sha256':hashlib.sha256(raw).hexdigest(),'observation_path':str(directory/'observation.json')}

# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact task-private current Weft candidate package and bounded API calls.

This index is separate from the released 5dd/0.4.1 command distribution. It
binds the modified 0.4.48 wheel and every installed distribution file. The
candidate remains unqualified for native execution until the host obligations
and native run complete.
"""
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import stat

INDEX_SHA256 = '15e92b6a43e03797fbb924b29d035bb536654e4760a92e166f61d941c92bd1d2'
INDEX_LIMIT = 1_048_576
FILE_LIMIT = 32 * 1024 * 1024
REQUEST_LIMIT = 16 * 1024 * 1024
RESPONSE_LIMIT = 16 * 1024 * 1024


class CurrentWeftDistributionError(ValueError):
    """Payload-free current candidate integrity refusal."""


def _refuse():
    raise CurrentWeftDistributionError('Current Weft candidate refused')


def _read(path: Path, limit: int) -> bytes:
    if type(path) is not type(Path()) or not path.is_absolute() or any(p.is_symlink() for p in (path, *path.parents)):
        _refuse()
    fd = None
    primary = None
    raw = None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            _refuse()
        chunks = []
        remaining = limit + 1
        while remaining:
            chunk = os.read(fd, min(remaining, 65536))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b''.join(chunks)
        if len(raw) > limit:
            _refuse()
    except BaseException as error:
        primary = error
    if fd is not None:
        try:
            os.close(fd)
        except BaseException as close_error:
            if primary is None:
                primary = close_error
    if primary is not None:
        raise primary
    return raw


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(raw: bytes):
    def pairs(entries):
        result = {}
        for key, value in entries:
            if key in result:
                _refuse()
            result[key] = value
        return result
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs)
    except Exception:
        _refuse()


@dataclass(frozen=True)
class CurrentWeftDistributionPaths:
    index: Path

    def __post_init__(self):
        if type(self.index) is not type(Path()) or not self.index.is_absolute():
            _refuse()


class CurrentWeftDistribution:
    """Open, recheck and invoke only the exact current candidate wheel APIs."""
    def __init__(self, paths: CurrentWeftDistributionPaths):
        if type(paths) is not CurrentWeftDistributionPaths:
            _refuse()
        paths.__post_init__()
        self.paths = paths
        self.index_bytes = _read(paths.index, INDEX_LIMIT)
        if _sha(self.index_bytes) != INDEX_SHA256:
            _refuse()
        self.index = _json(self.index_bytes)
        self.module = importlib.import_module('weft')
        self._verify()

    def _verify(self):
        if _sha(_read(self.paths.index, INDEX_LIMIT)) != INDEX_SHA256:
            _refuse()
        dist = importlib.metadata.distribution('weft-sql')
        distribution = self.index['distribution']
        target = distribution['targetPlatform']
        if (dist.version != distribution['version'] or dist.version != '0.4.48'
                or platform.system() != target['os'] or platform.machine() != target['machine']):
            _refuse()
        files = distribution['installedFiles']
        if type(files) is not list or not files or len(files) > 64:
            _refuse()
        package_root = Path(distribution['installedPackageRoot'])
        observed = []
        total = 0
        for entry in files:
            if type(entry) is not dict or set(entry) != {'path', 'bytes', 'sha256'}:
                _refuse()
            relative = entry['path']
            if type(relative) is not str or relative.startswith('/') or any(p in ('', '.', '..') for p in relative.split('/')):
                _refuse()
            path = Path(dist.locate_file(relative)).resolve()
            raw = _read(path, FILE_LIMIT)
            if len(raw) != entry['bytes'] or _sha(raw) != entry['sha256']:
                _refuse()
            total += len(raw)
            if total > 96 * 1024 * 1024:
                _refuse()
            observed.append(relative)
        if len(set(observed)) != len(observed):
            _refuse()
        expected_files = set(observed)
        actual_files = set()
        for package in (package_root / 'weft', package_root / 'weft_sql-0.4.48.dist-info'):
            if not package.is_dir() or package.is_symlink():
                _refuse()
            for directory, directories, names in os.walk(package, followlinks=False):
                base = Path(directory)
                if any((base / name).is_symlink() for name in directories + names):
                    _refuse()
                actual_files.update((base / name).relative_to(package_root).as_posix() for name in names)
        if actual_files != expected_files:
            _refuse()
        record = distribution['record']
        if not any(entry['path'] == record['path'] and entry['bytes'] == record['bytes'] and entry['sha256'] == record['sha256'] for entry in files):
            _refuse()
        module_path = Path(self.module.__file__).resolve()
        extension_path = module_path.parent / 'weft.abi3.so'
        expected_module = package_root / distribution['moduleOrigin']
        expected_extension = package_root / distribution['extensionOrigin']
        if module_path != expected_module or extension_path != expected_extension:
            _refuse()
        if self.module.__version__ != dist.version or not all(callable(getattr(self.module, name, None)) for name in self.index['apis']):
            _refuse()
        wheel = distribution['wheel']
        wheel_raw = _read(Path(wheel['path']), FILE_LIMIT)
        if len(wheel_raw) != wheel['bytes'] or _sha(wheel_raw) != wheel['sha256']:
            _refuse()
        direct_url = dist.read_text('direct_url.json')
        if type(direct_url) is not str:
            _refuse()
        parsed_url = _json(direct_url.encode())
        if (parsed_url.get('archive_info', {}).get('hashes', {}).get('sha256') != wheel['sha256']
                or parsed_url.get('url') != 'file://' + wheel['path']):
            _refuse()
        return True

    @contextmanager
    def interval(self):
        opening = self._verify()
        primary = None
        try:
            yield self
        except BaseException as error:
            primary = error
        try:
            self._verify()
        except BaseException as close_error:
            if primary is None:
                primary = close_error
        if primary is not None:
            raise primary

    def _call(self, name: str, *args: str) -> str:
        self._verify()
        if name not in self.index['apis'] or any(type(arg) is not str or not 0 < len(arg.encode()) <= REQUEST_LIMIT for arg in args):
            _refuse()
        result = getattr(self.module, name)(*args)
        if type(result) is not str or not 0 < len(result.encode()) <= RESPONSE_LIMIT:
            _refuse()
        self._verify()
        return result

    def compile_paths_keys_json(self, request: str) -> str:
        return self._call('compile_paths_keys_json', request)

    def compile_engine_page_json(self, request: str) -> str:
        return self._call('compile_engine_page_json', request)

    def check_engine_page_json(self, original: str, request: str, after: str | None = None) -> str:
        return self._call('check_engine_page_json', original, request, after)

    def check_engine_read_json(self, original: str, request: str) -> str:
        return self._call('check_engine_read_json', original, request)

    def schema_bundle(self) -> dict[str, bytes]:
        self._verify()
        result = {}
        for entry in self.index['schemas']:
            raw = _read(Path(entry['path']), FILE_LIMIT)
            if len(raw) != entry['bytes'] or _sha(raw) != entry['sha256']:
                _refuse()
            result[entry['name']] = raw
        if len(result) != 3:
            _refuse()
        self._verify()
        return result

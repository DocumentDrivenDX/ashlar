"""Owned bounded original protocol retention; never execution authority.

All filenames/record positions come from the original five-plus-twelve inventory.
Binary payloads and the complete index share the existing eight MiB budget.
"""
import hashlib
import json
import os
import stat
from pathlib import Path

from .lifecycle import finish
from .supply_chain_request import CASE_IDS

FORMAT = 'binary-protocol/0.1'
AGGREGATE_BYTES = 8 * 1024 * 1024
VIEW_BYTES = 1024 * 1024
INDEX_NAME = 'index.jsonl'
INVENTORY = tuple((name, iteration) for name in CASE_IDS for iteration in (0, 1)) + tuple(
    ('two-hop-' + str(ordinal), iteration) for ordinal in range(12) for iteration in (0, 1))


def _identity(info):
    values = (info.st_dev, info.st_ino, info.st_uid, info.st_mode, info.st_nlink,
              info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    if any(type(value) is not int or value.bit_length() > 127 for value in values):
        raise ValueError('Bounded exact protocol file identity required')
    return values


def _encode(value):
    # Only closed known names, bounded lengths/digests and checked integer
    # identities reach this encoder. Reservation precedes acquiring pair files.
    return (json.dumps(value, ensure_ascii=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


class SupplyChainBinaryEvidence:
    def __init__(self, output):
        if (type(output) is not type(Path('/')) or output.resolve() != output
                or not output.is_dir() or output.stat().st_uid != os.getuid() or output.stat().st_mode & 0o077):
            raise ValueError('Canonical private protocol output required')
        self.root = output / 'compiler-evidence'
        self.files = []
        self.records = []
        self.binary_bytes = 0
        self.index_bytes = b''
        self.index = None
        self.directory_fd = None
        self.closed = False
        self.failed = False
        primary = None
        try:
            self.root.mkdir(mode=0o700)
            self.directory_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            self.directory_identity = _identity(os.fstat(self.directory_fd))[:4]
            self.parent_identity = _identity(output.stat())[:4]
            self._directory()
        except BaseException as error:
            primary = error
        if primary is not None:
            finish(primary, [lambda: os.close(self.directory_fd)] if self.directory_fd is not None else [])

    def _directory(self):
        if (self.root.resolve() != self.root or self.root.parent.resolve() != self.root.parent
                or _identity(os.fstat(self.directory_fd))[:4] != self.directory_identity
                or _identity(self.root.lstat())[:4] != self.directory_identity
                or _identity(self.root.parent.stat())[:4] != self.parent_identity
                or self.root.stat().st_uid != os.getuid() or self.root.stat().st_mode & 0o077):
            raise ValueError('Original private protocol directory required')
        expected = {entry['name'] for entry in self.files}
        if self.index is not None:
            expected.add(INDEX_NAME)
        observed = set()
        with os.scandir(self.root) as entries:
            for entry in entries:
                if entry.name not in expected or entry.name in observed or len(observed) >= 69:
                    raise ValueError('Exact protocol directory inventory required')
                observed.add(entry.name)
        if observed != expected:
            raise ValueError('Complete protocol directory inventory required')

    def _acquire(self, name, raw):
        fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.directory_fd)
        entry = {'name': name, 'fd': fd, 'bytes': len(raw),
                 'sha256': hashlib.sha256(raw).hexdigest(), 'identity': None}
        self.files.append(entry)
        info = os.fstat(fd)
        entry['identity'] = _identity(info)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1
                or info.st_mode & 0o777 != 0o600
                or _identity(os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)) != entry['identity']):
            raise ValueError('Exclusive held original protocol file required')
        self._directory()
        return entry

    def _write(self, entry, raw):
        count = 0
        while count < len(raw):
            written = os.write(entry['fd'], raw[count:count + 65536])
            if type(written) is not int or not 0 < written <= min(65536, len(raw) - count):
                raise ValueError('Complete original protocol write required')
            count += written
        os.fsync(entry['fd'])
        entry['identity'] = _identity(os.fstat(entry['fd']))

    def _verify(self, entry):
        path = self.root / entry['name']
        before = os.fstat(entry['fd'])
        if (path.resolve() != path or _identity(before) != entry['identity']
                or _identity(path.lstat()) != entry['identity'] or not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.getuid() or before.st_nlink != 1
                or before.st_mode & 0o777 != 0o600 or before.st_size != entry['bytes']):
            raise ValueError('Original held named protocol file required')
        digest = hashlib.sha256()
        count = 0
        while True:
            raw = os.pread(entry['fd'], min(65536, entry['bytes'] - count + 1), count)
            if not raw:
                break
            count += len(raw)
            if count > entry['bytes']:
                raise ValueError('Protocol file exceeds original bound')
            digest.update(raw)
        if (count != entry['bytes'] or digest.hexdigest() != entry['sha256']
                or _identity(os.fstat(entry['fd'])) != entry['identity']
                or _identity(path.lstat()) != entry['identity'] or path.resolve() != path):
            raise ValueError('Complete original protocol bytes changed')

    @staticmethod
    def _view(entry):
        return {key: list(entry[key]) if key == 'identity' else entry[key]
                for key in ('name', 'bytes', 'sha256', 'identity')}

    def retain(self, name, iteration, request, response):
        if (self.closed or self.failed or type(name) is not str or type(iteration) is not int
                or len(self.records) >= 34 or (name, iteration) != INVENTORY[len(self.records)]
                or any(type(raw) is not bytes or len(raw) > VIEW_BYTES for raw in (request, response))):
            raise ValueError('Exact original bounded protocol position required')
        self._directory()
        filenames = [name + '-' + str(iteration) + '-' + side + '.bin' for side in ('request', 'response')]
        def record(views):
            return {'format': FORMAT, 'kind': 'authored' if name in CASE_IDS else 'two-hop',
                    'ordinal': CASE_IDS.index(name) if name in CASE_IDS else int(name.split('-')[-1]),
                    'case': name, 'iteration': iteration,
                    'request': views[0], 'response': views[1]}
        # All eight identity members have at most forty serialized characters.
        # The fixed template is itself bounded; no unknown inode JSON is allocated
        # before reserving its COMPLETE serialized record in the aggregate.
        reservation = len(_encode(record([{'name': filename, 'bytes': len(raw),
            'sha256': 'f' * 64, 'identity': [-(2**127)] * 8}
            for filename, raw in zip(filenames, (request, response))])))
        if reservation > 4096 or self.binary_bytes + len(request) + len(response) + len(self.index_bytes) + reservation > AGGREGATE_BYTES:
            raise ValueError('Complete binary protocol pair/index exceeds aggregate capacity')
        primary = None
        try:
            pair = []
            for filename, raw in zip(filenames, (request, response)):
                entry = self._acquire(filename, raw)
                pair.append(entry)
                self._write(entry, raw)
            finish(None, [lambda entry=entry: self._verify(entry) for entry in pair] + [self._directory])
            raw = _encode(record([self._view(entry) for entry in pair]))
            if len(raw) > reservation:
                raise ValueError('Complete identity index exceeds reserved capacity')
            if self.index is None:
                self.index = self._acquire(INDEX_NAME, b'')
                self.files.remove(self.index)
            else:
                self._verify(self.index)
            if os.lseek(self.index['fd'], 0, os.SEEK_END) != len(self.index_bytes):
                raise ValueError('Original protocol index position required')
            self._write(self.index, raw)
            complete_index = self.index_bytes + raw
            self.index['bytes'] = len(complete_index)
            self.index['sha256'] = hashlib.sha256(complete_index).hexdigest()
            self._verify(self.index)
            self._directory()
            self.index_bytes = complete_index
            self.binary_bytes += len(request) + len(response)
            self.records.append((name, iteration))
        except BaseException as error:
            primary = error
            self.failed = True
        finish(primary, [])

    def _inventory(self):
        if self.failed or tuple(self.records) != INVENTORY or len(self.files) != 68 or self.index is None:
            raise ValueError('Complete thirty-four-record binary protocol inventory required')
        if self.binary_bytes + len(self.index_bytes) > AGGREGATE_BYTES:
            raise ValueError('Complete original binary/index aggregate required')

    def require_complete(self):
        self._inventory()
        finish(None, [lambda entry=entry: self._verify(entry) for entry in self.files]
               + [lambda: self._verify(self.index), self._directory])

    def reference(self):
        self.require_complete()
        return {'format': FORMAT, 'index': 'compiler-evidence/' + INDEX_NAME,
                'index_bytes': len(self.index_bytes), 'index_sha256': self.index['sha256'],
                'records': 34, 'files': 68, 'binary_bytes': self.binary_bytes,
                'aggregate_bytes': self.binary_bytes + len(self.index_bytes)}

    def close(self):
        if self.closed:
            raise ValueError('Protocol owner already closed')
        self.closed = True
        entries = tuple(self.files) + ((self.index,) if self.index is not None else ())
        finish(None, [self._inventory] + [lambda entry=entry: self._verify(entry) for entry in entries]
               + [self._directory]
               + [lambda entry=entry: os.close(entry['fd']) for entry in entries]
               + [lambda: os.close(self.directory_fd)])

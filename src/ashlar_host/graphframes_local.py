# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit trusted-host retained GraphFrames composition; no production grant.

Operator-selected original digests and a separately selected runtime-plan digest
are required. Inspection/setup cannot authorize their own generated inventory.
The cooperative file lease covers immutable export use, not canonical snapshots.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import fcntl
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import re
import stat
import sys

from ashlar.graph_release import GraphRelease
from .finite_dataset import FiniteDatasetConfig, FinitePackSource, HeldFiniteDataset
from .finite_pack import FinitePackDefinition
from .graphframes_cli import GraphFramesInvocation
from .graphframes_release import (GraphFramesOfflineSession, GraphFramesRuntimeFile,
                                 GraphFramesRuntimeInventory, VERSIONS)
from .lifecycle import finish, owned_context
from .private_graph_custody import load_private_graph_release
from .puppy_release_cli import original_file, directory_identity
from .supply_chain_request import source_snapshot

RUNTIME_PROFILE = 'ashlar-local-graphframes-runtime/0.1'
MAX_PLAN_BYTES = 16777216


def local_encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def local_digest(raw):
    return hashlib.sha256(raw).hexdigest()


def local_original_equal(supplied, expected):
    """Exact passive JSON spelling; executable equality is never admission."""
    return local_encoded(source_snapshot(supplied)) == local_encoded(source_snapshot(expected))


def selected_digest(value):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise ValueError('Explicit independently selected digest required')
    return value


def selected_path(value):
    if type(value) is not type(Path()) or not value.is_absolute() or len(str(value)) > 4096:
        raise ValueError('Exact absolute operator-selected path required')
    if any(p.is_symlink() for p in (value, *value.parents)):
        raise ValueError('Canonical operator-selected path required')
    return value


def runtime_tree(root):
    """Bounded complete names and canonical regular files, including bytecode."""
    selected_path(root)
    if not root.is_dir():
        raise ValueError('Existing selected runtime root required')
    names = []; files = {}; total = 0
    for path in sorted(root.rglob('*')):
        if len(names) >= 16384:
            raise ValueError('Runtime namespace bound exceeded')
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            target = path.resolve(strict=True)
            # A directory alias cannot introduce an uninspected import subtree.
            if root not in target.parents and target != root:
                raise ValueError('Runtime alias escapes selected closure root')
            names.append({'path': str(path), 'kind': 'link', 'target': os.readlink(path), 'resolved': str(target)})
        elif stat.S_ISDIR(info.st_mode):
            names.append({'path': str(path), 'kind': 'directory'})
        elif stat.S_ISREG(info.st_mode):
            names.append({'path': str(path), 'kind': 'file'})
        else:
            raise ValueError('Special runtime namespace member refused')
        if path.is_file():
            canonical = path.resolve(strict=True)
            size = canonical.stat().st_size
            if size > 134217728:
                raise ValueError('Runtime member bound exceeded')
            # original_file requires a nonempty file; zero-byte members remain pinned.
            if size:
                raw, _ = original_file(canonical, 134217728)
            else:
                fd = os.open(canonical, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                primary = None; raw = None
                try:
                    opened = os.fstat(fd); named = canonical.lstat()
                    if not stat.S_ISREG(opened.st_mode) or opened.st_size != 0 or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
                        raise ValueError('Empty runtime member changed')
                    raw = os.read(fd, 1)
                    if raw: raise ValueError('Empty runtime member changed')
                    closing = os.fstat(fd); named = canonical.lstat()
                    start = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
                    if start != (closing.st_dev, closing.st_ino, closing.st_size, closing.st_mtime_ns) or start != (named.st_dev, named.st_ino, named.st_size, named.st_mtime_ns) or not stat.S_ISREG(named.st_mode):
                        raise ValueError('Empty runtime member changed during closing')
                except BaseException as error: primary = error
                finish(primary, [lambda: os.close(fd)])
            record = {'path': str(canonical), 'sha256': local_digest(raw), 'size': len(raw)}
            if str(canonical) not in files:
                total += len(raw)
                if total > 1073741824 or len(files) >= 8192:
                    raise ValueError('Runtime closure bound exceeded')
                files[str(canonical)] = record
    return {'namespace': names, 'files': sorted(files.values(), key=lambda r: r['path'])}


def read_local_runtime_plan(path, expected_sha256):
    selected_path(path); selected_digest(expected_sha256)
    raw, identity = original_file(path, MAX_PLAN_BYTES)
    if local_digest(raw) != expected_sha256:
        raise PermissionError('Independently selected runtime plan differs')
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value: raise ValueError('Duplicate runtime plan member')
            value[key] = item
        return value
    def integer(token):
        if len(token) > 20: raise ValueError('Runtime plan integer bound exceeded')
        return int(token)
    value = source_snapshot(json.loads(raw, object_pairs_hook=pairs, parse_int=integer,
        parse_float=lambda _: (_ for _ in ()).throw(ValueError('Floating runtime plan refused')),
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite runtime plan refused'))))
    keys = {'profile', 'revision', 'python', 'prefix', 'base_prefix', 'sys_path',
            'worker_origin', 'sdk_root', 'roots', 'absent_import_paths', 'worker', 'sdks'}
    if type(value) is not dict or set(value) != keys or value['profile'] != RUNTIME_PROFILE or local_encoded(value) != raw:
        raise ValueError('Closed canonical runtime plan required')
    for name in ('python', 'prefix', 'base_prefix', 'worker_origin', 'sdk_root'):
        if type(value[name]) is not str: raise ValueError('Exact runtime selection required')
        selected_path(Path(value[name]))
    if type(value['sys_path']) is not list or not value['sys_path'] or any(type(p) is not str or not Path(p).is_absolute() for p in value['sys_path']):
        raise ValueError('Complete actual import path selection required')
    if type(value['roots']) is not dict or not 0 < len(value['roots']) <= 16:
        raise ValueError('Bounded complete runtime roots required')
    for root, tree in value['roots'].items():
        selected_path(Path(root))
        if type(tree) is not dict or set(tree) != {'namespace', 'files'} or type(tree['namespace']) is not list or type(tree['files']) is not list:
            raise ValueError('Complete runtime namespace/files required')
    roots = tuple(Path(root) for root in value['roots'])
    if type(value['absent_import_paths']) is not list or any(type(p) is not str or p not in value['sys_path'] for p in value['absent_import_paths']):
        raise ValueError('Exact absent import selection inventory required')
    for entry in value['sys_path']:
        path = Path(entry)
        if entry in value['absent_import_paths']:
            if path.exists() or path.is_symlink(): raise ValueError('Previously absent import selector appeared')
        elif not path.is_dir() or not any(path == root or root in path.parents for root in roots):
            raise ValueError('Every actual import directory must be covered by a whole root')
    if not any(Path(value['prefix']) == root or root in Path(value['prefix']).parents for root in roots):
        raise ValueError('Complete selected virtual environment namespace required')
    def records(rows):
        if type(rows) is not list: raise ValueError('Owned runtime records required')
        result = []
        for row in rows:
            if type(row) is not dict or set(row) != {'path', 'sha256', 'size'} or type(row['path']) is not str:
                raise ValueError('Exact runtime record required')
            result.append(GraphFramesRuntimeFile(Path(row['path']), row['sha256'], row['size']))
        return tuple(result)
    if type(value['sdks']) is not list or any(type(e) is not list or len(e) != 2 for e in value['sdks']):
        raise ValueError('Exact SDK group inventory required')
    inventory = GraphFramesRuntimeInventory(value['revision'], records(value['worker']),
        tuple((entry[0], records(entry[1])) for entry in value['sdks']))
    selected = {str(f.path): {'path': str(f.path), 'sha256': f.sha256, 'size': f.size}
                for f in inventory.worker + tuple(f for _, rows in inventory.sdks for f in rows)}
    trees = {r['path']: r for tree in value['roots'].values() for r in tree['files']}
    if not local_original_equal(selected, trees):
        raise ValueError('Every selected closure file must belong to the complete inventory')
    return raw, identity, value, inventory


@dataclass(frozen=True)
class RetainedGraphFramesLocalConfig:
    producer: FiniteDatasetConfig
    model: Path
    model_sha256: str
    graph: Path
    graph_sha256: str
    pack: Path
    pack_sha256: str
    release: Path
    release_sha256: str
    custody: Path
    custody_sha256: str
    ack_scope_bytes: bytes
    runtime_plan: Path
    runtime_plan_sha256: str
    lease: Path

    def __post_init__(self):
        if type(self.producer) is not FiniteDatasetConfig:
            raise ValueError('Actual public producer configuration required')
        self.producer.__post_init__()
        if self.producer.definition.name not in ('archaeology', 'ecology'):
            raise ValueError('Original GraphFrames pack required')
        paths = (self.model, self.graph, self.pack, self.release, self.custody, self.runtime_plan, self.lease)
        for path in paths: selected_path(path)
        if len(set(paths)) != len(paths): raise ValueError('Distinct original and lease paths required')
        for digest in (self.model_sha256, self.graph_sha256, self.pack_sha256,
                       self.release_sha256, self.custody_sha256, self.runtime_plan_sha256): selected_digest(digest)
        if type(self.ack_scope_bytes) is not bytes or not 0 < len(self.ack_scope_bytes) <= 16384:
            raise ValueError('Explicit original protected ACK scope required')
        scope = json.loads(self.ack_scope_bytes)
        if type(scope) is not dict or set(scope) != {'service_schema', 'scope_id', 'installation_id', 'consumer', 'feed', 'epoch'} or any(type(v) is not str or not v or len(v) > 1024 for v in scope.values()) or local_encoded(scope) != self.ack_scope_bytes:
            raise ValueError('Exact canonical protected ACK scope required')


@dataclass(frozen=True)
class GraphFramesLocalRuntimeSelection:
    """Runtime-only selection; never an export or publication admission."""
    producer_source: Path
    runtime_plan: Path
    runtime_plan_sha256: str
    def __post_init__(self):
        for path in (self.producer_source,self.runtime_plan):selected_path(path)
        selected_digest(self.runtime_plan_sha256)
        if not self.producer_source.is_dir()or not self.runtime_plan.is_file():
            raise ValueError('Existing explicit source/runtime selection required')


class LocalGraphFramesRuntimePolicy:
    def __init__(self, config):
        if type(config) not in (RetainedGraphFramesLocalConfig,GraphFramesLocalRuntimeSelection):
            raise ValueError('Exact immutable local runtime configuration required')
        config.__post_init__()
        self.config = config
        self.raw, self.identity, _, self.inventory = read_local_runtime_plan(config.runtime_plan, config.runtime_plan_sha256)

    @property
    def plan(self):
        return json.loads(self.raw)

    def admit_owned_outputs(self, paths):
        if type(paths) is not tuple: raise ValueError('Immutable explicit owned outputs required')
        source=self.config.producer_source if type(self.config)is GraphFramesLocalRuntimeSelection else self.config.producer.source
        protected = (source,) + tuple(Path(p) for p in self.plan['roots'])
        for path in paths:
            selected_path(path)
            if any(path == root or root in path.parents for root in protected):
                raise PermissionError('Outputs must not modify selected producer or runtime roots')

    def renew_graphframes_runtime(self, inventory, runtime, context):
        from .graphframes_release import GraphFramesRuntimeConfig
        if type(runtime) is not GraphFramesRuntimeConfig:
            raise ValueError('Exact selected runtime configuration required')
        runtime.__post_init__()
        if context is not self or inventory is not self.inventory:
            raise PermissionError('Original admitted local runtime context required')
        raw, identity = original_file(self.config.runtime_plan, MAX_PLAN_BYTES)
        if raw != self.raw or identity != self.identity:
            raise PermissionError('Original runtime authority selection changed')
        plan = self.plan
        roots = tuple(Path(p) for p in plan['roots'])
        for selected in (runtime.java_home, runtime.jars):
            if not any(selected == root or root in selected.parents for root in roots):
                raise PermissionError('Whole selected Java and JAR namespaces must be admitted')
        if sys.flags.isolated != 1 or str(Path(sys.executable)) != plan['python'] or str(Path(sys.prefix)) != plan['prefix'] or str(Path(sys.base_prefix)) != plan['base_prefix'] or sys.prefix == sys.base_prefix:
            raise PermissionError('Actual separately installed isolated interpreter required')
        if [str(Path(p).resolve()) for p in sys.path if p] != plan['sys_path']:
            raise PermissionError('Actual selected import path changed')
        if any(Path(p).exists() or Path(p).is_symlink() for p in plan['absent_import_paths']):
            raise PermissionError('Unselected import path became available')
        for root, expected in plan['roots'].items():
            if not local_original_equal(runtime_tree(Path(root)), expected):
                raise PermissionError('Complete runtime namespace or bytecode changed')
        sdk = Path(plan['sdk_root'])
        for distribution_name, package in (('pyspark', 'pyspark'), ('delta-spark', 'delta'), ('graphframes-py', 'graphframes')):
            distribution = importlib.metadata.distribution(distribution_name)
            spec = importlib.util.find_spec(package)
            if distribution.version != VERSIONS[distribution_name] or Path(distribution.locate_file('')).resolve() != sdk or spec is None or str(Path(spec.origin).resolve()) != str(sdk / package / '__init__.py'):
                raise PermissionError('Current selected SDK metadata/resolution differs')
        worker = importlib.util.find_spec('ashlar_host.graphframes_release')
        if worker is None or worker.origin != plan['worker_origin']:
            raise PermissionError('Actual installed worker resolution differs')
        for hook in ('sitecustomize', 'usercustomize'):
            if importlib.util.find_spec(hook) is not None:
                raise PermissionError('Unselected startup hook refused')
        if runtime.python != Path(plan['python']): raise PermissionError('Invocation selected another interpreter')
        self.inventory.verify(runtime)
        runtime.renew()

    def admit_graphframes_runtime(self, runtime, context):
        self.renew_graphframes_runtime(self.inventory, runtime, context)
        return self.inventory


class LocalRetainedExportPolicy:
    def __init__(self, config, invocation, release, custody):
        if type(config) is not RetainedGraphFramesLocalConfig:
            raise ValueError('Exact immutable retained configuration required')
        config.__post_init__()
        if type(invocation) is not GraphFramesInvocation or invocation.mode != 'protected-ack-export':
            raise PermissionError('Explicit retained export mode required')
        invocation.__post_init__()
        if type(release) is not GraphRelease or type(custody) is not bytes or (invocation.pack, invocation.release, invocation.custody, invocation.release_sha256, invocation.custody_sha256) != (config.pack, config.release, config.custody, config.release_sha256, config.custody_sha256):
            raise PermissionError('Invocation differs from independently selected original scope')
        self.config = config; self.invocation = invocation; self.release = release; self.custody = custody
        self.fd = None; self.held = False
        self.parent_identity = directory_identity(config.lease.parent)
        self.originals = []
        for path, digest, bound in ((config.model, config.model_sha256, 1048576), (config.graph, config.graph_sha256, 1048576), (config.pack, config.pack_sha256, 1048576), (config.release, config.release_sha256, 16777216), (config.custody, config.custody_sha256, 16777216)):
            raw, identity = original_file(path, bound)
            if local_digest(raw) != digest: raise PermissionError('Independently admitted original file differs')
            self.originals.append((path, raw, identity, bound))
        self.originals = tuple(self.originals)
        self.facts = local_encoded(config.producer.definition.inputs(self.originals[0][1], self.originals[1][1], self.originals[2][1]))
        self.validate_original()

    def validate_original(self):
        for path, raw, identity, bound in self.originals:
            current, current_identity = original_file(path, bound)
            if current != raw or current_identity != identity:
                raise PermissionError('Original retained source/export changed')
        if self.release.payload != self.originals[3][1] or self.custody != self.originals[4][1]:
            raise PermissionError('Exact original supplied release/custody required')
        loaded = load_private_graph_release(self.release.payload, self.config.release_sha256, self.custody, self.config.custody_sha256)
        if local_encoded(json.loads(self.custody)['ack']['scope']) != self.config.ack_scope_bytes:
            raise PermissionError('Original protected ACK scope differs from current grant')
        # Validation above binds opening+closing observations, manifest and full vector.
        if loaded['publication'] is None: raise PermissionError('Original complete publication required')

    def renew(self):
        self.config.__post_init__()
        if not self.held or self.fd is None or directory_identity(self.config.lease.parent) != self.parent_identity:
            raise PermissionError('Current independently held retained-use lease required')
        named = self.config.lease.lstat(); opened = os.fstat(self.fd)
        if not stat.S_ISREG(named.st_mode) or named.st_uid != os.getuid() or stat.S_IMODE(named.st_mode) != 0o600 or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
            raise PermissionError('Original retained-use lease changed')
        self.validate_original()

    @contextmanager
    def hold(self):
        if self.fd is not None: raise PermissionError('Nonreentrant owned lease required')
        fd = os.open(self.config.lease, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        self.fd = fd; primary = None
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.held = True; self.renew(); yield
        except BaseException as error: primary = error
        def unlock():
            self.held = False; fcntl.flock(fd, fcntl.LOCK_UN)
        def close():
            self.fd = None; os.close(fd)
        def unlink_owned():
            if directory_identity(self.config.lease.parent) != self.parent_identity:
                raise PermissionError('Lease parent changed; do not remove another owner')
            named = self.config.lease.lstat(); opened = os.fstat(fd)
            if not stat.S_ISREG(named.st_mode) or named.st_uid != os.getuid() or stat.S_IMODE(named.st_mode) != 0o600 or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
                raise PermissionError('Lease changed; do not remove another owner')
            self.config.lease.unlink()
        finish(primary, [self.renew, unlock, unlink_owned, close])

    @contextmanager
    def writer(self, context):
        if context is not self: raise PermissionError('Exact current source context required')
        self.renew(); primary = None
        try: yield
        except BaseException as error: primary = error
        finish(primary, [self.renew])

    def admit_source(self, facts, context):
        self.renew()
        from types import MappingProxyType
        if type(facts) not in (dict, MappingProxyType):
            raise PermissionError('Exact passive owned source facts required')
        if context is not self or not local_original_equal(dict(facts), json.loads(self.facts)):
            raise PermissionError('Original independently admitted source differs')

    def admit_original_export(self, release, custody, source, context):
        self.renew()
        if context is not self or type(source) is not FinitePackSource or type(release) is not GraphRelease or release.payload != self.release.payload or custody != self.custody or source.model != self.originals[0][1] or source.graph != self.originals[1][1]:
            raise PermissionError('Original source and protected export admission differ')


@contextmanager
def open_retained_graphframes(config, invocation, release, custody_bytes):
    """Own source and runtime custody through the existing installed engine boundary."""
    if type(config) is not RetainedGraphFramesLocalConfig:
        raise ValueError('Exact immutable local retained configuration required')
    config.__post_init__()
    policy = LocalRetainedExportPolicy(config, invocation, release, custody_bytes)
    runtime = LocalGraphFramesRuntimePolicy(config)
    runtime.admit_owned_outputs((config.lease, invocation.output))
    with owned_context(policy.hold()):
        runtime.admit_graphframes_runtime(invocation.runtime, runtime)
        producer = HeldFiniteDataset(config.producer, config.model, config.graph, invocation.output / 'public-dataset.json')
        source = FinitePackSource(config.producer.definition, config.model, config.graph, producer, policy=policy, context=policy)
        primary = None
        try:
            yield GraphFramesOfflineSession(release, custody_bytes, config.custody_sha256, source, policy, policy, runtime, runtime)
        except BaseException as error: primary = error
        finish(primary, [source.renew, lambda: runtime.renew_graphframes_runtime(runtime.inventory, invocation.runtime, runtime), policy.renew])

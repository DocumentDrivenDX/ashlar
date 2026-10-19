# SPDX-License-Identifier: MIT OR Apache-2.0
"""Nonexecuting retained-fixture setup using explicitly selected existing runtimes.

Inspection emits a candidate, not a grant. Setup requires the operator's separate
runtime-plan selection and original source/export/ACK grant. No SDK or engine is
started, installed or downloaded; generated adapter code uses only public owners.
"""
from pathlib import Path
import importlib.util
import json
import re
import sys

from ashlar.graph_release import GraphRelease
from .finite_dataset import FiniteDatasetConfig
from .finite_pack import FinitePackDefinition
from .graphframes_cli import GraphFramesInvocation, parse_graphframes_invocation
from .graphframes_release import GraphFramesRuntimeConfig
from .graphframes_local import (RUNTIME_PROFILE, MAX_PLAN_BYTES,
    RetainedGraphFramesLocalConfig, LocalRetainedExportPolicy,
    LocalGraphFramesRuntimePolicy, local_encoded, local_digest, selected_path,
    runtime_tree, read_local_runtime_plan)
from .puppy_release_cli import original_file, directory_identity, retain
from .supply_chain_request import source_snapshot

LOCAL_PROFILE = 'ashlar-retained-graphframes-local/0.1'


def inspect_local_graphframes_runtime(worker_revision, worker_roots, sdk_root, transitive_roots):
    """Return a candidate whole inventory of the CURRENT selected interpreter.

The caller explicitly selects all import roots, then independently admits the
returned bytes before using setup. This call cannot mint runtime authority.
    """
    if type(worker_revision) is not str or re.fullmatch('[0-9a-f]{40}', worker_revision) is None:
        raise ValueError('Explicit selected worker revision required')
    if type(worker_roots) is not tuple or not worker_roots or type(transitive_roots) is not tuple or not transitive_roots:
        raise ValueError('Explicit immutable complete import roots required')
    roots = worker_roots + (sdk_root,) + transitive_roots
    for root in roots: selected_path(root)
    if len(roots) > 16 or len(set(roots)) != len(roots):
        raise ValueError('Distinct bounded actual import roots required')
    import_paths = [str(Path(p).resolve()) for p in sys.path if p]
    absent = []
    for entry in import_paths:
        path = Path(entry)
        if not path.exists() and not path.is_symlink(): absent.append(entry)
        elif not path.is_dir() or not any(path == root or root in path.parents for root in roots):
            raise ValueError('Complete actual import roots, including virtual environment, required')
    if not any(Path(sys.prefix) == root or root in Path(sys.prefix).parents for root in roots):
        raise ValueError('Complete selected virtual environment root required')
    trees = {str(root): runtime_tree(root) for root in roots}
    worker = {r['path']: r for root in worker_roots for r in trees[str(root)]['files']
              if sdk_root not in Path(r['path']).parents}
    groups = {name: [] for name in ('pyspark', 'delta-spark', 'graphframes-py', 'transitive-imports')}
    prefixes = {'pyspark': ('pyspark', 'pyspark-'), 'delta-spark': ('delta', 'delta_spark-'), 'graphframes-py': ('graphframes', 'graphframes_py-')}
    seen = set(worker)
    for root in (sdk_root,) + transitive_roots:
        for record in trees[str(root)]['files']:
            if record['path'] in seen: continue
            seen.add(record['path']); path = Path(record['path'])
            name = 'transitive-imports'
            if sdk_root in path.parents:
                head = path.relative_to(sdk_root).parts[0]
                for candidate, (package, metadata) in prefixes.items():
                    if head == package or head.startswith(metadata): name = candidate; break
            groups[name].append(record)
    spec = importlib.util.find_spec('ashlar_host.graphframes_release')
    if spec is None or spec.origin is None: raise ValueError('Actual installed worker selection required')
    value = {'profile': RUNTIME_PROFILE, 'revision': worker_revision,
        'python': str(Path(sys.executable)), 'prefix': str(Path(sys.prefix)),
        'base_prefix': str(Path(sys.base_prefix)),
        'sys_path': import_paths, 'absent_import_paths': absent,
        'worker_origin': spec.origin, 'sdk_root': str(sdk_root), 'roots': trees,
        'worker': sorted(worker.values(), key=lambda r: r['path']),
        'sdks': [[name, sorted(rows, key=lambda r: r['path'])] for name, rows in groups.items()]}
    raw = local_encoded(value)
    if len(raw) > MAX_PLAN_BYTES: raise ValueError('Complete candidate runtime plan exceeds bound')
    # No admission occurs here; selected public inventory shapes still must fit.
    from .graphframes_release import GraphFramesRuntimeFile, GraphFramesRuntimeInventory
    def records(rows): return tuple(GraphFramesRuntimeFile(Path(r['path']), r['sha256'], r['size']) for r in rows)
    GraphFramesRuntimeInventory(worker_revision, records(value['worker']), tuple((name, records(rows)) for name, rows in value['sdks']))
    return raw


def local_configuration_bytes(config):
    if type(config) is not RetainedGraphFramesLocalConfig: raise ValueError('Exact local config required')
    config.__post_init__()
    producer = config.producer
    value = {'profile': LOCAL_PROFILE, 'producer': {'pack': producer.definition.name,
        'source': str(producer.source), 'bun': str(producer.bun), 'git': str(producer.git),
        'timeout_seconds': producer.timeout_seconds, 'maximum_output_bytes': producer.maximum_output_bytes,
        'maximum_receipt_bytes': producer.maximum_receipt_bytes},
        'ack_scope': json.loads(config.ack_scope_bytes)}
    for name in ('model', 'graph', 'pack', 'release', 'custody', 'runtime_plan'):
        value[name] = str(getattr(config, name)); value[name + '_sha256'] = getattr(config, name + '_sha256')
    value['lease'] = str(config.lease)
    return local_encoded(value)


def parse_local_configuration(raw):
    if type(raw) is not bytes or not 0 < len(raw) <= 65536: raise ValueError('Bounded local config required')
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value: raise ValueError('Duplicate local config key')
            value[key] = item
        return value
    def integer(token):
        if len(token) > 8: raise ValueError('Local config integer bound exceeded')
        return int(token)
    value = source_snapshot(json.loads(raw, object_pairs_hook=pairs, parse_int=integer,
        parse_float=lambda _: (_ for _ in ()).throw(ValueError('Floating local setting refused')),
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite local setting refused'))))
    paths = ('model', 'graph', 'pack', 'release', 'custody', 'runtime_plan')
    expected = {'profile', 'producer', 'ack_scope', 'lease'} | set(paths) | {p + '_sha256' for p in paths}
    if type(value) is not dict or set(value) != expected or value['profile'] != LOCAL_PROFILE:
        raise ValueError('Closed retained-only local settings required')
    producer = value['producer']; fields = {'pack', 'source', 'bun', 'git', 'timeout_seconds', 'maximum_output_bytes', 'maximum_receipt_bytes'}
    if type(producer) is not dict or set(producer) != fields: raise ValueError('Closed public producer settings required')
    if any(type(value[p]) is not str for p in (*paths, 'lease')) or any(type(producer[p]) is not str for p in ('pack', 'source', 'bun', 'git')):
        raise ValueError('Exact operator string settings required')
    source = FiniteDatasetConfig(FinitePackDefinition(producer['pack']), Path(producer['source']), Path(producer['bun']), Path(producer['git']), producer['timeout_seconds'], producer['maximum_output_bytes'], producer['maximum_receipt_bytes'])
    config = RetainedGraphFramesLocalConfig(source, Path(value['model']), value['model_sha256'],
        Path(value['graph']), value['graph_sha256'], Path(value['pack']), value['pack_sha256'],
        Path(value['release']), value['release_sha256'], Path(value['custody']), value['custody_sha256'],
        local_encoded(value['ack_scope']), Path(value['runtime_plan']), value['runtime_plan_sha256'], Path(value['lease']))
    return config


def open_local_graphframes_configuration(configuration, configuration_sha256, invocation, release, custody_bytes):
    """Adapter entry: exact independently selected config bytes, no ambient lookup."""
    from .graphframes_local import selected_digest, open_retained_graphframes
    selected_digest(configuration_sha256); selected_path(configuration)
    raw, identity = original_file(configuration, 65536)
    if local_digest(raw) != configuration_sha256: raise PermissionError('Selected local operator configuration changed')
    config = parse_local_configuration(raw)
    from contextlib import contextmanager
    from .lifecycle import finish, owned_context
    @contextmanager
    def owner():
        with owned_context(open_retained_graphframes(config, invocation, release, custody_bytes)) as session:
            primary = None
            try: yield session
            except BaseException as error: primary = error
            def renew():
                current, current_identity = original_file(configuration, 65536)
                if current != raw or current_identity != identity: raise PermissionError('Original local operator config changed')
            finish(primary, [renew])
    return owner()


def setup_retained_graphframes(config, runtime, setup_directory, output):
    """Admit explicit existing settings, then write fresh closed operator artifacts.

No public producer or native engine is executed. The separately selected runtime
plan must already exist; generated digests are byte selection, not source grants.
    """
    if type(config) is not RetainedGraphFramesLocalConfig or type(runtime) is not GraphFramesRuntimeConfig:
        raise ValueError('Exact typed local and native runtime settings required')
    config.__post_init__(); runtime.__post_init__()
    selected_path(setup_directory); selected_path(output)
    if runtime.work != output / 'engine' or setup_directory == output or setup_directory.exists() or output.exists():
        raise ValueError('Distinct fresh setup and engine output paths required')
    directory_identity(setup_directory.parent); directory_identity(output.parent)
    current = LocalGraphFramesRuntimePolicy(config)
    current.admit_owned_outputs((setup_directory, output, config.lease))
    _, _, _, inventory = read_local_runtime_plan(config.runtime_plan, config.runtime_plan_sha256)
    inventory.verify(runtime); runtime.renew()
    value = {'profile': 'ashlar-graphframes-invocation/0.1', 'mode': 'protected-ack-export',
        'pack': str(config.pack), 'release': str(config.release), 'release_sha256': config.release_sha256,
        'custody': str(config.custody), 'custody_sha256': config.custody_sha256, 'output': str(output),
        'runtime': {name: str(getattr(runtime, name)) if isinstance(getattr(runtime, name), Path) else getattr(runtime, name)
            for name in ('python', 'python_sha256', 'java_home', 'java_sha256', 'worker_revision',
                'worker_inventory_sha256', 'sdk_inventory_sha256', 'jars', 'timeout_seconds',
                'maximum_stream_bytes', 'maximum_evidence_bytes')}}
    invocation_bytes = local_encoded(value); invocation = parse_graphframes_invocation(invocation_bytes)
    release_bytes, _ = original_file(config.release, 16777216); custody_bytes, _ = original_file(config.custody, 16777216)
    policy = LocalRetainedExportPolicy(config, invocation, GraphRelease(release_bytes, config.release_sha256), custody_bytes)
    # Validate exact actual runtime selection without acquiring source/native effects.
    current.admit_graphframes_runtime(runtime, current)
    config_bytes = local_configuration_bytes(config); config_digest = local_digest(config_bytes)
    provider = ("from pathlib import Path\nfrom ashlar_host.graphframes_local_setup import open_local_graphframes_configuration\n"
        "def open_graphframes(invocation, release, custody_bytes):\n"
        "    return open_local_graphframes_configuration(Path(" + repr(str(setup_directory / 'operator.json')) + "), " + repr(config_digest) + ", invocation, release, custody_bytes)\n").encode()
    setup_directory.mkdir(mode=0o700); identity = directory_identity(setup_directory)
    retain(setup_directory, 'operator.json', config_bytes, identity)
    retain(setup_directory, 'invocation.json', invocation_bytes, identity)
    retain(setup_directory, 'provider.py', provider, identity)
    # Renew original correspondence after owned writes; no generated grant.
    policy.validate_original(); current.renew_graphframes_runtime(current.inventory, runtime, current)
    return {'configuration': str(setup_directory / 'invocation.json'), 'provider': str(setup_directory / 'provider.py'),
            'provider_sha256': local_digest(provider), 'scope': 'Explicitly admitted retained protected-ACK export only; no engine execution'}

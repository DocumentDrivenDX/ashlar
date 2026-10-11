"""Filesystem authority and inert runtime-resolution controls; no SDK execution."""
import gzip
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest import TestCase, mock

from ashlar.graph_release import GraphRelease
from ashlar_host.finite_dataset import FiniteDatasetConfig
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.graphframes_cli import GraphFramesInvocation
from ashlar_host.graphframes_release import GraphFramesRuntimeConfig, VERSIONS
from ashlar_host import graphframes_local as local

ROOT = Path(__file__).resolve().parents[1]


class LocalFixture:
    def __init__(self, root):
        self.root = root
        upstream = ROOT / 'examples/domain-packs/archaeology/upstream'
        originals = ROOT / 'docs/helix/04-build/evidence/original17-graphframes-native-20261009/original'
        self.model = root / 'model'; self.model.write_bytes((upstream / 'ontology.json').read_bytes())
        self.graph = root / 'graph'; self.graph.write_bytes((upstream / 'graph/fixture.json').read_bytes())
        self.pack = root / 'pack'; self.pack.write_bytes((upstream / 'pack.json').read_bytes())
        self.release = root / 'release'; self.release.write_bytes((originals / '1-release.graph.json').read_bytes())
        self.custody = root / 'custody'; self.custody.write_bytes(gzip.decompress((originals / '0-custody.json.gz').read_bytes()))
        self.runtime_root = root / 'runtime'; self.runtime_root.mkdir()
        self.sdk = self.runtime_root / 'sdk'; self.sdk.mkdir()
        self.worker = self.runtime_root / 'worker.py'; self.worker.write_bytes(b'pass\n')
        for name in ('pyspark', 'delta', 'graphframes', 'dependency'):
            directory = self.sdk / name; directory.mkdir(); (directory / '__init__.py').write_bytes(name.encode())
        self.python = self.runtime_root / 'python'; self.python.write_bytes(b'python')
        self.java = self.runtime_root / 'java'; (self.java / 'bin').mkdir(parents=True); (self.java / 'bin/java').write_bytes(b'java')
        self.jars = self.runtime_root / 'jars'; self.jars.mkdir()
        tree = local.runtime_tree(self.runtime_root)
        records = {r['path']: r for r in tree['files']}
        worker = [records[str(self.worker)]]
        groups = []
        for distribution, package in (('pyspark', 'pyspark'), ('delta-spark', 'delta'), ('graphframes-py', 'graphframes')):
            groups.append([distribution, [records[str(self.sdk / package / '__init__.py')]]])
        used = {r['path'] for r in worker + [r for _, rows in groups for r in rows]}
        groups.append(['transitive-imports', [r for r in tree['files'] if r['path'] not in used]])
        self.plan = {'profile': local.RUNTIME_PROFILE, 'revision': 'a' * 40,
            'python': str(self.python), 'prefix': str(self.runtime_root), 'base_prefix': str(root / 'base'),
            'sys_path': [str(self.runtime_root), str(root / 'absent.zip')],
            'absent_import_paths': [str(root / 'absent.zip')],
            'worker_origin': str(self.worker), 'sdk_root': str(self.sdk),
            'roots': {str(self.runtime_root): tree}, 'worker': worker, 'sdks': groups}
        self.plan_file = root / 'runtime.json'; self.plan_file.write_bytes(local.local_encoded(self.plan))
        producer = FiniteDatasetConfig(FinitePackDefinition('archaeology'), root / 'umf', root / 'bun', root / 'git', 5, 1048576, 33554432)
        self.config = local.RetainedGraphFramesLocalConfig(producer,
            self.model, local.local_digest(self.model.read_bytes()), self.graph, local.local_digest(self.graph.read_bytes()),
            self.pack, local.local_digest(self.pack.read_bytes()), self.release, local.local_digest(self.release.read_bytes()),
            self.custody, local.local_digest(self.custody.read_bytes()),
            local.local_encoded(json.loads(self.custody.read_bytes())['ack']['scope']),
            self.plan_file, local.local_digest(self.plan_file.read_bytes()), root / 'lease')
        self.output = root / 'output'
        self.runtime = GraphFramesRuntimeConfig(self.python, local.local_digest(b'python'), self.java, local.local_digest(b'java'),
            'a' * 40, local.local_digest(local.local_encoded(worker)), local.local_digest(local.local_encoded(dict(groups))),
            self.jars, self.output / 'engine', 300, 1048576, 16777216)
        self.invocation = GraphFramesInvocation('protected-ack-export', self.pack, self.output, self.runtime,
            self.release, self.config.release_sha256, self.custody, self.config.custody_sha256)
        self.release_value = GraphRelease(self.release.read_bytes(), self.config.release_sha256)

    def runtime_environment(self):
        def distribution(name):
            return SimpleNamespace(version=VERSIONS[name], locate_file=lambda _: self.sdk)
        def spec(name):
            if name in ('sitecustomize', 'usercustomize'): return None
            if name == 'ashlar_host.graphframes_release': return SimpleNamespace(origin=str(self.worker))
            return SimpleNamespace(origin=str(self.sdk / name / '__init__.py'))
        return [mock.patch.multiple(local.sys, executable=str(self.python), prefix=str(self.runtime_root), base_prefix=str(self.root / 'base'),
                    path=self.plan['sys_path'], flags=SimpleNamespace(isolated=1)),
                mock.patch.object(local.importlib.metadata, 'distribution', side_effect=distribution),
                mock.patch.object(local.importlib.util, 'find_spec', side_effect=spec),
                mock.patch.object(GraphFramesRuntimeConfig, 'renew', return_value=None)]

    def producer_capture(self, argv, **kwargs):
        """Inert subprocess only; owned producer/source constructors stay real."""
        from ashlar_host.finite_pack import UMF_REVISION
        if argv[-2:] == ['rev-parse', 'HEAD']: return (UMF_REVISION + '\n').encode(), b''
        if argv[-2:] == ['status', '--porcelain']: return b'', b''
        graph = json.loads(self.graph.read_bytes()); model = json.loads(self.model.read_bytes())
        facts = self.config.producer.definition.facts()
        receipt = {'profile': 'ashlar-finite-pack-public-dataset/0.2', 'pack': 'archaeology', 'umfRevision': UMF_REVISION,
            'sourceSha256': facts['model_sha256'], 'graphSha256': facts['graph_sha256'], 'receipt': {
            'operation': 'validate-core-dataset-values-compact', 'version': '1.0.0', 'provenance': 'unverified', 'source': model,
            'scope': 'supplied-dataset-only', 'input': {'scope': {'id': 'ashlar-original-archaeology-fixture', 'closure': 'supplied-dataset-only'}},
            'datasetValidation': {'valid': True, 'complete': True, 'diagnostics': []},
            'records': [{'instanceId': o['key'], 'result': {'sourceRef': '#/source', 'validation': {'valid': True}}} for o in graph['objects']],
            'keys': [{'result': {'sourceRef': '#/source'}} for _ in graph['objects']],
            'relationships': [{'instanceId': e['key'], 'sourceInstanceId': e['source'], 'targetInstanceId': e['target'],
                'targetKey': {'sourceRef': '#/source'}} for e in graph['edges']]}}
        Path(argv[3]).write_bytes(local.local_encoded(receipt)); return b'inert producer transport', b''


class Tests(TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve(); self.fixture = LocalFixture(self.root)

    def policy(self):
        f = self.fixture
        return local.LocalRetainedExportPolicy(f.config, f.invocation, f.release_value, f.custody.read_bytes())

    def test_actual_original_ack_scope_and_source_hold(self):
        policy = self.policy(); f = self.fixture
        with policy.hold():
            policy.admit_source(FinitePackDefinition('archaeology').inputs(f.model.read_bytes(), f.graph.read_bytes()), policy)
            with policy.writer(policy): policy.renew()
        self.assertFalse(policy.held); self.assertIsNone(policy.fd)
        self.assertFalse(f.config.lease.exists())
        # The same immutable original grant supports another distinct invocation.
        with policy.hold(): policy.renew()
        self.assertFalse(f.config.lease.exists())

    def test_source_replacement_and_lease_loss_refuse(self):
        policy = self.policy()
        with self.assertRaises(PermissionError):
            with policy.hold():
                moved = self.root / 'old'; self.fixture.model.rename(moved)
                self.fixture.model.write_bytes(moved.read_bytes()); policy.renew()
        self.assertIsNone(policy.fd)

    def test_zero_member_closing_drift_and_complete_internal_directory_alias(self):
        root = self.root / 'small-runtime'; root.mkdir(); target = root / 'target'; target.mkdir()
        empty = target / 'empty.pyc'; empty.write_bytes(b'')
        (root / 'alias').symlink_to(target, target_is_directory=True)
        tree = local.runtime_tree(root)
        self.assertIn(str(empty), [r['path'] for r in tree['files']])
        self.assertIn({'path': str(root / 'alias'), 'kind': 'link', 'target': str(target), 'resolved': str(target)}, tree['namespace'])
        real_read = os.read
        def read(fd, size):
            raw = real_read(fd, size)
            if size == 1: empty.write_bytes(b'changed')
            return raw
        with mock.patch.object(local.os, 'read', side_effect=read):
            with self.assertRaises(ValueError): local.runtime_tree(root)

    def test_lease_replacement_cannot_authorize_current_use(self):
        policy = self.policy()
        with self.assertRaises(PermissionError):
            with policy.hold():
                self.fixture.config.lease.rename(self.root / 'old-lease')
                self.fixture.config.lease.write_bytes(b''); os.chmod(self.fixture.config.lease, 0o600)
                policy.renew()
        self.assertIsNone(policy.fd)
        self.assertTrue(self.fixture.config.lease.exists())
        self.assertEqual(self.fixture.config.lease.read_bytes(), b'')

    def test_hold_failure_still_attempts_closing_original_admission(self):
        policy = self.policy(); cancellation = GeneratorExit('body'); calls = []
        real = policy.renew
        def renew(): calls.append(True); return real()
        with mock.patch.object(policy, 'renew', side_effect=renew):
            with self.assertRaises(GeneratorExit) as raised:
                with policy.hold(): raise cancellation
        self.assertIs(raised.exception, cancellation); self.assertEqual(len(calls), 2)
        self.assertFalse(self.fixture.config.lease.exists()); self.assertIsNone(policy.fd)

    def test_cancel_identity_and_every_cleanup_attempt(self):
        policy = self.policy(); cancellation = KeyboardInterrupt('body'); calls = []
        real_close = os.close
        def close(fd): calls.append('close'); real_close(fd)
        real_flock = local.fcntl.flock
        def flock(fd, operation):
            if operation == local.fcntl.LOCK_UN: calls.append('unlock'); raise SystemExit('cleanup')
            return real_flock(fd, operation)
        with mock.patch.object(local.fcntl, 'flock', side_effect=flock), mock.patch.object(local.os, 'close', side_effect=close):
            with self.assertRaises(KeyboardInterrupt) as raised:
                with policy.hold(): raise cancellation
        self.assertIs(raised.exception, cancellation); self.assertIn('unlock', calls); self.assertEqual(calls[-1], 'close')
        self.assertIsNone(policy.fd); self.assertFalse(policy.held)

    def test_changed_protected_scope_rejected_before_lease(self):
        from dataclasses import replace
        scope = json.loads(self.fixture.config.ack_scope_bytes); scope['consumer'] = 'other'
        changed = replace(self.fixture.config, ack_scope_bytes=local.local_encoded(scope))
        with self.assertRaises(PermissionError):
            local.LocalRetainedExportPolicy(changed, self.fixture.invocation, self.fixture.release_value, self.fixture.custody.read_bytes())
        self.assertFalse(changed.lease.exists())

    def test_bytecode_new_alias_and_absent_import_path_refuse(self):
        from contextlib import ExitStack
        f = self.fixture
        with ExitStack() as stack:
            for patch in f.runtime_environment(): stack.enter_context(patch)
            policy = local.LocalGraphFramesRuntimePolicy(f.config)
            policy.admit_graphframes_runtime(f.runtime, policy)
            for path, kind in ((f.runtime_root / 'new.pyc', 'bytecode'), (f.runtime_root / 'alias.py', 'alias'), (self.root / 'absent.zip', 'zip')):
                if kind == 'alias': path.symlink_to(f.worker)
                else: path.write_bytes(b'new')
                with self.assertRaises(PermissionError): policy.renew_graphframes_runtime(policy.inventory, f.runtime, policy)
                path.unlink()
            policy.renew_graphframes_runtime(policy.inventory, f.runtime, policy)

    def test_runtime_selection_digest_and_external_alias_refuse(self):
        with self.assertRaises(PermissionError): local.read_local_runtime_plan(self.fixture.plan_file, 'b' * 64)
        (self.fixture.runtime_root / 'external').symlink_to(self.fixture.model)
        with self.assertRaises(ValueError): local.runtime_tree(self.fixture.runtime_root)

    def test_wrong_config_and_mode_never_start_producer(self):
        live = GraphFramesInvocation('live-publication', self.fixture.pack, self.fixture.output, self.fixture.runtime)
        with mock.patch.object(local, 'HeldFiniteDataset') as producer:
            with self.assertRaises(PermissionError):
                with local.open_retained_graphframes(self.fixture.config, live, self.fixture.release_value, self.fixture.custody.read_bytes()): pass
            producer.assert_not_called()

    def test_complete_owned_source_composition_and_closing_cancellation(self):
        from contextlib import ExitStack
        from ashlar_host import finite_dataset as dataset
        f = self.fixture
        f.output.mkdir(); cancellation = SystemExit('closing')
        with ExitStack() as stack:
            for patch in f.runtime_environment(): stack.enter_context(patch)
            stack.enter_context(mock.patch.object(dataset, 'capture', side_effect=f.producer_capture))
            with self.assertRaises(SystemExit) as raised:
                with local.open_retained_graphframes(f.config, f.invocation, f.release_value, f.custody.read_bytes()) as session:
                    self.assertEqual(len(session.source.changes), 85)
                    session.source.renew()
                    session.source.renew = mock.Mock(side_effect=cancellation)
        self.assertIs(raised.exception, cancellation)
        self.assertFalse(session.policy.held); self.assertIsNone(session.policy.fd)

    def test_forged_source_carriers_do_not_execute_equality(self):
        class Forged(str):
            def __eq__(self, other): raise AssertionError('Executable equality')
            __hash__ = str.__hash__
        policy = self.policy()
        with policy.hold():
            facts = dict(json.loads(policy.facts)); facts['source_system'] = Forged(facts['source_system'])
            with self.assertRaises(ValueError): policy.admit_source(facts, policy)

    def test_public_policy_constructor_and_renew_refuse_executable_carriers(self):
        class Forged:
            def __getattribute__(self, name): raise AssertionError('Executable carrier dispatched')
        with self.assertRaises(ValueError): local.LocalGraphFramesRuntimePolicy(Forged())
        with self.assertRaises(ValueError): local.LocalRetainedExportPolicy(Forged(), None, None, None)
        policy = local.LocalGraphFramesRuntimePolicy(self.fixture.config)
        with self.assertRaises(ValueError): policy.renew_graphframes_runtime(policy.inventory, Forged(), policy)

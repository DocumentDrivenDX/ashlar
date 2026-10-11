"""Closed setup/parser and genuine filesystem controls; producer/SDK ports inert."""
from contextlib import ExitStack
from pathlib import Path
import json
import unittest
from unittest import mock

from ashlar_host import graphframes_local as local
from ashlar_host import graphframes_local_setup as setup
from ashlar_host.graphframes_cli import parse_graphframes_invocation
from test_graphframes_local import LocalFixture


class Tests(unittest.TestCase):
    def setUp(self):
        import tempfile
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve(); self.fixture = LocalFixture(self.root)

    def test_setup_writes_real_closed_parser_configuration_and_public_adapter(self):
        f = self.fixture
        with ExitStack() as stack:
            for patch in f.runtime_environment(): stack.enter_context(patch)
            stack.enter_context(mock.patch.object(local, 'HeldFiniteDataset', side_effect=AssertionError('No setup producer effect')))
            result = setup.setup_retained_graphframes(f.config, f.runtime, self.root / 'setup', f.output)
        invocation = parse_graphframes_invocation(Path(result['configuration']).read_bytes())
        self.assertEqual(invocation.mode, 'protected-ack-export'); self.assertEqual(invocation.output, f.output)
        self.assertFalse(f.output.exists()); self.assertFalse(f.config.lease.exists())
        provider = Path(result['provider']).read_bytes()
        self.assertEqual(local.local_digest(provider), result['provider_sha256'])
        namespace = {}; exec(compile(provider, result['provider'], 'exec'), namespace)
        port = mock.Mock(return_value='owned-context')
        with mock.patch.dict(namespace, open_local_graphframes_configuration=port):
            self.assertEqual(namespace['open_graphframes'](invocation, f.release_value, f.custody.read_bytes()), 'owned-context')
            self.assertEqual(port.call_args.args[:2], (self.root / 'setup/operator.json', local.local_digest((self.root / 'setup/operator.json').read_bytes())))
        self.assertEqual(setup.local_configuration_bytes(setup.parse_local_configuration((self.root / 'setup/operator.json').read_bytes())), setup.local_configuration_bytes(f.config))

    def test_setup_refuses_grant_and_runtime_drift_before_writes(self):
        from dataclasses import replace
        f = self.fixture
        wrong = replace(f.config, runtime_plan_sha256='b' * 64)
        with self.assertRaises(PermissionError): setup.setup_retained_graphframes(wrong, f.runtime, self.root / 'setup', f.output)
        self.assertFalse((self.root / 'setup').exists())
        f.worker.write_bytes(b'changed')
        with self.assertRaises(ValueError): setup.setup_retained_graphframes(f.config, f.runtime, self.root / 'setup', f.output)
        self.assertFalse((self.root / 'setup').exists())

    def test_generated_provider_real_owned_source_and_closing_configuration(self):
        from ashlar_host import finite_dataset as dataset
        f = self.fixture
        with ExitStack() as stack:
            for patch in f.runtime_environment(): stack.enter_context(patch)
            result = setup.setup_retained_graphframes(f.config, f.runtime, self.root / 'setup', f.output)
            invocation = parse_graphframes_invocation(Path(result['configuration']).read_bytes())
            namespace = {}; exec(compile(Path(result['provider']).read_bytes(), result['provider'], 'exec'), namespace)
            f.output.mkdir()
            stack.enter_context(mock.patch.object(dataset, 'capture', side_effect=f.producer_capture))
            with namespace['open_graphframes'](invocation, f.release_value, f.custody.read_bytes()) as session:
                self.assertEqual(len(session.source.changes), 85)
                session.source.renew(); self.assertTrue(session.policy.held)
            self.assertFalse(session.policy.held); self.assertIsNone(session.policy.fd)
            self.assertFalse(f.config.lease.exists())
            (f.output / 'public-dataset.json').unlink()
            with self.assertRaises(PermissionError):
                with namespace['open_graphframes'](invocation, f.release_value, f.custody.read_bytes()) as session:
                    operator = self.root / 'setup/operator.json'; operator.chmod(0o600)
                    operator.write_bytes(operator.read_bytes() + b' ')
            self.assertFalse(session.policy.held); self.assertIsNone(session.policy.fd)

    def test_setup_postwrite_source_change_withholds_return(self):
        f = self.fixture; original = setup.retain
        def retain(*args):
            result = original(*args)
            if args[1] == 'provider.py': f.graph.write_bytes(f.graph.read_bytes() + b' ')
            return result
        with ExitStack() as stack:
            for patch in f.runtime_environment(): stack.enter_context(patch)
            stack.enter_context(mock.patch.object(setup, 'retain', side_effect=retain))
            with self.assertRaises(PermissionError): setup.setup_retained_graphframes(f.config, f.runtime, self.root / 'setup', f.output)
        self.assertTrue((self.root / 'setup/provider.py').exists())
        self.assertFalse(f.output.exists())

    def test_closed_parser_rejects_unknown_missing_bool_giant_and_duplicate(self):
        raw = setup.local_configuration_bytes(self.fixture.config); value = json.loads(raw)
        controls = []
        changed = dict(value); changed['grant'] = True; controls.append(local.local_encoded(changed))
        changed = dict(value); del changed['runtime_plan_sha256']; controls.append(local.local_encoded(changed))
        changed = json.loads(raw); changed['producer']['timeout_seconds'] = True; controls.append(local.local_encoded(changed))
        controls.append(raw.replace(b'"timeout_seconds":5', b'"timeout_seconds":' + b'9' * 60000))
        controls.append(raw.replace(b'"profile":', b'"profile":"duplicate","profile":'))
        for item in controls:
            with self.assertRaises(ValueError): setup.parse_local_configuration(item)

    def test_selected_runtime_namespace_is_never_setup_output(self):
        f = self.fixture
        with self.assertRaises(PermissionError):
            setup.setup_retained_graphframes(f.config, f.runtime, f.runtime_root / 'new-setup', f.output)
        self.assertFalse((f.runtime_root / 'new-setup').exists())

    def test_runtime_inspection_covers_actual_paths_and_is_not_admission(self):
        f = self.fixture
        # Inert namespace selection only, never a supported installed SDK claim.
        with ExitStack() as stack:
            for patch in f.runtime_environment(): stack.enter_context(patch)
            extra = self.root / 'extra'; extra.mkdir(); (extra / 'extra.py').write_bytes(b'')
            candidate = setup.inspect_local_graphframes_runtime('a' * 40, (f.runtime_root,), f.sdk, (extra,))
            self.assertIs(type(candidate), bytes)
            self.assertFalse(f.config.lease.exists()); self.assertFalse(f.output.exists())
            with mock.patch.object(local.sys, 'path', [str(self.root / 'outside')]):
                (self.root / 'outside').mkdir()
                with self.assertRaises(ValueError): setup.inspect_local_graphframes_runtime('a' * 40, (f.runtime_root,), f.sdk, (extra,))

# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit installed entry ownership controls; no native qualification."""
import contextlib
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from ashlar_host.config import HostError
from ashlar_host import evolution_cli as owner


class Controls(unittest.TestCase):
    def document(self, directory, mode='resume'):
        value = {'profile': owner.PROFILE, 'mode': mode,
            'ledger_path': str(directory / 'ledger'), 'receipt_path': str(directory / 'receipt'),
            'producer': {'source': '/selected/umf', 'bun': '/selected/bun', 'git': '/selected/git',
                'timeout_seconds': 5, 'maximum_output_bytes': 1024, 'maximum_receipt_bytes': 1024}}
        if mode == 'resume': value['expected_sha256'] = 'a' * 64
        else:
            value['request'] = {'source_order': ['A', 'B'], 'stream': 'shared', 'predecessor': 'empty',
                'clocks': [['2026-10-10T00:00:00+00:00'] * 4] * 2,
                'publication_ids': ['publication-' + str(i) for i in range(8)],
                'recorded_at': [str(i) for i in range(8)]}
        return value

    def files(self, directory, mode='resume'):
        config = directory / 'input.json'; source = directory / 'provider.py'
        config.write_text(json.dumps(self.document(directory, mode)))
        source.write_bytes(b'def open_evolution(invocation):\n    raise RuntimeError("unused native boundary")\n')
        return config, source, hashlib.sha256(source.read_bytes()).hexdigest()

    def answer(self):
        return {'profile': 'ashlar-commerce-evolution-run-result/0.1', 'run_sha256': 'b' * 64,
            'publications': tuple(SimpleNamespace(publication_id='p-' + str(i)) for i in range(8))}

    def test_fresh_bounded_parser_receipt_and_provider_mismatch(self):
        from dataclasses import replace
        import test_evolution_run as run_tests
        from ashlar_host.evolution_composition import NativeEvolutionRunPolicy
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root).resolve(); value = self.document(directory, 'fresh')
            self.assertIsNone(owner.parse_evolution_invocation(json.dumps(value).encode(), 'fresh').through_ordinal)
            value['through_ordinal'] = 2
            invocation = owner.parse_evolution_invocation(json.dumps(value).encode(), 'fresh')
            for bad in (None, True, -1, 8, '2'):
                value['through_ordinal'] = bad
                with self.assertRaises(HostError): owner.parse_evolution_invocation(json.dumps(value).encode(), 'fresh')
            resume = self.document(directory); resume['through_ordinal'] = 2
            with self.assertRaises(HostError): owner.parse_evolution_invocation(json.dumps(resume).encode(), 'resume')
            value['through_ordinal'] = 2
            config, source, pin = self.files(directory, 'fresh'); config.write_text(json.dumps(value))
            answer = self.answer(); answer.update(profile='ashlar-commerce-evolution-prefix-result/0.1',
                publications=answer['publications'][:3], through_ordinal=2, completed_count=3,
                total_publications=8, run_complete=False, source_progress={})
            for descriptor in answer['publications']: descriptor.raw = {'source_progress_json': '{}'}
            with patch.object(owner, 'run_evolution_invocation', return_value=answer):
                receipt = owner.run_evolution_command(config, source, pin, 'fresh')
            self.assertEqual(receipt['profile'], 'ashlar-commerce-evolution-prefix-receipt/0.1')
            self.assertEqual(receipt['completed_count'], 3); self.assertFalse(receipt['run_complete'])
        run_tests.Tests.setUpClass(); fixture = run_tests.Tests(); fixture.setUp()
        try:
            invocation = owner.FreshEvolutionInvocation(fixture.path, fixture.root/'receipt',fixture.producer,fixture.request,2)
            config = replace(fixture.config, policy=NativeEvolutionRunPolicy(fixture.driver,fixture.policy))
            closed=[]
            @contextlib.contextmanager
            def opened(value):
                try: yield config
                finally: closed.append(True)
            with patch.object(owner,'publish_commerce_evolution') as publish:
                with self.assertRaises(HostError): owner.run_evolution_invocation(invocation,SimpleNamespace(open_evolution=opened))
                publish.assert_not_called()
            self.assertEqual(closed,[True])
        finally: fixture.doCleanups()

    def test_continuation_closed_ordinal_parser_and_partial_receipt(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root).resolve()
            value = self.document(directory); value.update(mode='continue', through_ordinal=1)
            invocation = owner.parse_evolution_invocation(json.dumps(value).encode(), 'continue')
            self.assertIs(type(invocation), owner.ContinueEvolutionInvocation)
            for bad in (True, -1, 8, '1'):
                value['through_ordinal'] = bad
                with self.assertRaises(HostError): owner.parse_evolution_invocation(json.dumps(value).encode(), 'continue')
            value['through_ordinal'] = 1
            config, source, pin = self.files(directory)
            config.write_text(json.dumps(value))
            answer = self.answer(); answer.update(profile='ashlar-commerce-evolution-prefix-result/0.1',
                run_sha256=invocation.expected_sha256, publications=answer['publications'][:2],
                through_ordinal=1, completed_count=2, total_publications=8, run_complete=False, source_progress={})
            for descriptor in answer['publications']:
                descriptor.raw = {'source_progress_json': '{}'}
            with patch.object(owner, 'run_evolution_invocation', return_value=answer):
                receipt = owner.run_evolution_command(config, source, pin, 'continue')
            self.assertEqual(receipt['profile'], 'ashlar-commerce-evolution-prefix-receipt/0.1')
            self.assertFalse(receipt['run_complete']); self.assertEqual(len(receipt['publication_ids']), 2)

    def test_continuation_provider_ordinal_mismatch_refuses_before_coordinator(self):
        import test_evolution_run as run_tests
        from ashlar_host.evolution_composition import NativeEvolutionRunPolicy
        from ashlar_host.config import ContinueCommerceEvolutionConfig
        run_tests.Tests.setUpClass(); fixture = run_tests.Tests(); fixture.setUp()
        try:
            invocation = owner.ContinueEvolutionInvocation(fixture.path, fixture.root / 'receipt',
                fixture.producer, 'a' * 64, 0)
            config = ContinueCommerceEvolutionConfig(fixture.driver, fixture.path,
                NativeEvolutionRunPolicy(fixture.driver, fixture.policy), fixture.producer,
                fixture.driver.context, 'a' * 64, 1)
            closed = []
            @contextlib.contextmanager
            def opened(value):
                try: yield config
                finally: closed.append('closed')
            with patch.object(owner, 'continue_commerce_evolution') as coordinator:
                with self.assertRaises(HostError):
                    owner.run_evolution_invocation(invocation, SimpleNamespace(open_evolution=opened))
                coordinator.assert_not_called()
            self.assertEqual(closed, ['closed'])
        finally: fixture.doCleanups()

    def test_actual_cli_continuation_dispatch_is_explicit_and_lazy(self):
        import io
        from ashlar import cli
        args = ['continue-commerce-evolution', '--configuration', '/explicit/input',
            '--provider', '/explicit/provider', '--provider-sha256', 'a' * 64]
        with patch.object(owner, 'run_evolution_command', return_value={'profile': 'prefix'}) as run, \
                patch('sys.stdout', new_callable=io.StringIO) as stdout, \
                patch('sys.argv', ['ashlar', *args]):
            cli.main()
        run.assert_called_once_with('/explicit/input', '/explicit/provider', 'a' * 64, 'continue')
        self.assertEqual(json.loads(stdout.getvalue()), {'profile': 'prefix'})

    def test_distinct_original_configuration_types(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root).resolve()
            fresh = owner.parse_evolution_invocation(json.dumps(self.document(directory, 'fresh')).encode(), 'fresh')
            resume = owner.parse_evolution_invocation(json.dumps(self.document(directory)).encode(), 'resume')
            self.assertIs(type(fresh), owner.FreshEvolutionInvocation)
            self.assertIs(type(resume), owner.ResumeEvolutionInvocation)
            self.assertEqual(fresh.request.source_order, ('A', 'B'))
            self.assertEqual(resume.expected_sha256, 'a' * 64)
            with self.assertRaises(HostError):
                owner.ResumeEvolutionInvocation(directory / 'ledger', directory / 'ledger', resume.producer, 'a' * 64)

    def test_closed_config_before_any_provider_code(self):
        with tempfile.TemporaryDirectory() as root:
            config, source, pin = self.files(Path(root).resolve())
            for raw in (b'{"mode":"resume","mode":"resume"}', b'{}', b'null'):
                config.write_bytes(raw)
                with patch.object(owner, 'load_evolution_provider') as execute:
                    with self.assertRaises(HostError): owner.run_evolution_command(config, source, pin, 'resume')
                    execute.assert_not_called()
            value = self.document(Path(root).resolve()); value['producer']['timeout_seconds'] = True
            with self.assertRaises(HostError): owner.parse_evolution_invocation(json.dumps(value).encode(), 'resume')

    def test_giant_integer_refuses_before_producer_constructor_or_code(self):
        with tempfile.TemporaryDirectory() as root:
            config, source, pin = self.files(Path(root).resolve())
            raw = config.read_bytes().replace(b'"timeout_seconds": 5', b'"timeout_seconds": ' + b'9' * 60000)
            config.write_bytes(raw)
            with patch.object(owner, 'EvolutionAdmissionConfig') as constructor, patch.object(owner, 'load_evolution_provider') as execute:
                with self.assertRaises(HostError): owner.run_evolution_command(config, source, pin, 'resume')
                constructor.assert_not_called(); execute.assert_not_called()

    def test_provider_wrong_pin_never_executes(self):
        with tempfile.TemporaryDirectory() as root:
            config, source, pin = self.files(Path(root).resolve())
            source.write_text('raise AssertionError("executed")')
            with self.assertRaises(HostError): owner.run_evolution_command(config, source, pin, 'resume')

    def test_closing_original_custody_withholds_receipt(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root).resolve(); config, source, pin = self.files(directory)
            def run(*args):
                source.write_bytes(source.read_bytes() + b'\n# changed\n')
                return self.answer()
            with patch.object(owner, 'run_evolution_invocation', side_effect=run):
                with self.assertRaises(HostError): owner.run_evolution_command(config, source, pin, 'resume')
            self.assertFalse((directory / 'receipt').exists())

    def test_closed_receipt_and_exclusive_output(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root).resolve(); config, source, pin = self.files(directory)
            with patch.object(owner, 'run_evolution_invocation', return_value=self.answer()) as run:
                result = owner.run_evolution_command(config, source, pin, 'resume')
                self.assertEqual(json.loads((directory / 'receipt').read_bytes()), result)
                self.assertEqual(set(result), {'profile', 'run_sha256', 'publication_ids'})
                with self.assertRaises(HostError): owner.run_evolution_command(config, source, pin, 'resume')
                self.assertEqual(run.call_count, 1)

    def test_body_cancellation_preserved_and_renewed(self):
        with tempfile.TemporaryDirectory() as root:
            config, source, pin = self.files(Path(root).resolve()); cancel = KeyboardInterrupt()
            with patch.object(owner, 'run_evolution_invocation', side_effect=cancel), patch.object(owner.InvocationFile, 'renew', side_effect=OSError()) as renew:
                with self.assertRaises(KeyboardInterrupt) as caught: owner.run_evolution_command(config, source, pin, 'resume')
                self.assertIs(caught.exception, cancel); self.assertEqual(renew.call_count, 2)

    def test_supplied_ordinary_ports_cannot_be_skipped(self):
        with tempfile.TemporaryDirectory() as root:
            invocation = owner.parse_evolution_invocation(json.dumps(self.document(Path(root).resolve())).encode(), 'resume')
            @contextlib.contextmanager
            def opened(value): yield object()
            with self.assertRaises(HostError): owner.run_evolution_invocation(invocation, SimpleNamespace(open_evolution=opened))

    def test_actual_configuration_routes_fresh_and_resume_after_owned_close(self):
        # Actual owning NativeDriver/source/ACK carriers; only the already-tested
        # full-run functions are patched to isolate command dispatch/cleanup.
        import test_evolution_run as run_tests
        from ashlar_host.evolution_composition import NativeEvolutionRunPolicy
        from ashlar_host.config import FreshCommerceEvolutionConfig, ResumeCommerceEvolutionConfig
        run_tests.Tests.setUpClass()
        fixture = run_tests.Tests(); fixture.setUp()
        try:
            policy = NativeEvolutionRunPolicy(fixture.driver, fixture.policy)
            for mode in ('fresh', 'resume'):
                invocation = (owner.FreshEvolutionInvocation(fixture.path, fixture.root / 'receipt',
                    fixture.producer, fixture.request) if mode == 'fresh' else
                    owner.ResumeEvolutionInvocation(fixture.path, fixture.root / 'receipt', fixture.producer, 'a' * 64))
                config = (FreshCommerceEvolutionConfig(fixture.driver, fixture.path, policy,
                    fixture.producer, fixture.driver.context, fixture.request) if mode == 'fresh' else
                    ResumeCommerceEvolutionConfig(fixture.driver, fixture.path, policy,
                    fixture.producer, fixture.driver.context, 'a' * 64))
                closed = []
                @contextlib.contextmanager
                def opened(value):
                    self.assertIs(value, invocation)
                    try: yield config
                    finally: closed.append(True)
                with patch.object(owner, 'publish_commerce_evolution', return_value=self.answer()) as fresh, patch.object(owner, 'resume_commerce_evolution', return_value=self.answer()) as resume:
                    owner.run_evolution_invocation(invocation, SimpleNamespace(open_evolution=opened))
                    self.assertEqual(fresh.call_count, int(mode == 'fresh'))
                    self.assertEqual(resume.call_count, int(mode == 'resume'))
                    self.assertEqual(closed, [True])
                @contextlib.contextmanager
                def failing(value):
                    yield config
                    raise PermissionError('closing source admission')
                with patch.object(owner, 'publish_commerce_evolution', return_value=self.answer()), patch.object(owner, 'resume_commerce_evolution', return_value=self.answer()):
                    with self.assertRaises(PermissionError): owner.run_evolution_invocation(invocation, SimpleNamespace(open_evolution=failing))
        finally: fixture.doCleanups()

    def test_file_bound_symlink_and_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root).resolve(); config, source, pin = self.files(directory)
            link = directory / 'link'; link.symlink_to(config)
            with self.assertRaises(OSError): owner.InvocationFile.read(link, owner.MAX_CONFIG)
            with self.assertRaises(HostError): owner.InvocationFile.read(config, 1)
            snapshot = owner.InvocationFile.read(config, owner.MAX_CONFIG)
            saved = directory / 'saved'; config.rename(saved); config.write_bytes(saved.read_bytes())
            with self.assertRaises(HostError): snapshot.renew(owner.MAX_CONFIG)


if __name__ == '__main__': unittest.main()

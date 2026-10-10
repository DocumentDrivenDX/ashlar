"""Injected lifecycle controls; no native or installed workflow qualification."""
import unittest
from unittest.mock import patch
import test_host_paths_query as fixtures
from ashlar_host import paths_query as module
from ashlar_host.config import HostError
from ashlar_host import publication_reader as reader_owner


class Run:
    def __init__(self, hook=lambda *args: None):
        self.events = []
        self.hook = hook

    def begin_attempt(self, operation):
        self.events.append(('begin', operation))
        self.hook('begin')
        return object()

    def phase(self, attempt, phase, state):
        self.events.append((phase, state))
        self.hook(phase, state)

    def finish_attempt(self, attempt, outcome, **fields):
        self.events.append(('finish', outcome, fields))
        self.hook('finish', outcome)


class DiagnosticWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.LifecycleTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def compose(self, run, **overrides):
        original = module.query_commerce_paths
        reader = overrides.pop('open_commerce_reader', self.fixture.reader)
        def observed_reader(*args, cleanup_state=None):
            self.fixture.reader_cleanup = cleanup_state
            return reader(*args)
        overrides['open_commerce_reader'] = observed_reader
        with patch.object(module, 'query_commerce_paths',
                          lambda config: original(config, diagnostics=run)):
            return self.fixture.compose(**overrides)

    def test_success_observed_after_actual_report_publish_and_stop(self):
        def hook(phase, state=None):
            if phase == 'finish':
                self.assertTrue((self.fixture.config.output/'report.json').exists())
                self.fixture.spark.stop.assert_called_once()
        run = Run(hook)
        result = self.compose(run)
        self.assertEqual(len(result['cases']), 10)
        self.assertEqual(run.events[0], ('begin', 'held-read'))
        self.assertEqual(run.events[-1][0:2], ('finish', 'succeeded'))
        self.assertEqual([e for e in run.events if len(e) == 2 and e[1] == 'started'],
            [(p, 'started') for p in ('configuration','admission','prepare','guard',
                                      'capture','closing','cleanup','closing','commit')])

    def test_ordinary_sink_failures_do_not_abort_or_mark_native_cleanup(self):
        error = OSError('synthetic secret must not be observed')
        def hook(phase, *args):
            if phase != 'begin': raise error
        result = self.compose(Run(hook))
        self.assertEqual(len(result['cases']), 10)
        self.assertFalse(hasattr(error, 'cleanup_failed'))
        self.fixture.spark.stop.assert_called_once()

    def test_failed_begin_does_not_fabricate_attempt_or_finish(self):
        def hook(phase, *args):
            if phase == 'begin': raise OSError()
        run = Run(hook)
        self.compose(run)
        self.assertEqual(run.events, [('begin', 'held-read')])

    def test_post_acquisition_phase_cancellation_stops_spark_before_propagation(self):
        for phase, state in [('prepare','completed'),('guard','started'),
                             ('guard','completed'),('capture','started'),
                             ('capture','completed'),('closing','started'),
                             ('cleanup','started'),('cleanup','completed'),
                             ('closing','completed'),('commit','started')]:
            with self.subTest(phase=phase):
                self.fixture.doCleanups()
                self.fixture = fixtures.LifecycleTests(); self.fixture.setUp()
                self.addCleanup(self.fixture.doCleanups)
                primary = KeyboardInterrupt()
                def hook(p, s=None):
                    if (p,s) == (phase,state): raise primary
                with self.assertRaises(KeyboardInterrupt) as caught:
                    self.compose(Run(hook))
                self.assertIs(caught.exception, primary)
                self.fixture.spark.stop.assert_called_once()
                self.assertFalse((self.fixture.config.output/'report.json').exists())
                self.assertFalse(hasattr(primary, 'cleanup_failed'))
        self.fixture.doCleanups()

    def test_commit_completed_cancellation_preserves_published_report(self):
        primary = KeyboardInterrupt()
        def hook(phase, state=None):
            if (phase, state) == ('commit', 'completed'): raise primary
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.compose(Run(hook))
        self.assertIs(caught.exception, primary)
        self.assertTrue((self.fixture.config.output/'report.json').exists())
        self.fixture.spark.stop.assert_called_once()

    def test_cancellation_inside_reader_closes_reader_before_stop(self):
        from contextlib import contextmanager
        primary = KeyboardInterrupt()
        closed = []
        @contextmanager
        def reader(*args):
            try: yield self.fixture.opened
            finally: closed.append('reader')
        self.fixture.spark.stop.side_effect = lambda: closed.append('spark')
        def hook(phase, state=None):
            if (phase, state) == ('capture', 'started'): raise primary
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.compose(Run(hook), open_commerce_reader=reader)
        self.assertIs(caught.exception, primary)
        self.assertEqual(closed, ['reader', 'spark'])

    def test_post_cleanup_source_drift_uses_closing_category(self):
        calls = []
        actual = module.original_inputs
        def original(model, graph):
            calls.append(1)
            return actual(model, graph) if len(calls) == 1 else (b'changed', b'changed')
        run = Run()
        with self.assertRaises(HostError):
            self.compose(run, original_inputs=original)
        self.fixture.spark.stop.assert_called_once()
        self.assertEqual(run.events[-1][0:2], ('finish', 'failed'))
        self.assertEqual(run.events[-1][2],
                         {'error_category': 'closing', 'cleanup_failed': False})

    def test_guard_refusal_is_typed_refused_without_message_inspection(self):
        run = Run()
        self.fixture.spark.version = 'unsupported'
        with self.assertRaises(HostError): self.compose(run)
        self.assertEqual(run.events[-1][0:2], ('finish', 'refused'))
        self.assertEqual(run.events[-1][2]['error_category'], 'guard')
        self.fixture.spark.stop.assert_called_once()

    def test_native_cleanup_failure_is_failed_cleanup_category(self):
        run = Run()
        self.fixture.spark.stop.side_effect = HostError('arbitrary')
        with self.assertRaises(HostError): self.compose(run)
        self.assertEqual(run.events[-1][0:2], ('finish', 'failed'))
        self.assertEqual(run.events[-1][2]['error_category'], 'cleanup')
        self.assertTrue(run.events[-1][2]['cleanup_failed'])

    def test_reader_cleanup_only_failure_has_explicit_native_cleanup_fact(self):
        from contextlib import contextmanager
        run = Run()
        fault = HostError('arbitrary-cleanup')
        @contextmanager
        def reader(*args):
            try: yield self.fixture.opened
            finally:
                def close(): raise fault
                reader_owner._close_observed(close, self.fixture.reader_cleanup, None)
        with self.assertRaises(HostError) as caught:
            self.compose(run, open_commerce_reader=reader)
        self.assertIs(caught.exception, fault)
        self.assertFalse(hasattr(fault, 'cleanup_failed'))
        self.assertEqual(run.events[-1][0:2], ('finish', 'failed'))
        self.assertEqual(run.events[-1][2],
                         {'error_category': 'cleanup', 'cleanup_failed': True})
        self.fixture.spark.stop.assert_called_once()

    def test_reader_closing_drift_does_not_invent_cleanup_failure(self):
        from contextlib import contextmanager
        run = Run()
        @contextmanager
        def reader(*args):
            yield self.fixture.opened
            reader_owner._close_observed(lambda: None, self.fixture.reader_cleanup, None)
            raise HostError('closing-drift')
        with self.assertRaises(HostError):
            self.compose(run, open_commerce_reader=reader)
        self.assertEqual(run.events[-1][2],
                         {'error_category': 'closing', 'cleanup_failed': False})
        self.assertFalse(self.fixture.reader_cleanup.failed)

    def test_cleanup_state_tracks_actual_port_only_and_preserves_cancellation(self):
        state = reader_owner.ReaderCleanupState()
        business = ValueError('closing-guard')
        reader_owner._close_observed(lambda: None, state, business)
        self.assertFalse(state.failed)
        cancel = KeyboardInterrupt()
        def close(): raise cancel
        with self.assertRaises(KeyboardInterrupt) as caught:
            reader_owner._close_observed(close, state, business)
        self.assertIs(caught.exception, cancel)
        self.assertTrue(state.failed)
        self.assertFalse(state.cleanup_only)
        with self.assertRaises(AttributeError): state.failed = False
        with self.assertRaises(AttributeError): del state.cleanup_only

    def test_reader_acquired_region_preserves_primary_with_and_without_facts(self):
        """Exercise the owning acquired-region AST with inert ports, not native I/O."""
        import ast
        import inspect
        import json
        from contextlib import contextmanager
        from pathlib import Path
        from types import SimpleNamespace
        source = ast.parse(inspect.getsource(reader_owner.open_commerce_reader))
        acquired = next(node for node in source.body[0].body if isinstance(node, ast.Try))
        function = ast.FunctionDef(name='exercise',
            args=ast.arguments(posonlyargs=[], args=[], kwonlyargs=[],
                               kw_defaults=[], defaults=[]),
            body=ast.parse('transport = None\nprimary = None').body + [acquired], decorator_list=[])
        fragment = compile(ast.fix_missing_locations(ast.Module(body=[function],
            type_ignores=[])), '<reader-acquired-region>', 'exec')
        for observed in (False, True):
            for primary in (ValueError('closing-guard'), KeyboardInterrupt(),
                            SystemExit(), GeneratorExit()):
                with self.subTest(observed=observed, primary=type(primary).__name__):
                    state = reader_owner.ReaderCleanupState() if observed else None
                    closes, guards = [], []
                    manifest = {'table_versions_json': '{}', 'source_progress_json': '{}',
                                'publication_id': 'synthetic'}
                    class Database:
                        def execute(self, sql, *args):
                            row = (json.dumps({'request_digest': 'synthetic'}), json.dumps({'manifest': manifest})) if 'local_publication_artifact' in sql else (json.dumps({
                                'selected_steps': [], 'complete_prior_oracle': {},
                                'generated_steps': [], 'zero_match_elisions': []}),)
                            return SimpleNamespace(fetchone=lambda: row)
                    class Transport:
                        db = Database()
                        def close(self):
                            closes.append(True)
                            raise OSError('synthetic-close')
                    def native_files():
                        guards.append(True)
                        raise primary
                    namespace = dict(vars(reader_owner))
                    namespace.update(spark=None, publication=Path('/synthetic'), targets=[],
                        ROOT=Path('/synthetic'), SimpleNamespace=SimpleNamespace,
                        PrivatePolicy=lambda *args: SimpleNamespace(initializing=True),
                        ReadOnlyTransport=SimpleNamespace(open=lambda *args: Transport()),
                        original_report={'native_manifest': manifest, 'protected_ack_scope': {},
                            'source_schema': 'synthetic', 'source_signature_sha256': 'synthetic',
                            'table_registry': []},
                        AckScope=lambda **kwargs: SimpleNamespace(service_schema='ashlar_ack_pipeline_synthetic'),
                        NativeDriver=lambda *args, **kwargs: object(), batch=SimpleNamespace(feed='synthetic'),
                        admission=SimpleNamespace(changes=[]), fixture_columns=lambda *args: [],
                        expected={}, model=b'{}', graph=b'{}', bindings={}, original_native={},
                        compiler_request=lambda *args: {'target': {'bindingJson': '{}'}},
                        PublicationProvider=lambda *args, **kwargs: SimpleNamespace(_reader_closed=False),
                        native_files=native_files, cleanup_state=state)
                    exec(fragment, namespace)
                    with self.assertRaises(BaseException) as caught:
                        with contextmanager(namespace['exercise'])(): pass
                    self.assertEqual(guards, [True])
                    self.assertIs(caught.exception, primary)
                    self.assertTrue(primary.cleanup_failed)
                    self.assertEqual(closes, [True])
                    if state is not None:
                        self.assertTrue(state.failed)
                        self.assertFalse(state.cleanup_only)

    def test_actual_provider_distinguishes_ack_guard_from_cleanup_ports(self):
        from contextlib import nullcontext
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        for port in ('guard', 'rollback', 'close'):
            with self.subTest(port=port):
                state = reader_owner.ReaderCleanupState()
                context = object()
                driver = SimpleNamespace(
                    policy=SimpleNamespace(active={'manifest': {'table_versions_json': '{}'}}),
                    transport=SimpleNamespace(targets={}),
                    writer=lambda *a: nullcontext(), hold=lambda *a, **k: nullcontext())
                provider = reader_owner.PublicationProvider(driver, {}, {'role': 'synthetic'},
                    b'{}', b'{}', {}, context=context, cleanup_state=state)
                connection = MagicMock()
                fault = HostError('synthetic')
                provider._ack = MagicMock(side_effect=[{}, fault] if port == 'guard' else [{}, {}])
                if port != 'guard': getattr(connection, port).side_effect = fault
                with patch('ashlar.manifest.manifest_pin_vector', return_value=object()), \
                     patch('ashlar_host.connection.connect', return_value=connection), \
                     patch('ashlar_host.postgres.Session', return_value=object()):
                    with self.assertRaises(HostError) as caught:
                        with provider.interval(context): pass
                self.assertIs(caught.exception, fault)
                self.assertEqual(state.failed, port != 'guard')
                self.assertEqual(state.cleanup_only, port != 'guard')
                connection.rollback.assert_called_once()
                connection.close.assert_called_once()

    def test_business_category_preserved_when_native_cleanup_also_fails(self):
        run = Run()
        business = HostError('business-refused')
        def compiler(*args): raise business
        self.fixture.spark.stop.side_effect = OSError('cleanup-only')
        with self.assertRaises(HostError) as caught:
            self.compose(run, compile_paths_distribution=compiler)
        self.assertIs(caught.exception, business)
        self.assertTrue(business.cleanup_failed)  # Existing native finish policy.
        self.assertEqual(run.events[-1][2],
                         {'error_category': 'capture', 'cleanup_failed': True})

    def test_finish_cancellation_supersedes_ordinary_business_error(self):
        cancellation = KeyboardInterrupt()
        def hook(phase, *args):
            if phase == 'finish': raise cancellation
        def compiler(*args): raise HostError('business-refused')
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.compose(Run(hook), compile_paths_distribution=compiler)
        self.assertIs(caught.exception, cancellation)
        self.fixture.spark.stop.assert_called_once()

    def test_business_cancellation_wins_over_later_diagnostic_cancellation(self):
        business = KeyboardInterrupt(); later = SystemExit(7)
        def hook(phase, *args):
            if phase == 'finish': raise later
        def compiler(*args): raise business
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.compose(Run(hook), compile_paths_distribution=compiler)
        self.assertIs(caught.exception, business)
        self.assertFalse(hasattr(business, 'cleanup_failed'))
        self.fixture.spark.stop.assert_called_once()

    def test_ordinary_finish_failure_preserves_business_primary(self):
        business = HostError('business-refused')
        def hook(phase, *args):
            if phase == 'finish': raise OSError()
        def compiler(*args): raise business
        with self.assertRaises(HostError) as caught:
            self.compose(Run(hook), compile_paths_distribution=compiler)
        self.assertIs(caught.exception, business)
        self.assertFalse(hasattr(business, 'cleanup_failed'))


if __name__ == '__main__': unittest.main()

"""Small composition controls; inert runtime never qualifies native execution."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import tempfile
import unittest
from ashlar_host.puppy_scenario_native import execute_owned_puppy_scenarios, ScenarioQueryTrace
from ashlar_host.puppy_finite_local import PuppyFiniteLoopbackConfig


class Tests(unittest.TestCase):
    def test_original_request_is_retained_before_native_refusal(self):
        marker = RuntimeError('inert refusal')
        def query(script, bindings, context): raise marker
        trace = ScenarioQueryTrace(SimpleNamespace(query_original=query, context=object()))
        stage = {'pack': 'archaeology', 'case': 'cycle'}
        trace.set_stage(stage)
        stage['case'] = 'changed'
        with self.assertRaises(RuntimeError) as raised:
            trace('fixed original query', {'value': 'original'})
        self.assertIs(raised.exception, marker)
        self.assertEqual(trace.requests, [{'stage': {'pack': 'archaeology', 'case': 'cycle'},
            'query': 'fixed original query', 'bindings': {'value': 'original'}}])

    def test_first_carrier_cancellation_closes_native_before_any_result(self):
        marker = KeyboardInterrupt()
        events = []
        @contextmanager
        def hold(context):
            try: yield
            finally: events.append('held-closed')
        native = SimpleNamespace(hold=hold, context=object())
        @contextmanager
        def factory(*args, **kwargs):
            try: yield native
            finally: events.append('engine-closed')
        source = SimpleNamespace(definition=SimpleNamespace(build=lambda *args: (None, {})), model=b'm', graph=b'g')
        checked = []
        def refuse(profile, kind, *args, **kwargs):
            checked.append((profile['pack'], kind))
            raise marker
        with tempfile.TemporaryDirectory() as tmp:
            config = PuppyFiniteLoopbackConfig(Path('/usr/bin/false'), 'unix:///explicit',
                'sha256:'+'a'*64, 'ashlar-inert', Path(tmp).resolve(), 'inert', 'inert', 18081, 17687)
            with patch('ashlar_host.puppy_scenario_native.open_puppy_scenario_local', factory), \
                    patch('ashlar_host.puppy_scenario_native.carrier_check', refuse):
                with self.assertRaises(KeyboardInterrupt) as raised:
                    execute_owned_puppy_scenarios(config, pack_bytes=(b'p', b'p'),
                        sources=(source, source), exports=(object(), object()),
                        preparation={'reports': [{'profile': {'pack': name}}
                            for name in ('archaeology', 'ecology')]}, native_map_profile=None)
        self.assertIs(raised.exception, marker)
        self.assertEqual(events, ['held-closed', 'engine-closed'])
        self.assertEqual(checked, [('archaeology', 'node'),
            ('archaeology', 'node'), ('archaeology', 'edge'),
            ('ecology', 'node'), ('ecology', 'edge')])

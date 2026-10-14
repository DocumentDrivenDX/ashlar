"""Fixed preflight-stage controls with inert lower ports; no native authority."""
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
import unittest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch
from ashlar_host.evolution_continuation_timing import EvolutionContinuationTiming
from ashlar_host import commerce_evolution as composition


class Tests(unittest.TestCase):
    def test_ledger_open_interval_is_visible_and_cancel_still_closes_owners(self):
        for opened in (False, True):
            with self.subTest(opened=opened):
                records = []; actions = []; cancel = KeyboardInterrupt()
                def observer(row):
                    records.append(dict(row))
                    if row['stage'] == 'ledger-opening' and row['phase'] == 'begin':
                        raise cancel
                @contextmanager
                def writer(*args):
                    actions.append('writer-enter')
                    try: yield
                    finally: actions.append('writer-close')
                driver = SimpleNamespace(writer=writer,
                    source_admission=SimpleNamespace(metadata=lambda: {'sources': []}),
                    verify_evolution_sources=Mock(return_value=None),
                    evolution_run_identity=Mock())
                config = SimpleNamespace(__post_init__=lambda: None, driver=driver,
                    context=object(), ledger_path='/original', policy=object(),
                    expected_sha256='original', producer=object(), stage_observer=observer)
                ledger = SimpleNamespace(close=lambda: actions.append('ledger-close'))
                def open_ledger(*args, **kwargs):
                    actions.append('ledger-open-attempt')
                    if opened: return ledger
                    raise TimeoutError('inert blocking phase')
                with patch('ashlar_host.evolution_run.EvolutionRunLedger.open',side_effect=open_ledger), \
                     patch('ashlar_host.evolution_admission.EvolutionSourceSet',return_value=object()):
                    with self.assertRaises(KeyboardInterrupt) as raised:
                        composition._run_commerce_evolution(config, False, through_ordinal=2)
                self.assertIs(raised.exception, cancel)
                self.assertIn('ledger-open-attempt',actions)
                self.assertEqual(actions[-1],'writer-close')
                if opened: self.assertIn('ledger-close',actions)
                driver.verify_evolution_sources.assert_called_once()
                driver.evolution_run_identity.assert_called_once()
                self.assertTrue(any(r['stage']=='closing' for r in records))
                self.assertTrue(all(set(r) <= {'profile','sequence','stage','phase','duration_ns','outcome'} for r in records))

    def test_public_operator_forwards_explicit_continuation_observer_only(self):
        from ashlar_host.evolution_operator import ExistingEvolutionOperator
        from ashlar_host.evolution_cli import parse_evolution_invocation
        import test_evolution_cli
        with tempfile.TemporaryDirectory() as directory:
            document = test_evolution_cli.Controls().document(Path(directory))
            document.update(mode='continue', through_ordinal=2)
            invocation = parse_evolution_invocation(json.dumps(document).encode(),'continue')
            operator = object.__new__(ExistingEvolutionOperator)
            observer = lambda record: None
            selected = object()
            with patch.object(ExistingEvolutionOperator,'_open_existing',return_value=nullcontext(selected)) as lower:
                with operator.open_evolution(invocation,stage_observer=observer) as actual:
                    self.assertIs(actual,selected)
                lower.assert_called_once_with(invocation,stage_observer=observer)
            document['mode']='resume';document.pop('through_ordinal')
            resume = parse_evolution_invocation(json.dumps(document).encode(),'resume')
            with patch.object(ExistingEvolutionOperator,'_open_existing') as lower:
                with self.assertRaises(PermissionError):
                    with operator.open_evolution(resume,stage_observer=observer):pass
                lower.assert_not_called()

    def test_ordinary_observer_loss_and_capacity_do_not_skip_required_action(self):
        timing = EvolutionContinuationTiming(lambda record: (_ for _ in ()).throw(OSError('private path')))
        calls = []
        self.assertEqual(timing.call('closing',lambda: calls.append('closed') or 7),7)
        timing.records = 128
        timing.call('closing',lambda:calls.append('closed-again'))
        self.assertEqual(calls,['closed','closed-again'])
        self.assertEqual(timing.lost_records,4)

    def test_closed_records_and_first_cancellation(self):
        first = KeyboardInterrupt(); later = SystemExit(); records = []
        def observer(record):
            records.append(record)
            if record['phase']=='begin':raise first
        timing = EvolutionContinuationTiming(observer)
        with self.assertRaises(KeyboardInterrupt) as raised:
            timing.call('run-identity',lambda:(_ for _ in ()).throw(later))
        self.assertIs(raised.exception,first)
        with self.assertRaises(ValueError):timing.emit('run-identity','begin',outcome='completed')
        with self.assertRaises(ValueError):timing.emit('run-identity','end')
        with self.assertRaises(TypeError):records[0]['stage']='arbitrary'


if __name__ == '__main__': unittest.main()

import pathlib, tempfile, json, unittest
from unittest.mock import patch
import evolution_stage_observer as m

def owners():
    result = {}
    for (key, (module, name, methods)) in m.METHODS.items():

        def method(self, *args, **kwargs):
            return self.result
        cls = type(name, (), {name: method for name in methods})
        cls.__module__ = module
        result[key] = cls
    return result

class Tests(unittest.TestCase):

    def test_exact_result_arguments_order_and_restore(self):
        own = owners()
        value = object()
        c = own['driver']()
        c.result = value
        seen = []
        def actual(instance, *args, **kwargs):
            seen.append((instance, args, kwargs))
            return value
        own['driver'].source_admit = actual
        old = actual
        with tempfile.TemporaryDirectory() as d:
            with m.observe_fixed_owners(own, d):
                self.assertIs(c.source_admit('secret', payload='sensitive'), value)
            rows = [json.loads(x) for x in pathlib.Path(d, 'stage-events.jsonl').read_text().splitlines()]
            self.assertEqual([x['event'] for x in rows], ['begin', 'end'])
            self.assertEqual(rows[0]['method'], 'driver.source_admit')
            self.assertEqual(rows[1]['status'], 'ok')
            self.assertNotIn('secret', json.dumps(rows))
            self.assertNotIn('sensitive', json.dumps(rows))
            self.assertIs(own['driver'].source_admit, old)
            self.assertEqual(seen, [(c, ('secret',), {'payload': 'sensitive'})])

    def test_active_method_close_refuses_without_closing_owned_fds(self):
        with tempfile.TemporaryDirectory() as directory:
            observer = m.Observer(directory)
            observer.pending = 1
            with self.assertRaises(m.StageRefusal):
                observer.close()
            self.assertFalse(observer.closed)
            observer.renew()
            observer.pending = 0
            observer.close()
            self.assertTrue(observer.closed)

    def test_nesting_drift_preserves_original_error_and_cancellation(self):
        for primary in (KeyboardInterrupt(), ValueError()):
            own = owners()
            with tempfile.TemporaryDirectory() as directory:
                with m.observe_fixed_owners(own, directory) as observer:
                    def body(instance):
                        observer.local.stack.clear()
                        raise primary
                    # Invoke the fixed observer directly to force an owning nesting fault.
                    with self.assertRaises(type(primary)) as caught:
                        observer.invoke('driver.source_admit', body, (object(),), {})
                    self.assertIs(caught.exception, primary)
                    self.assertEqual(observer.pending, 0)

    def test_opening_cancel_independently_attempts_both_fd_closes(self):
        for failed_close in (0, 1):
            primary = KeyboardInterrupt()
            real_close = m.os.close
            closed = []
            def close(fd):
                index = len(closed)
                closed.append(fd)
                real_close(fd)
                if index == failed_close:
                    raise OSError()
            with tempfile.TemporaryDirectory() as directory:
                with patch.object(m.Observer, 'renew', side_effect=primary), patch.object(m.os, 'close', side_effect=close):
                    with self.assertRaises(KeyboardInterrupt) as caught:
                        m.Observer(directory)
                self.assertIs(caught.exception, primary)
                self.assertEqual(len(closed), 2)

    def test_close_gate_refuses_concurrent_invocation_before_effect(self):
        import threading
        effects = []
        errors = []
        with tempfile.TemporaryDirectory() as directory:
            observer = m.Observer(directory)
            real_renew = observer.renew
            def attempt():
                try:
                    observer.invoke('driver.source_admit', lambda: effects.append(True), (), {})
                except BaseException as error:
                    errors.append(error)
            def closing_renew():
                thread = threading.Thread(target=attempt)
                thread.start()
                thread.join(2)
                self.assertFalse(thread.is_alive())
                real_renew()
            with patch.object(observer, 'renew', side_effect=closing_renew):
                observer.close()
            self.assertEqual(effects, [])
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], m.StageRefusal)
            with self.assertRaises(m.StageRefusal):
                observer.invoke('driver.source_admit', lambda: effects.append(True), (), {})
            self.assertEqual(effects, [])

    def test_actual_selected_owner_shape_without_sdk(self):
        from ashlar_host.driver import NativeDriver
        from ashlar_host.evolution_run import EvolutionRunLedger
        from ashlar_host.delta_custody import LocalDeltaTransport
        from ashlar_host.ack_sessions import RegisteredOutboxAcks
        selection = {'driver': NativeDriver, 'ledger': EvolutionRunLedger, 'transport': LocalDeltaTransport, 'acks': RegisteredOutboxAcks}
        originals = {k: {name: c.__dict__[name] for name in m.METHODS[k][2]} for (k, c) in selection.items()}
        with tempfile.TemporaryDirectory() as d:
            with m.observe_fixed_owners(selection, d):
                pass
        for (k, c) in selection.items():
            for (name, original) in originals[k].items():
                self.assertIs(c.__dict__[name], original)

    def test_nested_fixed_owner_events(self):
        own = owners()
        c = own['driver']()
        c.result = object()

        def outer(self):
            return self.source_admit()
        own['driver'].validate_evolution_run = outer
        with tempfile.TemporaryDirectory() as d:
            with m.observe_fixed_owners(own, d):
                c.validate_evolution_run()
            rows = [json.loads(x) for x in pathlib.Path(d, 'stage-events.jsonl').read_text().splitlines()]
            self.assertEqual([x['event'] for x in rows], ['begin', 'begin', 'end', 'end'])
            self.assertEqual(rows[1]['parent'], rows[0]['span'])

    def test_count_capacity_discloses_loss_and_preserves_effect(self):
        own = owners()
        effects = []

        def call(self):
            effects.append(True)
        own['driver'].source_admit = call
        with tempfile.TemporaryDirectory() as d, patch.object(m, 'MAX_RECORDS', 1):
            with m.observe_fixed_owners(own, d):
                own['driver']().source_admit()
        self.assertEqual(effects, [True])

    def test_primary_cancel_and_error_survive_end_fsync_failure(self):
        for primary in (KeyboardInterrupt('sensitive'), ValueError('sensitive')):
            own = owners()

            def call(self):
                raise primary
            own['driver'].source_admit = call
            with tempfile.TemporaryDirectory() as d:
                with m.observe_fixed_owners(own, d) as observer:
                    original = observer.emit

                    def fail_end(row):
                        if row['event'] == 'end':
                            raise OSError('secret')
                        original(row)
                    with patch.object(observer, 'emit', side_effect=fail_end):
                        with self.assertRaises(type(primary)) as caught:
                            own['driver']().source_admit()
                    self.assertIs(caught.exception, primary)

    def test_ordinary_begin_and_end_sink_failures_preserve_result(self):
        for event in ('begin', 'end'):
            own = owners()
            effects = []
            result = object()
            def call(instance):
                effects.append(True)
                return result
            own['driver'].source_admit = call
            with tempfile.TemporaryDirectory() as directory:
                with m.observe_fixed_owners(own, directory) as observer:
                    real = observer.emit
                    def fail(row):
                        if row['event'] == event:
                            raise OSError()
                        real(row)
                    with patch.object(observer, 'emit', side_effect=fail):
                        self.assertIs(own['driver']().source_admit(), result)
                    self.assertFalse(observer.diagnostic_status()['complete'])
                self.assertEqual(effects, [True])

    def test_foreign_file_replacement_refuses_and_preserves(self):
        own = owners()
        effects = []

        def call(self):
            effects.append(True)
        own['driver'].source_admit = call
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(m.StageRefusal):
                with m.observe_fixed_owners(own, d):
                    path = pathlib.Path(d, 'stage-events.jsonl')
                    path.rename(path.with_suffix('.owned'))
                    path.write_text('foreign')
                    with self.assertRaises(m.StageRefusal):
                        own['driver']().source_admit()
            self.assertEqual(path.read_text(), 'foreign')
            self.assertEqual(effects, [])

    def test_unknown_selection_and_closed_error_redaction(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(m.StageRefusal):
                with m.observe_fixed_owners({}, d):
                    pass
            self.assertFalse(pathlib.Path(d, 'stage-events.jsonl').exists())
        own = owners()
        primary = type('sensitive_custom_error', (Exception,), {})('secret')

        def call(self):
            raise primary
        own['driver'].source_admit = call
        with tempfile.TemporaryDirectory() as d:
            with m.observe_fixed_owners(own, d):
                with self.assertRaises(type(primary)) as c:
                    own['driver']().source_admit()
            self.assertIs(c.exception, primary)
            raw = pathlib.Path(d, 'stage-events.jsonl').read_text()
            self.assertNotIn('secret', raw)
            self.assertNotIn('sensitive', raw)
            self.assertEqual(json.loads(raw.splitlines()[-1])['status'], 'error')

class CapacityLossTests(unittest.TestCase):
    def test_real_record_cap_many_calls_preserve_result_and_one_loss(self):
        own = owners()
        instance = own['driver']()
        instance.result = object()
        with tempfile.TemporaryDirectory() as directory:
            with m.observe_fixed_owners(own, directory) as observer:
                for _ in range(2100):
                    self.assertIs(instance.source_admit(), instance.result)
                status = observer.diagnostic_status()
                self.assertFalse(status['complete'])
                self.assertTrue(status['unknown_loss'])
                self.assertIsNone(status['dropped_count'])
                self.assertTrue(status['loss_record_retained'])
            raw = pathlib.Path(directory, 'stage-events.jsonl').read_bytes()
            rows = [json.loads(line) for line in raw.splitlines()]
            self.assertLessEqual(len(rows), 4096)
            self.assertLessEqual(len(raw), 1048576)
            self.assertEqual(sum(row['event'] == 'loss' for row in rows), 1)
            self.assertEqual({r['span'] for r in rows if r['event'] == 'begin'},
                             {r['span'] for r in rows if r['event'] == 'end'})

    def test_nested_depth_loss_preserves_pending_pairs_and_inner_call(self):
        own = owners()
        effects = []
        def outer(instance):
            return instance.source_admit()
        def inner(instance):
            effects.append(True)
            return 'result'
        own['driver'].validate_evolution_run = outer
        own['driver'].source_admit = inner
        with tempfile.TemporaryDirectory() as directory, patch.object(m, 'MAX_DEPTH', 1):
            with m.observe_fixed_owners(own, directory):
                self.assertEqual(own['driver']().validate_evolution_run(), 'result')
            rows = [json.loads(line) for line in pathlib.Path(directory, 'stage-events.jsonl').read_text().splitlines()]
            self.assertEqual([r['event'] for r in rows], ['begin', 'loss', 'end'])
            self.assertEqual(rows[0]['span'], rows[-1]['span'])
            self.assertEqual(rows[1]['reason'], 'depth')
        self.assertEqual(effects, [True])

    def test_active_record_and_byte_capacity_keep_ancestor_end_and_outcome(self):
        for cap, limit in (('MAX_RECORDS', 3), ('MAX_BYTES', 1536)):
            for primary in (None, ValueError(), KeyboardInterrupt()):
                own = owners()
                effects = []
                result = object()
                def outer(instance):
                    return instance.source_admit()
                def inner(instance):
                    effects.append(True)
                    if primary is not None:
                        raise primary
                    return result
                own['driver'].validate_evolution_run = outer
                own['driver'].source_admit = inner
                with tempfile.TemporaryDirectory() as directory, patch.object(m, cap, limit):
                    with m.observe_fixed_owners(own, directory) as observer:
                        if primary is None:
                            self.assertIs(own['driver']().validate_evolution_run(), result)
                        else:
                            with self.assertRaises(type(primary)) as caught:
                                own['driver']().validate_evolution_run()
                            self.assertIs(caught.exception, primary)
                        self.assertEqual(observer.pending, 0)
                        self.assertTrue(observer.diagnostic_status()['unknown_loss'])
                    raw = pathlib.Path(directory, 'stage-events.jsonl').read_bytes()
                    rows = [json.loads(line) for line in raw.splitlines()]
                    self.assertEqual([row['event'] for row in rows], ['begin', 'loss', 'end'])
                    self.assertEqual(rows[0]['span'], rows[-1]['span'])
                    self.assertEqual(rows[1]['reason'], 'records' if cap == 'MAX_RECORDS' else 'bytes')
                    self.assertLessEqual(len(rows), limit if cap == 'MAX_RECORDS' else 4096)
                    self.assertLessEqual(len(raw), limit if cap == 'MAX_BYTES' else 1048576)
                self.assertEqual(effects, [True])

    def test_marker_write_and_fsync_failure_preserve_every_business_outcome(self):
        for failure in ('write', 'fsync'):
            for primary in (None, ValueError(), KeyboardInterrupt()):
                own = owners()
                calls = []
                result = object()
                def body(instance):
                    calls.append(True)
                    if primary is not None:
                        raise primary
                    return result
                own['driver'].source_admit = body
                with tempfile.TemporaryDirectory() as directory, patch.object(m, 'MAX_RECORDS', 1):
                    with m.observe_fixed_owners(own, directory) as observer:
                        real = getattr(m.os, failure)
                        def fail(*args):
                            if not observer.recording:
                                raise OSError()
                            return real(*args)
                        with patch.object(m.os, failure, side_effect=fail):
                            for _ in range(3):
                                if primary is None:
                                    self.assertIs(own['driver']().source_admit(), result)
                                else:
                                    with self.assertRaises(type(primary)) as caught:
                                        own['driver']().source_admit()
                                    self.assertIs(caught.exception, primary)
                        status = observer.diagnostic_status()
                        self.assertFalse(status['complete'])
                        self.assertFalse(status['loss_record_retained'])
                        self.assertTrue(status['unknown_loss'])
                self.assertEqual(calls, [True, True, True])

    def test_partial_marker_write_reports_unknown_and_continues_calls(self):
        own = owners()
        instance = own['driver']()
        instance.result = object()
        with tempfile.TemporaryDirectory() as directory, patch.object(m, 'MAX_RECORDS', 1):
            with m.observe_fixed_owners(own, directory) as observer:
                real = m.os.write
                attempts = []
                def partial(fd, raw):
                    attempts.append(True)
                    if len(attempts) == 1:
                        return real(fd, raw[:7])
                    raise OSError()
                with patch.object(m.os, 'write', side_effect=partial):
                    self.assertIs(instance.source_admit(), instance.result)
                    self.assertIs(instance.source_admit(), instance.result)
                self.assertEqual(observer.bytes, 7)
                observer.renew()
                self.assertFalse(observer.diagnostic_status()['loss_record_retained'])
                self.assertTrue(observer.diagnostic_status()['unknown_loss'])
            self.assertEqual(pathlib.Path(directory, 'stage-events.jsonl').stat().st_size, 7)

    def test_zero_and_partial_zero_write_preserve_business_outcomes(self):
        for partial in (False, True):
            for primary in (None, ValueError(), KeyboardInterrupt()):
                own = owners()
                calls = []
                result = object()
                def body(instance):
                    calls.append(True)
                    if primary is not None:
                        raise primary
                    return result
                own['driver'].source_admit = body
                with tempfile.TemporaryDirectory() as directory, patch.object(m, 'MAX_RECORDS', 1):
                    with m.observe_fixed_owners(own, directory) as observer:
                        real = m.os.write
                        attempts = []
                        def write(fd, raw):
                            attempts.append(True)
                            if partial and len(attempts) == 1:
                                return real(fd, raw[:7])
                            return 0
                        with patch.object(m.os, 'write', side_effect=write):
                            for _ in range(3):
                                if primary is None:
                                    self.assertIs(own['driver']().source_admit(), result)
                                else:
                                    with self.assertRaises(type(primary)) as caught:
                                        own['driver']().source_admit()
                                    self.assertIs(caught.exception, primary)
                        self.assertEqual(observer.bytes, 7 if partial else 0)
                        self.assertTrue(observer.diagnostic_status()['unknown_loss'])
                        self.assertFalse(observer.diagnostic_status()['loss_record_retained'])
                self.assertEqual(calls, [True, True, True])

    def test_disappearance_between_entry_and_emit_renewal_refuses_effect(self):
        own = owners()
        effects = []
        def body(instance):
            effects.append(True)
        own['driver'].source_admit = body
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(m.StageRefusal):
                with m.observe_fixed_owners(own, directory) as observer:
                    real = observer.renew
                    renewals = []
                    def renew():
                        real()
                        renewals.append(True)
                        if len(renewals) == 1:
                            pathlib.Path(directory, 'stage-events.jsonl').unlink()
                    with patch.object(observer, 'renew', side_effect=renew):
                        with self.assertRaises(m.StageRefusal):
                            own['driver']().source_admit()
            self.assertTrue(observer.closed)
        self.assertEqual(effects, [])

    def test_invalid_write_return_is_ordinary_loss(self):
        for invalid in (True, 1.5, None):
            own = owners()
            instance = own['driver']()
            instance.result = object()
            with tempfile.TemporaryDirectory() as directory, patch.object(m, 'MAX_RECORDS', 1):
                with m.observe_fixed_owners(own, directory) as observer:
                    with patch.object(m.os, 'write', return_value=invalid):
                        self.assertIs(instance.source_admit(), instance.result)
                        self.assertIs(instance.source_admit(), instance.result)
                    self.assertTrue(observer.diagnostic_status()['unknown_loss'])

    def test_closing_disappearance_preserves_body_cancellation(self):
        primary = KeyboardInterrupt()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(KeyboardInterrupt) as caught:
                with m.observe_fixed_owners(owners(), directory) as observer:
                    pathlib.Path(directory, 'stage-events.jsonl').unlink()
                    raise primary
            self.assertIs(caught.exception, primary)
            self.assertTrue(observer.closed)

    def test_closing_disappearance_is_safety_refusal_not_sink_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            observer = m.Observer(directory)
            pathlib.Path(directory, 'stage-events.jsonl').unlink()
            with self.assertRaises(m.StageRefusal):
                observer.close()
            self.assertTrue(observer.closed)

    def test_marker_cancellation_wins_and_cleanup_is_attempted(self):
        primary = KeyboardInterrupt()
        own = owners()
        calls = []
        def body(instance):
            calls.append(True)
        own['driver'].source_admit = body
        with tempfile.TemporaryDirectory() as directory, patch.object(m, 'MAX_RECORDS', 1):
            with m.observe_fixed_owners(own, directory) as observer:
                with patch.object(observer, 'emit', side_effect=primary):
                    with self.assertRaises(KeyboardInterrupt) as caught:
                        own['driver']().source_admit()
                self.assertIs(caught.exception, primary)
            self.assertTrue(observer.closed)
        self.assertEqual(calls, [])

class BoundaryTests(unittest.TestCase):

    def test_byte_bound_and_unknown_method_before_effect(self):
        own = owners()
        effects = []

        def call(self):
            effects.append(True)
        own['driver'].source_admit = call
        with tempfile.TemporaryDirectory() as d:
            with m.observe_fixed_owners(own, d) as observer:
                with patch.object(m, 'MAX_BYTES', 512):
                    own['driver']().source_admit()
                with self.assertRaises(m.StageRefusal):
                    observer.invoke('sensitive-unknown', lambda : effects.append(True), (), {})
            self.assertEqual(effects, [True])

    def test_depth_bound_and_body_cancel_restore_every_owner(self):
        own = owners()
        effects = []
        primary = KeyboardInterrupt('sensitive')

        def outer(self):
            return self.source_admit()

        def inner(self):
            effects.append(True)
        own['driver'].validate_evolution_run = outer
        own['driver'].source_admit = inner
        original = inner
        with tempfile.TemporaryDirectory() as d, patch.object(m, 'MAX_DEPTH', 1):
            with self.assertRaises(KeyboardInterrupt) as caught:
                with m.observe_fixed_owners(own, d):
                    own['driver']().validate_evolution_run()
                    raise primary
            self.assertIs(caught.exception, primary)
            self.assertEqual(effects, [True])
            self.assertIs(own['driver'].source_admit, original)

    def test_actual_fsync_entry_and_terminal_precede_method_release(self):
        own = owners()
        events = []

        def call(self):
            events.append('method')
            return object()
        own['driver'].source_admit = call
        sync = m.os.fsync

        def observed(fd):
            events.append('fsync')
            return sync(fd)
        with tempfile.TemporaryDirectory() as d, patch.object(m.os, 'fsync', side_effect=observed):
            with m.observe_fixed_owners(own, d):
                own['driver']().source_admit()
        self.assertEqual(events, ['fsync', 'fsync', 'method', 'fsync'])
if __name__ == '__main__':
    unittest.main()

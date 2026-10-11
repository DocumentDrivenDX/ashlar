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

    def test_count_capacity_refuses_before_effect(self):
        own = owners()
        effects = []

        def call(self):
            effects.append(True)
        own['driver'].source_admit = call
        with tempfile.TemporaryDirectory() as d, patch.object(m, 'MAX_RECORDS', 1):
            with m.observe_fixed_owners(own, d):
                with self.assertRaises(m.StageRefusal):
                    own['driver']().source_admit()
        self.assertEqual(effects, [])

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

    def test_before_begin_failure_zero_effects_and_result_withheld_on_end_failure(self):
        own = owners()
        effects = []

        def call(self):
            effects.append(True)
            return object()
        own['driver'].source_admit = call
        with tempfile.TemporaryDirectory() as d:
            with m.observe_fixed_owners(own, d) as observer:
                original = observer.emit
                with patch.object(observer, 'emit', side_effect=OSError()):
                    with self.assertRaises(OSError):
                        own['driver']().source_admit()
                self.assertEqual(effects, [])

                def fail(row):
                    if row['event'] == 'end':
                        raise OSError()
                    original(row)
                with patch.object(observer, 'emit', side_effect=fail):
                    with self.assertRaises(OSError):
                        own['driver']().source_admit()
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

class BoundaryTests(unittest.TestCase):

    def test_byte_bound_and_unknown_method_before_effect(self):
        own = owners()
        effects = []

        def call(self):
            effects.append(True)
        own['driver'].source_admit = call
        with tempfile.TemporaryDirectory() as d:
            with m.observe_fixed_owners(own, d) as observer:
                with patch.object(m, 'MAX_BYTES', 1):
                    with self.assertRaises(m.StageRefusal):
                        own['driver']().source_admit()
                with self.assertRaises(m.StageRefusal):
                    observer.invoke('sensitive-unknown', lambda : effects.append(True), (), {})
            self.assertEqual(effects, [])

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
                    with self.assertRaises(m.StageRefusal):
                        own['driver']().validate_evolution_run()
                    raise primary
            self.assertIs(caught.exception, primary)
            self.assertEqual(effects, [])
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

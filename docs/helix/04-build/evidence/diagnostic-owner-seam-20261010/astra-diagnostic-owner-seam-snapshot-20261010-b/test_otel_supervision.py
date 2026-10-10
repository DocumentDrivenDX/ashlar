"""Actual private process/pipe controls, independent of SDK/receiver claims."""
from dataclasses import replace
import asyncio
import os
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch, Mock

from ashlar_host.otel import OtelRun, OtelStartupError, make_otel_run, unknown_loss
from ashlar_host.diagnostics import DiagnosticsError
import test_diagnostics_configuration as config_tests


CHILD = r'''
import json, os, struct, sys, time
def read():
    count = struct.unpack('>I', sys.stdin.buffer.read(4))[0]
    return json.loads(sys.stdin.buffer.read(count))
def write(value, ok=True):
    raw=json.dumps({'ok':ok,'value':value}).encode()
    sys.stdout.buffer.write(struct.pack('>I',len(raw))+raw)
    sys.stdout.buffer.flush()
request=read()
assert request['op']=='init'
assert not any(k.startswith('OTEL_') or 'proxy' in k.lower() for k in os.environ)
assert request['settings']['service_version']=='0.1.0.dev0'
write({'dependency_admitted':True})
write('ready')
while True:
    request=read()
    if request['op']=='context':
        if request['operation']=='block': time.sleep(20)
        write(None)
    elif request['op']=='close':
        write({k:{'submitted':0,'handed_off':0,'dropped':0,
                  'unknown':False,'flush':'complete'} for k in ('logs','spans','metrics')})
        break
    else: write(None)
'''


class SupervisionTests(unittest.TestCase):
    def test_startup_admission_refusal_and_post_admission_failure_are_distinct(self):
        for admitted in (False, True):
            child = CHILD.replace("write('ready')", "write('diagnostics-configuration',False); time.sleep(20)")
            if not admitted:
                child = child.replace("write({'dependency_admitted':True})", '')
            with self.assertRaises(OtelStartupError) as caught:
                self.create(child)
            error = caught.exception
            self.assertIs(error.dependency_admitted, admitted)
            self.assertIs(error.cleanup_complete, True)
            self.assertEqual(str(error), 'diagnostics-configuration')
            self.assertIsNone(error.__context__)
            self.assertIsNone(error.__cause__)
            self.assertTrue(all(child.poll() is not None for child in self.children))
            self.assertTrue(all(child.stdin.closed and child.stdout.closed for child in self.children))
            for field in ('cleanup_complete', 'dependency_admitted', '_cleanup_complete'):
                with self.assertRaises(AttributeError): setattr(error, field, False)
                with self.assertRaises(AttributeError): delattr(error, field)

    def test_missing_duplicate_or_false_admission_cannot_construct_run(self):
        for handshake in ('', "write({'dependency_admitted':False})",
                          "write({'dependency_admitted':True})\nwrite({'dependency_admitted':True})"):
            child = CHILD.replace("write({'dependency_admitted':True})", handshake)
            with self.assertRaises(OtelStartupError) as caught:
                self.create(child)
            self.assertTrue(caught.exception.cleanup_complete)
            self.assertFalse(caught.exception.dependency_admitted)
            self.assertTrue(all(child.poll() is not None for child in self.children))

    def test_failed_group_termination_cannot_become_positive_on_second_disposal(self):
        run = OtelRun.__new__(OtelRun)
        run._group_termination_attempted = False
        run._group_termination_complete = False
        run._cleanup_lock = threading.Lock()
        process = Mock(pid=1234567, returncode=0)
        run._process = process
        primary = OSError('private-primary')
        with patch('ashlar_host.otel.os.killpg', side_effect=PermissionError('private')) as kill:
            self.assertIs(run._dispose(primary=primary), False)
            self.assertIs(run._dispose(primary=primary), False)
            kill.assert_called_once()
        self.assertEqual(process.wait.call_count, 2)
        process.stdin.close.assert_called()
        process.stdout.close.assert_called()

    def test_cleanup_receipt_requires_pipes_reap_and_lock(self):
        for action in ('pipe', 'reap', 'lock'):
            run = OtelRun.__new__(OtelRun)
            run._group_termination_attempted = False
            run._group_termination_complete = False
            run._cleanup_lock = Mock()
            run._cleanup_lock.acquire.return_value = action != 'lock'
            process = Mock(pid=1234567, returncode=0)
            run._process = process
            if action == 'pipe': process.stdin.close.side_effect = OSError('private')
            if action == 'reap': process.wait.side_effect = subprocess.TimeoutExpired('private', 0)
            with patch('ashlar_host.otel.os.killpg'):
                self.assertIs(run._dispose(primary=OSError('primary')), False)

    def test_interleaved_async_operation_bindings_are_isolated_and_restored(self):
        run = self.create()
        requests = []
        async def interleave():
            entered = asyncio.Event()
            proceed = asyncio.Event()
            async def spanless():
                with run.operation_context(create_span=False):
                    entered.set()
                    await proceed.wait()
                    run.trace_context('1'*32, 'held-read', 'ashlar.operation.started')
            async def traced():
                await entered.wait()
                with run.operation_context():
                    run.trace_context('2'*32, 'held-read', 'ashlar.operation.started')
                    proceed.set()
                    await asyncio.sleep(0)
                    run.trace_context('3'*32, 'held-read', 'ashlar.operation.started')
            await asyncio.gather(spanless(), traced())
            run.trace_context('4'*32, 'held-read', 'ashlar.operation.started')
        try:
            with patch.object(run, '_invoke', side_effect=lambda request: requests.append(request)):
                asyncio.run(interleave())
            self.assertEqual([(request['attempt_id'][0], request['create_span']) for request in requests],
                             [('2', True), ('1', False), ('3', True), ('4', True)])
            run.shutdown(time.monotonic()+1)
        finally: run._dispose()

    def test_span_selection_exact_boolean_nested_restored_and_start_only(self):
        run = self.create()
        requests = []
        try:
            with patch.object(run, '_invoke', side_effect=lambda request: requests.append(request)):
                for value in (None, 0, 1, 'false'):
                    with self.assertRaises(DiagnosticsError):
                        with run.operation_context(create_span=value): pass
                for ports in ({'parent_context': object()}, {'retry_link': object()}):
                    with self.assertRaises(DiagnosticsError):
                        with run.operation_context(create_span=False, **ports): pass
                def start(): run.trace_context('1'*32, 'held-read', 'ashlar.operation.started')
                start()
                with run.operation_context(create_span=False):
                    start()
                    with run.operation_context(): start()
                    start()
                    run.trace_context('1'*32, 'held-read', 'ashlar.operation.phase')
                start()
                self.assertEqual([request.get('create_span') for request in requests],
                                 [True, False, True, False, None, True])
                self.assertNotIn('create_span', requests[-2])
                with run.operation_context(create_span=False):
                    other = threading.Thread(target=start)
                    other.start(); other.join(timeout=1)
                    self.assertFalse(other.is_alive())
                    start()
                self.assertEqual([request['create_span'] for request in requests[-2:]], [True, False])
            run.shutdown(time.monotonic()+1)
        finally: run._dispose()

    def configuration(self):
        original = config_tests.DiagnosticsConfigurationTests().configuration()
        return replace(original, limits=replace(original.limits,
                                               export_timeout_ms=100,
                                               shutdown_timeout_ms=1000))

    def create(self, child_source=CHILD):
        actual = subprocess.Popen
        self.children = []
        def start(argv, **kwargs):
            self.assertEqual(argv[-1], 'ashlar_host._otel_worker')
            self.assertEqual(kwargs['env'], {'PATH':'/usr/bin:/bin'})
            self.assertEqual(kwargs['stderr'], subprocess.DEVNULL)
            child = actual([sys.executable, '-I', '-B', '-c', child_source], **kwargs)
            self.children.append(child)
            return child
        with patch('ashlar_host.otel.subprocess.Popen', side_effect=start), \
             patch('ashlar_host.otel.metadata.version', return_value='0.1.0.dev0'), \
             patch.dict(os.environ, {'OTEL_SDK_DISABLED':'true', 'HTTPS_PROXY':'secret-sentinel'}):
            return OtelRun(self.configuration())

    def test_disabled_path_has_no_discovery_or_process(self):
        with patch('ashlar_host.otel.subprocess.Popen', side_effect=AssertionError), \
             patch('ashlar_host.otel.metadata.version', side_effect=AssertionError):
            self.assertIsNone(make_otel_run(None))

    def test_actual_clean_worker_exchange_and_reaping(self):
        run = self.create()
        try:
            self.assertIsNone(run.trace_context('1'*32, 'held-read', 'ashlar.operation.started'))
            loss = run.shutdown(time.monotonic() + 1)
            self.assertFalse(loss['logs']['unknown'])
            self.assertTrue(all(child.poll() is not None for child in self.children))
            self.assertTrue(run._process.stdin.closed and run._process.stdout.closed)
        finally:
            run._dispose()

    def test_blocked_response_is_killed_reaped_with_fixed_error(self):
        run = self.create()
        start = time.monotonic()
        try:
            with self.assertRaisesRegex(DiagnosticsError, '^diagnostics-configuration$'):
                run.trace_context('1'*32, 'block', 'ashlar.operation.started')
            self.assertLess(time.monotonic() - start, 1)
            self.assertIsNotNone(run._process.poll())
            self.assertEqual(run.shutdown(time.monotonic()+1), unknown_loss())
        finally:
            run._dispose()

    def test_busy_port_refuses_without_queue_or_interleaved_frames(self):
        run = self.create()
        run._lock.acquire()
        try:
            with self.assertRaises(DiagnosticsError):
                run.trace_context('1'*32, 'held-read', 'ashlar.operation.started')
            self.assertIsNone(run._process.poll())
        finally:
            run._lock.release()
            run._dispose()

    def test_per_request_progress_bounds_blocked_close_before_total_deadline(self):
        child = CHILD.replace("elif request['op']=='close':",
                              "elif request['op']=='close':\n"
                              "        write({'transport_deadline':time.monotonic()+.05})\n"
                              "        time.sleep(20)")
        run = self.create(child)
        start = time.monotonic()
        try:
            self.assertEqual(run.shutdown(start+1), unknown_loss())
            self.assertLess(time.monotonic()-start, .5)
            self.assertIsNotNone(run._process.poll())
        finally:
            run._dispose()

    def test_progress_completion_restores_shared_close_budget(self):
        child = CHILD.replace("elif request['op']=='close':",
                              "elif request['op']=='close':\n"
                              "        write({'transport_deadline':time.monotonic()+.02})\n"
                              "        write({'transport_complete':True})\n"
                              "        time.sleep(.04)")
        run = self.create(child)
        try:
            self.assertFalse(run.shutdown(time.monotonic()+1)['logs']['unknown'])
            self.assertIsNotNone(run._process.poll())
        finally:
            run._dispose()

    def test_original_cancellation_survives_cleanup_error(self):
        run = self.create()
        cancellation = KeyboardInterrupt('private-sentinel')
        try:
            with patch.object(run, '_exchange', side_effect=cancellation), \
                 patch.object(run._process.stdin, 'close', side_effect=OSError('other-sentinel')):
                with self.assertRaises(KeyboardInterrupt) as raised:
                    run.trace_context('1'*32, 'held-read', 'ashlar.operation.started')
                self.assertIs(raised.exception, cancellation)
                self.assertTrue(cancellation.cleanup_failed)
                self.assertIsNotNone(run._process.poll())
        finally:
            run._dispose()

    def test_explicit_sdk_parent_is_sent_only_when_attempt_starts(self):
        try:
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.resources import Resource
        except ImportError:
            self.skipTest('selected optional SDK not installed')
        provider = TracerProvider(resource=Resource({}), shutdown_on_exit=False)
        span = provider.get_tracer('supervision-control').start_span('parent')
        context = span.get_span_context()
        child = CHILD.replace("if request['operation']=='block': time.sleep(20)",
                              "if request['event_name']=='ashlar.operation.started':\n"
                              "            assert request['parent']['trace_id']=='"+format(context.trace_id,'032x')+"'\n"
                              "            assert request['retry_link'] is None\n"
                              "            assert request['create_span'] is True\n"
                              "        else:\n"
                              "            assert request['parent'] is None and request['retry_link'] is None\n"
                              "            assert 'create_span' not in request")
        run = self.create(child)
        try:
            with run.operation_context(parent_context=context):
                for event in ('ashlar.operation.started', 'ashlar.operation.phase',
                              'ashlar.operation.finished'):
                    self.assertIsNone(run.trace_context('1'*32, 'held-read', event))
            self.assertFalse(run.shutdown(time.monotonic()+1)['spans']['unknown'])
        finally:
            run._dispose()
            span.end()
            provider.shutdown()

    def test_cleanup_cancellation_survives_ordinary_diagnostic_failure(self):
        run = self.create()
        cancellation = SystemExit('private-sentinel')
        try:
            with patch.object(run, '_exchange', side_effect=OSError('other-sentinel')), \
                 patch.object(run._process.stdin, 'close', side_effect=cancellation):
                with self.assertRaises(SystemExit) as raised:
                    run.trace_context('1'*32, 'held-read', 'ashlar.operation.started')
                self.assertIs(raised.exception, cancellation)
                self.assertIsNotNone(run._process.poll())
        finally:
            run._dispose()

    def test_selector_cleanup_preserves_original_cancellation(self):
        run = self.create()
        cancellation = KeyboardInterrupt('private-sentinel')
        selector = Mock()
        selector.select.side_effect = cancellation
        selector.close.side_effect = OSError('other-sentinel')
        try:
            with patch('ashlar_host.otel.selectors.DefaultSelector', return_value=selector):
                with self.assertRaises(KeyboardInterrupt) as raised:
                    run.trace_context('1'*32, 'held-read', 'ashlar.operation.started')
                self.assertIs(raised.exception, cancellation)
                self.assertTrue(cancellation.cleanup_failed)
                self.assertIsNotNone(run._process.poll())
        finally:
            run._dispose()

    def test_group_cleanup_precedes_reaping_and_is_one_shot(self):
        run = OtelRun.__new__(OtelRun)
        run._closed = False
        run._group_termination_attempted = False
        run._cleanup_lock = threading.Lock()
        process = Mock(pid=1234567, returncode=0)
        run._process = process
        order = []
        process.wait.side_effect = lambda **kwargs: order.append('reap')
        with patch('ashlar_host.otel.os.killpg', side_effect=lambda *args: order.append('group')) as kill:
            run._dispose()
            run._dispose()
            self.assertEqual(order, ['group','reap','reap'])
            kill.assert_called_once_with(process.pid, 9)
            process.poll.assert_not_called()

    def test_concurrent_cleanup_cannot_reap_before_group_action(self):
        run = OtelRun.__new__(OtelRun)
        run._closed = False
        run._group_termination_attempted = False
        run._cleanup_lock = threading.Lock()
        process = Mock(pid=1234567, returncode=0)
        run._process = process
        entered = threading.Event()
        release = threading.Event()
        order = []
        failures = []
        def kill(*args):
            order.append('kill-entered')
            entered.set()
            if not release.wait(1):
                raise AssertionError('finite control timed out')
            order.append('kill-complete')
        process.wait.side_effect = lambda **kwargs: order.append('reap')
        def cleanup():
            try:
                run._dispose(time.monotonic()+1)
            except BaseException as exc:
                failures.append(exc)
        with patch('ashlar_host.otel.os.killpg', side_effect=kill) as killed:
            first = threading.Thread(target=cleanup)
            second = threading.Thread(target=cleanup)
            first.start()
            try:
                self.assertTrue(entered.wait(1))
                second.start()
                time.sleep(.01)
                self.assertEqual(order, ['kill-entered'])
            finally:
                release.set()
                first.join(1)
                if second.ident is not None:
                    second.join(1)
            self.assertFalse(first.is_alive() or second.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(order, ['kill-entered','kill-complete','reap','reap'])
            killed.assert_called_once()


if __name__ == '__main__':
    unittest.main()

"""Finite C006 local capture/port controls; no SDK or receiver qualification."""
import copy
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from ashlar_host.config import DiagnosticsConfig, DiagnosticsLimits, SecretText
from ashlar_host.diagnostics import (DiagnosticRun, DiagnosticSignalSink,
    DiagnosticsError, read_diagnostics, validate_event, validate_manifest)


class Sink:
    def __init__(self):
        self.events = []
        self.closed = False
        self.trace = None

    def emit(self, raw):
        assert type(raw) is bytes
        self.events.append(raw)

    def context(self, attempt, operation, name):
        return self.trace

    def shutdown(self, deadline):
        self.closed = True
        return {signal: {'submitted': len(self.events) if signal == 'logs' else 0,
                         'handed_off': len(self.events) if signal == 'logs' else 0,
                         'dropped': 0, 'unknown': False, 'flush': 'complete'}
                for signal in ('logs', 'spans', 'metrics')}

    def port(self):
        return DiagnosticSignalSink(self.emit, self.context, self.shutdown)


class DiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.root.chmod(0o700)
        self.limits = DiagnosticsLimits(4096, 128, 524288, 4096, 16384,
                                       10000, 64, 100, 200, 60)
        leaves = ('profile','capture_root','endpoint','headers','tls','ca_file','environment')
        origins = dict.fromkeys(leaves, 'explicit')
        origins.update({'limits.' + key: 'explicit' for key in self.limits.__dataclass_fields__})
        self.config = DiagnosticsConfig('ashlar-host-otel-http/0.1', self.root,
            SecretText('http://localhost:4318/'), (('authorization',SecretText('sentinel-secret')),),
            'loopback-test', None, 'test', self.limits, origins)
        self.version = patch('ashlar_host.diagnostics.metadata.version', return_value='0.1.0.dev0')
        self.stderr = patch('ashlar_host.diagnostics.sys.stderr', new_callable=io.StringIO)
        self.console = self.stderr.start()
        self.addCleanup(self.stderr.stop)
        self.version.start()
        self.addCleanup(self.version.stop)
        self.addCleanup(self.temporary.cleanup)

    def run_owner(self, **kwargs):
        self.sink = Sink()
        return DiagnosticRun(replace(self.config, **kwargs), self.sink.port())

    def test_constructor_io_refusals_are_safe_and_cancellation_is_exact(self):
        ports=('ashlar_host.diagnostics.Path.lstat','ashlar_host.diagnostics.Path.mkdir',
               'ashlar_host.diagnostics.os.open','ashlar_host.diagnostics.os.write')
        for port in ports:
            with self.subTest(port=port):
                with patch(port,side_effect=OSError('private-path-secret')):
                    with self.assertRaises(DiagnosticsError) as caught:self.run_owner()
                self.assertEqual(str(caught.exception),'diagnostics-configuration')
                self.assertIsNone(caught.exception.__cause__)
                primary=KeyboardInterrupt()
                with patch(port,side_effect=primary):
                    with self.assertRaises(BaseException) as cancelled:self.run_owner()
                self.assertIs(cancelled.exception,primary)

    def test_success_trace_filters_private_bytes_and_exact_manifest(self):
        run = self.run_owner()
        self.sink.trace = {'trace_id': 'a'*32, 'span_id':'b'*16, 'trace_flags':255}
        attempt = run.begin_attempt('held-read')
        run.phase(attempt, 'guard', 'completed')
        run.finish_attempt(attempt, 'succeeded')
        manifest = run.close()
        self.assertTrue(self.sink.closed)
        self.assertTrue(manifest['complete'])
        validate_manifest(manifest)
        result = read_diagnostics(run.directory, event_name='ashlar.operation.finished')
        self.assertEqual(result['matched'], 1)
        self.assertEqual(result['records'][0]['event']['trace']['trace_flags'],255)
        for raw in self.sink.events:
            validate_event(json.loads(raw))
            self.assertNotIn(b'sentinel-secret', raw)
            self.assertNotIn(b'localhost', raw)
            self.assertNotIn(str(self.root).encode(),raw)
        self.assertEqual(os.stat(run.directory).st_mode & 0o777,0o700)
        for path in run.directory.iterdir():self.assertEqual(path.stat().st_mode & 0o777,0o600)
        self.assertEqual(result['manifest_sha256'],hashlib.sha256((run.directory/'run.json').read_bytes()).hexdigest())

    def test_fixed_catalog_and_forged_attempt_refuse_before_sink(self):
        run = self.run_owner()
        attempt = run.begin_attempt('publication')
        before = len(self.sink.events)
        with self.assertRaises(DiagnosticsError):run.phase(attempt,'secret-query-text','completed')
        with self.assertRaises(DiagnosticsError):run.finish_attempt(copy.copy(attempt),'succeeded')
        self.assertEqual(len(self.sink.events),before)
        run.finish_attempt(attempt,'refused','guard')
        run.close()
        self.assertEqual(read_diagnostics(run.directory,min_severity=13)['matched'],1)

    def test_emission_loss_gaps_and_empty_filter_not_completeness(self):
        run = self.run_owner(limits=replace(self.limits,max_emissions=2))
        attempt = run.begin_attempt('publication')
        run.phase(attempt,'prepare','completed')
        run.finish_attempt(attempt,'succeeded')
        manifest = run.close()
        self.assertEqual(manifest['loss']['local_attempted'],3)
        self.assertEqual(manifest['loss']['local_written'],2)
        self.assertEqual(manifest['loss']['local_dropped'],1)
        self.assertFalse(manifest['complete'])
        result=read_diagnostics(run.directory,event_name='ashlar.operation.finished')
        self.assertEqual(result['matched'],0)
        self.assertFalse(result['capture_complete'])

    def test_four_segment_rotation_and_total_bound(self):
        run = self.run_owner()
        attempt=run.begin_attempt('publication')
        for _ in range(40):run.phase(attempt,'guard','completed')
        run.finish_attempt(attempt,'succeeded');manifest=run.close()
        self.assertLessEqual(len(manifest['segments']),4)
        self.assertLessEqual(sum(s['bytes']for s in manifest['segments']),16384)
        self.assertGreater(manifest['loss']['local_dropped'],0)
        self.assertEqual(read_diagnostics(run.directory,limit=1)['matched'],manifest['loss']['local_written'])
        self.assertTrue(read_diagnostics(run.directory,limit=1)['truncated'])

    def test_partial_segment_write_invalidates_prior_records(self):
        run=self.run_owner();attempt=run.begin_attempt('held-read')
        real_write=os.write
        def partial(fd,raw):
            real_write(fd,raw[:5]);raise OSError('payload-must-not-escape')
        with patch('ashlar_host.diagnostics.os.write',side_effect=partial):run.phase(attempt,'guard','completed')
        run.finish_attempt(attempt,'failed','io');manifest=run.close()
        self.assertEqual(manifest['loss']['local_dropped'],2)
        self.assertEqual(manifest['loss']['local_written'],1)
        self.assertEqual(read_diagnostics(run.directory)['matched'],1)

    def test_open_missing_expired_hash_and_symlink_refusal(self):
        run=self.run_owner()
        with self.assertRaisesRegex(DiagnosticsError,'diagnostics-open'):read_diagnostics(run.directory)
        attempt=run.begin_attempt('held-read');run.finish_attempt(attempt,'succeeded');run.close()
        with patch('ashlar_host.diagnostics.time.time_ns',return_value=2**64):
            with self.assertRaisesRegex(DiagnosticsError,'diagnostics-expired'):read_diagnostics(run.directory)
        segment=run.directory/'events-0000.jsonl';original=segment.read_bytes();segment.write_bytes(original+b' ')
        with self.assertRaises(DiagnosticsError):read_diagnostics(run.directory)
        segment.unlink();segment.symlink_to(self.root/'missing')
        with self.assertRaises(DiagnosticsError):read_diagnostics(run.directory)
        with self.assertRaisesRegex(DiagnosticsError,'diagnostics-unavailable'):read_diagnostics(self.root/'missing')

    def test_primary_and_cleanup_only_cancellation_identity(self):
        class Cancel(KeyboardInterrupt):
            def __setattr__(self,name,value):
                if name=='cleanup_failed':raise RuntimeError('marker refused')
                super().__setattr__(name,value)
        for primary in (Cancel(),None):
            run=self.run_owner();attempt=run.begin_attempt('publication');run.finish_attempt(attempt,'cancelled','cleanup')
            cancellation=GeneratorExit()
            with patch.object(self.sink,'shutdown',side_effect=cancellation):
                # Port is captured at construction, replace immutable owning port explicitly.
                run.sink=DiagnosticSignalSink(self.sink.emit,self.sink.context,self.sink.shutdown)
                with self.assertRaises(BaseException)as caught:run.close(primary)
            self.assertIs(caught.exception,primary if primary is not None else cancellation)
            self.assertEqual(read_diagnostics(run.directory)['loss']['logs']['flush'],'failed')

    def test_sink_mutation_cannot_change_local_event_and_shutdown_snapshot(self):
        run=self.run_owner();trace={'trace_id':'a'*32,'span_id':'b'*16,'trace_flags':1};self.sink.trace=trace
        attempt=run.begin_attempt('publication');trace['trace_id']='c'*32
        run.finish_attempt(attempt,'succeeded');run.close()
        self.assertEqual(read_diagnostics(run.directory)['records'][0]['event']['trace']['trace_id'],'a'*32)

    def test_concurrent_attempts_sequence_and_no_duplicate_finish(self):
        run=self.run_owner();attempts=[]
        def worker():
            a=run.begin_attempt('source-admission');run.finish_attempt(a,'succeeded');attempts.append(a)
        threads=[threading.Thread(target=worker)for _ in range(4)]
        for thread in threads:thread.start()
        for thread in threads:thread.join()
        with self.assertRaises(DiagnosticsError):run.finish_attempt(attempts[0],'succeeded')
        run.close();result=read_diagnostics(run.directory)
        self.assertEqual([r['event']['attributes']['ashlar.sequence']for r in result['records']],list(range(1,9)))
        self.assertEqual(len({a.attempt_id for a in attempts}),4)

    def test_reader_duplicate_numeric_and_closed_mutation_controls(self):
        run=self.run_owner();run.close();path=run.directory/'run.json';original=path.read_bytes()
        for invalid in (b'{"x":1,"x":1}',b'{"x":'+b'9'*10000+b'}',b'{"x":1.1}'):
            path.write_bytes(invalid)
            with self.assertRaises(DiagnosticsError):read_diagnostics(run.directory)
        path.write_bytes(original)
        from ashlar_host import diagnostics
        real_read=diagnostics._read;calls=[0]
        def changed(p,limit):
            calls[0]+=1
            if calls[0]==2:path.write_bytes(original+b' ')
            return real_read(p,limit)
        with patch('ashlar_host.diagnostics._read',changed):
            with self.assertRaises(DiagnosticsError):read_diagnostics(run.directory)

    def test_final_manifest_failure_stays_open_and_primary_survives(self):
        run=self.run_owner();a=run.begin_attempt('held-read');run.finish_attempt(a,'succeeded')
        primary=KeyboardInterrupt()
        with patch('ashlar_host.diagnostics.os.replace',side_effect=OSError('secret')):
            with self.assertRaises(BaseException)as caught:run.close(primary)
        self.assertIs(caught.exception,primary)
        self.assertTrue(primary.cleanup_failed)
        self.assertEqual(json.loads((run.directory/'run.json').read_bytes())['state'],'open')
        with self.assertRaisesRegex(DiagnosticsError,'diagnostics-open'):read_diagnostics(run.directory)

    def test_final_publication_ordinary_failure_does_not_reclassify(self):
        run=self.run_owner();a=run.begin_attempt('held-read');run.finish_attempt(a,'succeeded')
        with patch('ashlar_host.diagnostics.os.replace',side_effect=OSError('private-payload')):
            self.assertIsNone(run.close())
        self.assertEqual(json.loads((run.directory/'run.json').read_bytes())['state'],'open')
        self.assertNotIn('private-payload',self.console.getvalue())
        with self.assertRaisesRegex(DiagnosticsError,'diagnostics-open'):read_diagnostics(run.directory)

    def test_undeclared_segment_requires_declared_loss(self):
        run=self.run_owner();a=run.begin_attempt('held-read');run.finish_attempt(a,'succeeded');m=run.close()
        extra=run.directory/'events-0001.jsonl';extra.write_bytes(b'partial');extra.chmod(0o600)
        with self.assertRaises(DiagnosticsError):read_diagnostics(run.directory)
        m['complete']=False;m['loss']['local_attempted']+=1;m['loss']['local_dropped']+=1
        (run.directory/'run.json').write_text(json.dumps(m))
        self.assertFalse(read_diagnostics(run.directory)['capture_complete'])

    def test_started_after_phase_refuses_even_with_loss(self):
        for complete in (True,False):
            run=self.run_owner();a=run.begin_attempt('held-read');run.phase(a,'guard','completed');run.finish_attempt(a,'succeeded');m=run.close()
            path=run.directory/m['segments'][0]['source'];events=[json.loads(v) for v in path.read_bytes().splitlines()]
            events[0],events[1]=events[1],events[0]
            for n,e in enumerate(events,1):e['attributes']['ashlar.sequence']=n
            raw=b''.join(json.dumps(e,separators=(',',':')).encode()+b'\n' for e in events);path.write_bytes(raw)
            m['segments'][0].update(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
            if not complete:
                m['complete']=False;m['loss']['local_attempted']+=1;m['loss']['local_dropped']+=1
            (run.directory/'run.json').write_text(json.dumps(m))
            with self.assertRaises(DiagnosticsError):read_diagnostics(run.directory)

    def test_unknown_export_counts_and_fixed_notice_once(self):
        run=self.run_owner();a=run.begin_attempt('publication');run.finish_attempt(a,'succeeded')
        with patch.object(self.sink,'shutdown',side_effect=OSError('sentinel-secret')):
            run.sink=DiagnosticSignalSink(self.sink.emit,self.sink.context,self.sink.shutdown)
            manifest=run.close()
        self.assertTrue(manifest['complete'])  # local capture, never remote delivery
        self.assertIsNone(manifest['loss']['logs']['submitted'])
        self.assertTrue(manifest['loss']['logs']['unknown'])
        self.assertEqual(read_diagnostics(run.directory)['loss'],manifest['loss'])
        self.assertEqual(self.console.getvalue().count('ashlar diagnostics incomplete'),1)
        self.assertNotIn('sentinel-secret',self.console.getvalue())

    def test_snapshot_rechecks_earlier_segment_after_later_reads(self):
        run=self.run_owner();a=run.begin_attempt('publication')
        for _ in range(15):run.phase(a,'guard','completed')
        run.finish_attempt(a,'succeeded');manifest=run.close()
        self.assertGreaterEqual(len(manifest['segments']),2)
        from ashlar_host import diagnostics
        actual=diagnostics._read;seen=[0]
        def drift(path,limit):
            result=actual(path,limit)
            if path.name=='events-0001.jsonl':
                seen[0]+=1
                if seen[0]==1:
                    first=run.directory/'events-0000.jsonl'
                    raw=first.read_bytes();first.write_bytes(raw.replace(b'Operation started',b'Operation changed',1))
            return result
        with patch('ashlar_host.diagnostics._read',side_effect=drift):
            with self.assertRaises(DiagnosticsError):read_diagnostics(run.directory)

    def test_real_offline_schema_correspondence(self):
        try:
            from jsonschema import Draft202012Validator
            from referencing import Registry,Resource
        except ImportError:self.skipTest('Optional real schema test environment unavailable')
        root=Path(__file__).resolve().parents[1]/'docs/helix/02-design/contracts/schemas'
        event=json.loads((root/'diagnostic-event-v0.1.schema.json').read_bytes());run_schema=json.loads((root/'diagnostic-run-v0.1.schema.json').read_bytes())
        registry=Registry().with_resources([(event['$id'],Resource.from_contents(event)),(run_schema['$id'],Resource.from_contents(run_schema))])
        run=self.run_owner();a=run.begin_attempt('publication');run.finish_attempt(a,'succeeded');manifest=run.close()
        Draft202012Validator(run_schema,registry=registry).validate(manifest)
        for raw in self.sink.events:Draft202012Validator(event,registry=registry).validate(json.loads(raw))


if __name__=='__main__':unittest.main()

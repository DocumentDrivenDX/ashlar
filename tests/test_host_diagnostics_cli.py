"""Source CLI capture/retrieval and wiring controls; no installed/native claim."""
from contextlib import contextmanager
from dataclasses import asdict, replace
from pathlib import Path
import builtins
import io
import json
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ashlar.cli import main
from ashlar_host.config import load_diagnostics_config
from ashlar_host.diagnostics import DiagnosticRun, DiagnosticSignalSink, read_diagnostics
import test_diagnostics_configuration as fixtures


class DiagnosticCliTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name).resolve()
        self.config=replace(fixtures.DiagnosticsConfigurationTests().configuration(),capture_root=self.root)

    def invoke(self, args):
        binary=io.BytesIO();out=io.TextIOWrapper(binary,encoding='utf8',write_through=True)
        err=io.StringIO();code=0
        with patch.object(sys,'argv',['ashlar']+args),patch.object(sys,'stdout',out),patch.object(sys,'stderr',err):
            try:main()
            except SystemExit as error:code=error.code
        out.flush();raw=binary.getvalue();out.close()
        return code,raw,err.getvalue()

    def capture(self, close=True):
        events=[]
        def shutdown(deadline):
            return {name:{'submitted':len(events)if name=='logs'else 0,
                'handed_off':len(events)if name=='logs'else 0,'dropped':0,'unknown':False,'flush':'complete'}for name in ('logs','spans','metrics')}
        sink=DiagnosticSignalSink(events.append,lambda *args:None,shutdown)
        with patch('ashlar_host.diagnostics.metadata.version',return_value='0.1.0.dev0'),patch.object(sys,'stderr',io.StringIO()):
            run=DiagnosticRun(self.config,sink)
            attempts=[]
            for operation,outcome in (('held-read','succeeded'),('publication','refused'),('source-admission','succeeded')):
                attempt=run.begin_attempt(operation);attempts.append(attempt)
                run.phase(attempt,'guard','completed')
                run.finish_attempt(attempt,outcome,None if outcome=='succeeded'else 'guard')
            if close:run.close()
        return run,attempts

    def test_public_reader_equivalence_filters_zero_and_truncation_without_discovery(self):
        run,attempts=self.capture()
        cases=[([],{}),(['--limit','1'],{'limit':1}),
            (['--attempt-id',attempts[1].attempt_id,'--min-severity','13','--event-name','ashlar.operation.finished'],
             {'attempt_id':attempts[1].attempt_id,'min_severity':13,'event_name':'ashlar.operation.finished'}),
            (['--min-severity','17','--event-name','ashlar.operation.phase'],{'min_severity':17,'event_name':'ashlar.operation.phase'})]
        for args,kwargs in cases:
            expected=read_diagnostics(run.directory,**kwargs)
            with patch('ashlar_host.diagnostics.metadata.version',side_effect=AssertionError('metadata discovery')):
                code,raw,err=self.invoke(['diagnostics','--run-directory',str(run.directory)]+args)
            self.assertEqual(code,0);self.assertEqual(err,'')
            self.assertEqual(json.loads(raw),expected);self.assertTrue(raw.endswith(b'\n'))
            self.assertLessEqual(len(raw),524288)
        self.assertTrue(read_diagnostics(run.directory,limit=1)['truncated'])
        self.assertEqual(read_diagnostics(run.directory,min_severity=17)['matched'],0)

    def test_open_malformed_changed_and_invalid_filter_release_no_stdout(self):
        run,_=self.capture(close=False)
        args=['diagnostics','--run-directory',str(run.directory)]
        code,raw,err=self.invoke(args)
        self.assertEqual((code,raw,err),(2,b'','ashlar diagnostics refused\n'))
        with patch.object(sys,'stderr',io.StringIO()):run.close()
        manifest=run.directory/'run.json';original=manifest.read_bytes()
        manifest.write_bytes(b'{}')
        self.assertEqual(self.invoke(args),(2,b'','ashlar diagnostics refused\n'))
        manifest.write_bytes(original)
        segment=next(run.directory.glob('events-*.jsonl'));segment_original=segment.read_bytes();segment.write_bytes(segment_original+b' ')
        self.assertEqual(self.invoke(args),(2,b'','ashlar diagnostics refused\n'))
        segment.write_bytes(segment_original)
        self.assertEqual(self.invoke(args+['--limit','0']),(2,b'','ashlar diagnostics refused\n'))

    def query_args(self):
        args=['query-commerce-paths']
        values=dict(output=str(self.root/'business-output'),jars='/explicit/jars',model='/explicit/model',graph='/explicit/graph',
            umf_source='/explicit/umf',bun='/explicit/bun',git='/explicit/git',postgres_container='ashlar-e2e-truss-pg17',
            postgres_host='127.0.0.1',postgres_database='truss_e2e',postgres_port='15432',producer_timeout_seconds='20',
            producer_maximum_output_bytes='1048576',producer_maximum_receipt_bytes='4194304',index='/explicit/index',
            installation='/explicit/installation',publication='/explicit/publication',maximum_artifact_bytes='1048576',
            maximum_rows='100',maximum_cell_bytes='65536',maximum_total_cell_bytes='1048576')
        for name,value in values.items():args.extend(['--'+name.replace('_','-'),value])
        return args

    def selected_file(self):
        values=dict(profile=self.config.profile,capture_root=str(self.root),endpoint='http://127.0.0.1:4318/',headers=[],
                    tls='loopback-test',ca_file=None,environment='test',limits=asdict(self.config.limits))
        path=self.root/'selected.json';path.write_text(json.dumps(values));return path

    def test_selected_loader_composition_query_closure_console_order(self):
        selected=self.selected_file();order=[];identity=object()
        def loader(path):order.append('load');return load_diagnostics_config(path)
        @contextmanager
        def composition(config):
            order.append('enter')
            try:yield identity
            finally:
                self.assertEqual(sys.stdout.buffer.getvalue(),b'')
                order.append('close')
        def query(config,*,diagnostics):
            self.assertIs(diagnostics,identity);order.append('query')
        with patch('ashlar_host.config.load_diagnostics_config',side_effect=loader),patch('ashlar_host.diagnostic_composition.diagnostic_run',composition),patch('ashlar_host.paths_query.query_commerce_paths',side_effect=query):
            code,raw,err=self.invoke(self.query_args()+['--diagnostics-config',str(selected)])
        self.assertEqual(order,['load','enter','query','close'])
        self.assertEqual(code,0);self.assertEqual(err,'');self.assertTrue(raw.startswith(b'ashlar-host: report '))

    def test_absent_selection_avoids_composition_import_and_metadata(self):
        actual=builtins.__import__
        def imports(name,*args,**kwargs):
            if name=='ashlar_host.diagnostic_composition' or name.split('.')[0] in ('opentelemetry','pyspark','delta','psycopg'):
                raise AssertionError('disabled composition/native/SDK import')
            return actual(name,*args,**kwargs)
        with patch('builtins.__import__',side_effect=imports),patch('ashlar_host.config.load_diagnostics_config',side_effect=AssertionError),patch('ashlar_host.diagnostics.metadata.version',side_effect=AssertionError),patch('ashlar_host.paths_query.query_commerce_paths')as query:
            code,raw,err=self.invoke(self.query_args())
        self.assertEqual(code,0);self.assertEqual(err,'');query.assert_called_once()
        self.assertEqual(query.call_args.kwargs,{})

    def test_invalid_selected_file_refuses_before_composition_or_query(self):
        selected=self.selected_file();selected.write_text('{"endpoint":"synthetic-private-sentinel"}')
        with patch('ashlar_host.diagnostic_composition.diagnostic_run')as composition,patch('ashlar_host.paths_query.query_commerce_paths')as query:
            code,raw,err=self.invoke(self.query_args()+['--diagnostics-config',str(selected)])
        self.assertEqual((code,raw,err),(2,b'','ashlar-host: refused\n'))
        composition.assert_not_called();query.assert_not_called()

    def test_retrieval_short_write_reports_failure_without_inventing_atomic_output(self):
        run,_=self.capture()
        class ShortWriter:
            def __init__(self):self.raw=b''
            def write(self,raw):self.raw=raw[:7];return 7
            def flush(self):raise AssertionError('refused write must not flush as success')
        writer=ShortWriter();err=io.StringIO()
        with patch.object(sys,'argv',['ashlar','diagnostics','--run-directory',str(run.directory)]),patch.object(sys,'stdout',SimpleNamespace(buffer=writer)),patch.object(sys,'stderr',err):
            with self.assertRaises(SystemExit)as caught:main()
        self.assertEqual(caught.exception.code,2)
        self.assertEqual(writer.raw,b'{"schem')
        self.assertEqual(err.getvalue(),'ashlar diagnostics refused\n')

    def test_retrieval_output_cancellation_retains_identity_without_worker_discovery(self):
        run,_=self.capture()
        for cancellation in (KeyboardInterrupt(),SystemExit(),GeneratorExit()):
            class Writer:
                def write(self,raw):raise cancellation
                def flush(self):raise AssertionError('cancelled write must not flush')
            with patch.object(sys,'argv',['ashlar','diagnostics','--run-directory',str(run.directory)]),patch.object(sys,'stdout',SimpleNamespace(buffer=Writer())),patch.object(sys,'stderr',io.StringIO()),patch('ashlar_host.diagnostics.metadata.version',side_effect=AssertionError('metadata discovery')),patch('ashlar_host.diagnostic_composition.OtelRun',side_effect=AssertionError('worker construction')):
                with self.assertRaises(BaseException)as caught:main()
            self.assertIs(caught.exception,cancellation)


if __name__=='__main__':unittest.main()

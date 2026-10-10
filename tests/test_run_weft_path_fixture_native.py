"""Source-only composition/lifecycle controls; all native ports are mocked."""
from contextlib import contextmanager
from dataclasses import replace
import json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
import run_weft_path_fixture_native as module
from weft_path_fixture import fixture_inputs,expected_cases
from weft_path_compiler import PathCompilerConfig
from weft_path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig
from test_weft_path_plan import SCHEMAS
from weft_path_plan import PathSchemaValidation
from test_weft_path_fixture_request import RECEIPT

class FixtureNativeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name).resolve();self.addCleanup(self.temp.cleanup)
        self.publication=self.root/'publication';self.publication.mkdir();self.m,self.g,self.r=fixture_inputs()
        aliases={f'local.authored_paths.{r}':f'spark_catalog.authored_paths.{r}' for r in ['object_current','edge_current','tombstone','whole_source_history']}
        manifest={'publication_id':module.PUBLICATION_ID,'table_versions_json':json.dumps({t:0 for t in aliases})}
        registry=[{'table':t,'uuid':f'20000000-0000-0000-0000-{i+1:012d}','path':str(self.publication/t.split('.')[-1])} for i,t in enumerate(aliases)]
        registry.append({'table':'local.authored_paths.manifest','uuid':'10000000-0000-0000-0000-000000000002','path':str(self.publication/'manifest')})
        self.report={'table_registry':registry};self.files={'file':{'sha256':'abc','bytes':1}}
        for name,raw in [('model.umf.json',self.m),('graph.json',self.g),('registry.json',self.r),('public-dataset.json',RECEIPT),('source.jsonl',b'original'),('report.json',json.dumps(self.report).encode())]:(self.publication/name).write_bytes(raw)
        schema_paths=[]
        for name,raw in SCHEMAS.items():p=self.root/name;p.write_bytes(raw);schema_paths.append(p)
        self.jars=self.root/'jars';self.jars.mkdir();self.jar=self.jars/'selected.jar';self.jar.write_bytes(b'qualified fixture')
        for n,raw in [('model',self.m),('graph',self.g),('registry',self.r)]:(self.root/n).write_bytes(raw)
        self.config=module.FixtureNativeConfig(self.publication,self.root/'query',self.root/'model',self.root/'graph',self.root/'registry',self.root/'producer',self.root/'umf',self.root/'bun',self.jars,tuple(schema_paths),PathCompilerConfig(self.root/'compiler',16000000,16000000,30),PathCaptureConfig(100,100000,1000000),PathDecodeConfig(100000),16000000,16000000,1000,10000000,module.ACK)
        self.spark=Mock(version='4.0.1');self.provider=Mock(active=False)
        @contextmanager
        def interval(context):yield
        self.provider.interval=interval;self.provider.closed_interval_custody.return_value={'mock':'closed'}
        self.opened=SimpleNamespace(provider=self.provider,context=object(),model=self.m,graph=self.g,registry=self.r,manifest=manifest,original_report=self.report,aliases=aliases,
            original_native_files=self.files,native_files=lambda:dict(self.files),driver=SimpleNamespace(tables={'manifest':'local.authored_paths.manifest'},transport=SimpleNamespace(targets={t:SimpleNamespace(path=self.publication/t.split('.')[-1]) for t in aliases})))
    def run_query(self,*,body_error=None,close_error=None,stop_error=None,suppress_reader=False):
        @contextmanager
        def reader(*args):
            try:yield self.opened
            except BaseException:
                if not suppress_reader:raise
            finally:
                if close_error:raise close_error
        calls=[]
        def execute(opened,request,artifact,recompiled,**kwargs):
            if body_error:raise body_error
            oracle=kwargs['original_oracle'](self.m,self.g,request);calls.append((request['sql'],oracle))
            return {'mock':'provisional','oracle':oracle}
        self.spark.stop.side_effect=stop_error
        with patch.object(module,'make_offline_path_schema_validation',side_effect=lambda s:PathSchemaValidation(s,lambda *a:None)),patch.object(module,'preflight',return_value=[self.jar]),patch.object(module,'fresh_public',return_value=(RECEIPT,lambda *args:None)),patch.object(module,'create_fixture_spark',return_value=self.spark),patch.object(module,'open_fixture_reader',reader),patch.object(module,'compile_path_request',return_value=b'{}'),patch.object(module,'execute_commerce_path',side_effect=execute):
            result=module.query_fixture(self.config)
        return result,calls
    def test_all_eleven_original_intents_and_cleanup_before_report(self):
        def stopped():self.assertFalse((self.config.output/'report.json').exists())
        self.spark.stop.side_effect=stopped
        # Assign in wrapper after default side-effect setup.
        original=module.finalize_owned
        def finalize(*args):
            self.assertFalse((self.config.output/'report.json').exists());return original(*args)
        with patch.object(module,'finalize_owned',side_effect=finalize):result,calls=self.run_query(stop_error=stopped)
        expected=expected_cases(self.m,self.g);self.assertEqual([sql for sql,_ in calls],[c['sql'] for c in expected['cases']]+list(expected['expansionQueries'].values()))
        self.assertEqual(len(result['cases']),11);self.spark.stop.assert_called_once();self.assertTrue((self.config.output/'report.json').is_file())
    def test_body_cancellation_survives_reader_and_stop_failures(self):
        primary=KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt) as caught:self.run_query(body_error=primary,close_error=OSError('cleanup'),stop_error=OSError('stop'))
        self.assertIs(caught.exception,primary);self.assertTrue(primary.reader_cleanup_failed);self.assertTrue(primary.cleanup_failed)
        self.spark.stop.assert_called_once();self.assertFalse((self.config.output/'report.json').exists())
    def test_successful_body_close_failure_withholds(self):
        with self.assertRaises(OSError):self.run_query(close_error=OSError('close'))
        self.spark.stop.assert_called_once();self.assertFalse((self.config.output/'report.json').exists())
    def test_successful_body_stop_failure_withholds(self):
        with self.assertRaises(OSError):self.run_query(stop_error=OSError('stop'))
        self.assertFalse((self.config.output/'report.json').exists())
    def test_required_settings_and_source_drift_before_engine(self):
        with self.assertRaises(ValueError):replace(self.config,maximum_native_files=0)
        with self.assertRaises(ValueError):replace(self.config,ack=('caller',))
        (self.root/'model').write_bytes(self.m+b' ')
        with patch.object(module,'preflight',return_value=[self.jar]),patch.object(module,'create_fixture_spark') as start:
            with self.assertRaises(ValueError):module.query_fixture(self.config)
            start.assert_not_called()
    def test_source_oracle_rawcells_include_parallel_and_left_distinction(self):
        cases=expected_cases(self.m,self.g)['cases']
        six=module.fixture_oracle(self.m,self.g,{'sql':cases[0]['sql']})['rows'];self.assertEqual(len(json.loads(six[0][1])['items']),6)
        left=module.fixture_oracle(self.m,self.g,{'sql':cases[-1]['sql']})['rows']
        self.assertEqual(json.loads(left[0][1]),{'state':'value','value':{'items':[],'truncated':False}})
        self.assertEqual(json.loads(left[2][1]),{'state':'absent'})

    def test_publish_cleanup_attempts_both_and_withholds(self):
        output=self.root/'publish-cleanup';output.mkdir();transport=Mock();transport.close.side_effect=OSError('transport')
        with self.assertRaises(OSError):module.finalize_owned(self.spark,transport,None,{'success':'provisional'},output)
        self.spark.stop.assert_called_once();self.assertFalse((output/'report.json').exists())
    def test_bounded_native_inventory_refuses_nonregular_and_overflow(self):
        import os
        directory=self.root/'native';directory.mkdir();(directory/'one').write_bytes(b'123')
        target=SimpleNamespace(path=directory)
        with self.assertRaises(ValueError):module.native_files([target],replace(self.config,maximum_native_bytes=2))
        os.mkfifo(directory/'unowned')
        with self.assertRaises(ValueError):module.native_files([target],self.config)

    def test_each_direct_session_preserves_primary_and_finalizes(self):
        for role in ('writer', 'reader', 'ack'):
            with self.subTest(role=role):
                primary=KeyboardInterrupt();connection=Mock();connection.close.side_effect=OSError('close')
                with self.assertRaises(KeyboardInterrupt) as caught:
                    with module.owned_connection(connection):raise primary
                self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed)
                connection.close.assert_called_once()
                spark=Mock();transport=Mock()
                with self.assertRaises(KeyboardInterrupt):module.finalize_owned(spark,transport,primary,None,self.root)
                spark.stop.assert_called_once();transport.close.assert_called_once()
    def test_serialization_failure_creates_no_final_report(self):
        with self.assertRaises(TypeError):module.finalize_owned(self.spark,None,None,{'invalid':object()},self.root)
        self.assertFalse((self.root/'report.json').exists());self.spark.stop.assert_called_once()
    def test_alias_cancellation_survives_interval_reader_and_stop_cleanup(self):
        primary=KeyboardInterrupt()
        @contextmanager
        def interval(context):
            try:yield
            finally:raise OSError('interval cleanup')
        self.provider.interval=interval;self.spark.sql.side_effect=primary
        with self.assertRaises(KeyboardInterrupt) as caught:self.run_query(close_error=OSError('reader'),stop_error=OSError('stop'))
        self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed)
        self.spark.stop.assert_called_once();self.assertFalse((self.config.output/'report.json').exists())

    def test_alias_suppression_cannot_release_success(self):
        primary=KeyboardInterrupt()
        @contextmanager
        def interval(context):
            try:yield
            except BaseException:pass
        self.provider.interval=interval;self.spark.sql.side_effect=primary
        with self.assertRaises(KeyboardInterrupt) as caught:self.run_query()
        self.assertIs(caught.exception,primary);self.spark.stop.assert_called_once()
        self.assertFalse((self.config.output/'report.json').exists())
    def test_reader_suppression_cannot_release_success(self):
        primary=KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt) as caught:self.run_query(body_error=primary,suppress_reader=True)
        self.assertIs(caught.exception,primary);self.spark.stop.assert_called_once()
        self.assertFalse((self.config.output/'report.json').exists())

"""Composition lifecycle controls with mocked ports; no native qualification."""
from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import run_commerce_path_native as module
from weft_path_compiler import PathCompilerConfig
from weft_path_plan import PathSchemaValidation
from weft_path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig
from test_weft_path_plan import SCHEMAS


class CompositionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.pub=self.root/'publication';self.pub.mkdir()
        self.output=self.root/'output';self.events=[]
        for name in ('report.json','original-ontology.json','original-graph.json',
                     'development-bindings.json','source.jsonl','public-dataset.json'):
            (self.pub/name).write_bytes(b'{}')
        self.jars=self.root/'jars';self.jars.mkdir();self.jar=self.jars/'original.jar';self.jar.write_bytes(b'jar fixture')
        self.schemas=[]
        for name,raw in SCHEMAS.items():
            path=self.root/name;path.write_bytes(raw);self.schemas.append(path)
        self.config=module.PathCompositionConfig(self.pub,self.output,self.jars,self.root,
            Path(module.__file__).resolve().parent/'check_commerce_dataset.ts',self.root/'bun',tuple(self.schemas),
            PathCompilerConfig(self.root/'compiler',1024,1024,5),PathCaptureConfig(10,1000,10000),
            PathDecodeConfig(1000),16*1024*1024,16*1024*1024,
            ('ashlar-e2e-truss-pg17','127.0.0.1',15432,'truss_e2e'))
        self.spark=SimpleNamespace(version='4.0.1',sql=self.sql,stop=self.stop)
        self.provider=SimpleNamespace(active=False,context=object(),interval=self.interval,
            closed_interval_custody=lambda context:{'fixture_only':True},expected_binding='{}')
        self.opened=SimpleNamespace(original_report={'table_registry':{}},model=b'{}',graph=b'{}',
            bindings={},manifest={},aliases={'objects':'spark_catalog.commerce.object_current'},
            driver=SimpleNamespace(transport=SimpleNamespace(targets={'objects':SimpleNamespace(path=self.pub/'native')})),
            provider=self.provider,context=self.provider.context,
            original_native_files={'native':'fixed'},native_files=lambda:{'native':'fixed'})
        (self.pub/'report.json').write_text(json.dumps(self.opened.original_report))
        self.close_failure=False;self.stop_failure=False;self.post_close_mutation=False
    def sql(self,statement):
        self.events.append('alias')
        return SimpleNamespace(collect=lambda:[])
    def stop(self):
        self.assertFalse((self.output/'report.json').exists())
        self.events.append('stop')
        if self.stop_failure:raise OSError('fixture stop failure')
    @contextmanager
    def interval(self,context):
        self.provider.active=True
        try:yield
        finally:self.provider.active=False
    @contextmanager
    def reader(self,*args):
        self.events.append('reader-open')
        try:
            yield self.opened
        finally:
            self.events.append('reader-close')
            if self.post_close_mutation:(self.pub/'report.json').write_bytes(b'{"different":true}')
            if self.close_failure:raise OSError('fixture reader-close failure')
    def source(self,argv,**kwargs):
        Path(argv[-1]).write_bytes((self.pub/'public-dataset.json').read_bytes())
        self.events.append('source')
        return SimpleNamespace(returncode=0)
    def execute(self,opened,request,*args,**kwargs):
        self.assertIs(opened,self.opened)
        self.assertFalse(self.provider.active)
        self.assertEqual(json.loads(self.provider.expected_binding),{})
        self.events.append('case')
        return {'fixture_only':True,'rows':[['original']]}
    def compose(self, *, compile_failure=False):
        def compiler(raw,**kwargs):
            self.events.append('compile')
            if compile_failure:raise compile_failure if isinstance(compile_failure,BaseException) else ValueError('fixture compile refused')
            return b'{"status":"compiled"}\n'
        with patch.object(module,'preflight',return_value=[self.jar]), \
             patch.object(module,'make_offline_path_schema_validation',return_value=PathSchemaValidation(SCHEMAS,lambda *args:None)), \
             patch.object(module.subprocess,'run',side_effect=self.source), \
             patch.object(module,'create_path_spark',return_value=self.spark), \
             patch.object(module,'open_commerce_reader',side_effect=self.reader), \
             patch.object(module,'commerce_path_request',side_effect=lambda sql,*args:{'sql':sql,'target':{'bindingJson':'{}'}}), \
             patch.object(module,'compile_path_request',side_effect=compiler), \
             patch.object(module,'execute_commerce_path',side_effect=self.execute):
            return module.run_commerce_paths(self.config)
    def test_success_only_after_reader_and_spark_close(self):
        result=self.compose()
        self.assertEqual(len(result['cases']),3)
        self.assertEqual(self.events[-2:],['reader-close','stop'])
        self.assertTrue((self.output/'report.json').is_file())
        saved=json.loads((self.output/'report.json').read_bytes())
        self.assertEqual(saved['cleanup'],{'reader_closed':True,'spark_stopped':True})
        self.assertEqual(saved['opening_jar_hashes'],saved['closing_jar_hashes'])
        self.assertEqual(sum(e=='compile' for e in self.events),6)
    def test_reader_close_failure_withholds_report_and_stops(self):
        self.close_failure=True
        with self.assertRaises(OSError):self.compose()
        self.assertEqual(self.events[-2:],['reader-close','stop'])
        self.assertFalse((self.output/'report.json').exists())
    def test_stop_failure_withholds_report(self):
        self.stop_failure=True
        with self.assertRaises(OSError):self.compose()
        self.assertFalse((self.output/'report.json').exists())
    def test_compile_failure_closes_reader_and_spark(self):
        with self.assertRaises(ValueError):self.compose(compile_failure=True)
        self.assertEqual(self.events[-1],'stop')
        self.assertFalse((self.output/'report.json').exists())
    def test_body_and_cancellation_failures_survive_stop_failure(self):
        for error in (ValueError('fixture primary'), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                if self.output.exists():
                    import shutil
                    shutil.rmtree(self.output)
                self.stop_failure=True
                try:self.compose(compile_failure=error)
                except BaseException as observed:
                    self.assertIs(observed,error)
                    self.assertTrue(observed.cleanup_failed)
                else:self.fail('Primary failure not preserved')
                self.assertFalse((self.output/'report.json').exists())
                receipt=json.loads((self.output/'cleanup-failure.json').read_bytes())
                self.assertTrue(receipt['spark_stop_failed'])
                self.assertNotIn('fixture primary',(self.output/'cleanup-failure.json').read_text())

    def test_body_failure_survives_reader_and_optional_spark_cleanup_failure(self):
        for stop_failure in (False,True):
            for error in (KeyboardInterrupt(),ValueError('fixture primary body')):
                with self.subTest(stop=stop_failure,error=type(error).__name__):
                    if self.output.exists():
                        import shutil
                        shutil.rmtree(self.output)
                    self.close_failure=True;self.stop_failure=stop_failure
                    try:self.compose(compile_failure=error)
                    except BaseException as observed:
                        self.assertIs(observed,error)
                        self.assertTrue(observed.reader_cleanup_failed)
                        self.assertTrue(observed.cleanup_failed)
                    else:self.fail('Original body failure replaced')
                    self.assertFalse((self.output/'report.json').exists())
                    reader_receipt=json.loads((self.output/'reader-cleanup-failure.json').read_bytes())
                    self.assertTrue(reader_receipt['reader_close_failed'])
                    self.assertEqual((self.output/'cleanup-failure.json').exists(),stop_failure)
                    self.assertEqual(self.events[-2:],['reader-close','stop'])

    def test_post_reader_source_drift_withholds_report(self):
        self.post_close_mutation=True
        with self.assertRaises(module.PathCompositionError):self.compose()
        self.assertEqual(self.events[-1],'stop')
        self.assertFalse((self.output/'report.json').exists())
    def test_required_typed_limits_paths_and_fresh_output(self):
        for changes in ({'maximum_input_bytes':0},{'maximum_artifact_bytes':True},
                        {'output':Path('relative')},{'schemas':tuple(self.schemas[:2])}):
            with self.assertRaises(module.PathCompositionError):replace(self.config,**changes)
        self.output.mkdir()
        with self.assertRaises(FileExistsError):self.compose()
        self.assertNotIn('reader-open',self.events)
    def test_oversized_original_input_refuses_before_engine_or_source(self):
        self.config=replace(self.config,maximum_input_bytes=1)
        with self.assertRaises(module.PathCompositionError):self.compose()
        self.assertEqual(self.events,[])
        self.assertFalse(self.output.exists())


if __name__=='__main__':unittest.main()

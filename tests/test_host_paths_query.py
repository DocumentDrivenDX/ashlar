"""Installed host source controls; fake compiler/native ports confer no qualification."""
import ast
import base64
from contextlib import contextmanager
from copy import deepcopy
from decimal import localcontext
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace, ModuleType
import unittest
from unittest.mock import MagicMock, patch
import zlib

from ashlar_host import commerce_path_oracle as oracle
from ashlar_host.commerce_path_request import commerce_path_request
from ashlar_host.config import (HostError, ProducerConfig, PrivatePostgresConfig,
                                QueryCommercePathsConfig)
from ashlar_host.path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig, DecodedPaths
from ashlar_host.path_admission import PathPlanError, PathSchemaValidation
from ashlar_host.path_schema import make_offline_path_schema_validation
from ashlar_host.source_identity import build_transaction, SOURCE_SHA
from ashlar_host import paths_query as module

ROOT=Path(__file__).resolve().parents[1]
MODEL=(ROOT/'src/ashlar_host/resources/ontology.json').read_bytes()
GRAPH=(ROOT/'src/ashlar_host/resources/graph.json').read_bytes()
_, BINDINGS=build_transaction(MODEL,GRAPH,source_system='private-original-commerce-fixture',binding_profile='ashlar-commerce-development-bindings/0.2')

def schemas():
    # Read only the retained byte constant; no tools module/validator is imported.
    parsed=ast.parse((ROOT/'tests/test_weft_path_plan.py').read_text())
    raw=next(ast.literal_eval(n.value) for n in parsed.body if isinstance(n,ast.Assign)
             and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='SCHEMAS_B64')
    return {name:text.encode() for name,text in json.loads(zlib.decompress(base64.b64decode(raw))).items()}


def metadata():
    roles=('edge_current','object_current','tombstone','whole_source_history')
    versions={'local.commerce.'+name:2 for name in roles}
    manifest={'publication_id':'fixture-publication','table_versions_json':json.dumps(versions),
              'source_progress_json':json.dumps({'private-original-commerce-fixture':{}})}
    registry=[{'table':name,'uuid':'fixture-'+name} for name in (*versions,'local.commerce.manifest')]
    aliases={name:name.replace('local.','spark_catalog.',1) for name in versions}
    return manifest,registry,aliases


class OracleRequestTests(unittest.TestCase):
    def test_all_ten_independent_original_bags_and_opaque_identities(self):
        results={name:oracle.original_commerce_path_oracle(MODEL,GRAPH,{'sql':sql})['rows']
                 for name,sql in oracle.commerce_path_cases()}
        self.assertEqual(len(results),10)
        self.assertEqual(results['path-count'],[['1']]);self.assertEqual(results['target-count'],[['1']])
        self.assertEqual(results['grouped-path-count'],[['[42,0,"L1"]','1']])
        self.assertEqual(results['partial-return'],[['6','2','6']])
        self.assertEqual(results['fulfillment'],[['[42,0,"F2"]']])
        self.assertEqual(results['settlement'],[['[42,0,"PAY1"]']])
        self.assertEqual(results['refund'],[['[42,0,"RF1"]']])
        self.assertEqual(results['property-equality-join'],[['[42,0,"L1"]','[42,0,"P1"]']])
        self.assertEqual(json.loads(results['one-hop'][0][1]),{'items':[['[42,0,"S1"]']],'truncated':False})
        self.assertEqual(json.loads(results['collection'][0][1])['items'][0]['edges'],['4','1'])
        with localcontext() as context:
            context.prec=1
            self.assertEqual(oracle.original_commerce_path_oracle(MODEL,GRAPH,{'sql':oracle.REFUND})['rows'],results['refund'])
        for model,graph,request in ((MODEL+b' ',GRAPH,{'sql':oracle.COUNT}),(MODEL,GRAPH+b' ',{'sql':oracle.COUNT}),(MODEL,GRAPH,{'sql':'SELECT 0'})):
            with self.assertRaises(ValueError):oracle.original_commerce_path_oracle(model,graph,request)

    def test_complete_request_preserves_original_source_and_all_homes(self):
        manifest,registry,aliases=metadata();before=deepcopy(BINDINGS)
        request=commerce_path_request(oracle.REFUND,MODEL,BINDINGS,manifest,registry,aliases)
        self.assertEqual(request['modules'][0]['documentJson'].encode(),MODEL)
        self.assertEqual(request['modules'][0]['pin']['umfVersion'],'0.8.0')
        binding=json.loads(request['target']['bindingJson'])
        self.assertEqual(len(binding['records']),10);self.assertEqual(sum(len(x['properties']) for x in binding['records']),34)
        self.assertEqual(len(binding['relationships']),9);self.assertEqual(BINDINGS,before)
        self.assertEqual(hashlib.sha256(request['target']['bindingJson'].encode()).hexdigest(),request['target']['bindingSha256'])
        changed=deepcopy(BINDINGS);changed['properties'].pop()
        with self.assertRaises(ValueError):commerce_path_request(oracle.COUNT,MODEL,changed,manifest,registry,aliases)
        changed=deepcopy(BINDINGS);next(t for t in changed['types'] if t['identity'][0]=='edge')['type_id']='0'
        with self.assertRaises(ValueError):commerce_path_request(oracle.COUNT,MODEL,changed,manifest,registry,aliases)

    def test_schema_pin_refusal_before_dependency_and_optional_real_validator(self):
        original=schemas();bad=dict(original);bad[next(iter(bad))]+=b' '
        with self.assertRaises(PathPlanError):make_offline_path_schema_validation(bad)
        with patch.dict(sys.modules,{'jsonschema':None}):
            with self.assertRaises(PathPlanError):make_offline_path_schema_validation(original)
        try:import jsonschema,referencing
        except ImportError:return
        port=make_offline_path_schema_validation(original)
        manifest,registry,aliases=metadata();request=commerce_path_request(oracle.COUNT,MODEL,BINDINGS,manifest,registry,aliases)
        port.validate(port.schemas,'compile-request-v0.4.schema.json',request)
        request['unknown']=True
        with self.assertRaises(PathPlanError):port.validate(port.schemas,'compile-request-v0.4.schema.json',request)

    def test_package_import_without_tools_or_sdk(self):
        code="import sys;import ashlar_host.path_schema,ashlar_host.commerce_path_request,ashlar_host.commerce_path_oracle,ashlar_host.paths_query;assert not any(k=='pyspark' or k.startswith('pyspark.') or k=='jsonschema' for k in sys.modules);assert not any('/tools/' in str(getattr(v,'__file__','')) for v in sys.modules.values())"
        with tempfile.TemporaryDirectory() as d:
            process=subprocess.run([sys.executable,'-B','-S','-c',code],cwd=d,
                env={'PATH':'/usr/bin:/bin','PYTHONPATH':str(ROOT/'src')},capture_output=True,timeout=10)
            self.assertEqual(process.returncode,0,process.stderr.decode())


class PublicSourceTests(unittest.TestCase):
    def setUp(self):
        self.opened=SimpleNamespace(model=MODEL,graph=GRAPH,bindings=BINDINGS)
        self.dataset=json.dumps({'umfRevision':module.UMF_REVISION,
            'sourceSha256':hashlib.sha256(MODEL).hexdigest(),
            'graphSha256':hashlib.sha256(GRAPH).hexdigest(),
            'receipt':{'datasetValidation':{'valid':True,'complete':True}}}).encode()
        manifest,registry,aliases=metadata()
        self.request=commerce_path_request(oracle.PARTIAL_RETURN,MODEL,BINDINGS,manifest,registry,aliases)
        self.artifact={'modelPins':json.loads(self.request['target']['bindingJson'])['modelPins'],
                       'bindingSha256':self.request['target']['bindingSha256']}
        identity=lambda element:{'documentId':'urn:umf:domain:commerce','module':'domain','element':element,'revision':SOURCE_SHA}
        self.check={'record':identity('order_lines'),'field':identity('order_lines.quantity'),
            'propertyId':'13','publicSourceOnly':True,
            'logicalType':{'family':'integer','facets':{},'nullable':False}}
        self.row={'source_system':'private-original-commerce-fixture','type_id':'13','id':'5',
            'schema_revision':SOURCE_SHA,'props_json':'{"10":"[42,0,\\\"L1\\\"]","11":"[42,0,\\\"O1\\\"]","12":"[42,0,\\\"P1\\\"]","13":10}',
            'extracted_token':'10'}

    def test_native_projection_and_field_receipt_correspondence_refuse_independent_mutations(self):
        port=module._source_port(self.opened,self.dataset,16*1024*1024)
        projections=[{'check':self.check,'rows':[self.row]}]
        receipt=port(self.request,self.artifact,projections)
        self.assertEqual(hashlib.sha256(receipt['originalReceiptText'].encode()).hexdigest(),receipt['receiptSha256'])
        for target,key,value in [('check','propertyId','14'),('row','id','6'),('row','extracted_token','11'),('row','props_json','{}')]:
            changed=deepcopy(projections)
            if target=='check':changed[0]['check'][key]=value
            else:changed[0]['rows'][0][key]=value
            with self.assertRaises(HostError):port(self.request,self.artifact,changed)
        changed=json.loads(self.dataset);changed['receipt']['datasetValidation']['complete']=False
        with self.assertRaises(HostError):module._source_port(self.opened,json.dumps(changed).encode(),16*1024*1024)
        projections[0]['rows'][0]['id']='mutated-after-return'
        self.assertNotIn('mutated-after-return',receipt['originalReceiptText'])

    def test_preflight_refuses_before_encoder_and_numeric_conversion(self):
        with patch.object(module.json,'dumps',side_effect=AssertionError('encoder reached')):
            with self.assertRaises(HostError):module._bounded_encoded({'text':'\x7f'*1000},1100)
        with self.assertRaises(HostError):module._parse(b'{"x":'+b'1'*10000+b'}',20000)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name).resolve()
        self.addCleanup(self.temp.cleanup)
        self.publication=self.root/'publication';self.publication.mkdir()
        self.model=self.root/'model';self.model.write_bytes(MODEL);self.graph=self.root/'graph';self.graph.write_bytes(GRAPH)
        self.manifest,self.registry,self.aliases=metadata()
        self.report={'table_registry':self.registry};(self.publication/'report.json').write_text(json.dumps(self.report))
        self.dataset=b'{}';(self.publication/'public-dataset.json').write_bytes(self.dataset)
        producer=ProducerConfig(self.root/'producer',self.root/'bun',self.root/'git',60,1024,4*1024*1024)
        postgres=PrivatePostgresConfig('ashlar-e2e-truss-pg17','127.0.0.1',15432,'truss_e2e')
        self.config=QueryCommercePathsConfig(self.root/'index',self.root/'installation',self.publication,
            self.root/'output',self.root/'jars',self.model,self.graph,producer,postgres,
            16*1024*1024,PathCaptureConfig(1000,1024,4096),PathDecodeConfig(1024))
        self.spark=MagicMock();self.spark.version='4.0.1'
        self.provider=SimpleNamespace(active=False,expected_binding='',interval=self.interval,
            closed_interval_custody=lambda context:{'closed':True})
        self.opened=SimpleNamespace(model=MODEL,graph=GRAPH,bindings=BINDINGS,manifest=self.manifest,
            original_report=self.report,aliases=self.aliases,driver=SimpleNamespace(transport=SimpleNamespace(targets={name:SimpleNamespace(path=self.root/name) for name in self.aliases})),provider=self.provider,context=object(),
            original_native_files={'native':'hash'},native_files=lambda:{'native':'hash'})
        self.executions=[]

    @contextmanager
    def interval(self,context):yield

    @contextmanager
    def reader(self,*args):yield self.opened

    def compose(self,**overrides):
        port=PathSchemaValidation(schemas(),lambda *args:None)
        def producer(config,model,graph,output):output.write_bytes(self.dataset);return {'exit':0}
        def execute(opened,request,artifact,recompiled,**kwargs):
            self.executions.append(request['sql'])
            return {'oracle':kwargs['original_oracle'](MODEL,GRAPH,request),
                    'decoded':[DecodedPaths(b'{"items":[],"truncated":false}','value',(),False)]}
        values={'runtime_paths':lambda *a,**k:[], 'installed_paths_schema_bundle':lambda *a:tuple(schemas().items()),
            'make_offline_path_schema_validation':lambda *a:port,'recompute_dataset':producer,
            'open_commerce_reader':self.reader,'_source_port':lambda *a:lambda *k:None,
            'compile_paths_distribution':lambda *a:b'{"status":"compiled"}\n','execute_commerce_path':execute}
        values.update(overrides)
        sql=ModuleType('pyspark.sql');sql.SparkSession=SimpleNamespace(builder=self.spark)
        self.spark.master.return_value=self.spark;self.spark.appName.return_value=self.spark
        self.spark.config.return_value=self.spark;self.spark.getOrCreate.return_value=self.spark
        with patch.dict(sys.modules,{'pyspark':ModuleType('pyspark'),'pyspark.sql':sql}),patch.multiple(module,**values):
            return module.query_commerce_paths(self.config)

    def test_complete_ten_results_persist_only_after_reader_and_stop(self):
        def stop():self.assertFalse((self.config.output/'report.json').exists())
        self.spark.stop.side_effect=stop
        result=self.compose();self.assertEqual(len(result['cases']),10);self.assertEqual(len(self.executions),10)
        self.assertTrue((self.config.output/'report.json').exists());self.assertEqual(result['umfVersion'],'0.8.0')
        self.spark.stop.assert_called_once()
        retained=json.loads((self.config.output/'report.json').read_bytes())
        self.assertEqual(bytes.fromhex(retained['cases'][0]['provisional']['decoded'][0]['original']['hex']),b'{"items":[],"truncated":false}')

    def test_required_onehop_refusal_withholds_whole_report(self):
        def execute(opened,request,*args,**kwargs):
            if request['sql']==oracle.ONE_HOP:raise HostError('required-case-refused')
            return {}
        with self.assertRaises(HostError):self.compose(execute_commerce_path=execute)
        self.assertFalse((self.config.output/'report.json').exists());self.spark.stop.assert_called_once()

    def test_cancellation_reader_replacement_and_stop_failure_preserve_primary(self):
        primary=KeyboardInterrupt()
        @contextmanager
        def reader(*args):
            try:yield self.opened
            finally:raise OSError('reader-cleanup')
        self.spark.stop.side_effect=OSError('spark-cleanup')
        def compiler(*args):raise primary
        try:self.compose(open_commerce_reader=reader,compile_paths_distribution=compiler)
        except BaseException as caught:self.assertIs(caught,primary)
        else:self.fail('primary swallowed')
        self.assertTrue(primary.cleanup_failed);self.assertFalse((self.config.output/'report.json').exists())

    def test_reader_suppression_withholds_and_stop_runs(self):
        primary=KeyboardInterrupt()
        @contextmanager
        def reader(*args):
            try:yield self.opened
            except BaseException:pass
        def compiler(*args):raise primary
        try:self.compose(open_commerce_reader=reader,compile_paths_distribution=compiler)
        except BaseException as caught:self.assertIs(caught,primary)
        else:self.fail('suppression accepted')
        self.assertFalse((self.config.output/'report.json').exists());self.spark.stop.assert_called_once()

    def test_final_report_write_cancellation_survives_stream_close_failure(self):
        primary=KeyboardInterrupt()
        class Stream:
            def __enter__(self):return self
            def write(self,payload):raise primary
            def __exit__(self,*args):raise OSError('stream-close')
        original=Path.open
        def opened(path,*args,**kwargs):
            if path==self.config.output/'.report-stage.json':return Stream()
            return original(path,*args,**kwargs)
        with patch.object(Path,'open',opened):
            try:self.compose()
            except BaseException as caught:self.assertIs(caught,primary)
            else:self.fail('report cancellation masked')
        self.assertTrue(primary.cleanup_failed)
        self.spark.stop.assert_called_once()

    def test_report_close_failure_leaves_no_final_report(self):
        original=Path.open
        class Stream:
            def __init__(self,stream):self.stream=stream
            def __enter__(self):return self
            def write(self,payload):return self.stream.write(payload)
            def __exit__(self,*args):
                self.stream.close()
                raise OSError('stream-close')
        def opened(path,*args,**kwargs):
            stream=original(path,*args,**kwargs)
            return Stream(stream) if path==self.config.output/'.report-stage.json' else stream
        with patch.object(Path,'open',opened):
            with self.assertRaises(HostError):self.compose()
        self.assertFalse((self.config.output/'report.json').exists())
        self.assertFalse((self.config.output/'.report-stage.json').exists())

    def test_postcommit_report_cleanup_cancellation_is_not_swallowed(self):
        self.config.output.mkdir()
        original=Path.unlink;primary=KeyboardInterrupt()
        def unlink(path,*args,**kwargs):
            if path==self.config.output/'.report-stage.json':raise primary
            return original(path,*args,**kwargs)
        with patch.object(Path,'unlink',unlink):
            try:module._publish_report(self.config.output,b'{"complete":true}\n')
            except BaseException as caught:self.assertIs(caught,primary)
            else:self.fail('postcommit cancellation swallowed')
        self.assertEqual((self.config.output/'report.json').read_bytes(),b'{"complete":true}\n')

    def test_report_drift_after_reader_closes_withholds(self):
        @contextmanager
        def reader(*args):
            yield self.opened
            (self.publication/'report.json').write_bytes(b'{}')
        with self.assertRaises(HostError):self.compose(open_commerce_reader=reader)
        self.assertFalse((self.config.output/'report.json').exists());self.spark.stop.assert_called_once()

if __name__=='__main__':unittest.main()

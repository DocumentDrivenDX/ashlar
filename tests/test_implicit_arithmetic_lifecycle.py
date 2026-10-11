"""Inert SDK/session ports; public original leaf wiring, no native qualification."""
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType,SimpleNamespace
import sys,unittest
from unittest.mock import patch
import test_host_paths_query as fixtures
from test_implicit_arithmetic import SCHEMAS
from ashlar_host import commerce_query_lifecycle as owner
from ashlar_host import commerce_implicit_arithmetic as leaf
from ashlar_host.implicit_arithmetic_query import query_commerce_implicit_arithmetic
from ashlar_host.implicit_arithmetic_configuration import QueryCommerceImplicitArithmeticConfig
from ashlar_host.implicit_arithmetic_profile import ImplicitArithmeticSchemaValidation
from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
from ashlar_host.publication_reader import PublicationProvider,original_commerce_reader_bindings
from ashlar_host.config import HostError
from test_commerce_query_sources import factory

class Tests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.LifecycleTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        c=self.fixture.config
        self.config=QueryCommerceImplicitArithmeticConfig(*(getattr(c,k)for k in ('index','installation','publication','output','jars','model','graph','producer','postgres','maximum_artifact_bytes','capture','decoder')))
        self.pack=(Path(__file__).resolve().parents[1]/'examples/domain-packs/commerce/upstream/pack.json').read_bytes()
        self.source_factory=factory()
        self.session=OrdinaryQueryConnectionFactory(lambda role:(_ for _ in ()).throw(AssertionError('inert test must not acquire PG')))
        opened=self.fixture.opened
        provider=PublicationProvider(opened.driver,opened.aliases,None,b'original',b'original',{},context=opened.context,
            original_bindings=original_commerce_reader_bindings(opened.model,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases))
        provider.interval=self.fixture.interval
        provider.closed_interval_custody=lambda context:{'inert':True}
        opened.provider=provider

    def compose(self,*,changes=None,execute=None,custody=None):
        fixture=self.fixture;requests=[];reader_options=[]
        def producer(config,model,graph,output):output.write_bytes(fixture.dataset);return {'inert':True}
        @contextmanager
        def reader(*args,**kwargs):
            reader_options.append(kwargs)
            yield fixture.opened
        def compile(paths,raw):requests.append(raw);return b'{}\n'
        def execution(opened,request,*args,**kwargs):return {'original_oracle':kwargs['original_oracle'](opened.model,opened.graph,request)}
        values={'runtime_paths':lambda *args,**kwargs:[],
            'installed_implicit_arithmetic_schema_bundle':lambda *args:tuple(SCHEMAS.items()),
            'make_offline_implicit_arithmetic_schema_validation':lambda *args:ImplicitArithmeticSchemaValidation(SCHEMAS,lambda *args:None),
            'recompute_dataset':producer,'open_commerce_reader':reader,'commerce_query_source_port':lambda *args:lambda *args:None}
        values.update(changes or {})
        sql=ModuleType('pyspark.sql');sql.SparkSession=SimpleNamespace(builder=fixture.spark)
        for name in ('master','appName','config'):getattr(fixture.spark,name).return_value=fixture.spark
        fixture.spark.getOrCreate.return_value=fixture.spark
        with patch.dict(sys.modules,{'pyspark':ModuleType('pyspark'),'pyspark.sql':sql}),patch.multiple(owner,**values),patch.object(leaf,'compile_implicit_arithmetic_distribution',side_effect=compile),patch.object(leaf,'execute_commerce_implicit_arithmetic',side_effect=execute or execution):
            result=query_commerce_implicit_arithmetic(self.config,ordinary_session=self.session,source_factory=self.source_factory,pack_bytes=self.pack,invocation_custody=custody)
        return result,requests,reader_options

    def test_all_four_real_leaf_requests_close_and_publish_new_profile_only(self):
        result,requests,options=self.compose()
        self.assertEqual([c['case']for c in result['cases']],['partial-return','fulfillment','settlement','refund'])
        self.assertEqual(len(requests),8)
        self.assertIs(options[0]['ordinary_session'],self.session)
        self.assertIs(options[0]['source_factory'],self.source_factory)
        self.assertEqual(result['format'],'ashlar-installed-commerce-implicit-arithmetic/0.1')
        self.assertEqual(result['cleanup'],{'readerClosed':True,'sparkStopped':True})
        self.assertTrue((self.config.output/'report.json').is_file());self.fixture.spark.stop.assert_called_once()

    def test_missing_or_forged_ordinary_port_refuses_before_all_effects(self):
        for supplied in (None,object()):
            with patch.object(owner,'runtime_paths')as runtime,patch.object(owner,'recompute_dataset')as producer:
                with self.assertRaises(HostError):query_commerce_implicit_arithmetic(self.config,ordinary_session=supplied,source_factory=self.source_factory,pack_bytes=self.pack)
                runtime.assert_not_called();producer.assert_not_called()
        self.assertFalse(self.config.output.exists())

    def test_missing_source_factory_refuses_before_runtime(self):
        for supplied in (None,object()):
            with patch.object(owner,'runtime_paths') as runtime,patch('ashlar_host.connection.connect') as legacy:
                with self.assertRaises(HostError):query_commerce_implicit_arithmetic(self.config,ordinary_session=self.session,source_factory=supplied,pack_bytes=self.pack)
                runtime.assert_not_called();legacy.assert_not_called()

    def test_closing_runtime_refusal_stops_spark_and_withholds_report(self):
        calls=[]
        def runtime(*args,**kwargs):
            calls.append(kwargs)
            if len(calls)>1:raise HostError('closing-runtime')
            return []
        with self.assertRaises(HostError):self.compose(changes={'runtime_paths':runtime})
        self.fixture.spark.stop.assert_called_once();self.assertFalse((self.config.output/'report.json').exists())

    def test_body_cancellation_survives_cleanup_cancellation_without_report(self):
        primary=KeyboardInterrupt();self.fixture.spark.stop.side_effect=SystemExit()
        with self.assertRaises(KeyboardInterrupt)as caught:self.compose(execute=lambda *args,**kwargs:(_ for _ in ()).throw(primary))
        self.assertIs(caught.exception,primary);self.fixture.spark.stop.assert_called_once()
        self.assertFalse((self.config.output/'report.json').exists())

    def test_invalid_configuration_still_closes_diagnostic_attempt_without_effects(self):
        from ashlar_host.count_star_query import query_commerce_count_star
        class Run:
            def __init__(self):self.events=[]
            def begin_attempt(self,*args):self.events.append('begin');return object()
            def finish_attempt(self,*args,**kwargs):self.events.append(args[1])
        run=Run()
        with patch.object(owner,'runtime_paths')as runtime:
            with self.assertRaises(HostError):query_commerce_count_star(self.config,diagnostics=run)
            runtime.assert_not_called()
        self.assertEqual(run.events,['begin','refused'])

    def assert_invocation_drift(self,name):
        from ashlar_host.evolution_cli import InvocationFile,MAX_PROVIDER
        from ashlar_host.query_invocation_custody import CommerceQueryInvocationCustody
        provider=self.config.model.parent/'provider.py';pack=self.config.model.parent/'invocation-pack.json'
        provider.write_bytes(b'original');pack.write_bytes(self.pack)
        custody=CommerceQueryInvocationCustody(InvocationFile.read(provider,MAX_PROVIDER),InvocationFile.read(pack,MAX_PROVIDER))
        def execute(*args,**kwargs):
            (provider if name=='provider' else pack).write_bytes(b'drift')
            return {}
        with self.assertRaises(HostError):self.compose(execute=execute,custody=custody)
        self.assertFalse((self.config.output/'report.json').exists())
        self.fixture.spark.stop.assert_called_once()

    def test_captured_provider_drift_withholds_report_after_cleanup(self):self.assert_invocation_drift('provider')

    def test_captured_pack_drift_withholds_report_after_cleanup(self):self.assert_invocation_drift('pack')

    def test_invocation_cleanup_preserves_original_cancellation(self):
        from ashlar_host.evolution_cli import InvocationFile,MAX_PROVIDER
        from ashlar_host.query_invocation_custody import CommerceQueryInvocationCustody
        provider=self.config.model.parent/'provider.py';pack=self.config.model.parent/'invocation-pack.json'
        provider.write_bytes(b'original');pack.write_bytes(self.pack)
        custody=CommerceQueryInvocationCustody(InvocationFile.read(provider,MAX_PROVIDER),InvocationFile.read(pack,MAX_PROVIDER))
        primary=KeyboardInterrupt()
        def execute(*args,**kwargs):
            provider.write_bytes(b'drift');raise primary
        with self.assertRaises(KeyboardInterrupt)as caught:self.compose(execute=execute,custody=custody)
        self.assertIs(caught.exception,primary);self.fixture.spark.stop.assert_called_once()
        self.assertFalse((self.config.output/'report.json').exists())

    def test_pure041_builder_extraction_preserves_compatibility_identity(self):
        from ashlar_host.count_star_query import commerce_count_star_request as old
        from ashlar_host.count_star_request import commerce_count_star_request as new
        from ashlar_host.commerce_original_scenarios import commerce_count_star_request as original
        self.assertIs(old,new);self.assertIs(original,new)

if __name__=='__main__':unittest.main()

"""New public host composition/cleanup controls over inert SDK ports."""
from contextlib import contextmanager
from types import ModuleType,SimpleNamespace
import sys,unittest
from unittest.mock import patch
import test_host_paths_query as fixture
from test_count_star_admission import SCHEMAS
from ashlar_host import count_star_query as module
from ashlar_host.count_star_configuration import QueryCommerceCountStarConfig
from ashlar_host.count_star_admission import CountStarSchemaValidation
from ashlar_host.config import HostError
class CountStarLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.case=fixture.LifecycleTests();self.case.setUp();self.addCleanup(self.case.doCleanups);c=self.case.config
        self.config=QueryCommerceCountStarConfig(*(getattr(c,k)for k in ('index','installation','publication','output','jars','model','graph','producer','postgres','maximum_artifact_bytes','capture','decoder')))
    def compose(self,**changes):
        c=self.case;executions=[];port=CountStarSchemaValidation(SCHEMAS,lambda *a:None)
        def producer(config,model,graph,output):output.write_bytes(c.dataset);return {'exit':0}
        def execute(opened,request,artifact,recompiled,**kwargs):
            executions.append(request['sql']);return {'oracle':kwargs['original_oracle'](fixture.MODEL,fixture.GRAPH,request)}
        values={'runtime_paths':lambda *a,**k:[],'installed_count_star_schema_bundle':lambda *a:tuple(SCHEMAS.items()),'make_offline_count_star_schema_validation':lambda *a:port,'recompute_dataset':producer,'open_commerce_reader':c.reader,'_source_port':lambda *a:lambda *k:None,'compile_count_star_distribution':lambda *a:b'{"status":"compiled"}\n','execute_commerce_count_star':execute};values.update(changes)
        sql=ModuleType('pyspark.sql');sql.SparkSession=SimpleNamespace(builder=c.spark)
        c.spark.master.return_value=c.spark;c.spark.appName.return_value=c.spark;c.spark.config.return_value=c.spark;c.spark.getOrCreate.return_value=c.spark
        with patch.dict(sys.modules,{'pyspark':ModuleType('pyspark'),'pyspark.sql':sql}),patch.multiple(module,**values):result=module.query_commerce_count_star(self.config)
        return result,executions
    def test_original_ten_and_new_four_survive_closed_host_report(self):
        result,executions=self.compose();self.assertEqual(executions,[sql for _,sql in module.commerce_path_cases()+module.commerce_count_star_cases()]);self.assertEqual(len(result['cases']),14);self.case.spark.stop.assert_called_once();self.assertTrue((self.config.output/'report.json').exists())
    def test_spark_cleanup_failure_withholds_report(self):
        self.case.spark.stop.side_effect=OSError('cleanup')
        with self.assertRaises(HostError):self.compose()
        self.assertFalse((self.config.output/'report.json').exists())
    def test_reader_cleanup_cancellation_outranks_body_error_and_stops_spark(self):
        cancellation=KeyboardInterrupt()
        @contextmanager
        def reader(*args):
            try:yield self.case.opened
            finally:raise cancellation
        with self.assertRaises(KeyboardInterrupt)as caught:self.compose(open_commerce_reader=reader,compile_count_star_distribution=lambda *a:(_ for _ in ()).throw(ValueError('body')))
        self.assertIs(caught.exception,cancellation);self.case.spark.stop.assert_called_once();self.assertFalse((self.config.output/'report.json').exists())
if __name__=='__main__':unittest.main()

import sys,json
import run_weft_path_fixture_native as m
from pyspark import SparkContext
def check(config):
 paths=m.preflight(config,'publish')
 assert SparkContext._active_spark_context is None and SparkContext._gateway is None
 print(json.dumps({'actualPreflightPassed':True,'jars':[str(p) for p in paths],'noSparkContextOrGateway':True}))
 return {}
m.publish_fixture=check
sys.argv=json.loads(sys.argv[1])[1:]
m.main()

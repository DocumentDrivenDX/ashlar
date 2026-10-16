import os,sys,json,pathlib,hashlib,time
r=pathlib.Path(__file__).parent
os.environ['JAVA_HOME']='/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home'
os.environ['PYSPARK_PYTHON']=sys.executable
os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from ashlar_host.fabric_pack_delta import materialize_fabric_pack_delta
from ashlar_host.local_graph_export import LocalGraphExportSelection
from ashlar_host.lifecycle import finish
jars=pathlib.Path('/private/tmp/ashlar-current-commerce-preparation-20261015-9pn8k_h7/query-jars');selected=','.join(str(jars/n)for n in ['delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar'])
settings={'spark.master':'local[1]','spark.app.name':'Ashlar typed original17 local materialization','spark.driver.memory':'512m','spark.ui.enabled':'false','spark.sql.shuffle.partitions':'1','spark.databricks.delta.snapshotPartitions':'1','spark.sql.session.timeZone':'UTC','spark.sql.ansi.enabled':'true','spark.jars':selected,'spark.sql.extensions':'io.delta.sql.DeltaSparkSessionExtension','spark.sql.catalog.spark_catalog':'org.apache.spark.sql.delta.catalog.DeltaCatalog','spark.sql.warehouse.dir':str(r/'warehouse')}
context=SparkContext(conf=SparkConf(loadDefaults=False).setAll(list(settings.items())));spark=SparkSession(context);spark.sparkContext.setLogLevel('ERROR')
results=[];error=None;started=time.monotonic()
try:
 for pack in ['archaeology','ecology']:
  print(json.dumps({'pack':pack,'state':'started'}),flush=True)
  exports=pathlib.Path('/private/tmp/ashlar-fabric-original17-actual-preparation-20261016');release=exports/(pack+'-release.json');custody=exports/(pack+'-custody.json');src=pathlib.Path('/Users/erik/Projects/ashlar/examples/domain-packs')/pack/'upstream'
  selection=LocalGraphExportSelection(release,custody,hashlib.sha256(release.read_bytes()).hexdigest(),hashlib.sha256(custody.read_bytes()).hexdigest())
  receipt=materialize_fabric_pack_delta(spark,pack=pack,model=src/'ontology.json',graph=src/'graph/fixture.json',pack_file=src/'pack.json',selection=selection,node_path=r/(pack+'_nodes'),edge_path=r/(pack+'_edges'))
  results.append(receipt);print(json.dumps({'pack':pack,'state':'completed','rows':{k:v['rows']for k,v in receipt['targets'].items()}}),flush=True)
except BaseException as e:error=e
finish(error,[spark.stop,context.stop])
(r/'result.json').write_text(json.dumps({'scope':'Actual local typed Delta projection/readback only, no Fabric load/query or canonical publication authority','spark':spark.version,'python':sys.version.split()[0],'settings':settings,'receipts':results,'elapsed_after_spark_start_seconds':time.monotonic()-started,'closing':'Spark and source/file owners completed'},indent=2)+'\n')

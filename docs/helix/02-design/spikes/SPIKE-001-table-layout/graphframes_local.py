import json, hashlib, importlib.util, tempfile, pathlib, time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType
from delta import configure_spark_with_delta_pip
base=pathlib.Path(__file__).resolve().parent
root=pathlib.Path(tempfile.mkdtemp(prefix='ashlar-graphframes-'))
builder=SparkSession.builder.master('local[2]').appName('ashlar-small-integration').config('spark.sql.shuffle.partitions','2').config('spark.sql.warehouse.dir',str(root/'warehouse')).config('spark.jars.ivy',str(root/'ivy')).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
spark=configure_spark_with_delta_pip(builder,extra_packages=['io.graphframes:graphframes-spark3_2.12:0.12.3']).getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
spark.sql('CREATE DATABASE ashlar_small')
plan=json.loads((base/'adapters/release-r66/mapping-plan.json').read_text()); nodes=[]; edges=[]; versions={}; originals={}
for name,meta in plan['tables'].items():
 p=base/'adapters/release-r66'/meta['file']; assert hashlib.sha256(p.read_bytes()).hexdigest()==meta['sha256']; rows=json.loads(p.read_text()); originals[name]=rows
 schema=StructType([StructField(k,StringType(),True) for k in rows[0]])
 table='spark_catalog.ashlar_small.'+name
 spark.createDataFrame(rows,schema).write.format('delta').saveAsTable(table)
 versions[table]=0
 (nodes if meta['kind']=='node' else edges).append(table)
spec=importlib.util.spec_from_file_location('adapter',base/'adapters/graphframes.py'); module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
graph=module.create_v02_release_graph(spark,nodes,edges,versions)
assert graph.vertices.count()==3 and graph.edges.count()==3
assert graph.find('(a)-[e]->(b); (b)-[f]->(c)').count()==3
assert graph.edges.filter('src = dst').count()==1
assert graph.edges.groupBy('src','dst').count().filter('count = 2').count()==1
assert graph.vertices.join(graph.degrees,'id','left').filter('degree IS NULL').count()==1
for name,rows in originals.items():
 actual=spark.table('spark_catalog.ashlar_small.'+name).collect(); assert sorted([r.asDict() for r in actual],key=lambda r:r['id'])==sorted(rows,key=lambda r:r['id'])
# An unpublished append must not alter the pinned graph.
spark.table(nodes[0]).limit(1).write.format('delta').mode('append').saveAsTable(nodes[0])
pinned=module.create_v02_release_graph(spark,nodes,edges,versions); assert pinned.vertices.count()==3
report={'status':'passed','scope':'Local Delta rematerialization of checksum-pinned native export; not direct Unity Catalog protocol interoperability or scale admission','spark':spark.version,'delta':'3.2.1','graphframes':'0.12.3','vertices':3,'edges':3,'two_hop_paths':3,'parallel_edges':2,'self_loops':1,'isolates':1,'exact_carrier_roundtrip':True,'unpublished_append_excluded':True,'release_versions':versions}
(base/'out/graphframes-local-20261006.json').write_text(json.dumps(report,indent=2)+'\n'); print(json.dumps(report)); spark.stop()

"""Actual GraphFrames integration on the validated 4M/20M local Delta corpus."""
import pathlib,json,tempfile,time,importlib.util,sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
B=pathlib.Path(__file__).resolve().parent;O=B/'out/graphframes-scale-20261006';O.mkdir(parents=True,exist_ok=True)
prior=json.loads((B/'out/entropy-local-20261006/summary.json').read_text());assert prior['state']=='passed full-row parity and storage-profile constraints'
resume='--resume-reads' in sys.argv
root=pathlib.Path(json.loads((O/'summary.json').read_text())['root']) if resume else pathlib.Path(tempfile.mkdtemp(prefix='ashlar-graphframes-scale-',dir='/tmp'))
builder=SparkSession.builder.master('local[8]').appName('Ashlar GraphFrames 24M').config('spark.driver.memory','32g').config('spark.sql.shuffle.partitions','256').config('spark.sql.warehouse.dir',str(root/'warehouse')).config('spark.jars.ivy','/tmp/ashlar-scale-ivy').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
s=configure_spark_with_delta_pip(builder,extra_packages=['io.graphframes:graphframes-spark3_2.12:0.12.3']).getOrCreate();s.sparkContext.setLogLevel('ERROR')
report={'state':'running','root':str(root),'spark':s.version,'delta':'3.2.1','graphframes':'0.12.3','source_versions':{'objects':0,'edges':0},'phases':[],'scope':'Local immutable projection releases of validated 4M/20M carriers; no direct UC protocol, real producer, graph-engine latency or billion-scale admission'}
def save():(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
def phase(label,fn):
 t=time.perf_counter();v=fn();report['phases'].append({'label':label,'seconds':time.perf_counter()-t,'result':v});save();print(label,report['phases'][-1]['seconds'],flush=True);return v
s.sql('CREATE DATABASE ashlar_entropy_graph')
n=s.read.format('delta').option('versionAsOf',0).load(prior['object']['path']);e=s.read.format('delta').option('versionAsOf',0).load(prior['edge']['path'])
def key(prefix,typ,identity):return f"concat('{prefix}',length(encode(source_system,'UTF-8')),':',source_system,':',cast({typ} AS STRING),':',cast({identity} AS STRING))"
nt='spark_catalog.ashlar_entropy_graph.node_union';et='spark_catalog.ashlar_entropy_graph.edge_union'
v=n.selectExpr(key('N','type_id','id')+' node_key','source_system','type_id','cast(id AS STRING) native_id','logical_key_json','props_json','retained_json')
a=e.selectExpr(key('E','rel_type_id','id')+' edge_key',key('N','source_type','source_id')+' src',key('N','target_type','target_id')+' dst','source_system','rel_type_id','cast(id AS STRING) native_id','source_type','source_id','target_type','target_id','props_json','retained_json')
if resume:
 for table,name in [(nt,'node_union'),(et,'edge_union')]:
  location=root/'warehouse/ashlar_entropy_graph.db'/name
  assert (location/'_delta_log/00000000000000000000.json').exists()
  s.sql(f"CREATE TABLE {table} USING DELTA LOCATION '{location}'")
else:
 phase('node-release',lambda:v.write.format('delta').option('compression','zstd').saveAsTable(nt))
 phase('edge-release',lambda:a.write.format('delta').option('compression','zstd').saveAsTable(et))
spec=importlib.util.spec_from_file_location('adapter',B/'adapters/graphframes.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
def build():
 global g
 g=module.create_v02_release_graph(s,[nt],[et],{nt:0,et:0});return {nt:0,et:0}
phase('version-pinned-adapter-validation',build)
assert phase('vertex-count',lambda:g.vertices.count())==4000000
assert phase('edge-count',lambda:g.edges.count())==20000000
assert phase('out-degree-distribution',lambda:[r.asDict() for r in g.outDegrees.groupBy('outDegree').count().collect()])==[{'outDegree':5,'count':4000000}]
assert phase('in-degree-distribution',lambda:[r.asDict() for r in g.inDegrees.groupBy('inDegree').count().collect()])==[{'inDegree':5,'count':4000000}]
assert phase('two-hop-path-multiplicity',lambda:g.find('(a)-[e]->(b); (b)-[f]->(c)').count())==100000000
report['state']='passed 4M/20M GraphFrames counts, closure, identities and 100M path multiplicity';save();s.stop()

"""Higher-entropy local calibration and 24M-carrier scale phase.
No production source/profile or billion-scale admission claim.
"""
import pathlib,tempfile,json,time
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from scale_workload import select,NODES,EDGES
B=pathlib.Path(__file__).resolve().parent;O=B/'out/entropy-local-20261006';O.mkdir(parents=True,exist_ok=True)
root=pathlib.Path(tempfile.mkdtemp(prefix='ashlar-entropy-r86-'))
builder=SparkSession.builder.master('local[8]').appName('Ashlar higher entropy scale').config('spark.driver.memory','32g').config('spark.sql.shuffle.partitions','1024').config('spark.sql.session.timeZone','UTC').config('spark.jars.ivy','/tmp/ashlar-scale-ivy').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
s=configure_spark_with_delta_pip(builder).getOrCreate();s.sparkContext.setLogLevel('ERROR')
report={'state':'calibrating','root':str(root),'spark':s.version,'delta':'3.2.1','resources':{'driver_gib':32,'workers':8,'disk_free_tib_before':3.3},'phases':[],'profile':'synthetic Truss-shaped shared allocation/unique endpoint pairs, not real producer/feed qualification','scope':'0.3 current-carrier columns and entropy/file-count sensitivity; source/history/publication still absent'}
def save():(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
def phase(label,fn):
 t=time.perf_counter();v=fn();report['phases'].append({'label':label,'seconds':time.perf_counter()-t,'result':v});save();print(label,report['phases'][-1]['seconds'],flush=True);return v
path=str(root/'calibration')
phase('calibration-100k',lambda:s.sql(select('edge',100000,entropy=True,truss_shape=True)).repartition(8).write.format('delta').option('compression','zstd').option('delta.dataSkippingStatsColumns','lookup_hash,source_system,rel_type_id,id').save(path))
f=s.read.format('delta').load(path)
assert f.filter("json_object_keys(props_json) IS NULL OR exists(json_object_keys(props_json), k -> NOT (k RLIKE '^[0-9]+$'))").count()==0
size=sum(p.stat().st_size for p in pathlib.Path(path).glob('*.parquet'))
rows=f.count();assert rows==100000
width=f.selectExpr('avg(length(encode(props_json,"UTF-8"))) props_bytes','min(length(props_json)) min_props','max(length(props_json)) max_props').first().asDict()
report['calibration']={'rows':rows,'parquet_bytes':size,'bytes_per_row':size/rows,'width':width,'estimated_24m_current_bytes':size/rows*(NODES+EDGES),'additional_retention_shuffle_factor':4,'expected_disk_bound_bytes':size/rows*(NODES+EDGES)*4}
assert report['calibration']['expected_disk_bound_bytes']<500_000_000_000,'Reassess local disk bound before growth'
report['state']='scale-running';save()
for kind,count in [('object',NODES),('edge',EDGES)]:
 path=str(root/(kind+'_current'));frame=s.sql(select(kind,count,entropy=True,truss_shape=True))
 phase('write-'+kind,lambda:frame.repartitionByRange(1024,'lookup_hash').sortWithinPartitions('lookup_hash').write.format('delta').option('compression','zstd').option('delta.dataSkippingStatsColumns','lookup_hash,source_system,'+('type_id' if kind=='object' else 'rel_type_id')+',id').save(path))
 actual=s.read.format('delta').load(path)
 # Full bidirectional multiset equality across all generated carrier fields.
 typ='type_id' if kind=='object' else 'rel_type_id'
 expected=s.sql(select(kind,count,entropy=True,truss_shape=True))
 phase('full-parity-'+kind,lambda:actual.exceptAll(expected).count()+expected.exceptAll(actual).count())
 assert report['phases'][-1]['result']==0
 detail=s.sql(f'DESCRIBE DETAIL delta.`{path}`').first().asDict()
 report[kind]={'rows':count,'files':detail['numFiles'],'bytes':detail['sizeInBytes'],'version':0,'path':path};save()
# These generated endpoint tuples obey source authority/type and pair uniqueness.
n=s.read.format('delta').load(report['object']['path']);e=s.read.format('delta').load(report['edge']['path'])
for direction in ['source','target']:
 mismatches=phase('endpoint-'+direction,lambda:e.join(n,(e.source_system==n.source_system)&(e[direction+'_type']==n.type_id)&(e[direction+'_id']==n.id),'left_anti').count());assert mismatches==0
assert phase('duplicate-relationship-endpoints',lambda:e.groupBy('source_system','rel_type_id','source_type','source_id','target_type','target_id').count().filter('count>1').count())==0
assert phase('shared-allocation-overlap',lambda:e.select('source_system','id').join(n.select('source_system','id'),['source_system','id']).count())==0
report['state']='passed full-row parity and storage-profile constraints';save();s.stop()

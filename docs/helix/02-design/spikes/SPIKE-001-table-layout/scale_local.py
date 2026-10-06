"""Run the authorized local scale phase; retained data lives under /tmp.
Requires Java17/Python3.11/pyspark3.5.3/delta-spark3.2.1.
"""
import json,time,pathlib,statistics,concurrent.futures
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from scale_workload import select,NODES,EDGES,UPDATES
B=pathlib.Path(__file__).resolve().parent
O=B/'out/local-scale-20261006';O.mkdir(parents=True,exist_ok=True)
root=pathlib.Path('/tmp/ashlar-scale-20261006-v3');root.mkdir(exist_ok=False)
builder=SparkSession.builder.master('local[8]').appName('Ashlar 24M current carriers').config('spark.driver.memory','32g').config('spark.sql.shuffle.partitions','256').config('spark.sql.warehouse.dir',str(root/'warehouse')).config('spark.jars.ivy','/tmp/ashlar-scale-ivy').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.databricks.delta.snapshotPartitions','8').config('spark.databricks.delta.properties.defaults.dataSkippingStatsColumns','lookup_hash,source_system,type_id,rel_type_id,id').config('spark.sql.parquet.compression.codec','zstd')
spark=configure_spark_with_delta_pip(builder).getOrCreate();spark.sparkContext.setLogLevel('ERROR')
report={'state':'running','spark':spark.version,'delta':'3.2.1','root':str(root),'resources':{'driver_gib':32,'workers':8},'nodes':NODES,'edges':EDGES,'phases':[],'scope':'Synthetic full 0.3 current carriers only; no source/journal/publication/adjacency or billion-scale admission. Range-sorted hash files in OSS Delta; not native liquid clustering.'}
def save(): (O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
def phase(label, fn):
 t=time.perf_counter();value=fn();report['phases'].append({'label':label,'seconds':time.perf_counter()-t,'result':value});save();print(label,report['phases'][-1]['seconds'],flush=True);return value
spark.sql('CREATE DATABASE ashlar_scale')
for kind,count in [('object',NODES),('edge',EDGES)]:
 table='ashlar_scale.'+kind+'_current'
 frame=spark.sql(select(kind,count))
 phase('write-'+kind,lambda: frame.repartitionByRange(256,'lookup_hash').sortWithinPartitions('lookup_hash').write.format('delta').option('delta.dataSkippingStatsColumns','lookup_hash,source_system,'+('type_id' if kind=='object' else 'rel_type_id')+',id').option('compression','zstd').saveAsTable(table))
 def verify():
  actual=spark.table(table);expected=spark.sql(select(kind,count))
  # Bidirectional full-field multiset equality, not just key counts.
  a=actual.exceptAll(expected).count();b=expected.exceptAll(actual).count();assert a==b==0
  d=spark.sql('DESCRIBE DETAIL '+table).first().asDict();return {'mismatches':a+b,'files':d['numFiles'],'bytes':d['sizeInBytes'],'version':0}
 phase('exact-full-parity-'+kind,verify)
# Distributed update keys cover 200k independently located edges; stages outside timing.
phase('stage-updates',lambda: spark.sql(select('edge',UPDATES,True)).write.format('delta').saveAsTable('ashlar_scale.edge_stage'))
phase('merge-scattered-edges',lambda: spark.sql('MERGE INTO ashlar_scale.edge_current t USING ashlar_scale.edge_stage s ON t.lookup_hash=s.lookup_hash AND t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id WHEN MATCHED THEN UPDATE SET *').collect() and 'completed')
def check_updates():
 t=spark.table('ashlar_scale.edge_current');s=spark.table('ashlar_scale.edge_stage');a=t.join(s.select('lookup_hash'),'lookup_hash').select(t['*']);assert a.exceptAll(s).count()==s.exceptAll(a).count()==0
 assert spark.sql('SELECT count(*) FROM ashlar_scale.edge_current VERSION AS OF 0 WHERE entity_version <> 0').first()[0]==0
 assert t.filter('entity_version=1').count()==UPDATES
 return {'exact_updated_rows':UPDATES,'old_snapshot_unchanged':True,'history':[r.asDict() for r in spark.sql('DESCRIBE HISTORY ashlar_scale.edge_current').select('version','operation','operationMetrics').collect()]}
phase('update-and-old-snapshot-integrity',check_updates)
keys=[1+(i*15485863)%EDGES for i in range(50)]
def lookup(key):
 source='other' if key%10==0 else 'pilot';typ=key%32+1
 h=spark.sql(f"SELECT sha2(to_json(named_struct('source_system','{source}','rel_type_id',cast({typ} AS BIGINT),'id',cast({key} AS BIGINT))),256)").first()[0]
 t=time.perf_counter();r=spark.sql(f"SELECT * FROM ashlar_scale.edge_current WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={typ} AND id={key}").collect();ms=(time.perf_counter()-t)*1000;assert len(r)==1 and r[0].id==key;return ms
serial=phase('singleton-50',lambda:[lookup(k) for k in keys])
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool: parallel=phase('singleton-4-clients-50',lambda:list(pool.map(lookup,keys)))
report['caller_p95_ms']={'serial':sorted(serial)[47],'concurrent':sorted(parallel)[47]};report['state']='passed correctness; performance measured';save();spark.stop()

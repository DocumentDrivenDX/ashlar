"""Read-only full-carrier scan and exact singleton measurement on retained24M Delta."""
import json,time,hashlib,math
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from scale_workload import select
B=Path(__file__).resolve().parent
def main():
 out=B/'out/large-local-read-r415.json';assert not out.exists()
 prior=json.loads((B/'out/entropy-local-20261006/summary.json').read_text())
 a={'state':'running','source_sha256':hashlib.sha256((B/'out/entropy-local-20261006/summary.json').read_bytes()).hexdigest(),'scope':'Retained24M synthetic carriers; full-field scan fingerprint and exact sampled reads, not a new ingest or billion-scale test','phases':[],'reads':[]}
 def save():out.write_text(json.dumps(a,indent=2)+'\n')
 save();start=time.monotonic()
 builder=SparkSession.builder.master('local[8]').appName('Ashlar retained24M full scan').config('spark.driver.memory','32g').config('spark.sql.shuffle.partitions','256').config('spark.sql.session.timeZone','UTC').config('spark.jars.ivy','/tmp/ashlar-scale-ivy').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
 s=configure_spark_with_delta_pip(builder).getOrCreate();s.sparkContext.setLogLevel('ERROR');a.update(spark=s.version,delta='3.2.1',workers=8,driver_gib=32)
 try:
  for kind,count in [('object',4000000),('edge',20000000)]:
   path=prior[kind]['path'];assert Path(path).exists();f=s.read.format('delta').option('versionAsOf',0).load(path);f.createOrReplaceTempView('current_carriers');typ='type_id' if kind=='object' else 'rel_type_id'
   t=time.monotonic();r=s.sql(f"SELECT count(*) rows,sum(cast(xxhash64(*) AS DECIMAL(38,0))) full_field_fingerprint,sum(CASE WHEN lookup_hash != sha2(to_json(named_struct('source_system',source_system,'{typ}',{typ},'id',id)),256) OR entity_version != 0 OR json_object_keys(props_json) IS NULL THEN 1 ELSE 0 END) invalid FROM current_carriers").first().asDict();assert r['rows']==count and r['invalid']==0
   detail=s.sql(f'DESCRIBE DETAIL delta.`{path}`').first().asDict();assert detail['numFiles']==prior[kind]['files'] and detail['sizeInBytes']==prior[kind]['bytes']
   a['phases'].append({'kind':kind,'rows':count,'seconds':time.monotonic()-t,'fingerprint':str(r['full_field_fingerprint']),'invalid':r['invalid'],'files':detail['numFiles'],'bytes':detail['sizeInBytes'],'path':path,'pin':0});save();print(kind,a['phases'][-1],flush=True)
   keys=[1+(i*15485863)%count+(4000000 if kind=='edge' else 0) for i in range(16)]
   expected={r.id:r for r in s.sql(select(kind,count,entropy=True,truss_shape=True)).filter('id IN ('+','.join(map(str,keys))+')').collect()};assert len(expected)==16
   for phase in range(2):
    for key in keys:
     row=expected[key];t=time.perf_counter();got=f.filter((f.lookup_hash==row.lookup_hash)&(f.source_system==row.source_system)&(f[typ]==row[typ])&(f.id==key)).collect();ms=(time.perf_counter()-t)*1000;assert got==[row];a['reads'].append({'kind':kind,'phase':phase,'id':key,'caller_ms':ms});save()
  a['p95_ms']={kind+'-'+str(phase):sorted(r['caller_ms'] for r in a['reads'] if r['kind']==kind and r['phase']==phase)[15] for kind in ['object','edge'] for phase in range(2)};a.update(state='Full24M-carrier scan and64exact singleton reads pass',wall_s=time.monotonic()-start);save();print(json.dumps(a['p95_ms']),flush=True)
 except Exception as e:a.update(state='stopped',error=str(e));save();raise
 finally:s.stop()
if __name__=='__main__':main()

"""Run the same calibrated higher-entropy slice on existing native compute."""
import json,time,pathlib
from persistent_sql import Client
from scale_workload import select,parity_sql,NODES,EDGES
B=pathlib.Path(__file__).resolve().parent;O=B/'out/native/ashlar_entropy_20261006_r86';F='client_dev.ashlar_entropy_20261006_r86'
local=json.loads((B/'out/entropy-local-20261006/summary.json').read_text());assert local['state']=='passed full-row parity and storage-profile constraints'
c=Client(O,observation_timeout=960,cancel_after=900)
assert c.sql('private-schema-absent',"SHOW SCHEMAS IN client_dev LIKE 'ashlar_entropy_20261006_r86'")==[]
c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Authorized 24M high-entropy Ashlar storage-profile sensitivity; synthetic'")
c.sql('runtime','SELECT current_version(),current_timezone()')
report={'state':'running','nodes':NODES,'edges':EDGES,'schema':F,'local_calibration':local['calibration'],'phases':[],'compute':'Existing single-cluster 2X-Small warehouse, no resize/new compute; billing dollars unavailable','scope':'Full 0.3 current-carrier surface with synthetic shared allocation/unique endpoint pair shape; not real Truss feed, full publication or billion-scale admission'}
def save():(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
def phase(label,sql):
 t=time.perf_counter();r=c.sql(label,sql);report['phases'].append({'label':label,'seconds':time.perf_counter()-t,'result':r});save();print(label,report['phases'][-1]['seconds'],flush=True)
 if c.records[-1].get('cancel_requested'):raise RuntimeError('Phase hit cancellation bound; inspect committed history before any growth')
 return r
for kind,count in [('object',NODES),('edge',EDGES)]:
 table=F+'.'+kind+'_current'
 phase('create-'+kind,f"CREATE TABLE {table} USING DELTA CLUSTER BY (lookup_hash) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.dataSkippingStatsColumns'='lookup_hash,source_system,{('type_id' if kind=='object' else 'rel_type_id')},id','delta.parquet.compression.codec'='zstd') AS {select(kind,count,entropy=True,truss_shape=True)}")
 phase('optimize-'+kind,f'OPTIMIZE {table}')
 assert phase('identity-cardinality-'+kind,f'SELECT count(*),count(DISTINCT id) FROM {table}')==[[str(count),str(count)]]
 assert phase('full-parity-'+kind,parity_sql(table,kind,count,entropy=True,truss_shape=True))==[['0']]
 phase('detail-'+kind,f'DESCRIBE DETAIL {table}');phase('history-'+kind,f'DESCRIBE HISTORY {table} LIMIT 1')
assert phase('endpoint-closure',f'''SELECT count(*) FROM (
 SELECT e.id FROM {F}.edge_current e LEFT ANTI JOIN {F}.object_current n ON e.source_system=n.source_system AND e.source_type=n.type_id AND e.source_id=n.id
 UNION ALL
 SELECT e.id FROM {F}.edge_current e LEFT ANTI JOIN {F}.object_current n ON e.source_system=n.source_system AND e.target_type=n.type_id AND e.target_id=n.id)''')==[['0']]
assert phase('unique-relationship-endpoints',f'SELECT count(*) FROM (SELECT source_system,rel_type_id,source_type,source_id,target_type,target_id FROM {F}.edge_current GROUP BY ALL HAVING count(*)>1)')==[['0']]
assert phase('shared-allocation-overlap',f'SELECT count(*) FROM {F}.edge_current e JOIN {F}.object_current n ON e.source_system=n.source_system AND e.id=n.id')==[['0']]
c.history();report['state']='passed full-row parity and storage-profile constraints';save()

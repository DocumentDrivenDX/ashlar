"""Databricks counterpart, run only after local scale evidence is terminal.
Uses existing warehouse; no resize, new compute, cleanup, or blind write retry.
"""
import json,time,pathlib
from persistent_sql import Client
from scale_workload import select,NODES,EDGES,UPDATES
B=pathlib.Path(__file__).resolve().parent;O=B/'out/native/ashlar_scale_20261006_r85'
local=json.loads((B/'out/local-scale-20261006/summary.json').read_text())
assert local['state']=='passed correctness; performance measured',local['state']
c=Client(O,observation_timeout=960);schema='client_dev.ashlar_scale_20261006_r85';start=time.time()
c.sql('statement-time-limit','SET STATEMENT_TIMEOUT=900')
assert c.sql('private-schema-absent',"SHOW SCHEMAS IN client_dev LIKE 'ashlar_scale_20261006_r85'")==[]
c.sql('schema',f"CREATE SCHEMA {schema} COMMENT 'Authorized Ashlar 4M node 20M edge scale sensitivity; synthetic'")
c.sql('runtime','SELECT current_version()')
report={'state':'running','nodes':NODES,'edges':EDGES,'schema':schema,'phases':[],'scope':local['scope'].replace('Range-sorted hash files in OSS Delta; not native liquid clustering.','Native liquid clustering,64MiB target. No billion-scale admission.'),'compute':'existing data-gateway 2X-Small single cluster; billing dollars unavailable'}
def save():(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
def phase(label,sql):
 t=time.perf_counter();result=c.sql(label,sql);report['phases'].append({'label':label,'seconds':time.perf_counter()-t,'result':result});save();print(label,report['phases'][-1]['seconds'],flush=True);return result
for kind,count in [('object',NODES),('edge',EDGES)]:
 table=schema+'.'+kind+'_current'
 phase('create-'+kind,f"CREATE TABLE {table} USING DELTA CLUSTER BY (lookup_hash) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.dataSkippingStatsColumns'='lookup_hash,source_system,{('type_id' if kind=='object' else 'rel_type_id')},id','delta.parquet.compression.codec'='zstd') AS {select(kind,count)}")
 phase('optimize-'+kind,f'OPTIMIZE {table}')
 # Complete bidirectional multiset equality over all generated carrier columns.
 mismatch=phase('exact-full-parity-'+kind,f'SELECT count(*) FROM ((SELECT * FROM {table} EXCEPT ALL {select(kind,count)}) UNION ALL ({select(kind,count)} EXCEPT ALL SELECT * FROM {table}))')
 assert mismatch==[['0']],mismatch
 phase('detail-'+kind,f'DESCRIBE DETAIL {table}')
 phase('version-'+kind,f'DESCRIBE HISTORY {table} LIMIT 1')
phase('stage-updates',f'CREATE TABLE {schema}.edge_stage USING DELTA AS {select("edge",UPDATES,True)}')
old=int(c.sql('before-update-version',f'DESCRIBE HISTORY {schema}.edge_current LIMIT 1')[0][0]);report['old_edge_version']=old
phase('merge-scattered-edges',f'MERGE INTO {schema}.edge_current t USING {schema}.edge_stage s ON t.lookup_hash=s.lookup_hash AND t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id WHEN MATCHED THEN UPDATE SET *')
assert phase('changed-row-count',f'SELECT count(*) FROM {schema}.edge_current WHERE entity_version=1')==[[str(UPDATES)]]
assert phase('old-snapshot',f'SELECT count(*) FROM {schema}.edge_current VERSION AS OF {old} WHERE entity_version<>0')==[['0']]
assert phase('updated-full-parity',f'SELECT count(*) FROM ((SELECT t.* FROM {schema}.edge_current t JOIN {schema}.edge_stage s ON t.lookup_hash=s.lookup_hash EXCEPT ALL SELECT * FROM {schema}.edge_stage) UNION ALL (SELECT * FROM {schema}.edge_stage EXCEPT ALL SELECT t.* FROM {schema}.edge_current t JOIN {schema}.edge_stage s ON t.lookup_hash=s.lookup_hash))')==[['0']]
phase('merge-history',f'DESCRIBE HISTORY {schema}.edge_current LIMIT 1')
c.sql('disable-result-cache','SET use_cached_result=false')
for i in range(50):
 key=1+(i*15485863)%EDGES;source='other' if key%10==0 else 'pilot';typ=key%32+1
 rows=phase('singleton-'+str(i),f"SELECT * FROM {schema}.edge_current WHERE lookup_hash=sha2(to_json(named_struct('source_system','{source}','rel_type_id',cast({typ} AS BIGINT),'id',cast({key} AS BIGINT))),256) AND source_system='{source}' AND rel_type_id={typ} AND id={key}")
 assert len(rows)==1 and rows[0][2]==str(key)
c.history();report['elapsed_seconds']=time.time()-start;report['state']='passed correctness; performance measured';save()

import hashlib,json,os,signal,sys,time,uuid
from pathlib import Path
ROOT=Path('/Users/erik/Projects/ashlar')
COHORT=Path('/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029')
ATTEMPT=Path('/private/tmp/ashlar-r16-delta4-native-smoke-20261029')/('attempt-'+uuid.uuid4().hex)
ATTEMPT.parent.mkdir(mode=0o700,parents=True,exist_ok=True);os.chmod(ATTEMPT.parent,0o700);ATTEMPT.mkdir(mode=0o700)
os.environ.update(JAVA_HOME='/opt/homebrew/Cellar/openjdk@21/21.0.13.0.2/libexec/openjdk.jdk/Contents/Home',SPARK_LOCAL_IP='127.0.0.1',PYSPARK_PYTHON=str(COHORT/'venv/bin/python'),PYSPARK_DRIVER_PYTHON=str(COHORT/'venv/bin/python'),PYTHONDONTWRITEBYTECODE='1')
signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(TimeoutError('300-second native smoke deadline')));signal.alarm(300)
sys.path.insert(0,str(ROOT/'src'))
from ashlar_host.local_current_carrier_profile import create_local_current_xxhash64_tables
from ashlar_host.schema_rows import LOCAL_CURRENT_ROLE_TO_UMF

def require(v,msg):
 if not v:raise RuntimeError(msg)
# Byte-identical Delta 4.4.1 JAR alias required by the existing local adapter's
# filename allowlist; source artifact hashes remain the runtime cohort hashes.
jars=ATTEMPT/'jars';jars.mkdir(mode=0o700)
jar_names={'delta-spark_4.2_2.13-4.4.1.jar':'delta-spark_2.13-4.4.1.jar','delta-storage-4.4.1.jar':'delta-storage-4.4.1.jar'}
jar_hashes={}
for expected,observed in jar_names.items():
 src=COHORT/'jars'/observed;dst=jars/expected;dst.write_bytes(src.read_bytes());jar_hashes[expected]=hashlib.sha256(dst.read_bytes()).hexdigest()
from pyspark.sql import SparkSession
spark=(SparkSession.builder.master('local[1]').appName('Ashlar R16 Delta 4.4.1 bounded native smoke')
 .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
 .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC')
 .config('spark.sql.ansi.enabled','true').config('spark.sql.decimalOperations.allowPrecisionLoss','false')
 .config('spark.jars',','.join(str(jars/n) for n in jar_names)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
 .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
 .config('spark.sql.warehouse.dir',str(ATTEMPT/'warehouse')).getOrCreate())
start=time.monotonic();profile=None;targets=None;node=None;edge=None
try:
 require(spark.version=='4.2.0','unexpected Spark version '+spark.version)
 java_version=spark.sparkContext._jvm.java.lang.System.getProperty('java.specification.version')
 require(java_version=='21','unexpected Java version '+java_version)
 spark.sparkContext.setLogLevel('ERROR')
 # The pinned table creator owns its exact local.configured target bindings.
 tables={role:'local.configured.'+role for role in LOCAL_CURRENT_ROLE_TO_UMF}
 targets,profile,_=create_local_current_xxhash64_tables(spark,ATTEMPT/'installation',tables,table_prefix='local.configured')
 roles={t.table.rsplit('.',1)[1]:t for t in targets}
 require(len(roles)==6,'six distinct roles not created')
 obj=str(ATTEMPT/'installation'/'object_current');edge_path=str(ATTEMPT/'installation'/'edge_current')
 spark.sql("""INSERT INTO delta.`%s` (source_system,type_id,id,logical_key_json,schema_revision,entity_version,props_json,retained_json,source_feed,source_epoch,published_at,lookup_hash)
 SELECT 'smoke-source',CAST(11 AS BIGINT),CAST(22 AS BIGINT),'\"22\"','r1',CAST(1 AS BIGINT),'{}','{}','smoke-feed','smoke-epoch',TIMESTAMP '2026-10-29 00:00:00',xxhash64(CAST('smoke-source' AS STRING),CAST(11 AS BIGINT),CAST(22 AS BIGINT))"""%obj)
 spark.sql("""INSERT INTO delta.`%s` (source_system,rel_type_id,id,source_type,source_id,target_type,target_id,schema_revision,entity_version,props_json,retained_json,source_feed,source_epoch,published_at,lookup_hash)
 SELECT 'smoke-source',CAST(33 AS BIGINT),CAST(44 AS BIGINT),CAST(11 AS BIGINT),CAST(22 AS BIGINT),CAST(11 AS BIGINT),CAST(23 AS BIGINT),'r1',CAST(1 AS BIGINT),'{}','{}','smoke-feed','smoke-epoch',TIMESTAMP '2026-10-29 00:00:00',xxhash64(CAST('smoke-source' AS STRING),CAST(33 AS BIGINT),CAST(44 AS BIGINT))"""%edge_path)
 node=spark.sql("SELECT source_system,cast(type_id AS STRING) type_id,cast(id AS STRING) id,cast(lookup_hash AS STRING) lookup_hash,cast(xxhash64(CAST(source_system AS STRING),CAST(type_id AS BIGINT),CAST(id AS BIGINT)) AS STRING) expected FROM delta.`%s`"%obj).collect()
 edge=spark.sql("SELECT source_system,cast(rel_type_id AS STRING) rel_type_id,cast(id AS STRING) id,cast(lookup_hash AS STRING) lookup_hash,cast(xxhash64(CAST(source_system AS STRING),CAST(rel_type_id AS BIGINT),CAST(id AS BIGINT)) AS STRING) expected FROM delta.`%s`"%edge_path).collect()
 require(len(node)==1 and node[0]['type_id']=='11' and node[0]['id']=='22' and node[0]['lookup_hash']==node[0]['expected'],'node typed xxhash roundtrip mismatch')
 require(len(edge)==1 and edge[0]['rel_type_id']=='33' and edge[0]['id']=='44' and edge[0]['lookup_hash']==edge[0]['expected'],'edge typed xxhash roundtrip mismatch')
 role_receipts={}
 for role,t in roles.items():
  rows=spark.sql('SELECT cast(count(*) AS STRING) n FROM delta.`'+str(t.path)+'`').collect();role_receipts[role]={'uuid':t.uuid,'rows':int(rows[0]['n'])}
 require(role_receipts['object_current']['rows']==1 and role_receipts['edge_current']['rows']==1 and all(role_receipts[r]['rows']==0 for r in role_receipts if r not in ('object_current','edge_current')),'unexpected row counts')
finally:spark.stop()
result={'profile':'ashlar-delta4-r16-runtime-smoke/0.1','status':'passed-bounded-local-table-smoke','attemptRoot':str(ATTEMPT),'elapsedSeconds':round(time.monotonic()-start,3),'python':sys.version.split()[0],'spark':spark.version,'deltaSpark':'4.4.1','java':java_version,'master':'local[1]','carrierProfile':profile['physical_profile'],'lookupHashProfile':profile['lookup_hash_profile'],'umfRevision':profile['umf_revision'],'umfCandidateCommit':profile['umf_candidate_commit'],'bridgeSha256':profile['umf_bridge_sha256'],'deltaJarSha256':jar_hashes,'roles':role_receipts,'nodeTypedKey':{k:node[0][k] for k in ('source_system','type_id','id','lookup_hash')},'edgeTypedKey':{k:edge[0][k] for k in ('source_system','rel_type_id','id','lookup_hash')},'publication':False,'query':False,'sourceMutation':False,'ack':False,'cloud':False,'limitations':['Ashlar UMF Delta 4 six-role native table creator was exercised; two tiny identity rows were inserted with Spark Delta SQL because the configured-local .4 lifecycle adapter intentionally only allows initialization.','No Truss source/feed, publication manifest, singleton resolver, Weft query, protected ACK, Unity Catalog, or scale claim.','The R16 cohort Delta Spark JAR was copied byte-identically under the filename admitted by the existing local runtime. The hash records both the copied artifact and its cohort source.']}
raw=json.dumps(result,sort_keys=True,indent=2)+'\n';(ATTEMPT/'smoke-result.json').write_text(raw);print(json.dumps(result,sort_keys=True))

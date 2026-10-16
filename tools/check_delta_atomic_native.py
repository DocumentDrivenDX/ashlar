"""Native Delta4 same-commit data/marker/SetTransaction and conflict controls."""
from pathlib import Path
import argparse,hashlib,json,uuid
from pyspark.sql import SparkSession
from ashlar_host.delta_atomic import DeltaAtomic,DeltaAlreadySubmitted,DeltaAtomicRefused,DeltaAtomicConflict
parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--jars',type=Path,required=True);args=parser.parse_args();out=args.output.absolute();out.mkdir(parents=True,exist_ok=False)
expected={'delta-spark_2.13-4.0.0.jar':'538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e','delta-storage-4.0.0.jar':'9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d'}
assert all(hashlib.sha256((args.jars/n).read_bytes()).hexdigest()==sha for n,sha in expected.items())
spark=(SparkSession.builder.master('local[1]').appName('Ashlar native atomic Delta prerequisite').config('spark.ui.enabled','false').config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.ansi.enabled','true').config('spark.sql.session.timeZone','UTC').config('spark.jars',','.join(str(args.jars/n)for n in expected)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir',str(out/'warehouse')).getOrCreate());spark.sparkContext.setLogLevel('ERROR');checks=[]
try:
 path=out/'table';epoch=str(uuid.uuid4())
 spark.sql(f"CREATE TABLE delta.`{path}` (id STRING,status STRING) USING DELTA TBLPROPERTIES ('delta.isolationLevel'='Serializable','umf.actions.epoch'='{epoch}')")
 spark.sql(f"INSERT INTO delta.`{path}` VALUES ('a','draft'),('b','draft')")
 identity=spark.sql(f'DESCRIBE DETAIL delta.`{path}`').first()['id']
 def begin(label):return DeltaAtomic(spark,path=path,table_uuid=identity,epoch=epoch,request_sha256=hashlib.sha256(label.encode()).hexdigest(),maximum_rows=10)
 tx=begin('first');candidate=tx.preimage.selectExpr('id',"CASE WHEN id='a' THEN 'approved' ELSE status END AS status")
 receipt=tx.commit(candidate,markers_json=json.dumps({'pre':True,'post':True,'changed':[{'id':'a'}],'created':[],'noOp':False}))
 assert [(r['id'],r['status'])for r in spark.read.format('delta').load(str(path)).orderBy('id').collect()]==[('a','approved'),('b','draft')]
 events=[json.loads(line)for line in (path/'_delta_log'/f'{receipt.version:020d}.json').read_text().splitlines()]
 assert any('add'in e for e in events) and any('remove'in e for e in events) and any('txn'in e for e in events)
 assert any(json.loads(e['commitInfo']['userMetadata'])['markers']['changed']==[{'id':'a'}]for e in events if 'commitInfo'in e)
 checks.append('data replacement, original marker JSON and idempotency marker share one actual Delta commit')
 try:begin('first')
 except DeltaAlreadySubmitted:checks.append('same original request observes durable native SetTransaction across new transaction')
 else:raise AssertionError('duplicate original request accepted')
 stale=begin('concurrent');candidate=stale.preimage.selectExpr('id',"CASE WHEN id='a' THEN 'stale' ELSE status END AS status")
 spark.sql(f"UPDATE delta.`{path}` SET status='independent' WHERE id='b'")
 latest=int(spark.sql(f'DESCRIBE HISTORY delta.`{path}`').first()['version'])
 try:stale.commit(candidate,markers_json='{"pre":true,"post":true}')
 except DeltaAtomicConflict as error:
  assert any(s in str(error.original)for s in ('ConcurrentDeleteReadException','DELTA_CONCURRENT_DELETE_READ','ConcurrentAppendException','DELTA_CONCURRENT_APPEND'))
  checks.append('independent real Delta update conflicts with registered whole-table read; no retry')
 else:raise AssertionError('native concurrent commit accepted stale preimage')
 assert int(spark.sql(f'DESCRIBE HISTORY delta.`{path}`').first()['version'])==latest
 assert [(r['id'],r['status'])for r in spark.read.format('delta').load(str(path)).orderBy('id').collect()]==[('a','approved'),('b','independent')]
 checks.append('conflict leaves committed data and version unchanged')
 invalid=begin('invalid')
 try:invalid.commit(invalid.preimage,markers_json='{"post":true,"post":false}')
 except DeltaAtomicRefused:checks.append('duplicate marker JSON refuses before native write')
 else:raise AssertionError('ambiguous marker accepted')
 assert int(spark.sql(f'DESCRIBE HISTORY delta.`{path}`').first()['version'])==latest
finally:
 spark.stop();(out/'verification.json').write_text(json.dumps({'checks':checks,'sparkStopped':spark.sparkContext._jsc is None,'temporal':'HELD','scope':'native local Delta optimistic commit primitive; original action compilation, authorization, verification semantics and SDK invocation still separate gates'},indent=2)+'\n')
print(json.dumps({'status':'pass','checks':len(checks),'output':str(out)}))

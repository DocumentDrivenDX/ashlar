import sys,os,json,time,hashlib
from pathlib import Path
sys.path[:0]=['/Users/erik/Projects/ashlar/src','/private/tmp/ashlar-spark4-python39-bridge']
from pyspark.sql import SparkSession
from ashlar_host.path_capture import PathCaptureConfig
from ashlar_host.databricks_current_source_capture import capture_current_query_sources
root=Path(__file__).parent
jars=Path('/Users/erik/.ivy2.5.2/jars')
paths=[jars/n for n in ('io.delta_delta-spark_2.13-4.0.0.jar','io.delta_delta-storage-4.0.0.jar','org.antlr_antlr4-runtime-4.13.1.jar')]
started=time.monotonic()
spark=(SparkSession.builder.master('local[1]').appName('Ashlar three-row source capture')
.config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
.config('spark.databricks.delta.snapshotPartitions','1').config('spark.jars',','.join(map(str,paths)))
.config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
.config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
.config('spark.sql.warehouse.dir',str(root/'warehouse')).getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
try:
 table='spark_catalog.default.current_source_capture'
 spark.sql(f"CREATE TABLE {table} (private_key STRING, eligible BOOLEAN, text_value STRING, integer_value BIGINT) USING DELTA LOCATION '{root/'data'}'")
 rows=[('k1',True,'雪\x00é',-9223372036854775808),('k2',True,'',9223372036854775807),('excluded',False,'hidden',0)]
 spark.createDataFrame(rows,'private_key string, eligible boolean, text_value string, integer_value long').write.format('delta').mode('append').saveAsTable(table)
 detail=spark.sql('DESCRIBE DETAIL '+table).first().asDict()
 version=str(spark.sql('DESCRIBE HISTORY '+table).first()['version'])
 schema=spark.read.format('delta').option('versionAsOf',version).table(table).schema.json()
 binding={'version':'weft.databricks.current-values/0.1-candidate','scans':[{'scan':'s0','relation':{'catalog':'spark_catalog','schema':'default','table':'current_source_capture','uuid':detail['id'],'version':version,'schemaSha256':hashlib.sha256(schema.encode()).hexdigest(),'rowKeyColumn':'private_key','eligibleColumn':'eligible'}}], 'values':[{'scan':'s0','column':'text_value'},{'scan':'s0','column':'integer_value'},{'scan':'s0','column':'text_value'}]}
 # Source-capture-only fixture: no CQP binding or authority qualification.
 raw=json.dumps(binding,separators=(',',':'))
 spark.sql('UPDATE '+table+" SET text_value='later' WHERE private_key='k1'")
 config=PathCaptureConfig(3,1024,8192)
 capture=capture_current_query_sources(spark,raw,config=config)
 scans=json.loads(capture.scans_json)
 actual={r['key']:r['cells'] for r in scans[0]['rows']}
 assert scans[0]['expectedRowCount']=='2' and actual=={'k1':{'text_value':{'family':'string','value':'雪\x00é'},'integer_value':{'family':'integer','value':'-9223372036854775808'}},'k2':{'text_value':{'family':'string','value':''},'integer_value':{'family':'integer','value':'9223372036854775807'}}}
 assert spark.sql('DESCRIBE DETAIL '+table).first()['id']==detail['id']
 try:capture_current_query_sources(spark,raw,config=PathCaptureConfig(1,1024,8192))
 except ValueError:overflow=True
 else:raise AssertionError('Complete population overflow accepted')
 (root/'capture.json').write_text(capture.scans_json)
 report={'format':'ashlar-current-source-local-smoke/0.1','passed':True,'spark':spark.version,'delta':'4.0.0','input_rows':3,'eligible_rows':2,'pinned_version':version,'uuid_stable':True,'original_values_after_update':True,'i64_extrema_exact':True,'unicode_nul_empty_exact':True,'repeated_column_union':True,'population_overflow_refused':overflow,'scope':'Actual local pinned Delta source capture only; independently authored capture fixture, no CQP/native UC or source/action authority qualification','seconds_before_close':round(time.monotonic()-started,3)}
finally:spark.stop()
report['closed']=True;report['seconds']=round(time.monotonic()-started,3)
(root/'summary.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))

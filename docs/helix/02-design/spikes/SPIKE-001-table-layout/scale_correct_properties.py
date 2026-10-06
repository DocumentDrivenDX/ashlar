"""Correct synthetic update map shape, local first then native, without cleanup.
Initial measurements remain physical evidence; initial update profile was invalid.
"""
import os,json,time,sys
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from scale_workload import select,UPDATES
B=Path(__file__).resolve().parent;O=B/'out/property-map-correction-20261006';O.mkdir(parents=True,exist_ok=True)
if '--native-only' in sys.argv:
 report=json.loads((O/'summary.json').read_text());assert report['local']['state']=='passed'
else:
 local=json.loads((B/'out/local-scale-20261006/summary.json').read_text());root=Path(local['root']);path=str(root/'warehouse/ashlar_scale.db/edge_current');stage=str(root/'corrected-stage')
 builder=SparkSession.builder.master('local[8]').appName('Ashlar map correction').config('spark.driver.memory','32g').config('spark.sql.shuffle.partitions','256').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.jars.ivy','/tmp/ashlar-scale-ivy')
 s=configure_spark_with_delta_pip(builder).getOrCreate();s.sparkContext.setLogLevel('ERROR')
 s.sql(select('edge',UPDATES,True)).write.format('delta').save(stage)
 t=time.perf_counter();s.sql(f'MERGE INTO delta.`{path}` t USING delta.`{stage}` p ON t.lookup_hash=p.lookup_hash AND t.source_system=p.source_system AND t.rel_type_id=p.rel_type_id AND t.id=p.id WHEN MATCHED THEN UPDATE SET *').collect();elapsed=time.perf_counter()-t
 actual=s.read.format('delta').load(path);wanted=s.read.format('delta').load(stage);changed=actual.join(wanted.select('lookup_hash'),'lookup_hash').select(actual['*']);assert changed.exceptAll(wanted).count()==wanted.exceptAll(changed).count()==0
 assert actual.filter("json_object_keys(props_json) IS NULL OR exists(json_object_keys(props_json), k -> NOT (k RLIKE '^[0-9]+$'))").count()==0
 assert actual.count()==20_000_000
 report={'local':{'state':'passed','merge_seconds':elapsed,'exact_corrected_rows':UPDATES,'all_property_keys_decimal_ids':True,'edges':20_000_000,'delta_version':2},'scope':'Synthetic property 107 added; existing producer-key properties retained verbatim. No real source event/journal/ordering claim.'}
 (O/'summary.json').write_text(json.dumps(report,indent=2)+'\n');s.stop()
if '--local-only' in sys.argv: sys.exit(0)
from persistent_sql import Client
c=Client(O/'native',observation_timeout=960);F='client_dev.ashlar_scale_20261006_r85'
assert int(c.sql('prior-version',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0])==1
c.sql('statement-timeout','SET STATEMENT_TIMEOUT=900')
c.sql('corrected-stage',f'CREATE TABLE {F}.edge_stage_corrected USING DELTA AS {select("edge",UPDATES,True)}')
t=time.perf_counter();c.sql('corrected-merge',f'MERGE INTO {F}.edge_current t USING {F}.edge_stage_corrected p ON t.lookup_hash=p.lookup_hash AND t.source_system=p.source_system AND t.rel_type_id=p.rel_type_id AND t.id=p.id WHEN MATCHED THEN UPDATE SET *');elapsed=time.perf_counter()-t
v=int(c.sql('corrected-version',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0])
assert c.sql('exact-corrected-row-parity',f'SELECT count(*) FROM ((SELECT t.* FROM {F}.edge_current VERSION AS OF {v} t JOIN {F}.edge_stage_corrected p ON t.lookup_hash=p.lookup_hash EXCEPT ALL SELECT * FROM {F}.edge_stage_corrected) UNION ALL (SELECT * FROM {F}.edge_stage_corrected EXCEPT ALL SELECT t.* FROM {F}.edge_current VERSION AS OF {v} t JOIN {F}.edge_stage_corrected p ON t.lookup_hash=p.lookup_hash))')==[['0']]
assert c.sql('all-property-id-keys',f"SELECT count(*) FROM {F}.edge_current VERSION AS OF {v} WHERE json_object_keys(props_json) IS NULL OR exists(json_object_keys(props_json), k -> NOT (k RLIKE '^[0-9]+$'))")==[['0']]
assert c.sql('edge-count',f'SELECT count(*) FROM {F}.edge_current VERSION AS OF {v}')==[['20000000']]
c.sql('corrected-detail',f'DESCRIBE DETAIL {F}.edge_current');c.history()
report['native']={'state':'passed','merge_seconds':elapsed,'delta_version':v,'exact_corrected_rows':UPDATES,'all_property_keys_decimal_ids':True,'edges':20_000_000};report['initial_update_qualification']='Previous non-ID key wrapper is invalid as an Ashlar property-map fixture; its results qualify carrier/file behavior only. All pre-correction singleton timings refer to that prior version.';(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')

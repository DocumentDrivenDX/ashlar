"""One unprofiled 225M-edge .4 Parquet-statistics comparison case."""
from __future__ import annotations
import argparse, hashlib, importlib.util, json, os, subprocess, sys, time
from pathlib import Path
from urllib.parse import unquote, urlparse

N=225_000_000
PROFILE='ashlar-delta/0.4-xxhash64'
HASH_PROFILE='spark-xxhash64-typed-key/0.1'
GENERATED_SHA='cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa'
WHEEL_SHA='18401e7052202515da90c5c7a87d775c8c33b82f82b9526f9802d615356b76af'
INSERT_SHA='088d365593b311cd9c75f14b9129cf4beaab24657e126a0d538de3fa99f2ff5e'
PARQUET_STATS_KEY='parquet.column.statistics.enabled#source_delivery_id'
DELTA_JARS=Path('/private/tmp/ashlar-release-20261021-m3rde5p2/jars')
DELTA_JAR_SHA={
 'delta-spark_4.2_2.13-4.4.1.jar':'2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be',
 'delta-storage-4.4.1.jar':'f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f'}
PARQUET_JAR_SHA={
 'parquet-column-1.17.0.jar':'2fcd38628897c3b547b92d0abf7200ec84e9eb3fb9e8450b196142e4d5229044',
 'parquet-hadoop-1.17.0.jar':'c47b0a875951fa191fadf8c5d13742e8938bdda9779b2d8fec5c5345ac9e0184'}
EVIDENCE=Path(__file__).resolve().parent
PRIOR=EVIDENCE.parent/'edge-write-attribution-20261023'/'attempt-04'
GIT='/opt/homebrew/Cellar/git/2.56.0_1/bin/git'
JAVA_HOME='/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home'


def digest(path:Path)->str:return hashlib.sha256(path.read_bytes()).hexdigest()
def record(path:Path,obj:dict)->None:
 with path.open('a',encoding='utf-8') as f:
  f.write(json.dumps(obj,sort_keys=True,separators=(',',':'))+'\n');f.flush();os.fsync(f.fileno())
def table_properties(detail):
 value=detail.get('properties') or {}
 return dict(value.items()) if hasattr(value,'items') else dict(value)
def inspect_footer(spark,files):
 reader=spark._jvm.org.apache.parquet.hadoop.ParquetFileReader
 hp=spark._jvm.org.apache.hadoop.fs.Path; conf=spark.sparkContext._jsc.hadoopConfiguration()
 rows=0;byte_total=0;codecs=set();role_stats={};per_file=[];row_group_count=0
 for f in files:
  byte_total+=f.stat().st_size;footer=reader.readFooter(conf,hp(str(f)));file_rows=0;file_groups=[]
  for block in footer.getBlocks():
   file_rows+=int(block.getRowCount());rows+=int(block.getRowCount());row_group_count+=1;fields={}
   for col in block.getColumns():
    codecs.add(str(col.getCodec().name()));name=str(col.getPath().toDotString());stats=col.getStatistics()
    primitive=col.getPrimitiveType()
    item={'physical_type':str(primitive.getPrimitiveTypeName()),'logical_type':str(primitive.getLogicalTypeAnnotation()) if primitive.getLogicalTypeAnnotation() is not None else None,
          'empty':bool(stats.isEmpty()),'has_non_null_value':bool(stats.hasNonNullValue()),'num_nulls_set':bool(stats.isNumNullsSet()),
          'num_nulls':int(stats.getNumNulls()) if stats.isNumNullsSet() else None,
          'min':str(stats.genericGetMin()) if stats.hasNonNullValue() else None,
          'max':str(stats.genericGetMax()) if stats.hasNonNullValue() else None}
    old=role_stats.setdefault(name,{'column_chunks':0,'empty_chunks':0,'chunks_with_non_null_value':0,'chunks_with_num_nulls':0})
    old['column_chunks']+=1;old['empty_chunks']+=int(item['empty']);old['chunks_with_non_null_value']+=int(item['has_non_null_value']);old['chunks_with_num_nulls']+=int(item['num_nulls_set'])
    fields[name]=item
   file_groups.append({'rows':int(block.getRowCount()),'columns':fields})
  per_file.append({'name':f.name,'bytes':f.stat().st_size,'rows':file_rows,'row_group_count':len(file_groups),'row_groups':file_groups})
 return {'rows':rows,'files':len(files),'row_groups':row_group_count,'parquet_bytes':byte_total,'codecs':sorted(codecs),'column_statistics':role_stats,'per_file':per_file}
def delta_stats(table_path):
 result=[]
 for path in sorted((table_path/'_delta_log').glob('*.json')):
  for line in path.read_text(encoding='utf-8').splitlines():
   action=json.loads(line)
   add=action.get('add')
   if not add:continue
   raw=add.get('stats');parsed=json.loads(raw) if isinstance(raw,str) else (raw or {})
   mins=parsed.get('minValues') or {};maxs=parsed.get('maxValues') or {};nulls=parsed.get('nullCount') or {}
   selected={'lookup_hash','source_system','rel_type_id','id'}
   result.append({'commit':path.name,'path':add.get('path'),'numRecords':parsed.get('numRecords'),
    'min_keys':sorted(mins),'max_keys':sorted(maxs),'null_count_keys':sorted(nulls),
    'selected_min':{k:mins[k] for k in sorted(mins) if k in selected},
    'selected_max':{k:maxs[k] for k in sorted(maxs) if k in selected},
    'selected_null_count':{k:nulls[k] for k in sorted(nulls) if k in selected}})
 return result
def lookup_summary(row,source_file_inventory_count,source_file_inventory_bytes,executed_plan):
 return {'row':row,'source_file_inventory_count':source_file_inventory_count,
         'source_file_inventory_bytes':source_file_inventory_bytes,'executed_plan':executed_plan}
def validate_delta_stats(rows):
 selected={'lookup_hash','source_system','rel_type_id','id'}
 expected_types={'source_system':str,'lookup_hash':int,'rel_type_id':int,'id':int}
 if len(rows)!=18:raise ValueError(f'expected 18 Delta AddFile stats records, observed {len(rows)}')
 total=0
 for i,row in enumerate(rows):
  n=row.get('numRecords')
  if type(n)is not int or n<=0:raise ValueError(f'Delta AddFile {i} has no positive integer numRecords')
  total+=n
  for name in ('selected_min','selected_max','selected_null_count'):
   values=row.get(name)
   if type(values)is not dict or set(values)!=selected:raise ValueError(f'Delta AddFile {i} missing exact selected {name} keys')
  for name in ('min_keys','max_keys','null_count_keys'):
   if row.get(name)!=sorted(selected):raise ValueError(f'Delta AddFile {i} has unexpected {name}')
  if any(type(v)is not int or v<0 for v in row['selected_null_count'].values()):raise ValueError(f'Delta AddFile {i} has invalid selected null counts')
  for name in ('selected_min','selected_max'):
   if any(type(row[name][key]) is not expected_types[key] for key in selected):raise ValueError(f'Delta AddFile {i} has mistyped selected {name} values')
 if total!=N:raise ValueError(f'Delta AddFile statistics total {total} differs from expected {N}')
 return {'add_files':len(rows),'records':total,'selected_keys':sorted(selected),'selected_statistics_present_on_every_add_file':True}
def exact_lookup(spark,table_path,out):
 from pyspark.sql import functions as F
 edge_id=1_234_567
 relation=spark.read.format('delta').load(str(table_path))
 query=(relation.where((F.col('source_system')=='layout-probe')&(F.col('rel_type_id')==F.lit(1).cast('long'))&
     (F.col('id')==F.lit(edge_id).cast('long'))&
     (F.col('lookup_hash')==F.expr("xxhash64(CAST('layout-probe' AS STRING), CAST(1 AS BIGINT), CAST(1234567 AS BIGINT))")))
     .select('source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision','entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position',F.expr('CAST(unix_micros(published_at) AS STRING)').alias('published_at_micros'),'lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'))
 files=[]
 for raw in query.inputFiles():
  u=urlparse(raw);q=Path(unquote(u.path)) if u.scheme=='file' else Path(raw)
  files.append({'path':raw,'bytes':q.stat().st_size if q.is_file() else None})
 plan=query._jdf.queryExecution().executedPlan().toString()
 rows=[r.asDict(recursive=True) for r in query.limit(2).collect()]
 expected={'source_system':'layout-probe','rel_type_id':1,'id':edge_id,'source_type':1,'source_id':edge_id//5,'target_type':1,
  'target_id':((edge_id//5)+((edge_id%5)//2))%45_000_000,'schema_revision':'layout-probe-r1','entity_version':1,
  'props_json':'{}','retained_json':'{}','order_key':None,'source_feed':'direct-layout-fixture','source_epoch':'1','source_position':edge_id,
  'published_at_micros':'1792022400000000','apply_batch_id':'layout-probe','source_cursor_json':'{}','source_delivery_id':str(edge_id)}
 # Timestamp literal's expected UTC microseconds are asserted by date calculation below, not inferred from a Spark datetime conversion.
 from datetime import datetime,timezone
 expected['published_at_micros']=str(int(datetime(2026,10,15,tzinfo=timezone.utc).timestamp()*1_000_000))
 if len(rows)!=1:raise ValueError(f'exact full-key lookup returned {len(rows)} rows')
 row=rows[0];actual={k:v for k,v in row.items() if k!='lookup_hash'}
 if actual!=expected:raise ValueError('exact full-key lookup row differs from independent expected values')
 return {'lookup_id':edge_id,'full_key_predicate_includes_native_xxhash64':True,'rows':rows,'expected_non_hash_values':expected,
         'source_file_inventory':files,**lookup_summary(rows[0],len(files),sum(x['bytes'] or 0 for x in files),plan),
         'semantic_scope':'one exact full-key point result only; no full-table value-equivalence proof'}

def main():
 p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);p.add_argument('--wheel',type=Path,required=True);p.add_argument('--commit',required=True);p.add_argument('--out',type=Path,required=True);p.add_argument('--case',choices=['baseline','candidate'],required=True);a=p.parse_args()
 repo=a.repo.resolve();wheel=a.wheel.resolve();out=a.out.resolve()
 if out.exists() or not wheel.is_file() or digest(wheel)!=WHEEL_SHA:raise SystemExit('fresh case output and reviewed wheel required')
 if any(not (DELTA_JARS/n).is_file() or digest(DELTA_JARS/n)!=v for n,v in DELTA_JAR_SHA.items()):raise SystemExit('Delta runtime JAR missing or changed')
 import pyspark
 parquet_dir=Path(pyspark.__file__).resolve().parent/'jars'
 if any(not (parquet_dir/n).is_file() or digest(parquet_dir/n)!=v for n,v in PARQUET_JAR_SHA.items()):raise SystemExit('Parquet runtime JAR missing or changed')
 if os.environ.get('PYSPARK_SUBMIT_ARGS','').find('--driver-memory 24g')<0:raise SystemExit('explicit 24g driver-memory setting required')
 generated=subprocess.run([GIT,'-C',str(repo),'show',f'{a.commit}:src/ashlar_host/resources/managed-current-core-010-xxhash64/runtime-carriers.generated.json'],check=True,capture_output=True).stdout
 out.mkdir(mode=0o700,parents=True);event_dir=out/'spark-events';event_dir.mkdir(mode=0o700)
 progress=out/'progress.jsonl';start=time.monotonic()
 def log(stage,**kwargs):record(progress,{'stage':stage,'elapsed_seconds':time.monotonic()-start,**kwargs})
 try:
  import delta,shutil
  free=shutil.disk_usage(out).free
  if free<12_000_000_000:raise SystemExit('12 GB free disk required for the bounded pair')
  if sys.version_info[:2]!=(3,12) or pyspark.__version__!='4.2.0' or delta.__version__!='4.4.1':raise SystemExit('reviewed Python/PySpark/Delta runtime mismatch')
  java=subprocess.run([JAVA_HOME+'/bin/java','-version'],check=True,capture_output=True,text=True)
  java_version=(java.stderr+' '+java.stdout).strip()
  if '21.0.13' not in java_version:raise SystemExit('reviewed JDK 21.0.13 required')
  sys.path.insert(0,str(wheel));from ashlar_host.managed_current_carriers import load_current_managed_carriers
  c=load_current_managed_carriers(PROFILE)
  if c.physical_profile!=PROFILE or c.lookup_hash_profile!=HASH_PROFILE or hashlib.sha256(c.generated_bytes).hexdigest()!=GENERATED_SHA or c.generated_bytes!=generated:raise ValueError('current .4 carrier profile/source pin differs')
  sql_bytes=(PRIOR/'insert.sql').read_bytes()
  if hashlib.sha256(sql_bytes).hexdigest()!=INSERT_SHA:raise ValueError('retained attempt-04 SQL bytes changed')
  (out/'insert.sql').write_bytes(sql_bytes)
  log('preflight',case=a.case,python=sys.version.split()[0],pyspark=pyspark.__version__,delta=delta.__version__,java=java_version,free_bytes=free,
      master='local[18]',driver_memory='24g',wheel_sha256=WHEEL_SHA,generated_sha256=GENERATED_SHA,insert_sql_sha256=INSERT_SHA,
      delta_jars=DELTA_JAR_SHA,parquet_jars=PARQUET_JAR_SHA,parquet_statistics_key=PARQUET_STATS_KEY,
      candidate_override='false' if a.case=='candidate' else None)
  from pyspark.sql import SparkSession
  builder=(SparkSession.builder.master('local[18]').appName('ashlar-parquet-stats-'+a.case).config('spark.driver.memory','24g')
   .config('spark.ui.enabled','false').config('spark.ui.showConsoleProgress','false').config('spark.jars',','.join(str(DELTA_JARS/n) for n in DELTA_JAR_SHA))
   .config('spark.databricks.delta.allowArbitraryProperties.enabled','true').config('spark.sql.parquet.compression.codec','zstd')
   .config('spark.sql.shuffle.partitions','18').config('spark.default.parallelism','18').config('spark.sql.session.timeZone','UTC')
   .config('spark.sql.warehouse.dir',str(out/'warehouse')).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
   .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.eventLog.enabled','true')
   .config('spark.eventLog.compress','false').config('spark.eventLog.dir',event_dir.as_uri()))
  if a.case=='candidate':builder=builder.config('spark.hadoop.'+PARQUET_STATS_KEY,'false')
  spark=builder.getOrCreate()
  try:
   if spark.version!='4.2.0' or spark.sparkContext.master!='local[18]' or int(spark._jvm.java.lang.Runtime.getRuntime().availableProcessors())!=18:raise ValueError('actual Spark identity differs')
   hconf=spark.sparkContext._jsc.hadoopConfiguration();raw=hconf.get(PARQUET_STATS_KEY)
   effective=bool(spark._jvm.org.apache.parquet.hadoop.ParquetOutputFormat.getStatisticsEnabled(hconf,'source_delivery_id'))
   other={key:bool(spark._jvm.org.apache.parquet.hadoop.ParquetOutputFormat.getStatisticsEnabled(hconf,key)) for key in ('source_system','rel_type_id','id','lookup_hash')}
   if effective!=(a.case=='baseline') or not all(other.values()):raise ValueError('effective per-column Parquet statistics settings differ from selected case')
   log('writer_config_verified',raw_hadoop_value=raw,effective_source_delivery_id_statistics=effective,effective_selected_key_statistics=other)
   table='`spark_catalog`.`default`.`edge_current`';spark.sql(c.create_sql('edge_current',('spark_catalog','default','edge_current')))
   detail=spark.sql(f'DESCRIBE DETAIL {table}').first().asDict();props=table_properties(detail)
   selected='lookup_hash,source_system,rel_type_id,id'
   if props.get('delta.dataSkippingStatsColumns')!=selected:raise ValueError('created table selected Delta statistics list differs')
   loc=urlparse(detail.get('location',''));table_path=Path(unquote(loc.path)).resolve()
   if loc.scheme!='file' or not table_path.is_relative_to(out):raise ValueError('table location escaped case output')
   sql=sql_bytes.decode('utf-8');plan='\n'.join(r[0] for r in spark.sql('EXPLAIN FORMATTED '+sql).collect());(out/'insert-plan.txt').write_text(plan)
   t=time.monotonic();spark.sql(sql);insert_elapsed=time.monotonic()-t;log('insert_completed',write_seconds=insert_elapsed)
   commits=sorted((table_path/'_delta_log').glob('*.json'))
   if not commits:raise ValueError('Delta commit not observed')
   stats=delta_stats(table_path);delta_stats_check=validate_delta_stats(stats);(out/'delta-selected-stats.json').write_text(json.dumps(stats,sort_keys=True,indent=2)+'\n')
   files=sorted(f for f in table_path.rglob('*.parquet') if f.is_file());footer=inspect_footer(spark,files)
   expected_columns={'source_system','rel_type_id','id','lookup_hash'}
   for name in expected_columns:
    if footer['column_statistics'].get(name,{}).get('chunks_with_non_null_value')!=footer['column_statistics'].get(name,{}).get('column_chunks'):raise ValueError(f'selected key stats absent for {name}')
   delivery=footer['column_statistics'].get('source_delivery_id',{})
   if a.case=='baseline' and delivery.get('chunks_with_non_null_value')!=delivery.get('column_chunks'):raise ValueError('baseline delivery-id stats not present in every output column chunk')
   if a.case=='candidate' and delivery.get('chunks_with_non_null_value')!=0:raise ValueError('candidate still has regular delivery-id statistics in one or more Parquet column chunks')
   if footer['rows']!=N or footer['files']!=18 or footer['codecs']!=['ZSTD']:raise ValueError('row/file/codec footer check failed')
   (out/'parquet-footer-statistics.json').write_text(json.dumps(footer,sort_keys=True,indent=2)+'\n')
   log('footer_verified',rows=footer['rows'],files=footer['files'],parquet_bytes=footer['parquet_bytes'],codecs=footer['codecs'],source_delivery_id=delivery)
   lookup=exact_lookup(spark,table_path,out);(out/'exact-key-lookup.json').write_text(json.dumps(lookup,sort_keys=True,indent=2)+'\n')
   log('exact_key_lookup_verified',row_count=len(lookup['rows']),source_file_inventory_count=lookup['source_file_inventory_count'],source_file_inventory_bytes=lookup['source_file_inventory_bytes'])
   summary={'case':a.case,'settings':{'source_delivery_id_parquet_statistics':effective,'selected_key_statistics':other,'delta_data_skipping_stats_columns':selected,'writer_property_raw':raw},
    'insert_sql_sha256':INSERT_SHA,'insert_seconds':insert_elapsed,'rows':footer['rows'],'files':footer['files'],'parquet_bytes':footer['parquet_bytes'],'codecs':footer['codecs'],
    'footer_source_delivery_id_statistics':delivery,'footer_selected_key_statistics':{k:footer['column_statistics'][k]for k in sorted(expected_columns)},
    'footer_statistics_summary':footer['column_statistics'],'footer_row_group_count':footer['row_groups'],'delta_selected_stats':stats,'delta_stats_check':delta_stats_check,
    'footer_selected_stats_by_chunk':[{'rows':group['rows'],'columns':{name:group['columns'].get(name) for name in sorted(expected_columns)}} for file in footer['per_file'] for group in file['row_groups']],
    'lookup':lookup_summary(lookup['row'],lookup['source_file_inventory_count'],lookup['source_file_inventory_bytes'],lookup['executed_plan']),
    'plan_sha256':hashlib.sha256(plan.encode()).hexdigest()}
   (out/'case-summary.json').write_text(json.dumps(summary,sort_keys=True,indent=2)+'\n')
  finally:spark.stop()
  event_reader=EVIDENCE.parent/'edge-write-attribution-20261023'/'run_jfr.py'
  spec=importlib.util.spec_from_file_location('edge_event_parser',event_reader);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  attribution=module.read_event_logs(event_dir);(out/'spark-attribution.json').write_text(json.dumps(attribution,sort_keys=True,separators=(',',':'))+'\n')
  insert=next((x for x in attribution.get('sql',[])if x.get('kind')=='edge-insert'),None)
  write_stage=None
  if insert is not None:
   candidates=[]
   for sql_event in attribution.get('sql',[]):
    if str(sql_event.get('rootExecutionId'))!=str(insert.get('executionId')):continue
    for job in sql_event.get('jobs',[]):
     for sid in job.get('stage_ids',[]):
      stage=attribution.get('stages',{}).get(str(sid),{});metrics=stage.get('task_metric_totals',{})
      if metrics.get('output_records')==N:candidates.append((sql_event,job,sid,stage,metrics))
   if len(candidates)==1:
    sql_event,job,sid,stage,metrics=candidates[0]
    write_stage={'outer_execution_id':insert.get('executionId'),'child_execution_id':sql_event.get('executionId'),'child_root_execution_id':sql_event.get('rootExecutionId'),
     'job_id':job.get('job_id'),'stage_id':sid,'tasks_expected':stage.get('tasks_expected'),'tasks_observed':stage.get('tasks_observed'),
     'submission_time':stage.get('submission_time'),'completion_time':stage.get('completion_time'),
     'stage_span_ms':stage.get('completion_time')-stage.get('submission_time'),'task_metric_totals':metrics}
  if write_stage is None:raise ValueError('could not uniquely attribute 225M output stage to nested append SQL execution')
  summary=json.loads((out/'case-summary.json').read_text());summary['write_stage']=write_stage
  (out/'case-summary.json').write_text(json.dumps(summary,sort_keys=True,indent=2)+'\n')
 except BaseException as e:
  record(progress,{'stage':'failed','elapsed_seconds':time.monotonic()-start,'error_type':type(e).__name__,'error':str(e)[:1000]});raise

if __name__=='__main__':main()

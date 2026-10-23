"""One bounded edge-only .4 Delta write with Spark event attribution."""
from __future__ import annotations
import hashlib, json, os, sys, time
from pathlib import Path
from urllib.parse import unquote, urlparse

N=225_000_000
PROFILE='ashlar-delta/0.4-xxhash64'
HASH_PROFILE='spark-xxhash64-typed-key/0.1'
GENERATED_SHA='cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa'
WHEEL_SHA='18401e7052202515da90c5c7a87d775c8c33b82f82b9526f9802d615356b76af'
JARS=Path('/private/tmp/ashlar-release-20261021-m3rde5p2/jars')
CLASSPATH=','.join(str(JARS/n) for n in ('delta-spark_4.2_2.13-4.4.1.jar','delta-storage-4.4.1.jar'))
JAR_SHA256={
    'delta-spark_4.2_2.13-4.4.1.jar':'2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be',
    'delta-storage-4.4.1.jar':'f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f',
}

def summarize_events(events):
    attribution={'sql':[],'jobs':{},'stages':{},'task_totals':{'count':0,'executor_run_ms':0,'executor_cpu_ns':0,'jvm_gc_ms':0,'memory_spill_bytes':0,'disk_spill_bytes':0,'output_bytes':0,'output_records':0}}
    def val(obj,key,default=0):
        value=obj.get(key,default) if isinstance(obj,dict) else default
        return default if value is None else value
    for event in events:
        name=event.get('Event')
        if name and name.endswith('SparkListenerSQLExecutionStart'):
            description=str(event.get('description',''));details=str(event.get('details',''));plan=str(event.get('physicalPlanDescription',''));joined=(description+' '+details+' '+plan).lower()
            kind='explain' if 'explain formatted' in joined or 'explaincommand' in joined else 'edge-insert' if ('insert into' in joined and 'edge_current' in joined) or ('appenddataexec' in joined and 'edge_current' in joined) else 'ddl-or-metadata'
            attribution['sql'].append({'executionId':event.get('executionId'),'rootExecutionId':event.get('rootExecutionId'),'start_time':event.get('time'),'kind':kind,'description':description,'details':details,'physicalPlanDescription':event.get('physicalPlanDescription')})
        elif name and name.endswith('SparkListenerSQLExecutionEnd'):
            for row in attribution['sql']:
                if row.get('executionId')==event.get('executionId'):row.update({'end_time':event.get('time'),'error':event.get('errorMessage')})
        elif name and name.endswith('SparkListenerJobStart'):
            job_id=str(event.get('Job ID'));props=event.get('Properties',{});stage_ids=[str(s.get('Stage ID')) for s in event.get('Stage Infos',[]) if s.get('Stage ID') is not None]
            attribution['jobs'][job_id]={'job_id':event.get('Job ID'),'execution_id':props.get('spark.sql.execution.id'),'submission_time':event.get('Submission Time'),'stage_ids':stage_ids}
        elif name and name.endswith('SparkListenerJobEnd'):
            job=attribution['jobs'].get(str(event.get('Job ID')))
            if job is not None:job.update({'completion_time':event.get('Completion Time'),'result':event.get('Job Result')})
        elif name and name.endswith('SparkListenerStageCompleted'):
            info=event.get('Stage Info',{});sid=str(info.get('Stage ID'))
            row=attribution['stages'].setdefault(sid,{'tasks_observed':0})
            row.update({'name':info.get('Stage Name'),'attempt':info.get('Stage Attempt ID'),'tasks_expected':info.get('Number of Tasks'),'submission_time':info.get('Submission Time'),'completion_time':info.get('Completion Time')})
        elif name and name.endswith('SparkListenerTaskEnd'):
            m=event.get('Task Metrics',{});sid=str(event.get('Stage ID'));row=attribution['stages'].setdefault(sid,{'tasks_expected':None,'name':event.get('Stage Name'),'tasks_observed':0,'task_timings':[]})
            outm=val(m,'Output Metrics',{})
            row['tasks_observed']+=1
            task=event.get('Task Info',{});launch=val(task,'Launch Time',None);finish=val(task,'Finish Time',None)
            task_metrics={'executor_run_ms':val(m,'Executor Run Time'),'executor_cpu_ns':val(m,'Executor CPU Time'),'jvm_gc_ms':val(m,'JVM GC Time'),'memory_spill_bytes':val(m,'Memory Bytes Spilled'),'disk_spill_bytes':val(m,'Disk Bytes Spilled'),'output_bytes':val(outm,'Bytes Written'),'output_records':val(outm,'Records Written')}
            totals=row.setdefault('task_metric_totals',{key:0 for key in task_metrics})
            for key,value in task_metrics.items():totals[key]+=value
            if len(row['task_timings'])<1000:
                row['task_timings'].append({'task_id':val(task,'Task ID',None),'index':val(task,'Index',None),'attempt':val(task,'Attempt',None),'host':val(task,'Host',None),'launch_ms':launch,'finish_ms':finish,'duration_ms':(finish-launch if type(finish)is int and type(launch)is int else None),**task_metrics})
            totals=attribution['task_totals'];totals['count']+=1
            totals['executor_run_ms']+=val(m,'Executor Run Time');totals['executor_cpu_ns']+=val(m,'Executor CPU Time');totals['jvm_gc_ms']+=val(m,'JVM GC Time');totals['memory_spill_bytes']+=val(m,'Memory Bytes Spilled');totals['disk_spill_bytes']+=val(m,'Disk Bytes Spilled');totals['output_bytes']+=val(outm,'Bytes Written');totals['output_records']+=val(outm,'Records Written')
    for sql in attribution['sql']:
        sql['jobs']=[job for job in attribution['jobs'].values() if str(job.get('execution_id'))==str(sql.get('executionId'))]
        sql['stage_ids']=sorted({stage for job in sql['jobs'] for stage in job['stage_ids']})
    return attribution

def event_file_disposition(name):
    if name.startswith('.') or name.endswith('.crc'):return False,'hidden-or-checksum'
    if not (name.startswith('events_') or name.startswith('eventlog') or name.startswith('local-')):return False,'non-event-sidecar'
    return True,'event-stream'

def read_event_logs(event_root):
    events=[];files=[];excluded=[]
    for path in event_root.rglob('*') if event_root.exists() else ():
        if not path.is_file():continue
        accepted,reason=event_file_disposition(path.name)
        entry={'name':path.name,'bytes':path.stat().st_size}
        if not accepted:excluded.append({**entry,'reason':reason});continue
        files.append(entry)
        try:
            with path.open(encoding='utf-8') as stream:
                for line in stream:
                    try:events.append(json.loads(line))
                    except Exception:continue
        except UnicodeDecodeError:
            files.pop();excluded.append({**entry,'reason':'non-utf8-event-file'});continue
    result=summarize_events(events);result['event_files']=files;result['excluded_files']=excluded;return result

def main():
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);p.add_argument('--wheel',type=Path,required=True);p.add_argument('--commit',required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
    repo=a.repo.resolve();wheel=a.wheel.resolve();out=a.out.resolve()
    if out.exists() or not wheel.is_file() or hashlib.sha256(wheel.read_bytes()).hexdigest()!=WHEEL_SHA: raise SystemExit('fresh output and reviewed wheel required')
    if any(not (JARS/name).is_file() or hashlib.sha256((JARS/name).read_bytes()).hexdigest()!=digest for name,digest in JAR_SHA256.items()): raise SystemExit('Delta runtime JAR missing or hash differs')
    if os.environ.get('PYSPARK_SUBMIT_ARGS','').find('--driver-memory 24g')<0:raise SystemExit('explicit 24g driver-memory setting required')
    git=['/opt/homebrew/Cellar/git/2.56.0_1/bin/git','-C',str(repo),'show',f'{a.commit}:src/ashlar_host/resources/managed-current-core-010-xxhash64/runtime-carriers.generated.json']
    import subprocess
    generated=subprocess.run(git,check=True,capture_output=True).stdout
    out.mkdir(mode=0o700,parents=True)
    event_dir=out/'spark-events';event_dir.mkdir(mode=0o700)
    progress=out/'progress.jsonl'; start=time.monotonic()
    def record(x):
        with progress.open('a') as f:f.write(json.dumps(x,sort_keys=True,separators=(',',':'))+'\n');f.flush();os.fsync(f.fileno())
    spark=None
    try:
        import pyspark,delta,shutil,subprocess
        free=shutil.disk_usage(out).free
        if free<5_000_000_000:raise SystemExit('at least 5GB free disk required before Spark startup')
        if (sys.version_info[:2]!=(3,12) or pyspark.__version__!='4.2.0' or delta.__version__!='4.4.1'):
            raise SystemExit('reviewed Python/PySpark/Delta runtime mismatch')
        java=subprocess.run([os.environ.get('JAVA_HOME','')+'/bin/java','-version'],check=True,capture_output=True,text=True)
        java_version=(java.stderr+' '+java.stdout).strip()
        if '21.0.13' not in java_version:raise SystemExit('reviewed JDK 21.0.13 runtime required')
        record({'stage':'pre_spark_environment','python':sys.version.split()[0],'pyspark':pyspark.__version__,'delta':delta.__version__,'java_version':java_version,'master':'local[18]','driver_memory':'24g','jars':JAR_SHA256,'free_bytes':free})
        sys.path.insert(0,str(wheel));from ashlar_host.managed_current_carriers import load_current_managed_carriers
        c=load_current_managed_carriers(PROFILE)
        if c.physical_profile!=PROFILE or c.lookup_hash_profile!=HASH_PROFILE or hashlib.sha256(c.generated_bytes).hexdigest()!=GENERATED_SHA or c.generated_bytes!=generated: raise ValueError('carrier profile/hash mismatch')
        record({'stage':'admitted','wheel_sha256':WHEEL_SHA,'generated_sha256':GENERATED_SHA,'expected_rows':N})
        from pyspark.sql import SparkSession
        uri=event_dir.as_uri()
        spark=(SparkSession.builder.master('local[18]').appName('ashlar-edge-write-attribution-225m')
          .config('spark.driver.memory','24g').config('spark.ui.enabled','false').config('spark.ui.showConsoleProgress','false')
          .config('spark.jars',CLASSPATH).config('spark.databricks.delta.allowArbitraryProperties.enabled','true')
          .config('spark.sql.parquet.compression.codec','zstd').config('spark.sql.shuffle.partitions','18')
          .config('spark.default.parallelism','18').config('spark.sql.session.timeZone','UTC')
          .config('spark.sql.warehouse.dir',str(out/'warehouse')).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
          .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
          .config('spark.eventLog.enabled','true').config('spark.eventLog.compress','false').config('spark.eventLog.dir',uri)
          .getOrCreate())
        record({'stage':'spark_started','seconds':time.monotonic()-start,'spark':spark.version,'master':spark.sparkContext.master,'heap_max_bytes':int(spark._jvm.java.lang.Runtime.getRuntime().maxMemory())})
        if (spark.version!='4.2.0' or spark.sparkContext.master!='local[18]'
                or int(spark._jvm.java.lang.Runtime.getRuntime().maxMemory()) not in range(23*1024**3,26*1024**3)
                or int(spark._jvm.java.lang.Runtime.getRuntime().availableProcessors())!=18):
            raise ValueError('actual Spark master/version/JVM heap/processors differ from selected profile')
        role='edge_current'; table='`spark_catalog`.`default`.`edge_current`'
        ddl_started=time.monotonic()
        spark.sql(c.create_sql(role,('spark_catalog','default',role)))
        detail=spark.sql(f'DESCRIBE DETAIL {table}').first().asDict()
        location=urlparse(detail.get('location','')); tablepath=Path(unquote(location.path)).resolve()
        if location.scheme!='file' or not tablepath.is_relative_to(out):raise ValueError('table location outside bounded output')
        record({'stage':'carrier_table_ready','seconds':time.monotonic()-ddl_started,'elapsed_seconds':time.monotonic()-start,'table_path':str(tablepath)})
        sql=f"""INSERT INTO {table} SELECT source_system, rel_type_id, id, source_type,
          source_id, target_type, target_id, schema_revision, entity_version, props_json,
          retained_json, order_key, source_feed, source_epoch, source_position, published_at,
          xxhash64(CAST(source_system AS STRING), CAST(rel_type_id AS BIGINT), CAST(id AS BIGINT)),
          apply_batch_id, source_cursor_json, source_delivery_id
        FROM (SELECT 'layout-probe' AS source_system, CAST(1 AS BIGINT) AS rel_type_id, id,
          CAST(1 AS BIGINT) AS source_type, CAST(floor(id/5) AS BIGINT) AS source_id,
          CAST(1 AS BIGINT) AS target_type,
          pmod(CAST(floor(id/5) AS BIGINT) + CAST(floor(pmod(id,5)/2) AS BIGINT), 45000000) AS target_id,
          'layout-probe-r1' AS schema_revision, CAST(1 AS BIGINT) AS entity_version,
          '{{}}' AS props_json, '{{}}' AS retained_json, CAST(NULL AS STRING) AS order_key,
          'direct-layout-fixture' AS source_feed, '1' AS source_epoch, id AS source_position,
          TIMESTAMP '2026-10-15T00:00:00.000Z' AS published_at, 'layout-probe' AS apply_batch_id,
          '{{}}' AS source_cursor_json, CAST(id AS STRING) AS source_delivery_id
          FROM range({N})) fixture"""
        explain='\n'.join(r[0] for r in spark.sql('EXPLAIN FORMATTED '+sql).collect())
        (out/'insert-plan.txt').write_text(explain)
        (out/'insert.sql').write_text(sql+'\n')
        record({'stage':'plan_captured','seconds':time.monotonic()-start,'plan_bytes':len(explain.encode()),'insert_sql_sha256':hashlib.sha256(sql.encode()).hexdigest()})
        spark_before=time.monotonic(); spark.sql(sql); write_done=time.monotonic()
        record({'stage':'insert_completed','write_seconds':write_done-spark_before,'elapsed_seconds':write_done-start})
        delta_logs=sorted((tablepath/'_delta_log').glob('*.json'))
        if not delta_logs:raise ValueError('Delta commit log was not observed after successful INSERT return')
        record({'stage':'delta_commit_observed','commit_files':[{'name':f.name,'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()}for f in delta_logs],'elapsed_seconds':time.monotonic()-start})
        files=[x for x in tablepath.rglob('*.parquet') if x.is_file()]
        if len(files)!=18:raise ValueError(f'expected 18 Parquet files, got {len(files)}')
        rows=0; bytes_=0; codecs=set(); chunks={}
        reader=spark._jvm.org.apache.parquet.hadoop.ParquetFileReader; hp=spark._jvm.org.apache.hadoop.fs.Path; conf=spark._jsc.hadoopConfiguration()
        for f in files:
            bytes_+=f.stat().st_size; footer=reader.readFooter(conf,hp(str(f)))
            for b in footer.getBlocks():
                rows+=int(b.getRowCount())
                for col in b.getColumns():
                    codecs.add(str(col.getCodec().name())); k=str(col.getPath().toDotString());chunks[k]=chunks.get(k,0)+int(col.getTotalSize())
        if rows!=N or codecs!={'ZSTD'}:raise ValueError(f'footer mismatch rows={rows} codecs={sorted(codecs)}')
        record({'stage':'footer_verified','rows':rows,'files':len(files),'parquet_bytes':bytes_,'matches_prior_baseline_active_bytes':bytes_==2_569_031_961,'prior_baseline_active_bytes':2_569_031_961,'codecs':sorted(codecs),'compressed_column_chunk_bytes':chunks,'elapsed_seconds':time.monotonic()-start})
    except BaseException as e:
        record({'stage':'failed','elapsed_seconds':time.monotonic()-start,'error_type':type(e).__name__,'error':str(e)[:1000]});raise
    finally:
        if spark is not None:
            t=time.monotonic();spark.stop();record({'stage':'spark_stopped','seconds':time.monotonic()-t,'elapsed_seconds':time.monotonic()-start})
            attribution=read_event_logs(out/'spark-events')
            (out/'spark-attribution.json').write_text(json.dumps(attribution,sort_keys=True,separators=(',',':'))+'\n')

if __name__=='__main__':main()

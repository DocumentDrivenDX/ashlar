"""Static and inert-event checks; never imports or starts SparkContext."""
import argparse, ast, hashlib, importlib.util, json, shutil, subprocess, sys, tempfile
from pathlib import Path

HERE=Path(__file__).resolve().parent
RUN=HERE/'run.py'
SUPERVISOR=HERE/'supervise.py'
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--pre-run',action='store_true',help='also require the unique private attempt-03 output root to be absent')
args=parser.parse_args()
source=RUN.read_text();ast.parse(source);ast.parse(SUPERVISOR.read_text())
assert source.index("event_dir.mkdir") < source.index(".getOrCreate()")
spec=importlib.util.spec_from_file_location('edge_write_attribution_run',RUN)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
assert 'pyspark' not in sys.modules

events=[
 {'Event':'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart','executionId':6,'description':'EXPLAIN FORMATTED INSERT INTO spark_catalog.default.edge_current','details':'insert plan','physicalPlanDescription':'WriteIntoDelta'},
 {'Event':'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd','executionId':6,'time':99,'errorMessage':None},
 {'Event':'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart','executionId':7,'description':'INSERT INTO spark_catalog.default.edge_current','details':'insert plan','physicalPlanDescription':'WriteIntoDelta'},
 {'Event':'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart','executionId':8,'description':'sql at DirectMethodHandleAccessor.java:103','details':'SparkSession.sql','physicalPlanDescription':'AppendDataExec (1) Arguments: DeltaTableV2(.../warehouse/edge_current)'},
 {'Event':'org.apache.spark.scheduler.SparkListenerJobStart','Job ID':3,'Submission Time':100,'Stage Infos':[{'Stage ID':4}],'Properties':{'spark.sql.execution.id':'7'}},
 {'Event':'org.apache.spark.scheduler.SparkListenerTaskEnd','Stage ID':4,'Stage Name':'write edge','Task Info':{'Task ID':9,'Index':0,'Attempt':0,'Host':'localhost','Launch Time':110,'Finish Time':155},'Task Metrics':{'Executor Run Time':42,'Executor CPU Time':42000000,'JVM GC Time':2,'Memory Bytes Spilled':0,'Disk Bytes Spilled':0,'Output Metrics':{'Bytes Written':123,'Records Written':456}}},
 {'Event':'org.apache.spark.scheduler.SparkListenerStageCompleted','Stage Info':{'Stage ID':4,'Stage Attempt ID':0,'Stage Name':'write edge','Number of Tasks':1,'Submission Time':105,'Completion Time':160}},
 {'Event':'org.apache.spark.scheduler.SparkListenerJobEnd','Job ID':3,'Completion Time':161,'Job Result':'JobSucceeded'},
 {'Event':'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd','executionId':7,'time':162,'errorMessage':None},
]
result=module.summarize_events(events)
assert result['sql'][0]['kind']=='explain'
assert result['sql'][1]['kind']=='edge-insert'
assert result['sql'][1]['stage_ids']==['4']
assert result['stages']['4']['tasks_expected']==result['stages']['4']['tasks_observed']==1
assert result['stages']['4']['task_timings'][0]['duration_ms']==45
assert result['task_totals']['executor_run_ms']==42
assert result['task_totals']['executor_cpu_ns']==42_000_000
assert result['task_totals']['output_bytes']==123 and result['task_totals']['output_records']==456
assert result['stages']['4']['task_metric_totals']['executor_cpu_ns']==42_000_000
assert result['sql'][2]['kind']=='edge-insert'
assert result['jobs']['3']['result']=='JobSucceeded'
assert module.event_file_disposition('events_local-run.inprogress')==(True,'event-stream')
assert module.event_file_disposition('local-20261023-app.inprogress')==(True,'event-stream')
assert module.event_file_disposition('.events_local-run.inprogress.crc')==(False,'hidden-or-checksum')
assert module.event_file_disposition('appstatus_local-run')==(False,'non-event-sidecar')
with tempfile.TemporaryDirectory(prefix='ashlar-event-parser-') as temp:
 root=Path(temp);(root/'events_local-fixture').write_text(json.dumps(events[1])+'\n')
 (root/'.events_local-fixture.crc').write_bytes(b'\x00\xffcrc')
 (root/'appstatus_local-fixture').write_text('{}\n')
 parsed=module.read_event_logs(root)
 assert [f['name'] for f in parsed['event_files']]==['events_local-fixture']
 assert {f['reason'] for f in parsed['excluded_files']}=={'hidden-or-checksum','non-event-sidecar'}
assert 'pyspark' not in sys.modules
assert 'pyspark' not in sys.modules

venv=Path('/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python')
wheel=Path('/private/tmp/ashlar-release-20261021-m3rde5p2/wheelhouse-bc2accd8/ashlar_graph_toolkit-0.1.0.dev1-py3-none-any.whl')
assert venv.is_file() and wheel.is_file() and hashlib.sha256(wheel.read_bytes()).hexdigest()==module.WHEEL_SHA
for name,digest in module.JAR_SHA256.items():
 path=module.JARS/name
 assert path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest()==digest
assert shutil.disk_usage('/private/tmp').free>=5_000_000_000
versions=subprocess.run([str(venv),'-B','-c','import sys,pyspark,delta;print(sys.version.split()[0],pyspark.__version__,delta.__version__)'],check=True,capture_output=True,text=True).stdout.strip()
assert versions=='3.12.14 4.2.0 4.4.1',versions
java=subprocess.run(['/opt/homebrew/opt/openjdk@21/bin/java','-version'],check=True,capture_output=True,text=True)
assert '21.0.13' in java.stderr+java.stdout
assert "master('local[18]')" in source and "config('spark.driver.memory','24g')" in source
assert '23*1024**3' in source and '26*1024**3' in source
if args.pre_run:
 assert not Path('/private/tmp/ashlar-edge-write-attribution-20261023-attempt-03').exists()
print('static setup, exact runtime/wheel/JAR hashes, free-disk, and inert SQL/job/stage/task attribution checks passed; SparkContext not started')

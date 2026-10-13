import sys,json,time,tempfile,os,hashlib
from pathlib import Path
from unittest.mock import patch
root=Path(__file__).parent;(root/'ipc').mkdir();os.environ['TMPDIR']=str(root/'ipc');tempfile.tempdir=None
sys.path.insert(0,'/Users/erik/Projects/ashlar/src')
from ashlar_host.graphframes_export import run_graphframes_export_command
from pyspark import SparkContext
from pyspark.sql import SparkSession
start=time.monotonic();result=None
class InjectedImportInterruption(RuntimeError):pass
try:
 oldroot=Path('/private/tmp/ashlar-truss-typed-related-export-20261013-a');newroot=Path('/private/tmp/ashlar-truss-typed-edge-release-native-20261013-a')
 files=[source/name for source in (oldroot,newroot)for name in ('export.json-release.json','export.json-custody.json')]
 before={str(p):hashlib.sha256(p.read_bytes()).hexdigest()for p in files}
 environment={key:os.environ.get(key)for key in ('JAVA_HOME','PYSPARK_PYTHON','PYSPARK_DRIVER_PYTHON')}
 observed=[]
 original_create=SparkSession.createDataFrame
 imports=[]
 def interrupt(spark,*args,**kwargs):
  imports.append(len(imports)+1)
  if len(imports)==2:raise InjectedImportInterruption('after-native-vertex-import-before-edge-import')
  frame=original_create(spark,*args,**kwargs)
  observed.extend(row.asDict()for row in frame.collect())
  if len(observed)!=2 or {row['id']for row in observed}!={'1','2'}:raise ValueError('Actual native vertex import differs')
  return frame
 output=root/'interrupted-result.json'
 with patch.object(SparkSession,'createDataFrame',interrupt):
  try:run_graphframes_export_command(str(newroot/'graphframes.json'),output,maximum_hops=2)
  except InjectedImportInterruption as error:
   if str(error)!='after-native-vertex-import-before-edge-import':raise
  else:raise ValueError('Interrupted import released success')
 if output.exists()or SparkContext._active_spark_context is not None:raise ValueError('Interrupted owner leaked result/runtime')
 if environment!={key:os.environ.get(key)for key in environment}:raise ValueError('Interrupted environment not restored')
 if before!={str(p):hashlib.sha256(p.read_bytes()).hexdigest()for p in files}:raise ValueError('Original immutable release bytes changed')
 old=run_graphframes_export_command(str(oldroot/'graphframes.json'),root/'original-graphframes.json',maximum_hops=2)
 if old['publication_id']!='truss-graph-5'or(old['nodes'],old['edges'],len(old['one_hop']),len(old['two_hop']))!=(2,2,2,2):raise ValueError('Original snapshot after interruption differs')
 if before!={str(p):hashlib.sha256(p.read_bytes()).hexdigest()for p in files}or SparkContext._active_spark_context is not None:raise ValueError('Final original bytes/runtime differ')
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'injection':'after actual Spark vertex-frame creation and collect; before edge-frame creation','actual_imported_vertices':observed,'interrupted_success_output_absent':True,'spark_context_closed':True,'runtime_environment_restored':True,'original_release_and_custody_hashes_unchanged':before,'old_snapshot_after_interruption':{'publication_id':old['publication_id'],'nodes':old['nodes'],'edges':old['edges'],'release_sha256':old['release_sha256'],'custody_sha256':old['custody_sha256'],'runtime':old['runtime']}}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:(root/'run-result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)

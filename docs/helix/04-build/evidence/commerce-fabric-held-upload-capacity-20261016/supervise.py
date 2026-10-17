from pathlib import Path
import json,os,signal,subprocess,sys,time
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'source/src'))
from ashlar_host.lifecycle import finish
PYTHON='/private/tmp/ashlar-commerce-fabric-native-20261016-a/environment/bin/python3'
env={'PATH':'/usr/bin:/bin','JAVA_HOME':'/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home',
 'PYTHONDONTWRITEBYTECODE':'1','SPARK_LOCAL_IP':'127.0.0.1','LANG':'C','LC_ALL':'C','TZ':'UTC',
 'PYSPARK_PYTHON':PYTHON,'PYSPARK_DRIVER_PYTHON':PYTHON}
started=time.monotonic();timed_out=False;p=None;primary=None;out=None;err=None

def kill_group(sig):
 if p is not None:
  try:os.killpg(p.pid,sig)
  except ProcessLookupError:pass

def close_group():
 # Always clean descendants, even when the leader already exited normally.
 kill_group(signal.SIGTERM)
 if p is not None:
  try:p.wait(timeout=10)
  except subprocess.TimeoutExpired:pass
 # Spark descendants may outlive an already reaped leader. SIGKILL the original
 # process group unconditionally; never interpret leader exit as group closure.
 kill_group(signal.SIGKILL)

def reap():
 if p is not None:p.wait(timeout=10)

try:
 out=open(ROOT/'evidence/stdout.log','xb');err=open(ROOT/'evidence/stderr.log','xb')
 p=subprocess.Popen([PYTHON,'-I','-B',str(ROOT/'run-proof.py'),'--parent-approved-native-slot'],cwd=ROOT,env=env,stdout=out,stderr=err,start_new_session=True)
 try:p.wait(timeout=700)
 except subprocess.TimeoutExpired:
  timed_out=True
  raise TimeoutError('Fabric proof exceeded700second work budget')
except BaseException as error:primary=error
def terminal():
 with open(ROOT/'evidence/terminal.json','x') as stream:
  json.dump({'exit_code':None if p is None else p.returncode,'timed_out':timed_out,'elapsed_seconds':time.monotonic()-started,'maximum_seconds':720,'primary_type':None if primary is None else type(primary).__name__},stream)
finish(primary,[close_group,reap]+([out.close]if out is not None else[])+([err.close]if err is not None else[])+[terminal])
raise SystemExit(p.returncode)

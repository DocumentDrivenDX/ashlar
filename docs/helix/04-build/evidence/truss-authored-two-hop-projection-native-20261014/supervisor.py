import sys,subprocess,time,os,signal,json
from pathlib import Path
r=Path(__file__).parent;sys.path.insert(0,'/private/tmp/ashlar-truss15-root-projection-installed-20261014-a/target')
from host_postgres import host_postgres
from ashlar_host.lifecycle import finish
t=time.monotonic();answer=None;primary=None
try:
 with host_postgres() as source:
  (r/'live.json').write_text(json.dumps({'profile':'ashlar-local-truss-connection/0.1','data_directory':'/private/tmp/ashlar-truss15-parallel-example-20261013-a/example/data','connection_uri':source.info.connection_uri})+'\n');(r/'live.json').chmod(0o600)
  ready=time.monotonic()
  with (r/'run.log').open('xb') as f:
   p=subprocess.Popen(['/private/tmp/truss-pg18-consumer-env-20261011/bin/python',str(r/'run.py')],stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
   try:code=p.wait(timeout=180);status='completed'
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);code=p.wait(timeout=5);status='deadline'
   except BaseException as error:
    def kill_child():
     if p.poll() is None:os.killpg(p.pid,signal.SIGKILL)
    finish(error,[kill_child,lambda:p.wait(timeout=5)])
  answer={'status':status,'exit_code':code,'source_start_seconds':ready-t,'query_child_seconds':time.monotonic()-ready}
except BaseException as error:primary=error
if answer is None:answer={'status':'runtime_failed','exit_code':None}
answer['duration_seconds']=time.monotonic()-t;answer['owned_source_closed']=not Path('/private/tmp/ashlar-truss15-parallel-example-20261013-a/example/data/postmaster.pid').exists()
(r/'supervisor-result.json').write_text(json.dumps(answer,indent=2)+'\n');print(json.dumps(answer),flush=True)
finish(primary,[])

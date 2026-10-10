import fcntl,hashlib,json,os,selectors,signal,subprocess,sys,time
from pathlib import Path
phase=sys.argv[1];assert phase in ('publish','query')
cp=Path('/private/tmp/ashlar-authored-path-command-final-20261010-b/command.json');raw=cp.read_bytes();assert hashlib.sha256(raw).hexdigest()=='8866fdbb22eef9088b02c1406953b3c2563db14952926174c7ab8d070e2b642c';command=json.loads(raw);cmd=next(p for p in command['phases'] if p['phase']==phase)
root=Path(cmd['cwd']);custody=Path('/private/tmp/ashlar-authored-path-'+phase+'-custody-20261010-b');custody.mkdir(mode=0o700)
gate=Path('/private/tmp/ashlar-local-native-engine.lock');assert not gate.is_symlink();fd=os.open(gate,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600);fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
state={'command_sha256':hashlib.sha256(raw).hexdigest(),'scope':'Exact reviewed finite authored path phase, exclusive cooperating local engine custody','started':time.time(),'native_phase':phase,'phase':'preflight'}
def retain():(custody/'outcome.json').write_text(json.dumps(state,sort_keys=True,indent=2)+'\n')
def descriptor(f):
 b=Path(f['path']).read_bytes();assert len(b)==f['bytes'] and hashlib.sha256(b).hexdigest()==f['sha256'],f['path']
def verify():
 assert cp.read_bytes()==raw
 for f in command['references']+command['selectedResources']:descriptor(f)
 for f in json.loads(Path('/private/tmp/ashlar-authored-path-command-final-20261010-b/resources.json').read_bytes())['files']:descriptor(f)
 inv=json.loads(Path('/private/tmp/ashlar-authored-path-runtime-20261010-a/source-inventory.json').read_bytes())
 for f in inv['files']:descriptor({**f,'path':str(root/f['path'])})
def pg():
 s=subprocess.check_output(['docker','inspect','ashlar-e2e-truss-pg17','--format','{{.Id}} {{.Image}} {{.State.Running}} {{.State.OOMKilled}} {{.State.Health.Status}} {{.HostConfig.Memory}} {{.HostConfig.NanoCpus}} {{json .NetworkSettings.Ports}}'],timeout=10).decode().strip()
 assert s.startswith('37d798c29961d87549baabd1625ad946f45e8f476ade2eef360d7bf1211419c9 sha256:2a0d0fe14825b0939f78a8cad5cd4e6aa68bf94d0e5dd96e24b6d23af4315545 true false healthy 536870912 1000000000 ')
 assert '127.0.0.1' in s and '15432' in s
 return s
def engines():
 p=subprocess.run(['pgrep','-fal','org.apache.spark.deploy|SparkSubmit|puppygraph'],capture_output=True,timeout=10);assert p.returncode==1 and not p.stdout
process=None;selector=None
try:
 verify();engines();state['opening_pg']=pg();env={**os.environ,**cmd['environment']}
 for k in ('SPARK_HOME','SPARK_REMOTE','SPARK_CONNECT_MODE_ENABLED','PYSPARK_GATEWAY_PORT','PYSPARK_GATEWAY_SECRET','PYSPARK_SUBMIT_ARGS','JAVA_TOOL_OPTIONS','JDK_JAVA_OPTIONS','_JAVA_OPTIONS'):assert k not in env
 output=Path(cmd['argv'][cmd['argv'].index('--output')+1]);assert not output.exists()
 if phase=='query':
  p=Path('/private/tmp/ashlar-authored-path-publish-custody-20261010-b/outcome.json');prev=json.loads(p.read_bytes());assert prev['phase']=='terminal' and prev['exit_code']==0 and prev['report_exists'] is True
  pub=Path('/private/tmp/ashlar-authored-path-publication-20261010-a/report.json');assert pub.exists();state['original_publication_report_sha256']=hashlib.sha256(pub.read_bytes()).hexdigest()
 probe="import json;from pathlib import Path;from local_outbox_connection import connect;from postgres_transactions import Session;r=json.loads(Path('/private/tmp/ashlar-indexed-commerce-publication-20261010-c/report.json').read_bytes());role=r['protected_ack_scope']['service_schema'].replace('pipeline_','operator_');c=connect(role);s=Session(c);rows=s.query('SELECT current_user,session_user,current_database() AS database,CAST((SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS text) AS superuser',{}).rows;assert len(rows)==1 and rows[0]['current_user']==role and rows[0]['session_user']==role and rows[0]['database']=='truss_e2e' and rows[0]['superuser']=='false';c.rollback();c.close();print(json.dumps(rows))"
 state['ordinary_role_probe']=json.loads(subprocess.check_output([cmd['argv'][0],'-c',probe],cwd=root,env=env,timeout=20));state['phase']='running';retain()
 process=subprocess.Popen(cmd['argv'],cwd=root,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,bufsize=0,start_new_session=True);selector=selectors.DefaultSelector();buffers={'stdout':bytearray(),'stderr':bytearray()}
 for name,stream in (('stdout',process.stdout),('stderr',process.stderr)):os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,name)
 deadline=time.monotonic()+600
 while selector.get_map():
  left=deadline-time.monotonic();assert left>0
  for key,_ in selector.select(min(left,1)):
   try:b=os.read(key.fd,65536)
   except BlockingIOError:continue
   if not b:selector.unregister(key.fileobj);continue
   buffers[key.data].extend(b);assert len(buffers[key.data])<=1048576
 state['exit_code']=process.wait(timeout=max(.001,deadline-time.monotonic()))
 for name,b in buffers.items():(custody/(name+'.bin')).write_bytes(b);state[name+'_bytes']=len(b);state[name+'_sha256']=hashlib.sha256(b).hexdigest()
 verify();engines();state['closing_pg']=pg();state['report_exists']=(output/'report.json').exists();state['phase']='terminal';state['finished']=time.time();retain();print(json.dumps({k:state[k] for k in ('exit_code','report_exists','phase')}))
finally:
 if selector:selector.close()
 if process:
  try:os.killpg(process.pid,signal.SIGKILL)
  except ProcessLookupError:pass
  process.wait(timeout=10)
  for stream in (process.stdout,process.stderr):stream.close()
 fcntl.flock(fd,fcntl.LOCK_UN);os.close(fd);retain()

import json,subprocess,os,fcntl,datetime
from pathlib import Path
b=Path('/private/tmp/ashlar-installed-commerce-command-20261010-b')
lock=Path('/private/tmp/ashlar-local-native-engine.lock')
assert not lock.is_symlink()
fd=os.open(lock,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
def run(a):
 r=subprocess.run(a,capture_output=True,timeout=10);assert r.returncode==0,r.stderr;return r.stdout
try:
 d=json.loads(run(['/opt/homebrew/bin/docker','inspect','ashlar-e2e-truss-pg17']))[0]
 old=json.loads((b/'pg-observation.json').read_text())
 assert d['Id']==old['Id'] and d['Image']==old['Image']
 assert d['State']['Running'] and not d['State']['OOMKilled']
 assert d['HostConfig']['Memory']==536870912 and d['HostConfig']['NanoCpus']==1000000000
 assert d['NetworkSettings']['Ports']==old['ports']
 ps=run(['/bin/ps','-axo','pid=,command=']).decode()
 engines=[s.split(None,1)[0] for s in ps.splitlines() if any(t in s for t in ('org.apache.spark.deploy.SparkSubmit','pyspark.daemon'))]
 assert not engines,engines
 containers=[json.loads(s) for s in run(['/opt/homebrew/bin/docker','ps','--format','{{json .}}']).decode().splitlines()]
 assert not any('puppy' in (v['Image']+' '+v['Names']).lower() for v in containers)
 sql="SELECT count(*) FROM pg_roles WHERE rolname='ashlar_ack_operator_0fc843618939' AND NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls"
 assert run(['/opt/homebrew/bin/docker','exec','ashlar-e2e-truss-pg17','psql','-U','postgres','-d','truss_e2e','-Atc',sql]).strip()==b'1'
 (b/'root-publish-preflight.json').write_text(json.dumps({'observedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'container':d['Id'],'image':d['Image'],'running':True,'oomKilled':False,'memoryBytes':536870912,'nanoCpus':1000000000,'ports':old['ports'],'ordinaryRoleVerified':True,'otherSparkProcesses':engines,'runningPuppyContainers':0,'exclusiveLeaseHeld':True},indent=2)+'\n')
 x=json.loads((b/'execution.json').read_text())['phases'][0]
 assert x['phase']=='publish'
 r=subprocess.run(x['argv'],cwd=x['cwd'],env=x['environment'])
 (b/'root-publish-terminal.json').write_text(json.dumps({'exit':r.returncode,'exclusiveLeaseHeldThroughTerminal':True,'queryExecuted':False})+'\n')
 raise SystemExit(r.returncode)
finally:
 fcntl.flock(fd,fcntl.LOCK_UN);os.close(fd)

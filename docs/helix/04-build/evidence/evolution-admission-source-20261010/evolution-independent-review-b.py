import sys,os,json,hashlib,tempfile,subprocess
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,'/Users/erik/Projects/ashlar/src')
from ashlar_host.evolution_producer import capture
from ashlar_host.config import HostError
results=[]
for point in ('close-control','fdopen-control'):
 processes=[]; launch=subprocess.Popen; close=os.close; count=[0]
 def start(*a,**k):
  p=launch(*a,**k);processes.append(p);count[0]=0;return p
 def failingclose(fd):
  count[0]+=1
  if processes and count[0]==1: raise OSError('review setup close failure')
  return close(fd)
 with tempfile.TemporaryDirectory() as td, patch('ashlar_host.evolution_producer.subprocess.Popen',side_effect=start):
  seam=patch('ashlar_host.evolution_producer.os.close',side_effect=failingclose) if point=='close-control' else patch('ashlar_host.evolution_producer.os.fdopen',side_effect=OSError('review fdopen failure'))
  with seam:
   try: capture((sys.executable,'-c','pass'),cwd=Path(td),environment={},timeout_seconds=2,maximum_output_bytes=100)
   except HostError: pass
  p=processes[0]
  results.append({'point':point,'stdout_closed':p.stdout.closed,'stderr_closed':p.stderr.closed,'reaped':p.returncode is not None})
  p.stdout.close();p.stderr.close()
print(json.dumps(results,indent=2))

import subprocess,os,signal,time,json
from pathlib import Path
root=Path(__file__).parent;start=time.monotonic()
with (root/'run.log').open('xb')as output:
 process=subprocess.Popen(['/private/tmp/truss-pg18-consumer-env-20261011/bin/python',str(root/'run.py')],stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
 try:code=process.wait(timeout=120);outcome='completed'
 except subprocess.TimeoutExpired:
  os.killpg(process.pid,signal.SIGKILL);code=process.wait(timeout=5);outcome='deadline'
(root/'supervisor-result.json').write_text(json.dumps({'outcome':outcome,'exit_code':code,'duration_seconds':round(time.monotonic()-start,3)})+'\n')
raise SystemExit(code if outcome=='completed'else 124)

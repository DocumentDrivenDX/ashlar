import pathlib,subprocess,json,time,os,signal
r=pathlib.Path(__file__).parent;status=r/'status.json'
with status.open('x')as f:json.dump({'state':'starting','outer_seconds':180},f)
start=time.monotonic()
with (r/'stdout.log').open('xb')as out,(r/'stderr.log').open('xb')as err:
 p=subprocess.Popen(['/private/tmp/ashlar-fabric-typed-runtime-20261016/bin/python','-I','-B',str(r/'native.py')],cwd=r,stdout=out,stderr=err,start_new_session=True)
 try:code=p.wait(timeout=180);state='terminal'
 except subprocess.TimeoutExpired:
  state='timeout';os.killpg(p.pid,signal.SIGTERM)
  try:code=p.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);code=p.wait(timeout=10)
status.write_text(json.dumps({'state':state,'exit_code':code,'elapsed_seconds':time.monotonic()-start},indent=2)+'\n');print(status.read_text())

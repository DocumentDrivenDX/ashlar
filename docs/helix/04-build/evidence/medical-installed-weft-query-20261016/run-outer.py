import pathlib,json,subprocess,time,os,signal
r=pathlib.Path(__file__).parent;receipt=r/'outer-attempt.json'
with receipt.open('x')as f:json.dump({'state':'starting','outer_seconds':990},f)
argv=[str(r/'environment/bin/python3'),'-I','-B',str(r/'run-query.py')];start=time.monotonic()
with (r/'outer.stdout').open('xb')as out,(r/'outer.stderr').open('xb')as err:
 p=subprocess.Popen(argv,cwd=r,stdout=out,stderr=err,start_new_session=True)
 try:code=p.wait(timeout=990);state='terminal'
 except subprocess.TimeoutExpired:
  state='outer-timeout';os.killpg(p.pid,signal.SIGTERM)
  try:code=p.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);code=p.wait(timeout=10)
receipt.write_text(json.dumps({'state':state,'exit_code':code,'elapsed_seconds':time.monotonic()-start,'argv':argv},indent=2)+'\n')
print(receipt.read_text())

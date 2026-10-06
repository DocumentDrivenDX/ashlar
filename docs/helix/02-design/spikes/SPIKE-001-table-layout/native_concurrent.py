"""Eight bounded readers of a fixed old Delta version during ingest screening."""
import argparse,concurrent.futures,json,subprocess,time
from pathlib import Path
BASE=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--reads',type=int,default=24);p.add_argument('--label',default='initial');a=p.parse_args();assert 1<=a.reads<=256 and a.label.isalnum()
schema='ashlar_ingest_20261005_a2';out=BASE/'out/native'/schema
start=time.time()
def read(i):
 key=1+(i*7919)%1000000
 request=out/f'reader-request-{i}.json'
 statement=f"SELECT id,props_json,uuid() FROM client_dev.{schema}.serving VERSION AS OF 0 WHERE id={key}"
 request.write_text(json.dumps({'warehouse_id':'2439e1f2e37ac563','statement':statement,'wait_timeout':'10s','on_wait_timeout':'CONTINUE','disposition':'INLINE','format':'JSON_ARRAY','row_limit':10}))
 t=time.perf_counter();r=subprocess.run(['databricks','api','post','/api/2.0/sql/statements','--profile','aidev-cus','--json','@'+str(request)],capture_output=True,text=True,timeout=45)
 if r.returncode:raise RuntimeError(r.stderr)
 x=json.loads(r.stdout);deadline=time.monotonic()+120
 while x['status']['state'] in ('PENDING','RUNNING'):
  if time.monotonic()>deadline:raise RuntimeError('Reader observation deadline; inspect live '+x['statement_id'])
  time.sleep(1)
  r=subprocess.run(['databricks','api','get','/api/2.0/sql/statements/'+x['statement_id'],'--profile','aidev-cus'],capture_output=True,text=True,timeout=45)
  if r.returncode:raise RuntimeError(r.stderr)
  x=json.loads(r.stdout)
 assert x['status']['state']=='SUCCEEDED',x['status']
 rows=x['result']['data_array'];assert rows[0][0]==str(key)
 assert json.loads(rows[0][1])['103']=='x'*512
 return {'reader':i,'wall_ms':(time.perf_counter()-t)*1000,'response':x}
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:results=list(pool.map(read,range(a.reads)))
(out/('concurrent-readers-'+a.label+'.json')).write_text(json.dumps({'start_epoch':start,'end_epoch':time.time(),'max_clients':8,'results':results,'scope':'fixed version0 serving reads, 24 samples, CLI clients; check write interval overlap before claiming concurrent ingest'},indent=2)+'\n')
print('Passed',a.reads,'fixed-old-version reads with 8 clients',flush=True)

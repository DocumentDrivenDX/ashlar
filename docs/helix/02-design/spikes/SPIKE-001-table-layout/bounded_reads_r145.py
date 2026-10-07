"""Owned read-only worker, explicit socket/retry settings and60s process bound."""
import json,subprocess,sys,time
from pathlib import Path
from databricks import sql as dbsql
from driver_sql import DriverClient
from persistent_sql import Client
from property_apply_queries import COLS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bounded_reads_r145'
class BoundedReads(DriverClient):
 def __init__(self,out):
  Client.__init__(self,out)
  self.connection=dbsql.connect(server_hostname='adb-7405607548213398.18.azuredatabricks.net',http_path='/sql/1.0/warehouses/'+self.warehouse_id,credentials_provider=lambda:self.w.config.authenticate,session_configuration={'use_cached_result':'false'},use_cloud_fetch=False,_socket_timeout=10,_retry_stop_after_attempts_count=1,_retry_stop_after_attempts_duration=10,_retry_max_redirects=0)
  self.cursor=self.connection.cursor()
 def sql(self,label,statement,parameters=None,tag=True):
  assert tag,'Durable unique run/label correlation required'
  request={'label':label,'sql':statement,'parameters':parameters,'correlation':'/* ashlar '+self.out.name+' '+label+' */','started_epoch':time.time()}
  (self.out/'inflight-request.json').write_text(json.dumps(request)+'\n')
  result=super().sql(label,statement,parameters=parameters,tag=True)
  request.update(state='returned',query_id=self.records[-1]['statement_id'])
  (self.out/'inflight-request.json').write_text(json.dumps(request)+'\n');return result
def worker():
 c=BoundedReads(O)
 c.sql('timeout','SET STATEMENT_TIMEOUT=15')
 assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
 F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';S=F+'.schedule_r139_1'
 rows=c.sql('oracle',f"SELECT {','.join(COLS)} FROM {S} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 30")
 assert len(rows)==30
 query=f"SELECT {','.join(COLS)} FROM {E} VERSION AS OF 23 WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
 for i,row in enumerate(rows):assert c.sql('read-'+str(i),query,parameters={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]})==[row]
 c.close();print('Thirty exact bounded-worker reads passed',flush=True)
if __name__=='__main__':
 if sys.argv[1:]==['--worker']:worker()
 else:
  assert not (O/'process-bound.json').exists() and not (O/'statements.jsonl').exists(),'Inspect prior handle; no blind rerun'
  O.mkdir(parents=True,exist_ok=True);start=time.monotonic()
  with (O/'worker-output.txt').open('w') as log:
   process=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--worker'],stdout=log,stderr=log)
   record={'pid':process.pid,'max_worker_wall_s':60,'started_epoch':time.time(),'state':'running'}
   (O/'process-bound.json').write_text(json.dumps(record)+'\n')
   try:code=process.wait(timeout=60);record['state']='worker returned'
   except subprocess.TimeoutExpired:
    process.terminate()
    try:code=process.wait(timeout=5)
    except subprocess.TimeoutExpired:process.kill();code=process.wait()
    record['state']='worker wall bound reached; same server query status must be inspected; no re-execution'
   record.update(exit_code=code,worker_wall_s=time.monotonic()-start)
   (O/'process-bound.json').write_text(json.dumps(record,indent=2)+'\n')
  print(json.dumps(record));sys.exit(0 if code==0 else 1)

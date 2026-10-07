"""Persistent uncached SQL driver with the same evidence surface as Client."""
import datetime,json,time
from databricks import sql as dbsql
from databricks.sql.exc import ServerOperationError
from persistent_sql import Client, WAREHOUSE
class DriverClient(Client):
 def __init__(self,out,warehouse_id=WAREHOUSE):
  super().__init__(out,warehouse_id=warehouse_id)
  self.connection=dbsql.connect(server_hostname='adb-7405607548213398.18.azuredatabricks.net',http_path='/sql/1.0/warehouses/'+self.warehouse_id,credentials_provider=lambda:self.w.config.authenticate,session_configuration={'use_cached_result':'false'},use_cloud_fetch=False)
  self.cursor=self.connection.cursor()
  self.cursor.execute('SET use_cached_result=false');self.cursor.fetchall()
 def sql(self,label,statement,parameters=None,tag=True):
  start=time.perf_counter();started=time.time();prior_id=self.cursor.query_id
  try:
   query=('/* ashlar '+self.out.name+' '+label+' */ '+statement) if tag else statement
   if parameters is None:self.cursor.execute(query)
   else:self.cursor.execute(query,parameters=parameters)
   raw=[list(r) for r in self.cursor.fetchall()]
   # Match the statement API's string-valued result-array surface. JSON carriers
   # are already strings and are never parsed/coerced by this transport adapter.
   def scalar(v):
    if v is None:return None
    if isinstance(v,str):return v
    if isinstance(v,(dict,list)):return json.dumps(v,default=str)
    if isinstance(v,datetime.datetime):return v.isoformat()
    return str(v)
   rows=[[scalar(v) for v in r] for r in raw]
   response={'status':{'state':'SUCCEEDED'},'result':{'data_array':rows},'manifest':{'schema':{'columns':[{'name':d[0]} for d in (self.cursor.description or [])]}}}
  except Exception as e:
   response={'status':{'state':'FAILED' if isinstance(e,ServerOperationError) else 'UNKNOWN','error':{'class':type(e).__name__,'message':str(e)}}};rows=[]
  rec={'warehouse_id':self.warehouse_id,'label':label,'sql':statement,'parameters':parameters,'tag':tag,'statement_id':self.cursor.query_id if self.cursor.query_id!=prior_id else None,'start_epoch':started,'wall_ms':(time.perf_counter()-start)*1000,'transport':'sql-driver','response':response};self.records.append(rec)
  with (self.out/'statements.jsonl').open('a') as h:h.write(json.dumps(rec)+'\n')
  if response['status']['state']!='SUCCEEDED':raise RuntimeError(json.dumps(response['status']))
  return rows
 def close(self):self.cursor.close();self.connection.close()

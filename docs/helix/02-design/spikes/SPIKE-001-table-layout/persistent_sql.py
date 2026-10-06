"""Reusable authenticated SDK transport. Never prints or stores credentials.
Write submission exceptions require same-handle/history recovery, never blind retry.
"""
import json,time
from pathlib import Path
from urllib.parse import urlencode
from databricks.sdk import WorkspaceClient
WAREHOUSE='2439e1f2e37ac563'
class Client:
 def __init__(self,out,observation_timeout=180):
  self.out=Path(out);self.out.mkdir(parents=True,exist_ok=True)
  self.observation_timeout=observation_timeout
  self.w=WorkspaceClient(profile='aidev-cus');self.records=[]
 def sql(self,label,statement,parameters=None):
  start=time.perf_counter();started=time.time()
  body={'warehouse_id':WAREHOUSE,'statement':statement,'wait_timeout':'10s','on_wait_timeout':'CONTINUE','disposition':'INLINE','format':'JSON_ARRAY','row_limit':1000}
  if parameters is not None:body['parameters']=parameters
  r=self.w.api_client.do('POST','/api/2.0/sql/statements',body=body)
  sid=r['statement_id'];deadline=time.monotonic()+self.observation_timeout
  (self.out/'live-statement.json').write_text(json.dumps({'label':label,'statement_id':sid,'statement':statement})+'\n')
  while r['status']['state'] in ('PENDING','RUNNING'):
   if time.monotonic()>deadline:raise RuntimeError('Observation deadline; inspect existing handle '+sid)
   time.sleep(.2);r=self.w.api_client.do('GET','/api/2.0/sql/statements/'+sid)
  rec={'label':label,'sql':statement,'parameters':parameters,'statement_id':sid,'start_epoch':started,'wall_ms':(time.perf_counter()-start)*1000,'response':r};self.records.append(rec)
  with (self.out/'statements.jsonl').open('a') as h:h.write(json.dumps(rec)+'\n')
  if r['status']['state']!='SUCCEEDED':raise RuntimeError(json.dumps(r['status']))
  if r.get('manifest',{}).get('truncated'):raise RuntimeError('Truncated result')
  return r.get('result',{}).get('data_array',[])
 def history(self):
  ids={r['statement_id'] for r in self.records};history=[];token=None
  for _ in range(8):
   params={'max_results':1000,'include_metrics':'true'}
   if token:params['page_token']=token
   r=self.w.api_client.do('GET','/api/2.0/sql/history/queries',query=params)
   history.extend(q for q in r.get('res',[]) if q['query_id'] in ids)
   if ids.issubset({q['query_id'] for q in history}) or not r.get('has_next_page'):break
   token=r['next_page_token']
  (self.out/'query-history.json').write_text(json.dumps(history,indent=2)+'\n')
  return history

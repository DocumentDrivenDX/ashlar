"""Reusable authenticated SDK transport. Never prints or stores credentials.
Write submission exceptions require same-handle/history recovery, never blind retry.
"""
import json,time,re
from pathlib import Path
from urllib.parse import urlencode
from databricks.sdk import WorkspaceClient
WAREHOUSE='2439e1f2e37ac563'
class Client:
 def __init__(self,out,observation_timeout=180,cancel_after=None,warehouse_id=WAREHOUSE):
  if not re.fullmatch(r"[0-9a-f]{16}",warehouse_id):raise ValueError("Invalid warehouse ID")
  self.warehouse_id=warehouse_id
  self.out=Path(out);self.out.mkdir(parents=True,exist_ok=True)
  self.observation_timeout=observation_timeout
  self.cancel_after=cancel_after
  self.w=WorkspaceClient(profile='aidev-cus');self.records=[]
 def sql(self,label,statement,parameters=None):
  start=time.perf_counter();started=time.time()
  body={'warehouse_id':self.warehouse_id,'statement':statement,'wait_timeout':'10s','on_wait_timeout':'CONTINUE','disposition':'INLINE','format':'JSON_ARRAY','row_limit':1000}
  if parameters is not None:body['parameters']=parameters
  r=self.w.api_client.do('POST','/api/2.0/sql/statements',body=body)
  sid=r['statement_id'];deadline=time.monotonic()+self.observation_timeout
  cancel_deadline=None if self.cancel_after is None else time.monotonic()+self.cancel_after
  cancel_requested=False
  (self.out/'live-statement.json').write_text(json.dumps({'label':label,'statement_id':sid,'statement':statement})+'\n')
  while r['status']['state'] in ('PENDING','RUNNING'):
   if time.monotonic()>deadline:raise RuntimeError('Observation deadline; inspect existing handle '+sid)
   if cancel_deadline is not None and time.monotonic()>=cancel_deadline and not cancel_requested:
    cancel_requested=True
    (self.out/'live-statement.json').write_text(json.dumps({'label':label,'statement_id':sid,'statement':statement,'cancel_requested':True})+'\n')
    self.w.api_client.do('POST','/api/2.0/sql/statements/'+sid+'/cancel')
   time.sleep(.2);r=self.w.api_client.do('GET','/api/2.0/sql/statements/'+sid)
  rec={'warehouse_id':self.warehouse_id,'label':label,'sql':statement,'cancel_requested':cancel_requested,'parameters':parameters,'statement_id':sid,'start_epoch':started,'wall_ms':(time.perf_counter()-start)*1000,'response':r};self.records.append(rec)
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

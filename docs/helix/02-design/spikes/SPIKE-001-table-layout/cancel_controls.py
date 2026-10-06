"""Network-free cancellation controls: no retry/re-submit of an unknown write."""
import json,tempfile,types
from pathlib import Path
from persistent_sql import Client
class Api:
 def __init__(self,terminal):self.calls=[];self.terminal=terminal
 def do(self,method,path,**kw):
  self.calls.append((method,path))
  if path.endswith('/cancel'):return {}
  if method=='POST':return {'statement_id':'known-handle','status':{'state':'RUNNING'}}
  return {'statement_id':'known-handle','status':{'state':self.terminal},'result':{'data_array':[['ok']]}}
checks=[]
for terminal,cap in [('SUCCEEDED',0),('CANCELED',0),('SUCCEEDED',None)]:
 c=Client.__new__(Client);c.out=Path(tempfile.mkdtemp(prefix='ashlar-cancel-control-'));c.records=[];c.observation_timeout=2;c.cancel_after=cap;c.w=types.SimpleNamespace(api_client=Api(terminal))
 try:c.sql('probe','synthetic-query');assert terminal=='SUCCEEDED'
 except RuntimeError:assert terminal=='CANCELED'
 expected=[('POST','/api/2.0/sql/statements')]
 if cap is not None:expected.append(('POST','/api/2.0/sql/statements/known-handle/cancel'))
 expected.append(('GET','/api/2.0/sql/statements/known-handle'))
 assert c.w.api_client.calls==expected
 assert c.records[0]['cancel_requested']==(cap is not None)
 checks.append({'terminal':terminal,'cancel_after':cap,'requests':c.w.api_client.calls,'single_submit':True,'same_handle_observed':True})
p=Path(__file__).resolve().parent/'out/cancel-controls-20261006.json';p.write_text(json.dumps({'state':'passed','checks':checks,'scope':'Mock API transition controls, not proof of server rollback or no committed side effects'},indent=2)+'\n');print('Cancellation controls passed')

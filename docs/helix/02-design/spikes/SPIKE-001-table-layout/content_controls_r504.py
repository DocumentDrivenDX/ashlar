"""Local fail-closed cohort controls; no Databricks/client import or native calls."""
import hashlib,json,time
from pathlib import Path
from publisher_content_r503 import Check,ContentFailure,collect
B=Path(__file__).resolve().parent
class Fake:
 def __init__(self,i,result=None,mode='ok'):self.out='fixture-'+str(i);self.records=[];self.i=i;self.result=result if result is not None else [['hash','false',None,'1.23e4']];self.mode=mode
 def sql(self,label,q):
  if self.mode=='failure':raise RuntimeError('injected worker failure')
  r={'label':label,'sql':q,'statement_id':None if self.mode=='missing' else 'native-'+str(self.i),'wall_ms':1,'response':{'status':{'state':'UNKNOWN' if self.mode=='unknown' else 'SUCCEEDED'}}};self.records.append(r);return self.result

def main():
 out=B/'out/content-controls-r504.json';assert not out.exists();start=time.monotonic();checks=[Check('role'+str(i),'SELECT exact_fixture',(('hash','false',None,'1.23e4'),)) for i in range(4)];assert len(collect([Fake(i) for i in range(4)],checks))==4;controls=[]
 def refuse(label,clients,cs=checks):
  try:collect(clients,cs)
  except ContentFailure:controls.append(label)
  else:raise AssertionError('Partial/corrupt cohort accepted')
 refuse('worker failure after other successful roles',[Fake(i,mode='failure' if i==2 else 'ok') for i in range(4)])
 refuse('missing native handle',[Fake(i,mode='missing' if i==1 else 'ok') for i in range(4)])
 refuse('unknown outcome',[Fake(i,mode='unknown' if i==0 else 'ok') for i in range(4)])
 refuse('null coerced to string',[Fake(i,[['hash','false','null','1.23e4']] if i==3 else None) for i in range(4)])
 refuse('token spelling changed',[Fake(i,[['hash','false',None,'12300']] if i==2 else None) for i in range(4)])
 refuse('duplicate result row',[Fake(i,[['hash','false',None,'1.23e4']]*2 if i==1 else None) for i in range(4)])
 shared=Fake(0);refuse('client sharing',[shared]*4)
 refuse('missing role',[Fake(i) for i in range(3)])
 refuse('duplicate check label',[Fake(i) for i in range(4)],[checks[0]]*4)
 result={'state':'Complete cohort accepted; nine meaningful failure/corruption controls refuse without partial acceptance','controls':controls,'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['publisher_content_r503.py','content_controls_r504.py']},'wall_s':time.monotonic()-start,'qualification':'Local collector controls only; no native finality, version custody, fencing or source support proof.'};out.write_text(json.dumps(result,indent=2)+'\n');print(result['state'])
if __name__=='__main__':main()

import sys,json,contextlib,hashlib,time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
r=Path('/private/tmp/ashlar-public035-adapter-smoke-20261015-zc4r2os1');out=Path(__file__).parent
sys.path[:0]=[str(r/'wheel'),'/Users/erik/Projects/ashlar/src']
import weft
import ashlar_host.databricks_current_source_capture as source
from ashlar_host.databricks_current_spark import SparkCurrentQueryBackend
from ashlar_host.databricks_current_query import execute_databricks_current_query
from ashlar_host.path_capture import PathCaptureConfig
parent=weft.prepare_application_lineage((r/'request.json').read_text());child=parent.prepare_current_query()
binding=(r/'binding.json').read_text();frame=json.loads((r/'frame.json').read_text())
artifact=child.lower_databricks_current_contributors_candidate_json(binding)
class Authority:
 def __init__(self):self.events=[]
 @contextlib.contextmanager
 def open_current_query(self,c,ctx):
  assert c is child;self.events.append('open')
  try:yield self
  finally:self.events.append('close')
 def binding(self):return binding
 def admit(self,*args):self.events.append('admit')
 def renew(self,*args):self.events.append('renew')
class Spark:
 def __init__(self,wrong=False):self.calls=[];self.wrong=wrong
 def sql(self,sql,*,args):
  compiled=json.loads(artifact);assert sql==compiled['sql'];assert args=={'wft_1':'s0'}
  self.calls.append((sql,args))
  rows=[list(x) for x in frame['resultRows']]
  if self.wrong:rows[0][0]='3'
  fields=[SimpleNamespace(name=n,nullable=False,dataType=SimpleNamespace(simpleString=lambda:'string')) for n in ['output_0','__weft_contributors']]
  return SimpleNamespace(schema=SimpleNamespace(fields=fields),toLocalIterator=lambda prefetchPartitions:iter(rows))
start=time.monotonic();events=[]
for wrong in (False,True):
 authority=Authority();spark=Spark(wrong);limits=PathCaptureConfig(4,4096,8192)
 backend=SparkCurrentQueryBackend(spark,authority,limits,limits)
 with patch.object(source,'capture_current_query_sources',return_value=source.CurrentSourceCapture(binding,json.dumps(frame['scans']))):
  try:result=execute_databricks_current_query(child,object(),backend,refinement=False)
  except ValueError:
   assert wrong
  else:
   assert not wrong and json.loads(result.verified_result)=={'columns':['n'],'rows':[['2']]}
 assert authority.events[-1]=='close' and len(spark.calls)==1
 events.append({'wrong_public':wrong,'authority_events':authority.events})
report={'passed':True,'controls':2,'version':weft.__version__,'seconds':round(time.monotonic()-start,3),'mandatory_checker_with_adapter_refinement_false':True,'unchanged_sql_and_slots':True,'positive_public_projection':True,'wrong_public_refused':True,'closing_both_paths':True,'native_module_sha256':hashlib.sha256(Path(weft.weft.__file__).read_bytes()).hexdigest(),'scope':'Actual public035 CQP and native checker through concrete held provider with inert Spark/authority and supplied source scans; no SQL/source capture or UC authority qualification','events':events}
(out/'summary.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))

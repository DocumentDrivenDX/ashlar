"""Small result-cache comparison; repeated pinned reads versus uncached controls."""
import json,time,math,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/ashlar_lc_prefix_points_r352/audited-summary.json';s=json.loads(source.read_text());t=s['table'];O=B/'out/native/ashlar_connector_rpc_r357';assert not O.exists();c=BoundedReads(O);a={'state':'Testing native cache on exact pinned carriers','table':t,'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'reads':[],'bounds':{'read_bytes':12000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':300},'cohort_indices':[0,1,2,3,4,5,16,17],'rpc_trace':[]};start=time.monotonic()
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics():
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());assert time.monotonic()-start<300;save();return h
 backend=c.cursor.backend;assert hasattr(backend,'make_request');original=backend.make_request;current={'phase':None,'label':'setup'}
 def measured(method,request,retryable=True):
  began=time.perf_counter();response=None
  try:response=original(method,request,retryable=retryable);return response
  finally:
   direct=getattr(response,'directResults',None);a['rpc_trace'].append({'sequence':len(a['rpc_trace']),'phase':current['phase'],'label':current['label'],'method':getattr(method,'__name__',type(method).__name__),'wall_ms':(time.perf_counter()-began)*1000,'direct_requested':getattr(request,'getDirectResults',None) is not None,'direct_response':direct is not None,'direct_rows':getattr(direct,'resultSet',None) is not None,'direct_closed':bool(getattr(direct,'closeOperation',None))});save()
 backend.make_request=measured
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=20');rows=c.sql('detail','DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==t['id'];c.sql('cache-on','SET USE_CACHED_RESULT=true');assert c.sql('cache-on-check','SET USE_CACHED_RESULT')[0][-1].lower()=='true'
  w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();inverse=pow(104729,-1,40000000);fields=list(w.carrier('edge',0));cols=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in fields);query=f"SELECT {cols} FROM {t['table']} VERSION AS OF {t['version']} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)"
  for phase in range(2):
   current['phase']=phase
   for j,index in enumerate(a['cohort_indices']):
    current['label']='point-'+str(j);rpc_first=len(a['rpc_trace'])
    ordinal=s['reads'][index]['ordinal'];row=w.carrier('edge',ordinal);n=ordinal*inverse%40000000;after=second.change(n-100000)['after'] if 100000<=n<200000 else first.change(n)['after'] if n<100000 else row;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in fields]];parameters={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']};assert c.sql('point-'+str(j),query,parameters=parameters)==expected;a['reads'].append({'phase':phase,'cache_enabled':True,'rpc_first':rpc_first,'rpc_end':len(a['rpc_trace']),'ordinal':ordinal,'kind':s['reads'][index]['kind'],'statement_id':c.records[-1]['statement_id']});save()
   metrics()
  h=metrics();records={r['statement_id']:r for r in c.records};p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
  def cohort(reads):
   return {'queries':len(reads),'cache_hits':sum(bool(h[r['statement_id']]['metrics'].get('result_from_cache')) for r in reads),'caller_p95_ms':p95([records[r['statement_id']]['wall_ms'] for r in reads]),'engine_p95_ms':p95([h[r['statement_id']]['metrics'].get('execution_time_ms',0) for r in reads]),'compile_p95_ms':p95([h[r['statement_id']]['metrics'].get('compilation_time_ms',0) for r in reads])} if reads else None
  a['per_phase']={str(p):cohort([r for r in a['reads'] if r['phase']==p]) for p in range(2)};a['by_hit']={str(flag):cohort([r for r in a['reads'] if bool(h[r['statement_id']]['metrics'].get('result_from_cache'))==flag]) for flag in [True,False]};assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in a['reads'] if r['phase']==3);a['connector_source_sha256']={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__import__('databricks.sql.client',fromlist=['x']).__file__),Path(__import__('databricks.sql.backend.thrift_backend',fromlist=['x']).__file__),Path(__import__('databricks.sql.result_set',fromlist=['x']).__file__)]};a['wall_s']=time.monotonic()-start;a['state']='All16 instrumented connector singleton carriers match exactly';a['qualification']='Eight pinned full-identity keys, two passes with session result cache enabled; complete20fields/absence and native history. Local wrapper times RPC names/flags only, no request text/headers/credentials, no connector or table mutation. CloseOperation timing may close the preceding query and remains inside the following caller interval. Instrumentation overhead and sequential cache conditions prevent causal isolation; not service percentile, arbitrary1B working set, cold, production policy or freshness admission.';save();print(json.dumps({'state':a['state'],'per_phase':a['per_phase'],'by_hit':a['by_hit'],'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same read handles',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()

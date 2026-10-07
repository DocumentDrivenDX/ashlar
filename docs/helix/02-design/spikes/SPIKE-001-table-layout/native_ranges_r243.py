"""Qualify bounded range oracles against complete600k-node native staging prefix."""
import json,time
from pathlib import Path
from range_verification_r243 import oracle_chunks,digest_query
from scale_mixed_r219 import Workload
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_range_verification_r243';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_growth_r242/audited-summary.json').read_text());a={'state':'Preparing bounded range oracles','tables':prior['tables'],'chunks':[],'bounds':{'read_bytes':20000000000,'write_bytes':0,'statement_s':180,'wall_s':900}};started=time.monotonic()
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save();a['oracle']=list(oracle_chunks(Workload(8000000,40000000),'node',600000));a['oracle_s']=time.monotonic()-started;save();c=Client(O,observation_timeout=200,cancel_after=180)
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=20000000000 and a['costs']['write_remote_bytes']==0
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 for role,t in a['tables'].items():
  rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);fields=[q['name'] for q in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(fields,rows[0]))['id']==t['id']
  expected=sum(chunk['roles'][role]['rows'] for chunk in a['oracle']);assert expected==prior['checks'][role]['rows'];assert c.sql('complete-count-'+role,f"SELECT count(*) FROM {t['table']} VERSION AS OF {t['version']}")==[[str(expected)]]
 metrics(2000000000)
 for chunk in a['oracle']:
  assert time.monotonic()-started<900;metrics(2000000000)
  for role,expected in chunk['roles'].items():
   t=a['tables'][role];q=digest_query(t['table'],t['version'],role,expected['fields'],'node',8000000,chunk['start'],chunk['end']);assert c.sql(f"range-{chunk['start']}-{role}",q)==[[str(expected['rows']),expected['digest']]],role
  a['chunks'].append({'start':chunk['start'],'end':chunk['end'],'state':'All role fields verified'});save()
 metrics();a['state']='All6 disjoint100k-node ranges and complete-role membership verified';a['wall_s']=time.monotonic()-started;a['qualification']='Independent100k-entity range hash arrays; maximum400k journal hashes for this fixture. Full counts plus exact disjoint range cardinality/digest prove no excluded malformed/out-of-range role rows under SHA256 assumption. Each native aggregation and oracle array bounded, but scans may repeat and concurrent worker memory is not measured. No scale/performance/publication gate inferred.';save();print(json.dumps({k:a[k] for k in ['state','oracle_s','wall_s','costs']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles before further admission',error=str(e));save();raise

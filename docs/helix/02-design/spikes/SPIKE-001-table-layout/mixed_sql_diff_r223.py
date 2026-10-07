"""Native read-only range recipe differential with complete field oracle."""
import json,time
from pathlib import Path
from scale_mixed_r219 import Workload
from scale_mixed_sql_r222 import carrier_sql
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_sql_diff_r223';assert not O.exists();c=BoundedReads(O);a={'state':'running','cases':[]}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=30');a['runtime']=c.sql('runtime','SELECT current_version(),current_timezone()')
 cases=[('node',128,640,0,128),('edge',128,640,0,640),('node',8000000,40000000,7999984,8000000),('edge',8000000,40000000,39999984,40000000),('node',1000000000,5000000000,999999984,1000000000),('edge',1000000000,5000000000,4999999984,5000000000)]
 for index,(kind,n,e,start,end) in enumerate(cases):
  w=Workload(n,e);fields=list(w.carrier(kind,start));projection=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in fields)
  rows=c.sql('case-'+str(index),'SELECT '+projection+' FROM ('+carrier_sql(kind,n,e,start,end)+') ORDER BY id')
  expected=[[None if r[f] is None else r[f].replace('T',' ').removesuffix('Z') if f=='published_at' else r[f] for f in fields] for r in [w.carrier(kind,i) for i in range(start,end)]]
  assert rows==expected,(kind,n,e,start,end);a['cases'].append({'kind':kind,'nodes':n,'edges':e,'start':start,'end':end,'all_fields_exact':True});save()
 c.cursor.close();c.cursor=c.connection.cursor()
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert a['costs']['read_bytes']<=1000000000 and a['costs']['write_remote_bytes']==0;a['state']='Every native generated carrier field equals Python oracle';a['qualification']='All768 base rows plus64 tail boundary rows compared, no materialized larger graph. Native carrier generation only; raw/property/adjacency role SQL ports pending. Billion-tail arithmetic/token differential is not billion-scale runtime admission. No writes or performance claim.';save();print(json.dumps(a,indent=2))
except Exception as e:a.update(state='Stopped; inspect existing handles',error=str(e));save();raise
finally:c.close()

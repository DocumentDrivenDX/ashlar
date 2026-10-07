"""Complete native generated role all-field differential, small ranges only."""
import json,time
from pathlib import Path
from collections import defaultdict,Counter
from scale_mixed_r219 import Workload
from scale_mixed_roles_sql_r224 import role_sql
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_roles_diff_r225';assert not O.exists();c=BoundedReads(O);a={'state':'running','checks':[]}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=30');w=Workload(128,640)
 for kind,count in [('node',128),('edge',640)]:
  expected=defaultdict(list)
  for i in range(count):
   for role,row in w.roles(kind,i):expected[role].append(row)
  for role,rows in expected.items():
   fields=list(rows[0]);q=role_sql(role,kind,128,640,0,count);projection=','.join('CAST('+f+' AS STRING) AS '+f if f in ['published_at','received_at'] else f for f in fields)
   actual=c.sql(kind+'-'+role,'SELECT '+projection+' FROM ('+q+')')
   oracle=[[None if row[f] is None else row[f].replace('T',' ').removesuffix('Z') if f in ['published_at','received_at'] else str(row[f]) for f in fields] for row in rows]
   assert Counter(tuple(x) for x in actual)==Counter(tuple(x) for x in oracle),(kind,role)
   a['checks'].append({'kind':kind,'role':role,'rows':len(rows),'all_fields_exact':True});save()
 c.cursor.close();c.cursor=c.connection.cursor()
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert a['costs']['read_bytes']<=1000000000 and a['costs']['write_remote_bytes']==0;a['state']='All native generated bootstrap roles equal complete Python oracle';a['qualification']='128 nodes640edges bootstrap only. All role rows compared including exact raw carrier numeric string encoding, lexical property tokens, presence flags and adjacency. No writes/materialized larger graph, native physical layout/constraint or performance qualification.';save();print(json.dumps(a,indent=2))
except Exception as e:a.update(state='Stopped; inspect existing handles',error=str(e));save();raise
finally:c.close()

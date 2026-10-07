"""Qualify complete capped pure-role verification on existing8M edge snapshots."""
import json,time
from pathlib import Path
from pure_group_blocks_r254 import pure_bucket_sql,pure_grouped_block
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_pure_blocks_r254';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_edges_r252/audited-summary.json').read_text());a={'state':'Running complete pure-edge block verification','checks':{},'bounds':{'read_bytes':20000000000,'write_bytes':0,'wall_s':600,'statement_s':180}};started=time.monotonic();c=Client(O,observation_timeout=200,cancel_after=180)
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=20000000000 and a['costs']['write_remote_bytes']==0
 return h
save()
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 values="(cast(NULL AS BIGINT)),(cast('-9223372036854775808' AS BIGINT)),(cast(8000000 AS BIGINT)),(cast(16000001 AS BIGINT)),(cast(8000001 AS BIGINT)),(cast(16000000 AS BIGINT))"
 assert c.sql('invalid-id-control',f"SELECT {pure_bucket_sql(8000000,8000000)} bucket,count(*) FROM VALUES {values} AS t(id) GROUP BY bucket ORDER BY bucket")==[['-1','4'],['0','1'],['79','1']]
 for role in ['edge_current','adjacency_forward']:
  t=prior['tables'][role];rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);names=[f['name'] for f in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==t['id'];assert c.sql('full-count-'+role,f"SELECT count(*) FROM {t['table']} VERSION AS OF {t['version']}")==[['8000000']];expected=prior['checks']['complete-'+role]['groups'];actual=[]
  for first in range(0,80,20):
   assert time.monotonic()-started<600;metrics(4000000000);actual+=c.sql(f'block-{role}-{first}',pure_grouped_block(t['table'],t['version'],role,prior['edge_oracle'][0]['roles'][role]['fields'],8000000,8000000,100000,first,20))
  assert actual==expected;a['checks'][role]={'table':t,'groups':actual,'coverage':'All80 independent groups and complete same-snapshot counts; four disjoint20-group blocks'};save()
 metrics();a['wall_s']=time.monotonic()-started;a['state']='Every8M edge/forward field passes capped blocks, complete counts and invalid control';a['qualification']='Complete existing8M edge snapshots; helper admits up to50M synthetic prefix with100-group max per query, not measured50M/billion support. Explicit ID predicates improve pruning when native file stats permit. Invalid excluded membership cannot hide because whole count equals all independently verified block counts under SHA256 assumptions; no performance or publication gate.';save();print(json.dumps({k:a[k] for k in ['state','costs','wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect existing native handles before admission',error=str(e));save();raise

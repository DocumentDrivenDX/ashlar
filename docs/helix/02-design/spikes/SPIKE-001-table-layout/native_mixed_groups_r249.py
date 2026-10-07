"""Read-only complete mixed-bootstrap qualification plus invalid membership controls."""
import json,time
from pathlib import Path
from mixed_grouped_verification_r249 import mixed_bucket_sql,mixed_grouped_query,expected_mixed
from range_verification_r243 import oracle_chunks
from scale_mixed_r219 import Workload
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_groups_r249';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_mixed_materialize_r226/summary.json').read_text());a={'state':'Preparing complete mixed oracle','checks':{},'bounds':{'read_bytes':1000000000,'write_bytes':0,'statement_s':180,'wall_s':600}};started=time.monotonic();w=Workload(4096,20480);node=list(oracle_chunks(w,'node',4096,1024));edge=list(oracle_chunks(w,'edge',20480,1024));c=Client(O,observation_timeout=200,cancel_after=180)
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=1000000000 and a['costs']['write_remote_bytes']==0
 return h
save()
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 raw="(NULL),('node:01:1'),('other:1:1'),('node:4096:1'),('edge:20480:1'),('edge:99999999999999999999999:1'),('node:0:1'),('node:4095:1'),('edge:0:1'),('edge:20479:1')"
 assert c.sql('invalid-raw-control',f"SELECT {mixed_bucket_sql('source_record',4096,20480,1024)} bucket,count(*) FROM VALUES {raw} AS t(delivery_id) GROUP BY bucket ORDER BY bucket")==[['-1','6'],['0','1'],['3','1'],['4','1'],['23','1']]
 events="('node',cast(NULL AS BIGINT)),('edge',cast('-9223372036854775808' AS BIGINT)),('node',cast(4097 AS BIGINT)),('edge',cast(4096 AS BIGINT)),('other',cast(1 AS BIGINT)),('node',cast(1 AS BIGINT)),('node',cast(4096 AS BIGINT)),('edge',cast(4097 AS BIGINT)),('edge',cast(24576 AS BIGINT))"
 assert c.sql('invalid-event-control',f"SELECT {mixed_bucket_sql('property_journal',4096,20480,1024)} bucket,count(*) FROM VALUES {events} AS t(entity_kind,id) GROUP BY bucket ORDER BY bucket")==[['-1','5'],['0','1'],['3','1'],['4','1'],['23','1']]
 for role in ['source_record','property_journal']:
  t=prior['tables'][role];rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==t['id'];expected=expected_mixed(node,edge,role);assert c.sql('full-count-'+role,f"SELECT count(*) FROM {t['table']} VERSION AS OF 0")==[[str(sum(int(r[1]) for r in expected))]]
  actual=[]
  for first in range(0,len(expected),8):
   assert time.monotonic()-started<600;metrics(100000000);actual+=c.sql(f'block-{role}-{first}',mixed_grouped_query(t['table'],0,role,node[0]['roles'][role]['fields'],4096,20480,1024,first,8))
  assert actual==expected;assert sum(int(r[1]) for r in actual)==t['rows'];a['checks'][role]={'table':t['table'],'uuid':t['id'],'version':0,'groups':actual,'rows':t['rows'],'coverage':'Three disjoint8-group blocks; sum equals full same-snapshot count, all independent fields match'};save()
 metrics();a['state']='Complete node/edge raw and history digests pass bounded blocks and invalid controls';a['wall_s']=time.monotonic()-started;a['qualification']='Existing4096-node/20480-edge snapshot0, independent local full-field oracles; full count plus every disjoint block proves no excluded rows under SHA256 assumption. Blocks cap100 groups, width100k entities; native total memory/scan cost still grows. Small protocol evidence, not2M edges/1B admission or performance gate.';save();print(json.dumps({k:a[k] for k in ['state','costs','wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles before further admission',error=str(e));save();raise

"""Compare single-scan grouped verification to saved independent1M range oracles."""
import json,time
from pathlib import Path
from grouped_verification_r245 import grouped_query,bucket_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_grouped_verification_r245';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_growth_r244/audited-summary.json').read_text());a={'state':'Running grouped complete-prefix verification','tables':prior['tables'],'checks':{},'bounds':{'read_bytes':10000000000,'write_bytes':0,'statement_s':180,'wall_s':600}};started=time.monotonic();c=Client(O,observation_timeout=200,cancel_after=180)
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=10000000000 and a['costs']['write_remote_bytes']==0
 return h
save()
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 # Deliberate malformed/null/overflow/noncanonical delivery membership must be surfaced.
 values="(NULL),('node:01:1'),('edge:1:1'),('node:1000000:1'),('node:99999999999999999999999:1'),('node:0:1'),('node:999999:1')"
 assert c.sql('invalid-membership',f"SELECT {bucket_sql('source_record','node',8000000,1000000,100000)} AS bucket,count(*) FROM VALUES {values} AS t(delivery_id) GROUP BY bucket ORDER BY bucket")==[['-1','5'],['0','1'],['9','1']]
 for role,t in a['tables'].items():
  assert time.monotonic()-started<600;metrics(3000000000)
  rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==t['id']
  fields=prior['oracle'][0]['roles'][role]['fields'];expected=[[str(i),str(chunk['roles'][role]['rows']),chunk['roles'][role]['digest']] for i,chunk in enumerate(prior['oracle'])]
  actual=c.sql('grouped-'+role,grouped_query(t['table'],t['version'],role,fields,'node',8000000,1000000));assert actual==expected,role;a['checks'][role]={'ranges':len(actual),'rows':sum(int(r[1]) for r in actual),'all_field_digests':actual,'invalid_bucket_absent':True};save();metrics()
 h=metrics();a['group_metrics']={r['label']:dict(caller_ms=r['wall_ms'],**h[r['statement_id']]['metrics']) for r in c.records if r['label'].startswith('grouped-')};assert all(not q.get('result_from_cache') for q in a['group_metrics'].values());a['state']='All1M-node role ranges verified in3 uncached grouped scans; invalid membership control passes';a['wall_s']=time.monotonic()-started;a['qualification']='Exact saved independently generated100k-range digests for each field across current/raw/history. No row filtering; invalid membership retained as bucket-1 and fails equality. At most10 groups here, each100k carrier/raw or400k event rows. Total native executor memory not measured or bounded by per-group limits; report actual spill. No billion/ingest/read/publication gate claim.';save();print(json.dumps({k:a[k] for k in ['state','costs','wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles before further admission',error=str(e));save();raise

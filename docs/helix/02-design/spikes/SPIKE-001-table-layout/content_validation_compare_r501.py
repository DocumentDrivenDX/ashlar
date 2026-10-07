"""Serial/concurrent full-content validation at immutable fifth-publication pins."""
import concurrent.futures,hashlib,json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent
LABELS=('cdf-source_record','cdf-property_journal','cdf-edge_current','typed-endpoints')

def main():
 parent=B/'out/native/ashlar_fifth_guard_publish_r481/statements.jsonl';prior={r['label']:r for r in map(json.loads,parent.read_text().splitlines())};queries={k:prior[k]['sql'] for k in LABELS};expected={k:sorted(prior[k]['response']['result']['data_array']) for k in LABELS}
 out=B/'out/native/content_validation_compare_r501';assert not out.exists();out.mkdir();start=time.monotonic();clients=[]
 a={'state':'running immutable complete-content scheduler comparison','source_sha256':hashlib.sha256(parent.read_bytes()).hexdigest(),'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'bounds':{'read_bytes':60000000000,'write_remote_bytes':0,'spill_to_disk_bytes':2000000000,'wall_s':360},'labels':list(LABELS),'expected':expected,'phases':[],'qualification':'Three four-check cohorts serial-first/concurrent-middle/serial-last; exact prior full CDF digests and all39.95M typed endpoint checks. Immutable pins, no query cache, current2X-Small unchanged. Shared compute/order/storage caches remain; not a full publication timing or fencing proof.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def audit():
  allh={}
  for c in clients:
   c.cursor.close();c.cursor=c.connection.cursor()
   for i in range(30):
    try:h=collect_history(c.w,c.records,c.out/'shared-history.json');break
    except HistoryPending:
     if i==29:raise
     time.sleep(1)
   allh.update(h)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in allh.values()) for k in a['bounds'] if k!='wall_s'};a['wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=360
  return allh
 save()
 try:
  for i in range(4):
   c=BoundedReads(out/('worker'+str(i)),socket_timeout=60);clients.append(c);c.sql('timeout','SET STATEMENT_TIMEOUT=60');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  a['setup_s']=time.monotonic()-start;save()
  def run(i,phase):
   label=LABELS[i];c=clients[i];result=c.sql(phase+'-'+label,queries[label]);assert sorted(result)==expected[label];return {'label':label,'statement_id':c.records[-1]['statement_id'],'caller_ms':c.records[-1]['wall_ms'],'result':result}
  for phase in ['serial-before','concurrent','serial-after']:
   t=time.monotonic()
   if phase=='concurrent':
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(lambda i:run(i,phase),range(4)))
   else:results=[run(i,phase) for i in range(4)]
   cohort={'phase':phase,'wall_s':time.monotonic()-t,'results':results};a['phases'].append(cohort);save();h=audit()
   for r in results:r['metrics']=h[r['statement_id']]['metrics'];assert not r['metrics'].get('result_from_cache')
   save()
  a['state']='All12 uncached complete-content checks match fifth publication results';audit();save()
  for c in clients:
   marker=c.out/'inflight-request.json';marker.rename(c.out/'completed-last-request.json');c.close()
  print(json.dumps({k:a[k] for k in ['state','setup_s','costs','wall_s']},indent=2));print([(p['phase'],p['wall_s']) for p in a['phases']])
 except Exception as e:a.update(state='Stopped; inspect same durable worker handles without replay',error=str(e));save();raise
if __name__=='__main__':main()

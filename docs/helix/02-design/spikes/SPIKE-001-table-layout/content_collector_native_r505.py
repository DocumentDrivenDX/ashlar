"""One native verification of extracted fail-closed content cohort, immutable pins."""
import hashlib,json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from publisher_content_r503 import Check,collect
B=Path(__file__).resolve().parent

def main():
 pp=B/'out/native/ashlar_fifth_guard_publish_r481/statements.jsonl';prior={r['label']:r for r in map(json.loads,pp.read_text().splitlines())};labels=('cdf-source_record','cdf-property_journal','cdf-edge_current','typed-endpoints');checks=[Check(label,prior[label]['sql'],tuple(tuple(r) for r in prior[label]['response']['result']['data_array'])) for label in labels]
 out=B/'out/native/content_collector_native_r505';assert not out.exists();out.mkdir();start=time.monotonic();clients=[];a={'state':'running extracted content cohort','source_sha256':hashlib.sha256(pp.read_bytes()).hexdigest(),'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['publisher_content_r503.py','content_collector_native_r505.py']},'bounds':{'read_bytes':20000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':150},'qualification':'One extracted private collector verification at immutable fifth-publication versions. Caller must separately prove custody/finality/fencing and integrate with publication; no ready-clock or new source admission.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  for i in range(4):
   c=BoundedReads(out/('content_r505_worker'+str(i)),socket_timeout=60);clients.append(c);c.sql('timeout','SET STATEMENT_TIMEOUT=60');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  a['setup_s']=time.monotonic()-start;t=time.monotonic();a['accepted']=collect(clients,checks);a['cohort_wall_s']=time.monotonic()-t;save();allh={}
  for c in clients:
   c.cursor.close();c.cursor=c.connection.cursor()
   for i in range(30):
    try:h=collect_history(c.w,c.records,c.out/'shared-history.json');break
    except HistoryPending:
     if i==29:raise
     time.sleep(1)
   allh.update(h)
  for value in a['accepted'].values():value['metrics']=allh[value['statement_id']]['metrics'];assert not value['metrics'].get('result_from_cache')
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in allh.values()) for k in a['bounds'] if k!='wall_s'};a['wall_s']=time.monotonic()-start;assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=150;a['state']='Extracted collector accepts all four complete native immutable content checks';save()
  for c in clients:(c.out/'inflight-request.json').rename(c.out/'completed-last-request.json');c.close()
  print(json.dumps({k:a[k] for k in ['state','setup_s','cohort_wall_s','costs','wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect all durable worker handles; no partial publication/replay',error=str(e));save();raise
if __name__=='__main__':main()

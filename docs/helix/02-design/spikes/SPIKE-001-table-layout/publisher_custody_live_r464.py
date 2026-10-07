"""Exercise reusable custody collector on actual fourth publication/input cohort."""
import json,hashlib,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publisher_custody_r462 import collect
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;p=B/'out/native/ashlar_metadata_custody_r460/summary.json';old=json.loads(p.read_text());expected=[{'key':x['key'],'table':x['table']['table'],'head':x['physical_head'],'schema':x['schema'],'detail':x['detail']} for x in old['phases']['concurrent']['tables']];O=B/'out/native/ashlar_publisher_custody_live_r464';assert not O.exists();O.mkdir();start=time.monotonic();a={'state':'collecting live10table custody','source_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'code_sha256':hashlib.sha256((B/'publisher_custody_r462.py').read_bytes()).hexdigest(),'bounds':{'read_bytes':10000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':120}};clients=[]
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save()
try:
 for j in range(4):
  c=BoundedReads(O/('worker-'+str(j)),socket_timeout=30);clients.append(c);assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
 a['setup_s']=time.monotonic()-start;t=time.monotonic();a['accepted']=collect(clients,expected);a['collection_s']=time.monotonic()-t;save()
 for c in clients:c.cursor.close();c.cursor=c.connection.cursor()
 records=[r for c in clients for r in c.records]
 for i in range(20):
  try:h=collect_history(clients[0].w,records,O/'shared-history.json');break
  except HistoryPending:
   if i==19:raise
   time.sleep(1)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a.update(state='Reusable component accepts exact10table live custody cohort',wall_s=time.monotonic()-start);assert a['wall_s']<120;save()
 for c in clients:(c.out/'inflight-request.json').rename(c.out/'completed-last-request.json');c.close()
 print({k:v for k,v in a.items() if k!='accepted'})
except Exception as e:a.update(state='Stopped; inspect durable handles without replay',error=str(e));save();raise

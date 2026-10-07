"""Read-only full10table UUID/schema/head custody comparison."""
import json,time,concurrent.futures,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;src=B/'out/native/ashlar_fourth_guard_publish_r437/audited-summary.json';pub=json.loads(src.read_text());elig=json.loads((B/'out/native/ashlar_fourth_publisher_preflight_r436/summary.json').read_text());tables=[(prefix+'-'+role,t) for prefix,ts in [('base',pub['tables']),('input',pub['inputs'])] for role,t in ts.items()];O=B/'out/native/ashlar_metadata_custody_r460';assert not O.exists();O.mkdir();start=time.monotonic();a={'state':'running same10table custody checks','source_sha256':hashlib.sha256(src.read_bytes()).hexdigest(),'phases':{},'bounds':{'read_bytes':10000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':180},'qualification':'UUID/schema/properties/head custody checks only; do not fence writers or establish atomic cross-role snapshot.'};clients=[]
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def objects(c,label,q):
 rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
def check(c,index):
 key,t=tables[index];d=objects(c,'detail-'+key,'DESCRIBE DETAIL '+t['table'])[0];h=objects(c,'head-'+key,'DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0];s=objects(c,'schema-'+key,'DESCRIBE TABLE '+t['table']);assert d['id']==t['id'] and d['format']=='delta';e=elig['tables'][key];assert s==e['schema'];head=8 if key=='base-object_current' else t['version'];assert int(h['version'])==head
 for field in ['properties','partitionColumns','clusteringColumns','tableFeatures','minReaderVersion','minWriterVersion']:assert (json.loads(d[field])==json.loads(e['detail'][field]) if field in ['properties','partitionColumns','clusteringColumns','tableFeatures'] else d[field]==e['detail'][field]),(key,field)
 return {'key':key,'table':t,'physical_head':h,'schema':s,'detail':d}
save()
try:
 for phase in ['sequential','concurrent']:
  t=time.monotonic();cs=[]
  for j in range(1 if phase=='sequential' else 4):
   c=BoundedReads(O/(phase+'-'+str(j)),socket_timeout=30);cs.append(c);clients.append(c);assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  setup=time.monotonic()-t
  if phase=='sequential':results=[check(cs[0],i) for i in range(10)]
  else:
   def worker(j):return [check(cs[j],i) for i in range(j,10,4)]
   with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:results=[r for group in pool.map(worker,range(4)) for r in group]
  a['phases'][phase]={'wall_s':time.monotonic()-t,'setup_s':setup,'tables':sorted(results,key=lambda r:r['key'])};save()
 assert a['phases']['sequential']['tables']==a['phases']['concurrent']['tables']
 for c in clients:c.cursor.close();c.cursor=c.connection.cursor()
 records=[r for c in clients for r in c.records]
 for i in range(20):
  try:h=collect_history(clients[0].w,records,O/'shared-history.json');break
  except HistoryPending:
   if i==19:raise
   time.sleep(1)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a.update(state='All10table complete custody results equal across phases',wall_s=time.monotonic()-start);assert a['wall_s']<180;save()
 for c in clients:(c.out/'inflight-request.json').rename(c.out/'completed-last-request.json');c.close()
 print(json.dumps({k:v for k,v in a.items() if k!='phases'},indent=2));print({k:v['wall_s'] for k,v in a['phases'].items()})
except Exception as e:a.update(state='Stopped; inspect durable handles without replay',error=str(e));save();raise

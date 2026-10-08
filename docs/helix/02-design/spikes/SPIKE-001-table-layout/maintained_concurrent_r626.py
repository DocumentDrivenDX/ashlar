"""Four synchronized persistent readers over maintained E12 complete carriers.
Requires exhaustive common-file audit before any native client is created.
"""
import concurrent.futures,threading,json,hashlib,time,math
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from seventh_changes_r589 import SeventhChanges
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent

def main():
 preservation=B/'out/native/ashlar_common_carriers_remaining_r624/audited-summary.json';proof=json.loads(preservation.read_text());assert proof['audit']['all_live_edge_carriers']==39930000 and proof['state']=='All600 common-file complete carrier multisets and live typed identities preserved at E10/E12';t={**proof['table'],'version':12};w=SeventhChanges();groups={(inside,deleted):[] for inside in [True,False] for deleted in [True,False]}
 for index in range(100000):
  x=w.change(index);h=x['before']['lookup_hash'];key=('08'+'0'*62<=h<'0c'+'0'*62,x['after'] is None);limit=4 if key[1] else 12
  if len(groups[key])<limit:groups[key].append(index)
  if all(len(v)==(4 if k[1] else 12) for k,v in groups.items()):break
 indices=[]
 for inside in [True,False]:indices.extend(groups[inside,False]+groups[inside,True])
 assert len(indices)==len(set(indices))==32
 O=B/'out/native/ashlar_maintained_concurrent_r626';assert not O.exists();O.mkdir();start=time.monotonic();clients=[BoundedReads(O/('reader-'+str(i)),socket_timeout=30) for i in range(4)];barrier=threading.Barrier(4,timeout=30)
 a={'state':'Running four synchronized maintained snapshot readers','source_sha256':hashlib.sha256(preservation.read_bytes()).hexdigest(),'table':t,'indices':indices,'reads':[],'bounds':{'read_bytes':25000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':300},'qualification':'Two passes32stratified changed keys,16inside/16outside maintenance hash scope;24updates/8deletes. Four persistent clients/barrier per slot, full20fields/exact absence, SQL result caching disabled. Query interval overlap is measured, not proof of simultaneous engine scans. Warm-biased data after audits; no controlled cold/sustained throughput/ingest concurrency/globalp95/1B admission.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  c=clients[0];r=c.sql('detail','DESCRIBE DETAIL '+t['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,r[0]))['id']==t['id'];assert c.sql('head','DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0][0]=='12'
  fields=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in FIELDS);query=f"SELECT {fields} FROM {t['table']} VERSION AS OF 12 WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)";a['query']=query;save()
  def reader(number):
   client=clients[number];reads=[]
   try:
    client.sql('timeout','SET STATEMENT_TIMEOUT=15');assert client.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
    for phase in range(2):
     for slot in range(8):
      index=indices[slot*4+number];x=w.change(index);row=x['before'];expected=[] if x['after'] is None else [[x['after'][f].replace('T',' ').removesuffix('Z') if f=='published_at' else x['after'][f] for f in FIELDS]];barrier.wait();assert client.sql('point-'+str(phase)+'-'+str(slot),query,parameters={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']})==expected;reads.append({'reader':number,'phase':phase,'slot':slot,'index':index,'statement_id':client.records[-1]['statement_id']});assert time.monotonic()-start<300
    return reads
   except Exception:barrier.abort();raise
  with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:a['reads']=sorted(sum([f.result() for f in [pool.submit(reader,i) for i in range(4)]],[]),key=lambda x:(x['phase'],x['slot'],x['reader']))
  assert clients[0].sql('final-head','DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0][0]=='12'
  records=[]
  for c in clients:
   c.cursor.close();c.cursor=c.connection.cursor();records.extend(c.records)
  for j in range(20):
   try:h=collect_history(clients[0].w,records,O/'shared-history.json');break
   except HistoryPending:
    if j==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());points={r['statement_id']:r for r in records if r['label'].startswith('point-')};assert len(points)==64 and all(not h[sid]['metrics'].get('result_from_cache') for sid in points);p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1];a['metrics']={str(phase):{'caller_p95_ms':p95([points[x['statement_id']]['wall_ms'] for x in a['reads'] if x['phase']==phase]),'engine_p95_ms':p95([h[x['statement_id']]['metrics']['execution_time_ms'] for x in a['reads'] if x['phase']==phase])} for phase in range(2)};a['wall_s']=time.monotonic()-start;assert a['wall_s']<300;a['state']='All64 complete maintained carriers/absence pass four synchronized readers';save()
  for c in clients:(c.out/'inflight-request.json').rename(c.out/'completed-last-request.json')
  print(json.dumps({k:a[k] for k in ['state','costs','metrics','wall_s']}))
 except Exception as e:a.update(state='Stopped; inspect same child native handles; no blind replay',error=str(e));save();raise
 finally:
  for c in clients:
   try:c.close()
   except Exception:pass
if __name__=='__main__':main()

"""Two bounded persistent readers over one exact pinned full-carrier cohort."""
import concurrent.futures,json,time,threading,math,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/ashlar_range32_pruning_r310/audited-summary.json';prior=json.loads(source.read_text());p=B/'out/native/ashlar_range32_concurrent_r311';assert not p.exists();p.mkdir();start=time.monotonic();clients=[BoundedReads(p/('reader-'+str(i))) for i in range(2)];barrier=threading.Barrier(2,timeout=30);w=Workload(8000000,40000000);changes=Changes(8000000,40000000,100000);inverse=pow(104729,-1,40000000);t=prior['table'];fields=list(w.carrier('edge',0));cols=','.join('CAST(published_at AS STRING) AS published_at' if k=='published_at' else k for k in fields);query=f"SELECT {cols} FROM {t['table']} VERSION AS OF {t['version']} WHERE lookup_partition=CAST(:partition AS INT) AND lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)";a={'state':'Running2concurrent persistent readers','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'table':t,'bounds':{'read_bytes':12000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':300,'statement_timeout_s':15},'reads':[]}
 def save():(p/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 def reader(number):
  c=clients[number];reads=[]
  try:
   c.sql('timeout','SET STATEMENT_TIMEOUT=15');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
   for slot in range(16):
    i=slot*2+number;ordinal=prior['reads'][i]['ordinal'];row=w.carrier('edge',ordinal);index=ordinal*inverse%40000000;after=changes.change(index)['after'] if index<100000 else row;expected=[] if after is None else [[None if after[k] is None else after[k].replace('T',' ').removesuffix('Z') if k=='published_at' else after[k] for k in fields]];barrier.wait();assert c.sql('point-'+str(i),query,parameters={'partition':int(row['lookup_hash'][:2],16)//8,'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']})==expected;reads.append({'index':i,'reader':number,'ordinal':ordinal,'kind':prior['reads'][i]['kind'],'statement_id':c.records[-1]['statement_id']});assert time.monotonic()-start<300
   return reads
  except Exception:barrier.abort();raise
 try:
  with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
   fs=[pool.submit(reader,i) for i in range(2)];a['reads']=sorted(sum([f.result() for f in fs],[]),key=lambda r:r['index'])
  for c in clients:c.cursor.close()
  records=sum([c.records for c in clients],[]);(p/'statements.jsonl').write_text('\n'.join(json.dumps(r) for r in records)+'\n')
  for n in range(15):
   try:h=collect_history(clients[0].w,records,p/'shared-history.json');break
   except HistoryPending:
    if n==14:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());points=[r for r in records if r['label'].startswith('point-')];assert len(points)==32 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in points);p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1];a['point_metrics']={'caller_p95_ms':p95([r['wall_ms'] for r in points]),**{k+'_p95':p95([h[r['statement_id']]['metrics'].get(k,0) for r in points]) for k in ['execution_time_ms','compilation_time_ms','total_time_ms','read_bytes','read_files_count']}};a.update(state='All32 exact full-carrier/absence reads pass with2concurrent connections',wall_s=time.monotonic()-start,qualification='One synchronized2reader cohort over32stratified keys from qualified39.99M-edge range32 partition candidate. Uncached results, warmed prior data, all20fields/exact deletions. Pair barriers measure simultaneous requests, not sustained multi-user tail or concurrent ingestion. Existing2XSmall compute unchanged; no cold-data, write-fencing, throughput, or billion admission.');save()
  for c in clients:(c.out/'inflight-request.json').unlink()
  print(json.dumps({'state':a['state'],'point_metrics':a['point_metrics'],'costs':a['costs']},indent=2))
 except Exception as exc:a.update(state='Stopped; inspect child same handles before replay',error=str(exc));save();raise
 finally:
  for c in clients:
   try:c.connection.close()
   except Exception:pass
if __name__=='__main__':main()

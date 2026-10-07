"""Same-client serial/concurrent/serial closing metadata comparison, read-only."""
import copy,hashlib,json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publisher_closing_r548 import close
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json';pub=json.loads(source.read_text());elig=B/pub['sources']['eligibility'];e=json.loads(elig.read_text());events={x['role']:x for x in pub['commit_events']};expected=[]
 for key,v in e['tables'].items():
  item={'key':key,'table':v['pin']['table'],'detail':v['detail'],'head':v['head'],'schema':v['schema']}
  if key.startswith('base-') and key!='base-object_current':item['head']=events[key.removeprefix('base-')]['history']
  expected.append(item)
 out=B/'out/native/closing_metadata_native_r549';assert not out.exists();out.mkdir();start=time.monotonic();workers=[];a={'state':'comparing complete closing metadata cohorts','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'eligibility_sha256':hashlib.sha256(elig.read_bytes()).hexdigest(),'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['closing_metadata_native_r549.py','publisher_custody_r462.py','publisher_closing_r548.py']},'expected':expected,'cohorts':[],'bounds':{'read_bytes':10000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':180},'qualification':'Read-only accepted sixth pins, ten current UUID/head/protocol/schema profiles, four owned clients; Extracted module executes one complete concurrent cohort with unique phase tags. No role/manifest writes, production fence or full ready-input clock.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics():
  for w in workers:w.cursor.close();w.cursor=w.connection.cursor()
  records=[r for w in workers for r in w.records]
  for i in range(20):
   try:h=collect_history(workers[0].w,records,out/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=180;return h
 save()
 try:
  setup=time.monotonic()
  for i in range(4):
   w=BoundedReads(out/('closing_r549_worker'+str(i)),socket_timeout=30);workers.append(w);w.sql('timeout','SET STATEMENT_TIMEOUT=30');assert w.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  a['setup_s']=time.monotonic()-setup
  for phase in ['concurrent']:
   wanted=[{**copy.deepcopy(x),'key':phase+'-'+x['key']} for x in expected];t=time.monotonic()
   actual=close(workers,expected,phase)
   wall=time.monotonic()-t;known={x['key']:x for x in wanted}
   for x in actual:assert x['head']['queryHistoryStatementId']==known[x['key']]['head']['queryHistoryStatementId']
   a['cohorts'].append({'phase':phase,'collection_s':wall,'accepted':actual});save();metrics()
  h=metrics();records=[r for w in workers for r in w.records]
  for cohort in a['cohorts']:
   phase=cohort['phase'];selected=[r for r in records if r['label'].startswith(('detail-'+phase+'-','head-'+phase+'-','schema-'+phase+'-'))];assert len(selected)==30;cohort['statement_ids']=[r['statement_id'] for r in selected];cohort['engine_sum_ms']=sum(h[r['statement_id']]['metrics'].get('execution_time_ms',0) for r in selected);cohort['caller_sum_ms']=sum(r['wall_ms'] for r in selected)
  assert len(records)==38;a['state']='Extracted ten-table closing module preserve exact UUID/profile/schema and actual commit IDs';save()
  for w in workers:(w.out/'inflight-request.json').rename(w.out/'completed-last-request.json');w.close()
  print(json.dumps({'state':a['state'],'cohorts':[{k:c[k] for k in ['phase','collection_s','engine_sum_ms','caller_sum_ms']} for c in a['cohorts']],'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as exc:a.update(state='Stopped; inspect same worker handles, no writes or read replay',error=str(exc));save();raise
if __name__=='__main__':main()

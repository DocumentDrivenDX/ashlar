"""Integrated post-commit stage at existing pins; no role/descriptor writes or replay."""
import hashlib,json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from publisher_validation_r507 import validate
from validation_plan_oracle_r508 import source
B=Path(__file__).resolve().parent

def main():
 pub,s,prior=source();out=B/'out/native/validation_stage_native_r510';assert not out.exists();out.mkdir();start=time.monotonic();c=Client(out,observation_timeout=120,cancel_after=60);workers=[]
 a={'state':'running integrated post-commit stage at already accepted immutable vector','tables':pub['tables'],'inputs':pub['inputs'],'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['publisher_validation_r507.py','publisher_content_r503.py','validation_plan_oracle_r508.py','validation_stage_native_r510.py']},'source_sha256':hashlib.sha256((B/'out/native/ashlar_fifth_guard_publish_r481/summary.json').read_bytes()).hexdigest(),'bounds':{'read_bytes':60000000000,'write_remote_bytes':0,'spill_to_disk_bytes':2000000000,'wall_s':240},'cohorts':[],'qualification':'Read-only integrated validation after previously accepted fifth role commits; includes custody observations, all15 post-commit checks and final history. No new source apply, descriptor/ACK, ready clock or production fence.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objects(label,q):
  result=c.sql(label,q);cols=[v['name'] for v in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in result]
 def custody(label):
  observed={}
  for key,pin in {**pub['tables'],**{'input-'+k:v for k,v in pub['inputs'].items()}}.items():
   detail=objects(label+'-'+key+'-detail','DESCRIBE DETAIL '+pin['table'])[0];assert detail['id']==pin['id'];head=objects(label+'-'+key+'-head','DESCRIBE HISTORY '+pin['table']+' LIMIT 1')[0]
   expected=8 if key=='object_current' else pin['version'];assert int(head['version'])==expected
   observed[key]={'table':pin['table'],'selected_version':pin['version'],'id':detail['id'],'head':head,'partitionColumns':detail['partitionColumns'],'clusteringColumns':detail['clusteringColumns'],'properties':json.loads(detail['properties']),'tableFeatures':json.loads(detail['tableFeatures']),'minReaderVersion':detail['minReaderVersion'],'minWriterVersion':detail['minWriterVersion']}
  return observed
 def telemetry():
  # Every returned result is consumed; close operations before final-metric polling.
  for w in workers:w.cursor.close();w.cursor=w.connection.cursor()
  records=c.records+[r for w in workers for r in w.records]
  for i in range(30):
   try:h=collect_history(c.w,records,out/'shared-history.json');break
   except HistoryPending:
    if i==29:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in a['bounds'] if k!='wall_s'};a['wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=240;return h
 save()
 try:
  a['custody_before']=custody('before');save()
  setup=time.monotonic()
  for i in range(4):
   w=BoundedReads(out/('validation_r510_worker'+str(i)),socket_timeout=60);workers.append(w);w.sql('timeout','SET STATEMENT_TIMEOUT=60');assert w.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  a['setup_s']=time.monotonic()-setup
  def checkpoint(offset,values):
   assert time.monotonic()-start<240;a['cohorts'].append({'offset':offset,'labels':list(values),'statement_ids':[v['statement_id'] for v in values.values()]});telemetry()
  t=time.monotonic();a['accepted']=validate(workers,pub['tables'],pub['inputs'],s,checkpoint);a['validation_with_telemetry_s']=time.monotonic()-t;save()
  a['custody_after']=custody('after');assert a['custody_after']==a['custody_before']
  for w in workers:w.cursor.close();w.cursor=w.connection.cursor()
  h=telemetry()
  for label,value in a['accepted'].items():
   value['metrics']=h[value['statement_id']]['metrics'];assert not value['metrics'].get('result_from_cache');assert sorted(value['result'])==sorted(prior[label]['response']['result']['data_array'])
  a['state']='Integrated15-check stage passes exact corpus and unchanged ten-table custody at existing committed pins';save();(out/'live-statement.json').rename(out/'completed-last-statement.json')
  for w in workers:(w.out/'inflight-request.json').rename(w.out/'completed-last-request.json');w.close()
  print(json.dumps({k:a[k] for k in ['state','setup_s','validation_with_telemetry_s','costs','wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same durable handles; no replay or publication',error=str(e));save();raise
if __name__=='__main__':main()

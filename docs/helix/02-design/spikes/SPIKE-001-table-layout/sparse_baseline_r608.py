"""Read-only complete carrier baseline for smallest E10 sparse-maintenance scope."""
import json,hashlib,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from mixed_change_queries_r230 import row_hash_sql
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent
p=B/'out/native/ashlar_current_file_ranges_r606/summary.json';inventory=json.loads(p.read_text());m=B/'out/file-overlap-model-r607.json';model=json.loads(m.read_text());assert model['source_sha256']==hashlib.sha256(p.read_bytes()).hexdigest()
scope=min(model['hash_ranges_64'],key=lambda x:x['overlapping_physical_bytes']);assert scope['range']==2 and scope['overlapping_physical_bytes']<=1000000000
t=inventory['table'];assert t['version']==10
files=[r for r in inventory['files'] if r[4]>=scope['lower'] and r[3]<scope['upper']];assert len(files)==20
O=B/'out/native/ashlar_sparse_baseline_r608';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=150,cancel_after=90);start=time.monotonic()
a={'state':'Reading complete selected-file baseline; no maintenance submitted','table':t,'scope':scope,'files':files,'source_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bounds':{'read_bytes':10000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':180},'qualification':'Complete20field baseline of entire overlapping files, including rows outside the narrow hash range. Live extrema are not Delta optimizer eligibility. No write/manifest/ACK or maintenance cost claim.'}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save()
try:
 d=c.sql('detail','DESCRIBE DETAIL '+t['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,d[0]))['id']==t['id']
 assert c.sql('head','DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0][0]=='10'
 literals=','.join("decode(unhex('"+r[0].encode().hex()+"'),'UTF-8')" for r in files)
 query=f"SELECT _metadata.file_path,count(*),count(DISTINCT struct(source_system,rel_type_id,id)),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM {t['table']} VERSION AS OF 10 WHERE _metadata.file_path IN ({literals}) GROUP BY _metadata.file_path ORDER BY _metadata.file_path"
 a['baseline']=c.sql('complete-selected-files',query);expected={r[0]:int(r[2]) for r in files};assert len(a['baseline'])==20 and {r[0]:int(r[1]) for r in a['baseline']}==expected and all(r[1]==r[2] and len(r[3])==64 for r in a['baseline'])
 assert c.sql('final-head','DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0][0]=='10'
 for i in range(20):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if i==19:raise
   time.sleep(1)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());assert not h[c.records[2]['statement_id']]['metrics'].get('result_from_cache');a['wall_s']=time.monotonic()-start;assert a['wall_s']<180;a['state']='Complete20field selected-file baseline and typed identity uniqueness measured at E10';save();(O/'live-statement.json').rename(O/'completed-last-statement.json');print(json.dumps({'state':a['state'],'rows':sum(int(r[1]) for r in a['baseline']),'costs':a['costs'],'wall_s':a['wall_s']}))
except Exception as e:a.update(state='Stopped; inspect same native handle, no replay',error=str(e));save();raise

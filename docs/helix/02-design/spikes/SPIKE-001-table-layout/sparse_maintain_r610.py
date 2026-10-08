"""One bounded E10 sparse OPTIMIZE, actual file-set parity, no publication advance."""
import json,time,hashlib
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent
source=B/'out/native/ashlar_sparse_baseline_r608/audited-summary.json';baseline=json.loads(source.read_text());assert baseline['state']=='Complete20field selected-file baseline and typed identity uniqueness measured at E10'
inventory_path=B/'out/native/ashlar_current_file_ranges_r606/summary.json';assert hashlib.sha256(inventory_path.read_bytes()).hexdigest()==baseline['source_sha256'];inv=json.loads(inventory_path.read_text());t=baseline['table'];scope=baseline['scope'];assert t['version']==10 and scope['overlapping_physical_bytes']<=1000000000
O=B/'out/native/ashlar_sparse_maintain_r610';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=180,cancel_after=90);start=time.monotonic()
a={'state':'Preflighting one sparse maintenance','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'table':t,'scope':scope,'bounds':{'read_bytes':25000000000,'write_remote_bytes':3000000000,'spill_to_disk_bytes':1000000000,'wall_s':600},'qualification':'Owned synthetic table only, no manifest/ACK/VACUUM/cleanup. Actual removed/added complete20field multisets checked bidirectionally. Common physical-file path/size/livecount/extrema custody and closed OPTIMIZE SID interval checked; not independent Delta-log deletion-vector action proof or production writer fencing. E10 publication remains pinned. No60s/p95 or1B admission.'}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def objs(label,sql):
 rows=c.sql(label,sql);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
def metrics():
 for j in range(20):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if j==19:raise
   time.sleep(1)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and time.monotonic()-start<600;return h
save()
try:
 a['before_detail']=objs('detail-before','DESCRIBE DETAIL '+t['table'])[0];assert a['before_detail']['id']==t['id'];a['before_schema']=objs('schema-before','DESCRIBE TABLE '+t['table']);head=objs('head-before','DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0];assert int(head['version'])==10
 predicate="lookup_hash>='"+scope['lower']+"' AND lookup_hash<'"+scope['upper']+"'";a['predicate']=predicate;metrics();a['state']='Submitting one guarded sparse FULL rewrite';save();ts=time.monotonic();c.sql('optimize','OPTIMIZE '+t['table']+' FULL WHERE '+predicate);a['optimize_s']=time.monotonic()-ts;a['optimize_statement_id']=c.records[-1]['statement_id'];save()
 history=objs('history-after','DESCRIBE HISTORY '+t['table']+' LIMIT 100');commits=[x for x in history if int(x['version'])>10];assert commits and all(x['operation']=='OPTIMIZE' and x['queryHistoryStatementId']==a['optimize_statement_id'] for x in commits);v=int(history[0]['version']);assert [int(x['version']) for x in commits]==list(range(v,10,-1));a['commits']=commits;a['after_version']=v;metrics()
 a['after_files']=c.sql('files-after',f"SELECT _metadata.file_path,max(_metadata.file_size),count(*),min(lookup_hash),max(lookup_hash) FROM {t['table']} VERSION AS OF {v} GROUP BY _metadata.file_path ORDER BY _metadata.file_path");old={r[0]:r for r in inv['files']};new={r[0]:r for r in a['after_files']};assert len(new)==len(a['after_files']) and sum(int(r[2]) for r in new.values())==39930000;common=set(old)&set(new);assert all(old[k]==new[k] for k in common);removed=sorted(set(old)-set(new));added=sorted(set(new)-set(old));a['removed_files']=[old[k] for k in removed];a['added_files']=[new[k] for k in added];assert sum(int(old[k][1]) for k in removed)<=2000000000 and sum(int(new[k][1]) for k in added)<=3000000000;save();metrics()
 def relation(version,paths):
  literals=','.join("decode(unhex('"+k.encode().hex()+"'),'UTF-8')" for k in paths)
  return 'SELECT '+','.join(FIELDS)+f" FROM {t['table']} VERSION AS OF {version} WHERE "+('_metadata.file_path IN ('+literals+')' if paths else 'false')
 before=relation(10,removed);after=relation(v,added)
 for label,left,right in [('removed-minus-added',before,after),('added-minus-removed',after,before)]:assert c.sql(label,'SELECT count(*) FROM (('+left+') EXCEPT ALL ('+right+'))')==[['0']];metrics()
 assert c.sql('maintenance-cdf',f"SELECT count(*) FROM table_changes('{t['table']}',11,{v})")==[['0']]
 a['after_detail']=objs('detail-after','DESCRIBE DETAIL '+t['table'])[0];assert a['after_detail']['id']==t['id'];assert objs('schema-after','DESCRIBE TABLE '+t['table'])==a['before_schema'];final=objs('final-head','DESCRIBE HISTORY '+t['table']+' LIMIT 100');assert [x for x in final if int(x['version'])>10]==commits
 h=metrics();assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records if r['label'] in ['removed-minus-added','added-minus-removed','files-after','maintenance-cdf']);a['wall_s']=time.monotonic()-start;a['optimize_metrics']=h[a['optimize_statement_id']]['metrics'];a['state']='Sparse maintenance complete with exact changed-file carrier parity and physical common-file custody';save();(O/'live-statement.json').rename(O/'completed-last-statement.json');print(json.dumps({k:a[k] for k in ['state','after_version','optimize_s','wall_s','costs']}))
except Exception as e:a.update(state='Stopped; inspect same native handles and actual commits, never replay',error=str(e));save();raise

"""One matched private full-field guard MERGE per file-target fixture. No replay."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from overlay_sql_r395 import FIELDS
from inline_guard_sql_r421 import guard_merge_relation
B=Path(__file__).resolve().parent

def main():
 paths={'reference':'out/native/scaled_fingerprint_merge_r497/summary.json','source':'out/native/scaled_fingerprint_r493/summary.json','larger':'out/native/file256_recover_r571/summary.json'};p={k:json.loads((B/v).read_text()) for k,v in paths.items()};original=p['reference']['table'];source=f"SELECT * FROM {p['source']['source']['table']} VERSION AS OF 0 WHERE lookup_hash<'{p['source']['upper_exclusive_lookup_hash']}'";O=B/'out/native/file_target_merge_r577';assert not O.exists();start=time.monotonic();c=Client(O,observation_timeout=150,cancel_after=90)
 a={'state':'matched full-guard file-target pilot running','sources':paths,'source_sha256':{k:hashlib.sha256((B/v).read_bytes()).hexdigest() for k,v in paths.items()},'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['file_target_merge_r577.py','inline_guard_sql_r421.py']},'source_relation':source,'fixtures':{},'bounds':{'read_bytes':30000000000,'write_remote_bytes':1000000000,'spill_to_disk_bytes':0,'wall_s':300},'qualification':'Private matched2.5M full-carrier fixtures, same6227 fourth-input changes and atomic full20field guards, one MERGE per layout. Different maintenance/file geometry/cache/order; no causal p95, full multi-role publication, production fence or1B/5B admission. Prior larger-file maintenance remains charged separately.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objects(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def metrics():
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=300;return h
 def parity(label,left,right,count,cdf=False):
  on=' AND '.join('b.'+f+'=v.'+f for f in ['lookup_hash','source_system','rel_type_id','id']);on+=' AND b._change_type=v._change_type' if cdf else ''
  ints={'rel_type_id','id','source_type','source_id','target_type','target_id','entity_version','source_position'};equal=' AND '.join(f'(b.{f}<=>v.{f})' if f in ints or f=='published_at' else f"(hex(encode(b.{f},'UTF-8'))<=>hex(encode(v.{f},'UTF-8')))" for f in FIELDS+('carrier_fingerprint',));rows=c.sql(label,f'SELECT count(*),count_if(b.id IS NULL OR v.id IS NULL OR NOT ({equal})) FROM {left} b FULL OUTER JOIN {right} v ON {on}');assert rows==[[str(count),'0']];a[label]=rows;metrics()
 save()
 try:
  d=objects('reference-detail','DESCRIBE DETAIL '+original)[0];assert d['id']==p['reference']['table_id'];a['reference_head']=objects('reference-head','DESCRIBE HISTORY '+original+' LIMIT 1')[0];assert int(a['reference_head']['version'])==3
  d=objects('larger-source-detail','DESCRIBE DETAIL '+p['larger']['table'])[0];assert d['id']==p['larger']['id'];a['larger_source_head']=objects('larger-source-head','DESCRIBE HISTORY '+p['larger']['table']+' LIMIT 1')[0];assert int(a['larger_source_head']['version'])==2
  a['source_counts']=c.sql('source-counts',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_delete),count_if(NOT is_delete) FROM ({source})');assert a['source_counts']==p['reference']['source_counts'];assert a['source_counts']==[['6227','6227','618','5609']];metrics()
  for family,parent,version,size in [('file64',original,2,67108864),('file256',p['larger']['table'],2,268435456)]:
   table=original.rsplit('.',1)[0]+'.'+family+'_merge_r577';f={'table':table,'clone_source':{'table':parent,'version':version},'target_bytes':size};a['fixtures'][family]=f
   f['clone_result']=objects('clone-'+family,f"CREATE TABLE {table} SHALLOW CLONE {parent} VERSION AS OF {version} TBLPROPERTIES ('delta.targetFileSize'='{size}','delta.enableChangeDataFeed'='true')");f['clone_statement_id']=c.records[-1]['statement_id'];assert int(f['clone_result'][0]['num_copied_files'])==0
   c.sql('disable-predictive-'+family,'ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION');f['before']=objects('detail-before-'+family,'DESCRIBE DETAIL '+table)[0];f['id']=f['before']['id'];f['schema']=objects('schema-'+family,'DESCRIBE TABLE '+table);f['initial_history']=objects('initial-history-'+family,'DESCRIBE HISTORY '+table+' LIMIT 100');assert len(f['initial_history'])==1 and int(f['initial_history'][0]['version'])==0 and f['initial_history'][0]['queryHistoryStatementId']==f['clone_statement_id'];assert json.loads(f['before']['properties'])['delta.targetFileSize']==str(size) and json.loads(f['before']['properties'])['delta.enableChangeDataFeed']=='true';metrics()
  assert a['fixtures']['file64']['schema']==a['fixtures']['file256']['schema']
  parity('complete-initial-pair',a['fixtures']['file64']['table']+' VERSION AS OF 0',a['fixtures']['file256']['table']+' VERSION AS OF 0',2498646)
  for family,f in a['fixtures'].items():
   table=f['table'];q=guard_merge_relation(table,source);f['merge_sql']=q;c.sql('merge-'+family,q);sid=c.records[-1]['statement_id'];f['merge_statement_id']=sid;f['history']=objects('post-history-'+family,'DESCRIBE HISTORY '+table+' LIMIT 100');assert len(f['history'])==2 and int(f['history'][0]['version'])==1 and f['history'][0]['queryHistoryStatementId']==sid;metrics()
   parity('complete-cdf-'+family,f"table_changes('{table}',1,1)",f"table_changes('{original}',3,3)",11836,True)
   parity('complete-result-'+family,table+' VERSION AS OF 1',original+' VERSION AS OF 3',2498028)
   f['counts']=c.sql('unique-counts-'+family,f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {table} VERSION AS OF 1');assert f['counts']==[['2498028','2498028']];f['after']=objects('detail-after-'+family,'DESCRIBE DETAIL '+table)[0];assert f['after']['id']==f['id'];assert objects('closing-history-'+family,'DESCRIBE HISTORY '+table+' LIMIT 100')==f['history'];metrics()
  assert objects('reference-closing-head','DESCRIBE HISTORY '+original+' LIMIT 1')[0]==a['reference_head'];assert objects('larger-source-closing-head','DESCRIBE HISTORY '+p['larger']['table']+' LIMIT 1')[0]==a['larger_source_head'];h=metrics()
  for f in a['fixtures'].values():f['merge_metrics']=h[f['merge_statement_id']]['metrics'];f['merge_caller_ms']=next(x['wall_ms'] for x in c.records if x['statement_id']==f['merge_statement_id'])
  a['state']='Matched64/256 full-guard mutations preserve all21fields/CDF and complete typed uniqueness';save();(O/'live-statement.json').rename(O/'completed-last-statement.json');print(json.dumps({'state':a['state'],'costs':a['costs'],'wall_s':a['wall_s'],'merges':{k:{'caller_ms':f['merge_caller_ms'],'metrics':f['merge_metrics']} for k,f in a['fixtures'].items()}},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles and exact fixture heads; no write replay',error=str(e));save();raise
if __name__=='__main__':main()

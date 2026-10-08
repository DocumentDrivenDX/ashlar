"""Read-only complete common-file field/live-row preservation at E10/E12."""
import json,hashlib,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from mixed_change_queries_r230 import row_hash_sql
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent
source=B/'out/native/ashlar_sparse_maintain_r610/audited-summary.json';s=json.loads(source.read_text());assert s['state']=='Sparse maintenance complete with exact changed-file carrier parity and physical common-file custody';t=s['table'];assert t['version']==10 and s['after_version']==12
inv=json.loads((B/'out/native/ashlar_current_file_ranges_r606/summary.json').read_text());old={r[0]:r for r in inv['files']};new={r[0]:r for r in s['after_files']};common=set(old)&set(new);assert len(common)==600 and all(old[k]==new[k] for k in common)
O=B/'out/native/ashlar_common_carriers_r618';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=200,cancel_after=120);start=time.monotonic();a={'state':'Reading complete common-file multisets','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'table':t,'versions':[10,12],'common_files':sorted(common),'expected_rows':sum(int(old[k][2]) for k in common),'checks':{},'bounds':{'read_bytes':100000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':600},'qualification':'Read-only full20field SHA256 multisets and typed identity uniqueness per common file at two actual Delta snapshots, with live deletion-vector filtering. Together with exact changed-file EXCEPT ALL covers all39.93M live carriers. Hash collision assumption remains; no independent Delta-log action proof/source fence/global schema-constraint/manifest or performance admission.'}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def metrics():
 for j in range(20):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if j==19:raise
   time.sleep(1)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and time.monotonic()-start<600;return h
save()
try:
 d=c.sql('detail','DESCRIBE DETAIL '+t['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,d[0]))['id']==t['id'];assert c.sql('head','DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0][0]=='12'
 for version,exclude in [(10,s['removed_files']),(12,s['added_files'])]:
  if version==12:assert a['costs']['read_bytes']+45000000000<=a['bounds']['read_bytes']
  literals=','.join("decode(unhex('"+r[0].encode().hex()+"'),'UTF-8')" for r in exclude)
  query=f"SELECT _metadata.file_path,count(*),count(DISTINCT struct(source_system,rel_type_id,id)),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM {t['table']} VERSION AS OF {version} WHERE _metadata.file_path NOT IN ({literals}) GROUP BY _metadata.file_path ORDER BY _metadata.file_path"
  rows=c.sql('common-'+str(version),query);assert len(rows)==600 and {r[0]:int(r[1]) for r in rows}=={k:int(old[k][2]) for k in common} and all(r[1]==r[2] and len(r[3])==64 for r in rows);a['checks'][str(version)]=rows;save();metrics()
 assert a['checks']['10']==a['checks']['12'];assert c.sql('final-head','DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0][0]=='12';h=metrics();assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records if r['label'].startswith('common-'));a['wall_s']=time.monotonic()-start;a['state']='All600 common-file complete carrier multisets and live typed identities preserved at E10/E12';save();(O/'live-statement.json').rename(O/'completed-last-statement.json');print(json.dumps({'state':a['state'],'rows':a['expected_rows'],'costs':a['costs'],'wall_s':a['wall_s']}))
except Exception as e:a.update(state='Stopped; inspect same submitted handle without replay',error=str(e));save();raise

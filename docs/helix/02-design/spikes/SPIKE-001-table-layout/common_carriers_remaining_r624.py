"""Read-only complete common-file field/live-row preservation at E10/E12."""
import json,hashlib,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from mixed_change_queries_r230 import row_hash_sql
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent
source=B/'out/native/ashlar_sparse_maintain_r610/audited-summary.json';s=json.loads(source.read_text());assert s['state']=='Sparse maintenance complete with exact changed-file carrier parity and physical common-file custody';t=s['table'];assert t['version']==10 and s['after_version']==12
inv=json.loads((B/'out/native/ashlar_current_file_ranges_r606/summary.json').read_text());old={r[0]:r for r in inv['files']};new={r[0]:r for r in s['after_files']};all_common=set(old)&set(new);assert len(all_common)==600;pilot_path=B/'out/native/ashlar_common_carriers_pilot_r621/audited-summary.json';pilot=json.loads(pilot_path.read_text());assert pilot['state']=='All20 pilot common-file complete carrier multisets and live typed identities preserved at E10/E12' and pilot['source_sha256']==hashlib.sha256(source.read_bytes()).hexdigest();done=set(pilot['common_files']);assert done==set(sorted(all_common)[:20]);remaining=sorted(all_common-done);assert len(remaining)==580;groups=[remaining[i:i+20] for i in range(0,580,20)];common=all_common;assert all(old[k]==new[k] for k in common)
O=B/'out/native/ashlar_common_carriers_remaining_r624';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=200,cancel_after=120);start=time.monotonic();a={'state':'Reading complete common-file multisets','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'table':t,'versions':[10,12],'common_files':sorted(common),'expected_rows':sum(int(old[k][2]) for k in common),'checks':{},'pilot_sha256':hashlib.sha256(pilot_path.read_bytes()).hexdigest(),'completed_groups':[],'bounds':{'read_bytes':85000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':1800},'combined_read_bound':125000000000,'prior_read_bytes':33424291972+pilot['costs']['read_bytes'],'qualification':'Read-only full20field SHA256 multisets and typed identity uniqueness per common file at two actual Delta snapshots, with live deletion-vector filtering. All600 common files covered by audited20-file pilot plus29disjoint20-file groups. Prior canceled33.424GB and pilot2.259GB remain charged under explicit125GBcombinedread ceiling; current remaining phase85GB/1800s. No byte-bound revision retroactively qualifies canceled whole query. Changed-file exact R611 proof combines to cover39.93M live carriers. Hash collision assumption remains; no independent Delta-log action proof/source fence/global schema-constraint/manifest or performance admission.'}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def metrics():
 for j in range(20):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if j==19:raise
   time.sleep(1)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+a['prior_read_bytes']<=a['combined_read_bound'];assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and time.monotonic()-start<1800;return h
save()
try:
 d=c.sql('detail','DESCRIBE DETAIL '+t['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,d[0]))['id']==t['id'];assert c.sql('head','DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0][0]=='12'
 a['checks']={'10':list(pilot['checks']['10']),'12':list(pilot['checks']['12'])}
 for index,paths in enumerate(groups):
  assert time.monotonic()-start<1800
  group={}
  for version in [10,12]:
   if a.get('costs'):assert a['costs']['read_bytes']+3000000000<=a['bounds']['read_bytes']
   literals=','.join("decode(unhex('"+k.encode().hex()+"'),'UTF-8')" for k in paths)
   query=f"SELECT _metadata.file_path,count(*),count(DISTINCT struct(source_system,rel_type_id,id)),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM {t['table']} VERSION AS OF {version} WHERE _metadata.file_path IN ({literals}) GROUP BY _metadata.file_path ORDER BY _metadata.file_path"
   rows=c.sql('common-'+str(version)+'-'+str(index),query);assert len(rows)==20 and {r[0]:int(r[1]) for r in rows}=={k:int(old[k][2]) for k in paths} and all(r[1]==r[2] and len(r[3])==64 for r in rows);group[str(version)]=rows;save();metrics()
  assert group['10']==group['12'];a['completed_groups'].append({'index':index,'files':paths,'rows':sum(int(r[1]) for r in group['10'])});a['checks']['10'].extend(group['10']);a['checks']['12'].extend(group['12']);save()
 for v in ['10','12']:a['checks'][v].sort(key=lambda r:r[0])
 assert a['checks']['10']==a['checks']['12'];assert c.sql('final-head','DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0][0]=='12';h=metrics();assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records if r['label'].startswith('common-'));a['wall_s']=time.monotonic()-start;a['state']='All600 common-file complete carrier multisets and live typed identities preserved at E10/E12';save();(O/'live-statement.json').rename(O/'completed-last-statement.json');print(json.dumps({'state':a['state'],'rows':a['expected_rows'],'costs':a['costs'],'wall_s':a['wall_s']}))
except Exception as e:a.update(state='Stopped; inspect same submitted handle without replay',error=str(e));save();raise

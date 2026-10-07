"""Read-only owned staging active/history retention-work proxy; no physical inventory claim."""
import json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_storage_history_r255';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_edges_r252/audited-summary.json').read_text());a={'state':'Reading owned active details and complete Delta histories','roles':{},'checks':{},'bounds':{'read_bytes':100000000,'write_bytes':0}};c=Client(O,observation_timeout=200,cancel_after=180)
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def decoded(v):return json.loads(v) if isinstance(v,str) else v or {}
save()
try:
 for role,t in prior['tables'].items():
  rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(cols,rows[0]));assert d['id']==t['id']
  rows=c.sql('history-'+role,'DESCRIBE HISTORY '+t['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];history=[dict(zip(cols,r)) for r in rows];versions=sorted(int(r['version']) for r in history);assert versions==list(range(versions[-1]+1)),role
  opts=[r for r in history if r['operation']=='OPTIMIZE'];opmetrics=[decoded(r.get('operationMetrics')) for r in opts];removed=sum(int(m.get('numRemovedBytes',0)) for m in opmetrics);added=sum(int(m.get('numAddedBytes',0)) for m in opmetrics);missing=[int(r['version']) for r,m in zip(opts,opmetrics) if 'numRemovedBytes' not in m or 'numAddedBytes' not in m];operations=sorted(set(r['operation'] for r in history));a['roles'][role]={'table':t['table'],'uuid':t['id'],'pinned_logical_version':t['version'],'observed_head':versions[-1],'active_bytes':int(d['sizeInBytes']),'active_files':int(d['numFiles']),'properties':decoded(d.get('properties')),'history':history,'operations':operations,'optimize_commits':len(opts),'reported_optimize_removed_bytes':removed,'reported_optimize_added_bytes':added,'missing_optimize_byte_versions':missing,'active_plus_reported_removed_proxy':int(d['sizeInBytes'])+removed};save()
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert a['costs']['read_bytes']<=100000000 and a['costs']['write_remote_bytes']==0
 a['totals']={k:sum(r[k] for r in a['roles'].values()) for k in ['active_bytes','active_files','optimize_commits','reported_optimize_removed_bytes','reported_optimize_added_bytes','active_plus_reported_removed_proxy']};a['state']='Active details and contiguous complete histories audited for five owned roles';a['qualification']='Metadata audit, not object-store file inventory or billing. Reported removed-file sums can approximate retained historical Parquet only if files remain, removals counted once and metrics complete; snapshots observed sequentially, log/checkpoint/DV/failed files and autonomous future work excluded. Missing metrics remain explicit. No VACUUM/expiry/maintenance/compute mutation or graph publication.';save();print(json.dumps({'state':a['state'],'totals':a['totals'],'missing':{k:v['missing_optimize_byte_versions'] for k,v in a['roles'].items()},'costs':a['costs']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles before admission',error=str(e));save();raise

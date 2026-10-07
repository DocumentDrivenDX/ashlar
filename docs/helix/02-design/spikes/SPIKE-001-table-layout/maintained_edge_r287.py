"""Private incremental OPTIMIZE experiment; never repoint published snapshot."""
import json,time,hashlib
from pathlib import Path
from normalized_apply_sql_r276 import pin,current_merge
from mixed_preservation_sql_r280 import unchanged_groups,changed_digest
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 paths={'publisher':'out/native/ashlar_mixed_publish_r281/audited-summary.json','points':'out/native/ashlar_growth_pruning_r282/audited-summary.json','inputs':'out/native/ashlar_normalized_delta_stage_r275/audited-summary.json','changed':'out/mixed-changed-oracle-r277.json','grandparent':'out/native/ashlar_scale_edges_r274/audited-summary.json'};sources={k:json.loads((B/p).read_text()) for k,p in paths.items()};parent=sources['publisher']['tables']['edge_current'];grandparent=sources['grandparent']['tables']['edge_current'];current=sources['inputs']['tables']['current_replacement'];raw=sources['inputs']['tables']['source_record'];expected=sources['changed']['roles']['edge_current'];budget=json.loads((B/'out/maintenance-budget-r287.json').read_text())
 assert all(hashlib.sha256((B/p).read_bytes()).hexdigest()==budget['source_sha256'][k] for k,p in paths.items())
 O=B/'out/native/ashlar_maintained_edge_r287';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=200,cancel_after=180);a={'state':'Preparing private post-change edge clone','parent':parent,'source_sha256':budget['source_sha256'],'bounds':budget['bounds'],'tables':{},'checks':{}}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def detail(table,label):
  rows=c.sql('detail-'+label,'DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(names,rows[0]))
 def history(table,label):
  rows=c.sql('history-'+label,'DESCRIBE HISTORY '+table+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(names,r)) for r in rows]
 def metrics(reserve=0):
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
  for k,v in a['costs'].items():assert v+(reserve if k=='read_bytes' else 0)<=a['bounds'][k],k
  assert time.monotonic()-start<a['bounds']['wall_s'];save();return h
 save()
 try:
  assert detail(parent['table'],'parent')['id']==parent['id'];assert detail(raw['table'],'input')['id']==raw['id'];assert detail(grandparent['table'],'grandparent')['id']==grandparent['id'];assert detail(current['table'],'current-input')['id']==current['id'];table='client_dev.ashlar_entropy_20261006_r86.maintained_edge_r287';c.sql('clone',f"CREATE TABLE {table} SHALLOW CLONE {pin(grandparent['table'],grandparent['version'])}");clone_sid=c.records[-1]['statement_id'];d=detail(table,'clone');a['tables']['edge_current']={'table':table,'id':d['id']};receipts=history(table,'clone');assert len([r for r in receipts if r.get('queryHistoryStatementId')==clone_sid])==1
  c.sql('disable-maintenance','ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION');settings=c.sql('maintenance-status','DESCRIBE TABLE EXTENDED '+table);assert [r[1] for r in settings if r[0]=='Predictive Optimization']==['DISABLE']
  metrics(10000000000);c.sql('reconstruct-qualified-changes',current_merge(table,raw['table'],raw['version'],current['table'],current['version']));replay_sid=c.records[-1]['statement_id'];found=[r for r in history(table,'reconstruct') if r.get('queryHistoryStatementId')==replay_sid];assert len(found)==1;replay_metrics=json.loads(found[0]['operationMetrics']);assert replay_metrics['numTargetRowsUpdated']=='90000' and replay_metrics['numTargetRowsDeleted']=='10000' and replay_metrics['numTargetRowsInserted']=='0';a['reconstruction_commit']=found[0];save()
  c.sql('target-size',"ALTER TABLE "+table+" SET TBLPROPERTIES ('delta.targetFileSize'='67108864')");d=detail(table,'configured');assert json.loads(d['properties'])['delta.targetFileSize']=='67108864';a['configured_detail']=d;head=int(history(table,'before-opt')[0]['version']);a['before_opt_version']=head;metrics(10000000000);opt_start=time.monotonic();a['state']='Running incremental OPTIMIZE on private clone';save();result=c.sql('optimize','OPTIMIZE '+table);sid=c.records[-1]['statement_id'];a['optimize_s']=time.monotonic()-opt_start;a['optimize_statement_id']=sid;a['optimize_result']=result;receipts=history(table,'optimize');matched=[r for r in receipts if r.get('queryHistoryStatementId')==sid];assert len(matched)==1,'Inspect OPTIMIZE receipt; no blind replay';version=int(matched[0]['version']);a['optimize_commit']=matched[0];a['tables']['edge_current']['version']=version;a['state']='Checking every maintained carrier before lookup comparison';save();metrics()
  assert c.sql('complete-count','SELECT count(*) FROM '+pin(table,version))==[['39990000']];groups=[]
  for first in [0,100,200,300]:
   metrics(10000000000);groups+=c.sql('unchanged-'+str(first),unchanged_groups(table,version,expected['fields'],raw['table'],raw['version'],first))
  assert groups==sources['publisher']['checks']['unchanged-edge_current']['groups'];a['checks']['unchanged']={'rows':39900000,'groups':groups};assert c.sql('changed',changed_digest(table,version,expected['fields'],raw['table'],raw['version']))==[['90000',expected['digest']]];a['checks']['changed']={'rows':90000,'digest':expected['digest']};a['active_detail']=detail(table,'final');assert a['active_detail']['id']==a['tables']['edge_current']['id'];h=metrics();assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records if r['label'].startswith('unchanged-') or r['label']=='changed');a['wall_s']=time.monotonic()-start;a['state']='Private maintained39.99M-edge snapshot preserves every carrier field';a['qualification']='One incremental OPTIMIZE with64MiB target on private grandparent clone plus exact qualified change replay (nested shallow cloning unsupported). Reconstruction time/cost included separately in receipts; full final equality binds the maintained result to published parent. Full unchanged/changed row SHA multisets and total count match qualified parent, under explicit collision-resistance assumption; all20fields including identity/endpoints/version/exact property and retained strings covered. Parent descriptor unchanged; maintained clone has no published graph vector. Target setting not actual file-size guarantee. No causal/service-tail/cold/concurrency/producer or billion admission; no VACUUM/retention/compute changes. Retained physical overlap not inventoried.';save();print(json.dumps({'state':a['state'],'optimize_s':a['optimize_s'],'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same native handles and clone commits; never blindly replay OPTIMIZE',error=str(e));save();raise
if __name__=='__main__':main()

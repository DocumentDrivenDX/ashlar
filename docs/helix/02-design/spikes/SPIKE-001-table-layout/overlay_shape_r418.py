"""Authorized private16MiB setting/OPTIMIZE; bind actual commits, no publication."""
import json,hashlib,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent
def main():
 pp=B/'out/overlay-file-shaping-plan-r414.json';plan=json.loads(pp.read_text())
 for k,p in plan['sources'].items():assert hashlib.sha256((B/p).read_bytes()).hexdigest()==plan['source_sha256'][k]
 O=B/'out/native/ashlar_overlay_shape_r418';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=120,cancel_after=90);table=plan['table']['table'];a={'state':'preflight','plan_sha256':hashlib.sha256(pp.read_bytes()).hexdigest(),'plan':'out/overlay-file-shaping-plan-r414.json','code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'bounds':plan['bounds'],'table':plan['table'],'old_version':1,'heads':{}}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objs(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 save()
 try:
  a['old_detail']=objs('old-detail','DESCRIBE DETAIL '+table)[0];assert a['old_detail']['id']==plan['table']['id'] and int(a['old_detail']['sizeInBytes'])==80370684
  a['old_history']=objs('old-history','DESCRIBE HISTORY '+table+' LIMIT 10');assert int(a['old_history'][0]['version'])==1
  a['old_schema']=c.sql('old-schema','DESCRIBE TABLE '+table)
  extended=c.sql('predictive-setting','DESCRIBE TABLE EXTENDED '+table);assert [r[1] for r in extended if r[0]=='Predictive Optimization']==['DISABLE']
  roles=json.loads((B/'out/native/ashlar_third_lc_publish_r391/summary.json').read_text())['tables']
  for role,t in roles.items():a['heads'][role]={'table':t['table'],'head':objs('head-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0]['version']}
  a['state']='applying target16MiB to private overlay';save();t=time.monotonic();c.sql('target16MiB',f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.targetFileSize'='16777216')");a['alter_statement_id']=c.records[-1]['statement_id'];c.sql('optimize-full','OPTIMIZE '+table+' FULL');a['optimize_statement_id']=c.records[-1]['statement_id'];a['maintenance_caller_s']=time.monotonic()-t
  a['new_history']=objs('new-history','DESCRIBE HISTORY '+table+' LIMIT 10');new=int(a['new_history'][0]['version']);assert new>1
  commits=[r for r in a['new_history'] if int(r['version'])>1];assert [int(r['version']) for r in commits]==list(range(new,1,-1)) and all(r['queryHistoryStatementId'] in [a['alter_statement_id'],a['optimize_statement_id']] for r in commits);assert any(r['queryHistoryStatementId']==a['alter_statement_id'] for r in commits) and any(r['queryHistoryStatementId']==a['optimize_statement_id'] for r in commits)
  a['new_version']=new;a['new_detail']=objs('new-detail','DESCRIBE DETAIL '+table)[0];assert a['new_detail']['id']==a['old_detail']['id'] and a['new_detail']['tableFeatures']==a['old_detail']['tableFeatures'];assert c.sql('new-schema','DESCRIBE TABLE '+table)==a['old_schema']
  for role,t in a['heads'].items():assert objs('final-head-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0]['version']==t['head']
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=plan['bounds'][k] for k,v in a['costs'].items());a['wall_s']=time.monotonic()-start;assert a['wall_s']<300;a['state']='Private overlay maintenance terminal; complete preservation/read qualification pending';save();print(json.dumps(a,indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles/actual history, no write replay',error=str(e));save();raise
if __name__=='__main__':main()

"""Repair control fixture only; reuses authoritative unchanged private pins."""
import copy,hashlib,json,time
from pathlib import Path
from third_changes_r378 import ThirdChanges
from normalized_input_sql_r264 import inline_query
from normalized_apply_sql_r276 import pin
from overlay_sql_r395 import FIELDS,KEY,lookup_relations
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 prior=B/'out/native/ashlar_overlay_controls_r396';a=json.loads((prior/'summary.json').read_text());assert a['state']=='Stopped; same native handles only; no blind replay' and a['error']=='Conflicting accepted carrier resolved arbitrarily';old=[json.loads(x) for x in (prior/'statements.jsonl').read_text().splitlines()];assert len(old)==16 and all(r['response']['status']['state']=='SUCCEEDED' for r in old) and old[-1]['label']=='conflicting-live'
 O=B/'out/native/ashlar_overlay_conflicts_r397';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=150,cancel_after=90);summary={'state':'Correcting literal field in conflict fixture','source_sha256':{n:hashlib.sha256((prior/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl']},'tables':a['tables'],'checks':{},'bounds':{'read_bytes':10000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':180},'qualification':'Read-only correction of prior internally misspelled fixture field. Both true full-carrier conflicts must native-fail, including the deletion winner. No table creation, mutation, prior-query replay, publication or performance claim.'}
 def save():(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
 def objs(label,q):
  rs=c.sql(label,q);return [dict(zip([x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']],r)) for r in rs]
 save()
 try:
  for role,t in a['tables'].items():assert objs('initial-history-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 10')==t['history'] and objs('initial-detail-'+role,'DESCRIBE DETAIL '+t['table'])[0]['id']==t['id']
  changes=ThirdChanges();x=changes.change(1);live=copy.deepcopy(x['after']);x=changes.change(0);deleted=copy.deepcopy(x['before']);r=x['raw'];deleted.update(entity_version='2',apply_batch_id='mixed-change/3',source_feed=r['source_feed'],source_epoch=r['source_epoch'],source_position=None,source_cursor_json=r['source_cursor_json'],source_delivery_id=r['delivery_id'])
  bt=a['tables']['base'];ot=a['tables']['overlay'];failed=[]
  for label,row,flag in [('conflicting-live',live,False),('conflicting-deleted',deleted,True)]:
   bad=copy.deepcopy(row);bad['props_json']='{"lexical":12300}';assert set(bad)==set(FIELDS) and bad['props_json']!=row['props_json'];q=inline_query('current_replacement',[json.dumps(bad,ensure_ascii=False,separators=(',',':'))]);extra='SELECT '+','.join(FIELDS)+','+str(flag).lower()+' AS is_deleted FROM ('+q+')';key={k:int(row[k]) if k in ['rel_type_id','id'] else row[k] for k in KEY+('lookup_hash',)};query=lookup_relations(pin(bt['table'],0),'(SELECT * FROM '+pin(ot['table'],0)+' UNION ALL '+extra+')',key)
   try:c.sql(label,query)
   except RuntimeError:
    rec=c.records[-1];assert rec['label']==label and rec['response']['status']['state']=='FAILED' and 'ASHLAR_OVERLAY_VERSION_CONFLICT' in json.dumps(rec['response']['status']);final=c.w.api_client.do('GET','/api/2.0/sql/statements/'+rec['statement_id']);assert final['status']['state']=='FAILED';failed.append(rec);summary['checks'][label]={'statement_id':rec['statement_id'],'expected_error':'ASHLAR_OVERLAY_VERSION_CONFLICT'};save()
   else:raise AssertionError('Real carrier conflict resolved arbitrarily')
  for i in range(20):
   native={q['query_id']:q for q in c.history()}
   if all(r['statement_id'] in native and native[r['statement_id']]['is_final'] and native[r['statement_id']]['status']=='FAILED' for r in failed):break
   if i==19:raise RuntimeError('Failed handles not native final')
   time.sleep(2)
  fn={r['statement_id']:native[r['statement_id']] for r in failed};(O/'failed-native-final.json').write_text(json.dumps(fn,indent=2)+'\n');c.records=[r for r in c.records if r not in failed]
  for role,t in a['tables'].items():assert objs('final-history-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 10')==t['history']
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  summary['costs']={k:sum(q['metrics'].get(k,0) for q in list(h.values())+list(fn.values())) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=summary['bounds'][k] for k,v in summary['costs'].items());summary['wall_s']=time.monotonic()-start;assert summary['wall_s']<180;summary['state']='Both true accepted-carrier conflicts native-fail with unchanged pinned tables';summary['successful_statements']=len(c.records);summary['failed_expected_statements']=len(failed);save();print(json.dumps({k:summary[k] for k in ['state','successful_statements','failed_expected_statements','wall_s','costs']},indent=2))
 except Exception as e:summary.update(state='Stopped; inspect same read-only handles',error=str(e));save();raise
if __name__=='__main__':main()

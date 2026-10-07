"""Small native Delta accepted-overlay semantics control; no latency admission."""
import hashlib,json,time,copy
from pathlib import Path
from third_changes_r378 import ThirdChanges
from normalized_input_sql_r264 import inline_query
from normalized_apply_sql_r276 import pin
from mixed_change_queries_r230 import row_hash
from overlay_sql_r395 import FIELDS,KEY,lookup,lookup_relations
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;F='client_dev.ashlar_entropy_20261006_r86'

def main():
 O=B/'out/native/ashlar_overlay_controls_r396';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=150,cancel_after=90);changes=ThirdChanges();xs=[changes.change(i) for i in range(3)]
 base=[copy.deepcopy(x['before']) for x in xs];cross=copy.deepcopy(base[1]);cross['rel_type_id']='999999';base.append(cross)
 update=copy.deepcopy(xs[1]['after']);delete=copy.deepcopy(xs[0]['before']);r=xs[0]['raw'];delete.update(entity_version='2',apply_batch_id='mixed-change/3',source_feed=r['source_feed'],source_epoch=r['source_epoch'],source_position=None,source_cursor_json=r['source_cursor_json'],source_delivery_id=r['delivery_id'])
 overlay=[{**update,'is_deleted':False},{**delete,'is_deleted':True},{**base[1],'is_deleted':False},{**update,'is_deleted':False}]
 a={'state':'Creating tiny private overlay control','bounds':{'read_bytes':10000000,'write_remote_bytes':1000000,'spill_to_disk_bytes':0,'wall_s':240,'statement_cancel_after_s':90},'tables':{},'checks':{},'qualification':'Four-row base and four-row accepted overlay only; deliberate identical hash across different typed keys. Exact accepted-carrier replay, stale winner, deletion and same-version full-carrier conflict controls, not producer no-op classification, publishing/fencing/history/compaction/latency/scale.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objects(label,q):
  rs=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rs]
 def relation(rows,is_overlay=False):
  parts=[]
  for row in rows:
   q=inline_query('current_replacement',[json.dumps({f:row[f] for f in FIELDS},ensure_ascii=False,separators=(',',':'))]);parts.append('SELECT '+','.join(FIELDS)+((','+str(row['is_deleted']).lower()+' AS is_deleted') if is_overlay else '')+' FROM ('+q+')')
  return ' UNION ALL '.join(parts)
 save()
 try:
  for role,rows in [('base',base),('overlay',overlay)]:
   table=F+'.overlay_control_'+role+'_r396';q=relation(rows,role=='overlay');c.sql('create-'+role,f"CREATE TABLE {table} USING DELTA CLUSTER BY (lookup_hash) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.appendOnly'='true') AS {q}");sid=c.records[-1]['statement_id'];d=objects('detail-'+role,'DESCRIBE DETAIL '+table)[0];h=objects('history-'+role,'DESCRIBE HISTORY '+table+' LIMIT 10');assert len(h)==1 and int(h[0]['version'])==0 and h[0]['queryHistoryStatementId']==sid
   a['tables'][role]={'table':table,'id':d['id'],'version':0,'create_statement_id':sid,'history':h,'detail':d};c.sql('disable-maintenance-'+role,'ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION');settings=c.sql('maintenance-'+role,'DESCRIBE TABLE EXTENDED '+table);assert [x[1] for x in settings if x[0]=='Predictive Optimization']==['DISABLE'];save()
  bt=a['tables']['base'];ot=a['tables']['overlay'];cases=[('updated',base[1],update,False),('deleted',base[0],delete,True),('unchanged',base[2],base[2],False),('hash-collision-other-type',cross,cross,False)]
  for label,keyrow,expected,deleted in cases:
   key={k:int(keyrow[k]) if k in ['rel_type_id','id'] else keyrow[k] for k in KEY+('lookup_hash',)};want=[['deleted' if deleted else 'live',row_hash(expected,FIELDS),row_hash({**expected,'is_deleted':deleted},FIELDS+('is_deleted',)),expected['entity_version']]];q=lookup(bt['table'],0,ot['table'],0,key);assert c.sql(label,q)==want;a['checks'][label]={'key':key,'expected':want,'sql':q};save()
  key={k:int(base[1][k]) if k in ['rel_type_id','id'] else base[1][k] for k in KEY+('lookup_hash',)};key['id']=9223372036854775807;assert c.sql('missing-key',lookup(bt['table'],0,ot['table'],0,key))==[['missing',None,None,None]];a['checks']['missing-key']={'key':key,'expected':[['missing',None,None,None]]}
  failed=[]
  for label,row,keyrow in [('conflicting-live',update,base[1]),('conflicting-deleted',delete,base[0])]:
   bad=copy.deepcopy(row);bad['properties_json']='{"lexical":12300}';bad['is_deleted']=label=='conflicting-deleted';key={k:int(keyrow[k]) if k in ['rel_type_id','id'] else keyrow[k] for k in KEY+('lookup_hash',)};q=lookup_relations(pin(bt['table'],0),'(SELECT * FROM '+pin(ot['table'],0)+' UNION ALL '+relation([bad],True)+')',key)
   try:c.sql(label,q)
   except RuntimeError:
    rec=c.records[-1];assert rec['label']==label and rec['response']['status']['state']=='FAILED' and 'ASHLAR_OVERLAY_VERSION_CONFLICT' in json.dumps(rec['response']['status']);final=c.w.api_client.do('GET','/api/2.0/sql/statements/'+rec['statement_id']);assert final['status']['state']=='FAILED';failed.append(rec);a['checks'][label]={'statement_id':rec['statement_id'],'state':'FAILED','expected_error':'ASHLAR_OVERLAY_VERSION_CONFLICT'};save()
   else:raise AssertionError('Conflicting accepted carrier resolved arbitrarily')
  all_records=list(c.records)
  for i in range(20):
   native={q['query_id']:q for q in c.history()}
   if all(rec['statement_id'] in native and native[rec['statement_id']]['is_final'] and native[rec['statement_id']]['status']=='FAILED' for rec in failed):break
   if i==19:raise RuntimeError('Failed controls not native final')
   time.sleep(2)
  failed_native={rec['statement_id']:native[rec['statement_id']] for rec in failed};(O/'failed-native-final.json').write_text(json.dumps(failed_native,indent=2)+'\n');c.records=[r for r in c.records if r not in failed]
  for role,t in a['tables'].items():assert objects('final-history-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 10')==t['history']
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in list(h.values())+list(failed_native.values())) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a['wall_s']=time.monotonic()-start;assert a['wall_s']<240;a['state']='Native accepted-overlay update/delete/replay/typed-key/conflict controls pass';a['successful_statements']=len(c.records);a['failed_expected_statements']=len(failed);a['fixture']={'base':base,'overlay':overlay};a['source_sha256']={n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['overlay_sql_r395.py','overlay_controls_r396.py','third_changes_r378.py','mixed_change_queries_r230.py']};save();print(json.dumps({k:a[k] for k in ['state','successful_statements','failed_expected_statements','wall_s','costs']},indent=2))
 except Exception as e:a.update(state='Stopped; same native handles only; no blind replay',error=str(e));save();raise
if __name__=='__main__':main()

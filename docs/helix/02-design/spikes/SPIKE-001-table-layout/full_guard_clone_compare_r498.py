"""Full-field guard on shallow clone of the exact r497 predecessor snapshot."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from overlay_sql_r395 import FIELDS
from inline_guard_sql_r421 import guard_merge_relation
B=Path(__file__).resolve().parent

def main():
 parent=B/'out/native/scaled_fingerprint_merge_r497/summary.json';a497=json.loads(parent.read_text());assert a497['state'].startswith('Private fingerprint MERGE passes')
 a493=json.loads((B/'out/native/scaled_fingerprint_r493/summary.json').read_text());original=a497['table'];table=original.rsplit('.',1)[0]+'.full_guard_clone_r498';source=f"SELECT * FROM {a493['source']['table']} VERSION AS OF 0 WHERE lookup_hash<'{a493['upper_exclusive_lookup_hash']}'"
 out=B/'out/native/full_guard_clone_compare_r498';assert not out.exists();c=Client(out,observation_timeout=140,cancel_after=90);start=time.monotonic()
 a={'state':'running full-field guard comparator','table':table,'clone_source':{'table':original,'id':a497['table_id'],'version':2},'source_sha256':hashlib.sha256(parent.read_bytes()).hexdigest(),'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['full_guard_clone_compare_r498.py','inline_guard_sql_r421.py']},'bounds':{'read_bytes':20000000000,'write_remote_bytes':1000000000,'spill_to_disk_bytes':1000000000,'wall_s':300},'qualification':'One full-field MERGE after fingerprint MERGE on same predecessor files/input via new private shallow clone. Full before/after carrier and CDF parity. Fixed order/clone/cache/compiler prevent causal timing admission. No production publication.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objects(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def audit():
  for i in range(30):
   try:h=collect_history(c.w,c.records,out/'shared-history.json');break
   except HistoryPending:
    if i==29:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in a['bounds'] if k!='wall_s'};a['wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=300
  return h
 def parity(label,left,right,expected,cdf=False):
  fields=FIELDS+('carrier_fingerprint',);on=' AND '.join('b.'+f+'=v.'+f for f in ['lookup_hash','source_system','rel_type_id','id']);on+= ' AND b._change_type=v._change_type' if cdf else ''
  ints={'rel_type_id','id','source_type','source_id','target_type','target_id','entity_version','source_position'}
  equal=' AND '.join(f'(b.{f}<=>v.{f})' if f in ints or f=='published_at' else f"(hex(encode(b.{f},'UTF-8'))<=>hex(encode(v.{f},'UTF-8')))" for f in fields)
  result=c.sql(label,f'SELECT count(*),count_if(b.id IS NULL OR v.id IS NULL OR NOT ({equal})) FROM {left} b FULL OUTER JOIN {right} v ON {on}');assert result==[[str(expected),'0']];a[label]=result;audit()
 save()
 try:
  assert objects('source-detail','DESCRIBE DETAIL '+original)[0]['id']==a497['table_id'];assert int(objects('source-head','DESCRIBE HISTORY '+original+' LIMIT 1')[0]['version'])==3
  result=objects('clone',f'CREATE TABLE {table} SHALLOW CLONE {original} VERSION AS OF 2');a['clone_result']=result;assert int(result[0]['num_copied_files'])==0 and int(result[0]['copied_files_size'])==0
  clone_sid=c.records[-1]['statement_id'];hist=objects('clone-history','DESCRIBE HISTORY '+table+' LIMIT 10');assert len(hist)==1 and int(hist[0]['version'])==0 and hist[0]['queryHistoryStatementId']==clone_sid;a['clone_statement_id']=clone_sid
  c.sql('disable-predictive','ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION');d=objects('clone-detail','DESCRIBE DETAIL '+table)[0];a['table_id']=d['id'];a['detail_before']=d;assert d['id']!=a497['table_id'];assert json.loads(d['properties'])['delta.enableChangeDataFeed']=='true';assert 'generatedColumns' in json.loads(d['tableFeatures']) and json.loads(d['clusteringColumns'])==['lookup_hash'];assert int(objects('clone-head','DESCRIBE HISTORY '+table+' LIMIT 1')[0]['version'])==0;audit()
  parity('complete-before-parity',table+' VERSION AS OF 0',original+' VERSION AS OF 2',2498646)
  c.sql('full-field-merge',guard_merge_relation(table,source));sid=c.records[-1]['statement_id'];after=objects('merge-head','DESCRIBE HISTORY '+table+' LIMIT 1')[0];assert int(after['version'])==1 and after['queryHistoryStatementId']==sid;a.update(merge_statement_id=sid,after_version=1);audit()
  parity('complete-cdf-parity',f"table_changes('{table}',1,1)",f"table_changes('{original}',3,3)",11836,cdf=True)
  parity('complete-result-parity',table+' VERSION AS OF 1',original+' VERSION AS OF 3',2498028)
  assert int(objects('original-head-after','DESCRIBE HISTORY '+original+' LIMIT 1')[0]['version'])==3
  h=audit();a['merge_metrics']=h[sid]['metrics'];assert not a['merge_metrics'].get('result_from_cache');a['merge_caller_ms']=next(r['wall_ms'] for r in c.records if r['statement_id']==sid);a['fingerprint_merge_caller_ms']=next(json.loads(l)['wall_ms'] for l in (B/'out/native/scaled_fingerprint_merge_r497/statements.jsonl').read_text().splitlines() if json.loads(l)['label']=='guarded-merge');a['fingerprint_merge_metrics']=a497['merge_metrics'];a['state']='Full-field MERGE comparator preserves complete before/after21 fields and11836 CDF rows';save();(out/'live-statement.json').rename(out/'completed-last-statement.json');print(json.dumps({k:a[k] for k in ['state','merge_caller_ms','merge_metrics','costs','wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles and clone version; no replay',error=str(e));save();raise
if __name__=='__main__':main()

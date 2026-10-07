"""Single guarded private MERGE plus atomic refusals and exact full-field CDF."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from overlay_sql_r395 import FIELDS
from fingerprint_guard_r496 import merge,qualified_expression
B=Path(__file__).resolve().parent

def main():
 pp=B/'out/native/scaled_fingerprint_r493/summary.json';p=json.loads(pp.read_text());accepted=B/'out/scaled-fingerprint-audit-r495.json';assert json.loads(accepted.read_text())['state'].startswith('Retained final handles')
 out=B/'out/native/scaled_fingerprint_merge_r497';assert not out.exists();start=time.monotonic();c=Client(out,observation_timeout=140,cancel_after=90);table=p['table'];source=f"SELECT * FROM {p['source']['table']} VERSION AS OF 0 WHERE lookup_hash<'{p['upper_exclusive_lookup_hash']}'"
 a={'state':'running private fingerprint MERGE','table':table,'table_id':p['table_id'],'source_sha256':{str(v.relative_to(B)):hashlib.sha256(v.read_bytes()).hexdigest() for v in [pp,accepted]},'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['scaled_fingerprint_merge_r497.py','fingerprint_guard_r496.py','generated_fingerprint_r491.py']},'bounds':{'read_bytes':20000000000,'write_remote_bytes':1000000000,'spill_to_disk_bytes':1000000000,'wall_s':300},'controls':[],'qualification':'Private2.498646M-row fixture; one accepted6227-row fourth-input update/delete MERGE; exact20-field CDF and generated digest. No canonical or multi-role publication, source ACK, general insert/replay/resurrection/fencing or external-reader admission.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objects(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def head(label):return objects(label,'DESCRIBE HISTORY '+table+' LIMIT 1')[0]
 def audit():
  for i in range(30):
   h=c.history()
   if len(h)==len(c.records) and all(q.get('is_final') for q in h):break
   time.sleep(1)
  assert len(h)==len(c.records) and all(q.get('is_final') for q in h)
  idx={q['query_id']:q for q in h}
  for r in c.records:
   q=idx[r['statement_id']];assert q['query_text']==r['sql'];assert q['status']==('FINISHED' if r['response']['status']['state']=='SUCCEEDED' else 'FAILED')
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h) for k in a['bounds'] if k!='wall_s'};a['wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=300
  return idx
 save()
 try:
  d=objects('detail-before','DESCRIBE DETAIL '+table)[0];assert d['id']==p['table_id'];assert int(head('head-before')['version'])==1
  sd=objects('source-detail','DESCRIBE DETAIL '+p['source']['table'])[0];assert sd['id']==p['source']['id']
  counts=c.sql('source-counts',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_delete),count_if(NOT is_delete) FROM ({source})');n,unique,deletes,updates=map(int,counts[0]);assert n==unique==6227 and updates+deletes==n;a['source_counts']=counts
  one='SELECT * FROM ('+source+') ORDER BY id LIMIT 1'
  negatives={'changed_property_token':'SELECT * EXCEPT(props_json),concat(props_json,\' \') AS props_json FROM ('+one+')','missing_predecessor':'SELECT * EXCEPT(id),cast(-1 AS BIGINT) AS id FROM ('+one+')'}
  for label,relation in negatives.items():
   try:c.sql(label,merge(table,relation))
   except RuntimeError:
    r=c.records[-1];assert r['response']['status']['state']=='FAILED' and 'ASHLAR_FINGERPRINT_' in json.dumps(r['response']['status'])
   else:raise AssertionError('Corrupt predecessor accepted')
   assert int(head(label+'-head')['version'])==1;a['controls'].append({'label':label,'atomic_refusal':True,'statement_id':r['statement_id']});audit()
  c.sql('enable-cdf',"ALTER TABLE "+table+" SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')");sid=c.records[-1]['statement_id'];baseline=head('cdf-head');assert baseline['queryHistoryStatementId']==sid and int(baseline['version'])==2;a['baseline_version']=2
  result=c.sql('guarded-merge',merge(table,source));sid=c.records[-1]['statement_id'];after=head('merge-head');assert after['queryHistoryStatementId']==sid and int(after['version'])==3;a.update(merge_statement_id=sid,after_version=3,merge_result=result);audit()
  before=','.join('s.'+f+' AS '+f for f in FIELDS);after_cols=','.join('s.after_'+f+' AS '+f for f in FIELDS)
  expected=f"SELECT {before},CASE WHEN s.is_delete THEN 'delete' ELSE 'update_preimage' END AS change_type FROM ({source}) s UNION ALL SELECT {after_cols},'update_postimage' AS change_type FROM ({source}) s WHERE NOT s.is_delete"
  on=' AND '.join('e.'+f+'=v.'+f for f in ['lookup_hash','source_system','rel_type_id','id'])+' AND e.change_type=v._change_type'
  ints={'rel_type_id','id','source_type','source_id','target_type','target_id','entity_version','source_position'}
  comparisons=[]
  for f in FIELDS:comparisons.append(f'(e.{f}<=>v.{f})' if f in ints or f=='published_at' else f"(hex(encode(e.{f},'UTF-8'))<=>hex(encode(v.{f},'UTF-8')))")
  full=' AND '.join(comparisons)
  cdf=f"table_changes('{table}',3,3)"
  result=c.sql('full-cdf-parity',f"SELECT count(*),count_if(e.id IS NULL OR v.id IS NULL OR NOT ({full})),count_if(v._change_type='delete'),count_if(v._change_type='update_preimage'),count_if(v._change_type='update_postimage'),count_if(NOT (v.carrier_fingerprint<=>{qualified_expression('v')})),count_if(v._commit_version<>3) FROM ({expected}) e FULL OUTER JOIN {cdf} v ON {on}")
  assert result==[[str(deletes+2*updates),'0',str(deletes),str(updates),str(updates),'0','0']];a['full_cdf_parity']=result;audit()
  result=c.sql('result-integrity',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(NOT (b.carrier_fingerprint<=>{qualified_expression("b")})) FROM {table} VERSION AS OF 3 b');expected_count=2498646-deletes;assert result==[[str(expected_count),str(expected_count),'0']];a['result_integrity']=result
  assert int(head('final-head')['version'])==3;a['detail_after']=objects('detail-after','DESCRIBE DETAIL '+table)[0];assert a['detail_after']['id']==p['table_id'];h=audit();a['merge_metrics']=h[sid]['metrics'];assert not a['merge_metrics'].get('result_from_cache');a['state']='Private fingerprint MERGE passes atomic refusals, exact20-field CDF and full result digest/key integrity';save();(out/'live-statement.json').rename(out/'completed-last-statement.json');print(json.dumps({k:a[k] for k in ['state','source_counts','full_cdf_parity','result_integrity','merge_metrics','costs','wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same native handles and private table head; no replay',error=str(e));save();raise
if __name__=='__main__':main()

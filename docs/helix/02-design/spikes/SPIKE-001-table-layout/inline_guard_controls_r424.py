"""Small native whole-statement rollback controls before large guarded ingest."""
import copy,json,hashlib,time
from pathlib import Path
from inline_guard_sql_r421 import guard_merge_relation
from normalized_input_sql_r264 import inline_query
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import row_hash,row_hash_sql,INTS
from third_changes_r378 import ThirdChanges
from persistent_sql import Client
B=Path(__file__).resolve().parent
def relation(changes):
 rows=[]
 for x in changes:
  before=inline_query('current_replacement',[json.dumps(x['before'],separators=(',',':'))]);after=inline_query('current_replacement',[json.dumps(x['after'] or x['before'],separators=(',',':'))])
  rows.append('SELECT '+','.join('b.'+f for f in FIELDS)+','+','.join('a.'+f+' AS after_'+f for f in FIELDS)+','+str(x['after'] is None).lower()+' AS is_delete FROM ('+before+') b CROSS JOIN ('+after+') a')
 return ' UNION ALL '.join(rows)
def digest(rows):return hashlib.sha256(''.join(sorted(row_hash(r,FIELDS) for r in rows)).encode()).hexdigest()
def main():
 O=B/'out/native/ashlar_inline_guard_controls_r424';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=90,cancel_after=60);table='client_dev.ashlar_entropy_20261006_r86.inline_guard_controls_r424';x=ThirdChanges();indices=[0,1,2,3,4,19];changes=[x.change(i) for i in indices];before=[x['before'] for x in changes];after=[x['after'] for x in changes if x['after'] is not None];a={'state':'running controls','table':table,'indices':indices,'fields':list(FIELDS),'before_digest':digest(before),'after_digest':digest(after),'controls':[],'bounds':{'wall_s':300,'read_bytes':100000000,'write_remote_bytes':10000000,'spill_to_disk_bytes':0},'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['inline_guard_controls_r424.py','inline_guard_sql_r421.py','third_changes_r378.py','normalized_input_sql_r264.py']},'qualification':'Synthetic update/delete-only inline full predecessor guard. Qualified unique normalized input and a private serialized writer required. No current-row insertion, replay/resurrection, production fence, real source ACK or freshness admission.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def sql(label,q):assert time.monotonic()-start<300;return c.sql(label,q)
 def verify(label,version,count,expected):
  assert sql(label+'-head','DESCRIBE HISTORY '+table+' LIMIT 1')[0][0]==str(version)
  q=f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM {table} VERSION AS OF {version}"
  assert sql(label+'-digest',q)==[[str(count),str(count),expected]]
 save()
 try:
  source=inline_query('current_replacement',[json.dumps(r,separators=(',',':')) for r in before]);sql('create',f"CREATE TABLE {table} USING DELTA CLUSTER BY (lookup_hash) TBLPROPERTIES ('delta.enableChangeDataFeed'='true') AS SELECT {','.join(FIELDS)} FROM ({source})");a['create_statement_id']=c.records[-1]['statement_id'];detail=sql('detail','DESCRIBE DETAIL '+table);cols=[v['name'] for v in c.records[-1]['response']['manifest']['schema']['columns']];a['table_id']=dict(zip(cols,detail[0]))['id'];sql('disable-predictive','ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION');verify('initial',0,6,a['before_digest'])
  def reject(label,changed,expected_codes):
   q=guard_merge_relation(table,relation(changed));failure=None
   try:sql(label,q)
   except RuntimeError as e:
    rec=c.records[-1];assert rec['sql']==q and rec['response']['status']['state']=='FAILED';error=json.dumps(rec['response']['status']);assert any(code in error for code in expected_codes);failure={'label':label,'statement_id':rec['statement_id'],'expected_codes':expected_codes,'native_error':rec['response']['status']}
   assert failure is not None,'Invalid source was accepted: '+label
   verify(label,0,6,a['before_digest']);a['controls'].append(failure);save()
  codes=['ASHLAR_INLINE_PREDECESSOR_MISMATCH','ASHLAR_INLINE_MISSING_PREDECESSOR']
  for field in FIELDS:
   bad=copy.deepcopy(changes);v=bad[-1]['before'][field];bad[-1]['before'][field]=str(int(v or '0')+1) if field in INTS else '2026-10-08T00:00:00Z' if field=='published_at' else ('altered' if v is None else v+'x');assert bad[-1]['before'][field]!=v;reject('mismatch-'+field,bad,codes)
  bad=copy.deepcopy(changes);bad[0]['before']['props_json']='{}';reject('deletion-before-mismatch',bad,codes)
  bad=copy.deepcopy(changes);bad[0]['before']['id']='999999999';reject('missing-deletion',bad,['ASHLAR_INLINE_MISSING_PREDECESSOR'])
  bad=copy.deepcopy(changes);bad[-1]['before']['id']='999999999';reject('missing-update',bad,['ASHLAR_INLINE_MISSING_PREDECESSOR'])
  bad=copy.deepcopy(changes);bad[-1]['after']['entity_version']='1';reject('stale-after-version',bad,codes)
  bad=copy.deepcopy(changes);bad[-1]['after']['id']='999999999';reject('changed-after-identity',bad,codes)
  bad=copy.deepcopy(changes);bad.append(copy.deepcopy(bad[-1]));reject('duplicate-update-source',bad,['DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE'])
  sql('clean-update-and-delete',guard_merge_relation(table,relation(changes)));a['clean_statement_id']=c.records[-1]['statement_id'];verify('clean',1,5,a['after_digest']);assert sql('old-pin-after-clean',f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM {table} VERSION AS OF 0")==[['6',a['before_digest']]]
  a['history']=sql('final-history','DESCRIBE HISTORY '+table+' LIMIT 10');assert len(a['history'])==2 and [r[0] for r in a['history']]==['1','0']
  for i in range(25):
   history=c.history();ids={r['statement_id'] for r in c.records};qs={q['query_id']:q for q in history}
   if ids==set(qs) and all(q.get('is_final') for q in qs.values()):break
   if i==24:raise RuntimeError('Native telemetry not final; inspect same handles')
   time.sleep(2)
  assert all(q['status'] in ['FINISHED','FAILED'] for q in qs.values());a['costs']={k:sum(q['metrics'].get(k,0) for q in qs.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a.update(state='Full20field mismatch and26atomic refusal controls pass; clean5updates/1delete and old pin exact',wall_s=time.monotonic()-start,native_final_statements=len(qs));assert a['wall_s']<300 and len(a['controls'])==26;save();print(json.dumps({k:a[k] for k in ['state','wall_s','costs','native_final_statements']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same native handles and actual private history; no write replay',error=str(e));save();raise
if __name__=='__main__':main()

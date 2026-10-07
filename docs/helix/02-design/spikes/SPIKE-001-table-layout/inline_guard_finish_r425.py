"""Continue after missing-deletion refusal used the other valid guard error code."""
import copy,json,time,hashlib
from pathlib import Path
from inline_guard_controls_r424 import relation,digest
from inline_guard_sql_r421 import guard_merge_relation
from third_changes_r378 import ThirdChanges
from mixed_change_queries_r230 import row_hash_sql
from overlay_sql_r395 import FIELDS
from persistent_sql import Client
B=Path(__file__).resolve().parent
def main():
 prior=B/'out/native/ashlar_inline_guard_controls_r424';old=json.loads((prior/'summary.json').read_text());rs=[json.loads(x) for x in (prior/'statements.jsonl').read_text().splitlines()];last=rs[-1];assert old['state'].startswith('Stopped;') and len(old['controls'])==21 and last['label']=='missing-deletion' and last['response']['status']['state']=='FAILED' and 'ASHLAR_INLINE_PREDECESSOR_MISMATCH' in json.dumps(last['response']['status'])
 O=B/'out/native/ashlar_inline_guard_finish_r425';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=90,cancel_after=60);start=time.monotonic();table=old['table'];changes=[ThirdChanges().change(i) for i in old['indices']];a={'state':'recovering refused missing-deletion, no replay','table':table,'controls':copy.deepcopy(old['controls'])+[{'label':'missing-deletion','statement_id':last['statement_id'],'expected_codes':['ASHLAR_INLINE_PREDECESSOR_MISMATCH','ASHLAR_INLINE_MISSING_PREDECESSOR'],'native_error':last['response']['status']}],'source_sha256':{n:hashlib.sha256((prior/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl']},'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['inline_guard_finish_r425.py','inline_guard_controls_r424.py','inline_guard_sql_r421.py']},'bounds':{'wall_s':180,'read_bytes':100000000,'write_remote_bytes':10000000,'spill_to_disk_bytes':0}}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def verify(label,version,count,expected):
  assert c.sql(label+'-head','DESCRIBE HISTORY '+table+' LIMIT 1')[0][0]==str(version)
  assert c.sql(label+'-digest',f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM {table} VERSION AS OF {version}")==[[str(count),str(count),expected]]
 save()
 try:
  # Verify authoritative unchanged head/carriers before any new MERGE. Do not
  # replay the already-final missing-deletion statement.
  verify('missing-deletion-recovery',0,6,old['before_digest'])
  def reject(label,bad,codes):
   q=guard_merge_relation(table,relation(bad));failed=None
   try:c.sql(label,q)
   except RuntimeError:
    rec=c.records[-1];assert rec['sql']==q and rec['response']['status']['state']=='FAILED';assert any(code in json.dumps(rec['response']['status']) for code in codes);failed={'label':label,'statement_id':rec['statement_id'],'expected_codes':codes,'native_error':rec['response']['status']}
   assert failed;verify(label,0,6,old['before_digest']);a['controls'].append(failed);save()
  codes=['ASHLAR_INLINE_PREDECESSOR_MISMATCH','ASHLAR_INLINE_MISSING_PREDECESSOR']
  bad=copy.deepcopy(changes);bad[-1]['before']['id']='999999999';reject('missing-update',bad,codes)
  bad=copy.deepcopy(changes);bad[-1]['after']['entity_version']='1';reject('stale-after-version',bad,codes)
  bad=copy.deepcopy(changes);bad[-1]['after']['id']='999999999';reject('changed-after-identity',bad,codes)
  bad=copy.deepcopy(changes);bad.append(copy.deepcopy(bad[-1]));reject('duplicate-update-source',bad,['DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE'])
  c.sql('clean-update-and-delete',guard_merge_relation(table,relation(changes)));a['clean_statement_id']=c.records[-1]['statement_id'];verify('clean',1,5,old['after_digest']);assert c.sql('old-pin-after-clean',f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM {table} VERSION AS OF 0")==[['6',old['before_digest']]]
  q=f"SELECT _change_type,count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM table_changes('{table}',1,1) GROUP BY _change_type";actual={r[0]:r[1:] for r in c.sql('complete-CDF',q)};expected={'delete':['1',digest([changes[0]['before']])],'update_preimage':['5',digest([x['before'] for x in changes if x['after'] is not None])],'update_postimage':['5',old['after_digest']]};assert actual==expected;a['cdf']=actual;a['history']=c.sql('final-history','DESCRIBE HISTORY '+table+' LIMIT 10');assert [r[0] for r in a['history']]==['1','0']
  original=Client(prior);original.records=rs
  for client in [original,c]:
   for i in range(25):
    hh=client.history();qs={q['query_id']:q for q in hh};ids={r['statement_id'] for r in client.records}
    if ids==set(qs) and all(q.get('is_final') for q in qs.values()):break
    if i==24:raise RuntimeError('Inspect same native telemetry handles')
    time.sleep(2)
   assert all(q['status'] in ['FINISHED','FAILED'] for q in qs.values())
  recovery=json.loads((B/'out/native/ashlar_inline_guard_recover_r423/summary.json').read_text());a['costs']={k:recovery['costs'][k]+sum(q['metrics'].get(k,0) for client in [original,c] for q in json.loads((client.out/'query-history.json').read_text())) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items())
  for directory,records in [(prior,rs),(O,c.records)]:
   live=json.loads((directory/'live-statement.json').read_text());assert live['statement_id']==records[-1]['statement_id'];(directory/'live-statement.json').rename(directory/'completed-last-statement.json')
  a.update(state='All20field/26atomic refusals and clean5update/1delete complete CDF pass',wall_s=time.monotonic()-start,native_corrected_original_statements=len(rs),native_continuation_statements=len(c.records),qualification='Missing predecessor can report either inline guard code under native evaluation; both abort. Source uniqueness and before/after qualification remain prerequisite. No write replay, source ACK or production fence.');assert len(a['controls'])==26 and a['wall_s']<180;save();print(json.dumps({k:a[k] for k in ['state','wall_s','costs']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles/actual history; no replay',error=str(e));save();raise
if __name__=='__main__':main()

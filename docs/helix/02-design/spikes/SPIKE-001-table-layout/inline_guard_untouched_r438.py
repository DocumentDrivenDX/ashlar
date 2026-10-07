"""Qualify untouched target rows before executing guard over40M canonical rows."""
import json,hashlib,time
from pathlib import Path
from inline_guard_controls_r424 import relation,digest
from inline_guard_sql_r421 import guard_merge_relation
from normalized_input_sql_r264 import inline_query
from overlay_sql_r395 import FIELDS
from third_changes_r378 import ThirdChanges
from mixed_change_queries_r230 import row_hash_sql
from persistent_sql import Client
B=Path(__file__).resolve().parent
def main():
 O=B/'out/native/ashlar_inline_guard_untouched_r438';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=60,cancel_after=30);table='client_dev.ashlar_entropy_20261006_r86.inline_guard_untouched_r438';changes=[ThirdChanges().change(i) for i in [0,1,2,3,4,19]];extra=ThirdChanges().change(31)['before'];before=[x['before'] for x in changes]+[extra];after=[x['after'] for x in changes if x['after'] is not None]+[extra];a={'state':'qualifying untouched target','table':table,'before_digest':digest(before),'after_digest':digest(after),'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['inline_guard_untouched_r438.py','inline_guard_sql_r421.py']}}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def check(version,count,expected):assert c.sql('digest-'+str(version),f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM {table} VERSION AS OF {version}")==[[str(count),expected]]
 save()
 try:
  source=inline_query('current_replacement',[json.dumps(r,separators=(',',':')) for r in before]);c.sql('create',f"CREATE TABLE {table} USING DELTA CLUSTER BY (lookup_hash) AS SELECT {','.join(FIELDS)} FROM ({source})");c.sql('disable-predictive','ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION');check(0,7,a['before_digest']);c.sql('guarded-change',guard_merge_relation(table,relation(changes)));a['merge_statement_id']=c.records[-1]['statement_id'];check(1,6,a['after_digest']);check(0,7,a['before_digest']);a['history']=c.sql('history','DESCRIBE HISTORY '+table+' LIMIT 10');assert [r[0] for r in a['history']]==['1','0'];a['state']='Guard preserves exact unmatched target and old pin;5updates/1delete pass'
 except Exception as e:a.update(state='Stopped; inspect same native handle/actual history before large mutation',error=str(e));save();raise
 finally:
  for i in range(20):
   hh=c.history();qs={q['query_id']:q for q in hh}
   if set(qs)=={r['statement_id'] for r in c.records} and all(q.get('is_final') for q in qs.values()):break
   time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in hh) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['wall_s']=time.monotonic()-start;save();print(json.dumps(a,indent=2))
if __name__=='__main__':main()

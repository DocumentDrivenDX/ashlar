"""Read-only server text observation for existing completed source CTAS handle."""
import json,hashlib,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_sixth_delta_stage_r533';r=next(x for x in map(json.loads,(p/'statements.jsonl').read_text().splitlines()) if x['label']=='create-source_record');out=B/'out/native/sixth_stage_text_recovery_r538';assert not out.exists();start=time.monotonic();c=Client(out,observation_timeout=100,cancel_after=60);a={'state':'observing full server-side statement text','source_statement_id':r['statement_id'],'submitted_sql_sha256':hashlib.sha256(r['sql'].encode()).hexdigest(),'records_sha256':hashlib.sha256((p/'statements.jsonl').read_bytes()).hexdigest(),'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'bounds':{'read_bytes':1000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':120}}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  a['rows']=c.sql('server-text',"SELECT statement_id,statement_text,execution_status FROM system.query.history WHERE statement_id='"+r['statement_id']+"' AND start_time>=current_timestamp()-INTERVAL 2 HOURS");a['matches_full_submitted_sql']=a['rows']==[[r['statement_id'],r['sql'],'FINISHED']]
  for i in range(20):
   try:h=collect_history(c.w,c.records,out/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['wall_s']=time.monotonic()-start;assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=120;a['state']='Full server text observed' if a['matches_full_submitted_sql'] else 'Observation terminal; full SQL not yet matched';save();(out/'live-statement.json').rename(out/'completed-last-statement.json');print(json.dumps({k:a[k] for k in ['state','matches_full_submitted_sql','costs','wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same observation handle, no CTAS replay',error=str(e));save();raise
if __name__=='__main__':main()

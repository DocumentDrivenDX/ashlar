"""Finish preservation after completed private maintenance; never replay OPTIMIZE."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from overlay_sql_r395 import FIELDS
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 P=B/'out/native/file256_pilot_r569/summary.json';a=json.loads(P.read_text());assert a['state']=='Stopped; inspect same submitted handles, no write replay' and 'concatenate tuple' in a['error'];a['stopped_source_sha256']=hashlib.sha256(P.read_bytes()).hexdigest();O=B/'out/native/file256_recover_r571';assert not O.exists();c=Client(O,observation_timeout=150,cancel_after=90);start=time.monotonic();table=a['table'];after=a['after_version'];a['recovery_code_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
 def objects(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  assert objects('initial-history','DESCRIBE HISTORY '+table+' LIMIT 100')==a['history']
  fields=tuple(FIELDS)+('carrier_fingerprint',);cols=','.join(fields);before=f'{table} VERSION AS OF 0';current=f'{table} VERSION AS OF {after}'
  a['parity']=c.sql('all21-multiset-parity',f'SELECT count(*) FROM ((SELECT {cols} FROM {before} EXCEPT ALL SELECT {cols} FROM {current}) UNION ALL (SELECT {cols} FROM {current} EXCEPT ALL SELECT {cols} FROM {before}))');assert a['parity']==[['0']]
  a['counts']=c.sql('typed-unique-count',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {current}');assert a['counts']==[['2498646','2498646']]
  a['after']=objects('after-detail','DESCRIBE DETAIL '+table)[0];assert a['after']['id']==a['id'];a['schema_after']=objects('after-schema','DESCRIBE TABLE '+table);assert a['schema_after']==a['schema_before'];a['closing_history']=objects('closing-history','DESCRIBE HISTORY '+table+' LIMIT 100');assert a['closing_history']==a['history']
  old=[json.loads(x) for x in (P.parent/'statements.jsonl').read_text().splitlines()];records=old+c.records
  for i in range(20):
   try:h=collect_history(c.w,records,O/'combined-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a['recovery_s']=time.monotonic()-start;assert a['recovery_s']<=180;a['original_stopped_wall_s']=a.pop('wall_s');a['whole_wall_s']=None;a['optimize_metrics']=h[a['optimize_statement_id']]['metrics'];a['state']='Completed private256MiB maintenance recovered with full21field parity and exact schema/typed uniqueness';save();(O/'live-statement.json').rename(O/'completed-last-statement.json');print(json.dumps({k:a[k] for k in ['state','counts','costs','recovery_s','optimize_metrics']},indent=2))
 except Exception as e:a.update(state='Recovery stopped; inspect same handles without writes',recovery_error=str(e));save();raise
if __name__=='__main__':main()

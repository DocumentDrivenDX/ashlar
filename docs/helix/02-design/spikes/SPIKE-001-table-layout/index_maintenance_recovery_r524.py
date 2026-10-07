"""Same-handle maintenance recovery: inspect two commits, never replay OPTIMIZE."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from index_carrier_spike_r513 import IFIELDS,KEY
B=Path(__file__).resolve().parent

def main():
 old=B/'out/native/index_maintenance_r522';source=old/'summary.json';p=json.loads(source.read_text());prior=list(map(json.loads,(old/'statements.jsonl').read_text().splitlines()));assert len(prior)==4 and all(x['response']['status']['state']=='SUCCEEDED' for x in prior);assert p['after_head']['version']=='3' and p['after_head']['queryHistoryStatementId']==p['optimize_statement_id'];out=B/'out/native/index_maintenance_recovery_r524';assert not out.exists();start=time.monotonic();c=Client(out,observation_timeout=100,cancel_after=60);table=p['table'];a={'state':'same-handle recovery and complete parity','table':table,'id':p['id'],'prior_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'before':p['before'],'optimize_statement_id':p['optimize_statement_id'],'bounds':{'read_bytes':5000000000,'write_remote_bytes':1000000000,'spill_to_disk_bytes':0,'recovery_wall_s':120},'qualification':'Original private OPTIMIZE finished successfully but asserted one commit; actual versions2/3 share exact native statement ID. No OPTIMIZE replay. Costs include original execution and new read-only proofs; stopped original whole wall unknown.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objects(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def audit():
  for i in range(20):
   try:h=collect_history(c.w,prior+c.records,out/'combined-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['recovery_wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['recovery_wall_s']<=120;return h
 save()
 try:
  audit();a['history']=objects('bind-commits','DESCRIBE HISTORY '+table+' LIMIT 3');assert [int(x['version']) for x in a['history']]==[3,2,1]
  assert all(x['operation']=='OPTIMIZE' and x['queryHistoryStatementId']==p['optimize_statement_id'] for x in a['history'][:2]);assert a['history'][0]==p['after_head'];a['after']=objects('after-detail','DESCRIBE DETAIL '+table)[0];assert a['after']['id']==a['id']
  on=' AND '.join('b.'+f+'=n.'+f for f in KEY);equal=' AND '.join(f'(b.{f}<=>n.{f})' for f in IFIELDS)
  a['parity']=c.sql('all11-parity',f'SELECT count(*),count_if(b.id IS NULL OR n.id IS NULL OR NOT ({equal})) FROM {table} VERSION AS OF 1 b FULL OUTER JOIN {table} VERSION AS OF 3 n ON {on}');assert a['parity']==[['2498646','0']]
  a['counts']=c.sql('unique-counts',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_deleted) FROM {table} VERSION AS OF 3');assert a['counts']==[['2498646','2498646','618']]
  a['cdf']=c.sql('logical-cdf',f"SELECT count(*) FROM table_changes('{table}',2,3)");assert a['cdf']==[['0']]
  a['closing_head']=objects('closing-head','DESCRIBE HISTORY '+table+' LIMIT 1')[0];assert a['closing_head']==a['history'][0];h=audit();a['optimize_metrics']=h[p['optimize_statement_id']]['metrics'];a['state']='Private index maintenance preserves all11 fields, typed uniqueness, deletions and empty logical CDF';save();(out/'live-statement.json').rename(out/'completed-last-statement.json');(old/'live-statement.json').rename(old/'completed-last-statement.json');print(json.dumps({k:a[k] for k in ['state','costs','recovery_wall_s','after']},indent=2))
 except Exception as e:a.update(state='Stopped; same handles only, no mutation replay',error=str(e));save();raise
if __name__=='__main__':main()

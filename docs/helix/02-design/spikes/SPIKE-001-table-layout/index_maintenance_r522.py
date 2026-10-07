"""Bounded OPTIMIZE of private narrow index with exhaustive version parity."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from index_carrier_spike_r513 import IFIELDS,KEY
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/index_carrier_spike_r513/summary.json';p=json.loads(source.read_text());out=B/'out/native/index_maintenance_r522';assert not out.exists();start=time.monotonic();c=Client(out,observation_timeout=100,cancel_after=60);table=p['index'];a={'state':'private index maintenance','table':table,'id':p['index_after']['id'],'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'bounds':{'read_bytes':5000000000,'write_remote_bytes':1000000000,'spill_to_disk_bytes':0,'wall_s':180},'qualification':'Only private2.498646M-row index; whole11-field parity before/after; no canonical maintenance, publication or warehouse resize.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objects(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def audit():
  for i in range(20):
   h=c.history()
   if len(h)==len(c.records) and all(q.get('is_final') for q in h):break
   time.sleep(1)
  assert len(h)==len(c.records) and all(q.get('is_final') and q['status']=='FINISHED' for q in h);a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h) for k in a['bounds'] if k!='wall_s'};a['wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=180;return {q['query_id']:q for q in h}
 save()
 try:
  a['before']=objects('before-detail','DESCRIBE DETAIL '+table)[0];a['before_head']=objects('before-head','DESCRIBE HISTORY '+table+' LIMIT 1')[0];assert a['before']['id']==a['id'] and int(a['before_head']['version'])==1;assert json.loads(a['before']['clusteringColumns'])==['lookup_hash'];assert json.loads(a['before']['properties'])['delta.targetFileSize']=='67108864';assert int(a['before']['sizeInBytes'])<250000000;audit()
  a['optimize_result']=objects('optimize','OPTIMIZE '+table);a['optimize_statement_id']=c.records[-1]['statement_id'];a['after_head']=objects('after-head','DESCRIBE HISTORY '+table+' LIMIT 1')[0];assert int(a['after_head']['version'])==2 and a['after_head']['queryHistoryStatementId']==a['optimize_statement_id'];audit()
  on=' AND '.join('b.'+f+'=n.'+f for f in KEY);equal=' AND '.join(f'(b.{f}<=>n.{f})' for f in IFIELDS)
  a['parity']=c.sql('all11-parity',f'SELECT count(*),count_if(b.id IS NULL OR n.id IS NULL OR NOT ({equal})) FROM {table} VERSION AS OF 1 b FULL OUTER JOIN {table} VERSION AS OF 2 n ON {on}');assert a['parity']==[['2498646','0']]
  a['counts']=c.sql('unique-counts',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_deleted) FROM {table} VERSION AS OF 2');assert a['counts']==[['2498646','2498646','618']]
  a['cdf']=c.sql('logical-cdf',f"SELECT count(*) FROM table_changes('{table}',2,2)");assert a['cdf']==[['0']]
  a['after']=objects('after-detail','DESCRIBE DETAIL '+table)[0];assert a['after']['id']==a['id'];a['closing_head']=objects('closing-head','DESCRIBE HISTORY '+table+' LIMIT 1')[0];assert a['closing_head']==a['after_head'];h=audit();a['optimize_metrics']=h[a['optimize_statement_id']]['metrics'];a['state']='Private index maintenance preserves all11 fields, typed uniqueness, deletions and empty logical CDF';save();(out/'live-statement.json').rename(out/'completed-last-statement.json');print(json.dumps({k:a[k] for k in ['state','optimize_result','costs','wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same submitted handles and versions; no replay',error=str(e));save();raise
if __name__=='__main__':main()

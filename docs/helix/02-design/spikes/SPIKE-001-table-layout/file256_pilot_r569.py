"""Private 256MiB target comparison; clone/maintenance with full parity."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from overlay_sql_r395 import FIELDS
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 P=B/'out/native/scaled_fingerprint_r493/summary.json';p=json.loads(P.read_text());O=B/'out/native/file256_pilot_r569';assert not O.exists();start=time.monotonic();c=Client(O,observation_timeout=150,cancel_after=90);base=p['table'];table='client_dev.ashlar_entropy_20261006_r86.file256_pilot_r569';version=p['copy_version'];assert version==1
 a={'state':'private larger-file pilot running','source':{'table':base,'version':version,'id':p['table_id']},'table':table,'source_sha256':hashlib.sha256(P.read_bytes()).hexdigest(),'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'bounds':{'read_bytes':20000000000,'write_remote_bytes':3000000000,'spill_to_disk_bytes':0,'wall_s':240},'qualification':'Private1/16 parent hash slice, about2.5M full20field carriers plus generated fingerprint. Clone preserves schema/features; generated-column interoperability is unqualified. Maintenance counted separately; no publisher, ACK or1B/5B admission.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objects(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def metrics():
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=240;return h
 save()
 try:
  d=objects('source-detail','DESCRIBE DETAIL '+base)[0];assert d['id']==p['table_id'];a['source_current_detail']=d
  c.sql('clone',f"CREATE TABLE {table} SHALLOW CLONE {base} VERSION AS OF {version} TBLPROPERTIES ('delta.targetFileSize'='268435456')");a['clone_statement_id']=c.records[-1]['statement_id'];a['clone_history']=objects('clone-history','DESCRIBE HISTORY '+table+' LIMIT 20');assert len(a['clone_history'])==1 and int(a['clone_history'][0]['version'])==0 and a['clone_history'][0]['queryHistoryStatementId']==a['clone_statement_id']
  a['before']=objects('before-detail','DESCRIBE DETAIL '+table)[0];assert json.loads(a['before']['properties'])['delta.targetFileSize']=='268435456' and int(a['before']['sizeInBytes'])<2500000000 and json.loads(a['before']['clusteringColumns'])==['lookup_hash'];a['id']=a['before']['id'];a['schema_before']=objects('before-schema','DESCRIBE TABLE '+table);metrics()
  a['optimize_result']=objects('optimize','OPTIMIZE '+table+' FULL');a['optimize_statement_id']=c.records[-1]['statement_id'];a['history']=objects('after-history','DESCRIBE HISTORY '+table+' LIMIT 100');new=[x for x in a['history'] if int(x['version'])>0];assert new and all(x['queryHistoryStatementId']==a['optimize_statement_id'] for x in new);versions=sorted(int(x['version']) for x in new);assert versions==list(range(1,max(versions)+1));after=max(versions);a['after_version']=after;metrics()
  fields=FIELDS+['carrier_fingerprint'];cols=','.join(fields);before=f'{table} VERSION AS OF 0';current=f'{table} VERSION AS OF {after}'
  a['parity']=c.sql('all21-multiset-parity',f'SELECT count(*) FROM ((SELECT {cols} FROM {before} EXCEPT ALL SELECT {cols} FROM {current}) UNION ALL (SELECT {cols} FROM {current} EXCEPT ALL SELECT {cols} FROM {before}))');assert a['parity']==[['0']];metrics()
  a['counts']=c.sql('typed-unique-count',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {current}');assert a['counts']==p['fixture_integrity'][0][:2] or a['counts']==[p['fixture_integrity'][0][:2]]
  a['after']=objects('after-detail','DESCRIBE DETAIL '+table)[0];assert a['after']['id']==a['id'];a['schema_after']=objects('after-schema','DESCRIBE TABLE '+table);assert a['schema_after']==a['schema_before'];a['closing_history']=objects('closing-history','DESCRIBE HISTORY '+table+' LIMIT 100');assert a['closing_history']==a['history'];h=metrics();a['optimize_metrics']=h[a['optimize_statement_id']]['metrics'];a['state']='Private256MiB maintenance preserves complete21field multisets, schema and unique typed keys';save();(O/'live-statement.json').rename(O/'completed-last-statement.json');print(json.dumps({k:a[k] for k in ['state','counts','before','after','costs','wall_s','optimize_metrics']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same submitted handles, no write replay',error=str(e));save();raise
if __name__=='__main__':main()

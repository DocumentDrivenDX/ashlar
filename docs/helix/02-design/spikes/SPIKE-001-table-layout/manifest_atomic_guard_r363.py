"""Native immutable-descriptor guard and mixed-insert rollback on private clone."""
import json,time,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent

def main():
 O=B/'out/native/ashlar_manifest_atomic_guard_r363';assert not O.exists(),'No blind replay'
 source=B/'out/native/ashlar_maintenance_manifest_r360/audited-summary.json';a=json.loads(source.read_text());m=json.loads((B/'out/native/ashlar_maintenance_manifest_inventory_r362/audited-summary.json').read_text());T='client_dev.ashlar_entropy_20261006_r86.manifest_atomic_guard_r363';c=BoundedReads(O,socket_timeout=60);start=time.monotonic();out={'state':'running','table':T,'parent_table':m['table'],'parent_uuid':m['uuid'],'parent_version':3,'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'bounds':{'read_bytes':10000000,'write_remote_bytes':10000000,'spill_to_disk_bytes':0,'wall_s':300},'controls':[]}
 def save():(O/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
 def sql(label,q,params=None):assert time.monotonic()-start<300;return c.sql(label,q,parameters=params)
 def history(label):
  rows=sql(label,'DESCRIBE HISTORY '+T);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 save()
 try:
  sql('timeout','SET STATEMENT_TIMEOUT=30');sql('clone',f'CREATE TABLE {T} SHALLOW CLONE {m["table"]} VERSION AS OF 3');rows=sql('detail','DESCRIBE DETAIL '+T);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];out['detail']=dict(zip(cols,rows[0]));out['before_history']=history('before-history')
  expected=sorted([[r['descriptor']['id'],r['canonical_json'],r['sha256']] for r in a['receipts'][:2]])
  assert sql('baseline',f'SELECT * FROM {T} VERSION AS OF 0 ORDER BY publication_id')==expected
  child=a['receipts'][1];params={'id':child['descriptor']['id'],'text':child['canonical_json'],'digest':child['sha256']}
  source_sql='SELECT :id publication_id,:text descriptor_json,:digest descriptor_sha256'
  def merge(src):return f"MERGE INTO {T} t USING ({src}) s ON t.publication_id=s.publication_id WHEN MATCHED AND (t.descriptor_json<>s.descriptor_json OR t.descriptor_sha256<>s.descriptor_sha256) THEN UPDATE SET descriptor_json=CAST(raise_error('ASHLAR_PUBLICATION_ID_CONFLICT') AS STRING) WHEN NOT MATCHED THEN INSERT (publication_id,descriptor_json,descriptor_sha256) VALUES(s.publication_id,s.descriptor_json,s.descriptor_sha256)"
  sql('exact-replay',merge(source_sql),params);assert sql('replay-readback',f'SELECT * FROM {T} ORDER BY publication_id')==expected;out['replay_history']=history('replay-history')
  changed=json.loads(child['canonical_json']);changed['progress']['synthetic-fixture']='illegal-advance';text=json.dumps(changed,sort_keys=True,separators=(',',':'));bad=dict(params,text=text,digest=hashlib.sha256(text.encode()).hexdigest())
  for label,p in [('conflict-content',bad),('conflict-digest',dict(params,digest='0'*64))]:
   # A novel row in the same MERGE must roll back with the conflicting match.
   src=source_sql+" UNION ALL SELECT 'rollback-sentinel','{}',sha2('{}',256)"
   try:sql(label,merge(src),p)
   except RuntimeError:
    r=c.records[-1];assert r['label']==label and r['response']['status']['state']=='FAILED' and 'ASHLAR_PUBLICATION_ID_CONFLICT' in r['response']['status']['error']['message'];out['controls'].append({'label':label,'statement_id':r['statement_id'],'result':'native failure; verify rollback'})
   else:raise AssertionError('Conflict accepted')
   assert sql(label+'-readback',f'SELECT * FROM {T} ORDER BY publication_id')==expected;ledger=history(label+'-history');assert ledger[0]['version']==out['replay_history'][0]['version'];out['controls'][-1].update(result='native refusal and whole-MERGE rollback',head_version=int(ledger[0]['version']));save()
  out['after_history']=ledger;c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   h=c.history();ids={r['statement_id'] for r in c.records};native={q['query_id']:q for q in h};
   if ids==set(native) and all(q.get('is_final') for q in native.values()):break
   if i==19:raise RuntimeError('Native metrics pending; inspect same IDs')
   time.sleep(2)
  for r in c.records:
   q=native[r['statement_id']];assert q['query_text']=='/* ashlar '+O.name+' '+r['label']+' */ '+r['sql'];assert q['status']==('FAILED' if r['label'] in ['conflict-content','conflict-digest'] else 'FINISHED')
  out['costs']={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in out['bounds'] if k!='wall_s'};out['wall_s']=time.monotonic()-start;assert all(v<=out['bounds'][k] for k,v in out['costs'].items()) and out['wall_s']<300
  out['state']='Native exact replay and two conflicting mixed-MERGE rollback controls pass';out['qualification']='Private two-row serial clone. Guard executes inside MERGE and failures preserve entire table/head. No concurrent new-ID uniqueness or authority fence, full-role graph publication, retention, freshness or scale qualification.';save();print(json.dumps({'state':out['state'],'costs':out['costs'],'wall_s':out['wall_s']}))
 except Exception as e:out.update(state='Stopped; inspect same handles',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()

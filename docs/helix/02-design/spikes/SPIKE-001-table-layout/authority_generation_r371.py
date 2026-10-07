"""Private native monotonic-generation refusal and two-contender handover."""
import concurrent.futures,hashlib,json,threading,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/ashlar_full_vector_authority_r369/audited-summary.json';a=json.loads(source.read_text());O=B/'out/native/ashlar_authority_generation_r371';assert not O.exists(),'No blind replay';T='client_dev.ashlar_entropy_20261006_r86.authority_generation_r371';c=BoundedReads(O,socket_timeout=60);start=time.monotonic();out={'state':'running','table':T,'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'parent':{'table':a['table'],'uuid':a['detail']['id'],'version':4},'controls':[],'bounds':{'read_bytes':20000000,'write_remote_bytes':20000000,'spill_to_disk_bytes':0,'wall_s':300}}
 def save():(O/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
 def sql(label,q,params=None):assert time.monotonic()-start<300;return c.sql(label,q,parameters=params)
 def ledger(label):
  rows=sql(label,'DESCRIBE HISTORY '+T);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def merge(publication=False):
  src="SELECT 'authority' row_key,'authority' kind,:owner owner,CAST(:next_token AS BIGINT) token,CAST(NULL AS STRING) descriptor_json,CAST(NULL AS STRING) descriptor_sha256,:expected_owner expected_owner,CAST(:expected_token AS BIGINT) expected_token"
  if publication:src+=" UNION ALL SELECT :id,'publication',:owner,CAST(:next_token AS BIGINT),:text,:digest,:expected_owner,CAST(:expected_token AS BIGINT)"
  return f"MERGE INTO {T} t USING ({src}) s ON t.row_key=s.row_key WHEN MATCHED AND s.kind='authority' THEN UPDATE SET token=CASE WHEN t.kind='authority' AND t.owner=s.expected_owner AND t.token=s.expected_token AND s.token=t.token+1 AND s.token>t.token THEN s.token ELSE CAST(raise_error('ASHLAR_AUTHORITY_GUARD') AS BIGINT) END,owner=s.owner WHEN MATCHED AND s.kind='publication' AND (t.kind<>'publication' OR t.descriptor_json<>s.descriptor_json OR t.descriptor_sha256<>s.descriptor_sha256) THEN UPDATE SET descriptor_json=CAST(raise_error('ASHLAR_PUBLICATION_ID_CONFLICT') AS STRING) WHEN NOT MATCHED AND s.kind='publication' THEN INSERT (row_key,kind,owner,token,descriptor_json,descriptor_sha256) VALUES(s.row_key,s.kind,s.owner,s.token,s.descriptor_json,s.descriptor_sha256)"
 save()
 try:
  sql('timeout','SET STATEMENT_TIMEOUT=30');rows=sql('parent-detail','DESCRIBE DETAIL '+a['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,rows[0]))['id']==a['detail']['id'];sql('clone',f'CREATE TABLE {T} SHALLOW CLONE {a["table"]} VERSION AS OF 4');rows=sql('detail','DESCRIBE DETAIL '+T);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];out['detail']=dict(zip(cols,rows[0]));out['baseline']=sql('baseline',f'SELECT * FROM {T} ORDER BY row_key');assert out['baseline']==a['final_rows'];out['before_history']=ledger('before-history')
  text=json.dumps(a['accepted_b']['descriptor'],sort_keys=True,separators=(',',':'))
  for label,next_token in [('regressing','2'),('skipped','5')]:
   params={'owner':'writer-b','expected_owner':'writer-b','expected_token':'3','next_token':next_token,'id':'r371-invalid-'+label,'text':text,'digest':hashlib.sha256(text.encode()).hexdigest()}
   try:sql(label,merge(True),params)
   except RuntimeError:
    r=c.records[-1];assert r['response']['status']['state']=='FAILED' and 'ASHLAR_AUTHORITY_GUARD' in r['response']['status']['error']['message'];out['controls'].append({'label':label,'statement_id':r['statement_id'],'result':'native guard refusal'})
   else:raise AssertionError('Invalid generation accepted')
   assert sql(label+'-readback',f'SELECT * FROM {T} ORDER BY row_key')==out['baseline'];assert ledger(label+'-history')==out['before_history'];save()
  clients=[BoundedReads(O/('lane-'+str(i)),socket_timeout=60) for i in range(2)];barrier=threading.Barrier(2,timeout=30)
  def takeover(i):
   client=clients[i];params={'owner':'writer-'+str(i),'expected_owner':'writer-b','expected_token':'3','next_token':'4'}
   try:barrier.wait();client.sql('takeover',merge(),parameters=params);state='SUCCEEDED'
   except RuntimeError:
    r=client.records[-1];assert r['response']['status']['state']=='FAILED';state='FAILED'
   finally:client.close()
   r=client.records[-1];return {'lane':client.out.name,'state':state,'parameters':params,'statement_id':r['statement_id'],'error':r['response']['status'].get('error')}
  with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:out['contenders']=list(pool.map(takeover,range(2)))
  for client in clients:c.records.extend(client.records)
  assert sorted(w['state'] for w in out['contenders'])==['FAILED','SUCCEEDED'];winner=next(w for w in out['contenders'] if w['state']=='SUCCEEDED');out['final_rows']=sql('final-rows',f'SELECT * FROM {T} ORDER BY row_key');assert out['final_rows'][0][:4]==['authority','authority',winner['parameters']['owner'],'4'] and out['final_rows'][1:]==out['baseline'][1:];out['final_history']=ledger('final-history');save();c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   h=c.history();native={q['query_id']:q for q in h}
   if set(native)=={r['statement_id'] for r in c.records} and all(q.get('is_final') for q in native.values()):break
   if i==19:raise RuntimeError('Final native history pending; inspect same IDs')
   time.sleep(2)
  q0,q1=[native[w['statement_id']] for w in out['contenders']];out['native_statement_overlap_ms']=max(0,min(q0['execution_end_time_ms'],q1['execution_end_time_ms'])-max(q0['query_start_time_ms'],q1['query_start_time_ms']));out['costs']={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in out['bounds'] if k!='wall_s'};out['wall_s']=time.monotonic()-start;assert all(v<=out['bounds'][k] for k,v in out['costs'].items()) and out['wall_s']<300;out['state']='Two invalid generations roll back; one concurrent handover wins';out['qualification']='Seeded unique authority row in private control table; two negative samples and one two-contender race. Does not fence separate role writes, enforce source authority/access policy, missing authority behavior, arbitrary API/descriptor-token validation, crash retention or performance/scale.';(O/'all-statements.json').write_text(json.dumps(c.records,indent=2)+'\n');save();print(json.dumps({'state':out['state'],'native_statement_overlap_ms':out['native_statement_overlap_ms'],'costs':out['costs'],'wall_s':out['wall_s']}))
 except Exception as e:out.update(state='Stopped; inspect same handles',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()

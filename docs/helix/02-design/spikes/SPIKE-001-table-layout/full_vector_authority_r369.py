"""Atomic private authority row plus full-vector reference descriptor append."""
import hashlib,json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent

def main():
 O=B/'out/native/ashlar_full_vector_authority_r369';assert not O.exists(),'Inspect same handles; no replay';source=B/'out/native/ashlar_incremental_publish_r332/audited-summary.json';a=json.loads(source.read_text());old=[json.loads(x) for x in (source.parent/'statements.jsonl').read_text().splitlines()];expected=next(r for r in old if r['label']=='descriptor-readback')['response']['result']['data_array'][0];T='client_dev.ashlar_entropy_20261006_r86.full_vector_authority_r369';c=BoundedReads(O,socket_timeout=60);start=time.monotonic();out={'state':'running','table':T,'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'source_manifest':a['manifest'],'role_pins':a['tables'],'controls':[],'bounds':{'read_bytes':2000000000,'write_remote_bytes':10000000,'spill_to_disk_bytes':0,'wall_s':300}}
 def save():(O/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
 def sql(label,q,params=None):assert time.monotonic()-start<300;return c.sql(label,q,parameters=params)
 def detail(label,table):
  rows=sql(label,'DESCRIBE DETAIL '+table);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(cols,rows[0]))
 def ledger(label):
  rows=sql(label,'DESCRIBE HISTORY '+T);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 save()
 try:
  sql('timeout','SET STATEMENT_TIMEOUT=30');assert detail('source-manifest-detail',a['manifest']['table'])['id']==a['manifest']['id'];cols='publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json,CAST(recorded_at AS STRING) AS recorded_at';original=sql('source-descriptor',f'SELECT {cols} FROM {a["manifest"]["table"]}')[0];assert original[:6]==expected;out['original_descriptor']=original
  for role,t in a['tables'].items():
   assert detail('detail-'+role,t['table'])['id']==t['id'];assert sql('available-'+role,f'SELECT count(*) FROM (SELECT 1 FROM {t["table"]} VERSION AS OF {t["version"]} LIMIT 1)')==[['1']]
  sql('create',f'CREATE TABLE {T} (row_key STRING NOT NULL,kind STRING NOT NULL,owner STRING,token BIGINT,descriptor_json STRING,descriptor_sha256 STRING) USING DELTA');sql('seed',f"INSERT INTO {T} (row_key,kind,owner,token,descriptor_json,descriptor_sha256) VALUES ('authority','authority','writer-a',0,NULL,NULL)");out['detail']=detail('reference-detail',T)
  def descriptor(id,owner,token):return {'id':id,'kind':'full-vector-reference','authority_owner':owner,'authority_token':token,'source_descriptor':dict(zip(['publication_id','profile_version','table_versions_json','source_progress_json','schema_revisions_json','validation_report_json','recorded_at'],original)),'role_pins':a['tables'],'scope':'immutable reference only; source roles not mutated or fenced'}
  def apply(label,owner,expected_owner,expected_token,next_token,desc=None):
   params={'owner':owner,'expected_owner':expected_owner,'expected_token':str(expected_token),'next_token':str(next_token)};src="SELECT 'authority' row_key,'authority' kind,:owner owner,CAST(:next_token AS BIGINT) token,CAST(NULL AS STRING) descriptor_json,CAST(NULL AS STRING) descriptor_sha256,:expected_owner expected_owner,CAST(:expected_token AS BIGINT) expected_token"
   if desc is not None:
    text=json.dumps(desc,sort_keys=True,separators=(',',':'));params.update(id=desc['id'],text=text,digest=hashlib.sha256(text.encode()).hexdigest());src+=" UNION ALL SELECT :id,'publication',:owner,CAST(:next_token AS BIGINT),:text,:digest,:expected_owner,CAST(:expected_token AS BIGINT)"
   query=f"MERGE INTO {T} t USING ({src}) s ON t.row_key=s.row_key WHEN MATCHED AND s.kind='authority' THEN UPDATE SET token=CASE WHEN t.kind='authority' AND t.owner=s.expected_owner AND t.token=s.expected_token THEN s.token ELSE CAST(raise_error('ASHLAR_AUTHORITY_STALE') AS BIGINT) END,owner=s.owner WHEN MATCHED AND s.kind='publication' AND (t.descriptor_json<>s.descriptor_json OR t.descriptor_sha256<>s.descriptor_sha256) THEN UPDATE SET descriptor_json=CAST(raise_error('ASHLAR_PUBLICATION_ID_CONFLICT') AS STRING) WHEN NOT MATCHED AND s.kind='publication' THEN INSERT (row_key,kind,owner,token,descriptor_json,descriptor_sha256) VALUES(s.row_key,s.kind,s.owner,s.token,s.descriptor_json,s.descriptor_sha256)"
   sql(label,query,params)
   return {'label':label,'descriptor':desc,'parameters':params,'statement_id':c.records[-1]['statement_id']}
  out['accepted_a']=apply('publish-a','writer-a','writer-a',0,1,descriptor('r369-a','writer-a',0));out['transfer']=apply('transfer','writer-b','writer-a',1,2);before=ledger('before-stale');out['before_stale_rows']=sql('before-stale-rows',f'SELECT * FROM {T} ORDER BY row_key')
  try:apply('stale-a','writer-a','writer-a',1,2,descriptor('r369-stale','writer-a',1))
  except RuntimeError:
   r=c.records[-1];assert r['response']['status']['state']=='FAILED' and 'ASHLAR_AUTHORITY_STALE' in r['response']['status']['error']['message'];out['stale_statement_id']=r['statement_id']
  else:raise AssertionError('Stale publication accepted')
  out['after_stale_rows']=sql('after-stale-rows',f'SELECT * FROM {T} ORDER BY row_key');assert out['after_stale_rows']==out['before_stale_rows'];after=ledger('after-stale');assert before==after;out['stale_history']=after;out['accepted_b']=apply('publish-b','writer-b','writer-b',2,3,descriptor('r369-b','writer-b',2));out['final_rows']=sql('final-rows',f'SELECT * FROM {T} ORDER BY row_key');assert len(out['final_rows'])==3 and out['final_rows'][0][:4]==['authority','authority','writer-b','3'];out['final_history']=ledger('final-history');save()
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   h=c.history();native={q['query_id']:q for q in h}
   if set(native)=={r['statement_id'] for r in c.records} and all(q.get('is_final') for q in native.values()):break
   if i==19:raise RuntimeError('History pending; inspect same IDs')
   time.sleep(2)
  out['costs']={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in out['bounds'] if k!='wall_s'};out['wall_s']=time.monotonic()-start;assert all(v<=out['bounds'][k] for k,v in out['costs'].items()) and out['wall_s']<300;out['state']='Full-vector reference append and stale-owner whole-MERGE refusal pass';out['qualification']='Six exact source pins and original descriptor bytes retained. Guard atomic only for authority/descriptor rows within private Delta table; does not prevent stale writes to referenced current/history roles, implement source authority, active-pin retention, concurrent ownership takeover or sustained/scale performance.';save();print(json.dumps({'state':out['state'],'costs':out['costs'],'wall_s':out['wall_s']}))
 except Exception as e:out.update(state='Stopped; inspect same handles',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()

"""Two bounded absent-ID races on private manifest clones, no uniqueness assumption."""
import concurrent.futures,hashlib,json,threading,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent

def main():
 O=B/'out/native/ashlar_manifest_creation_race_r365';assert not O.exists(),'Inspect prior handles; no blind replay';c=BoundedReads(O,socket_timeout=60);start=time.monotonic();source=B/'out/native/ashlar_maintenance_manifest_inventory_r362/audited-summary.json';s=json.loads(source.read_text());out={'state':'running','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'parent':{'table':s['table'],'uuid':s['uuid'],'version':3},'pairs':[],'bounds':{'read_bytes':20000000,'write_remote_bytes':20000000,'spill_to_disk_bytes':0,'wall_s':300}}
 def save():(O/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
 def sql(label,q,params=None):assert time.monotonic()-start<300;return c.sql(label,q,parameters=params)
 def table_history(label,T):
  rows=sql(label,'DESCRIBE HISTORY '+T);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 save()
 try:
  sql('timeout','SET STATEMENT_TIMEOUT=30');rows=sql('parent-detail','DESCRIBE DETAIL '+s['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,rows[0]))['id']==s['uuid']
  for kind in ['identical','conflicting']:
   T='client_dev.ashlar_entropy_20261006_r86.manifest_race_'+kind+'_r365';sql(kind+'-clone',f'CREATE TABLE {T} SHALLOW CLONE {s["table"]} VERSION AS OF 3');rows=sql(kind+'-detail','DESCRIBE DETAIL '+T);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];detail=dict(zip(cols,rows[0]));assert sql(kind+'-absent',f"SELECT count(*) FROM {T} WHERE publication_id='r365-race'")==[['0']]
   pair={'kind':kind,'table':T,'uuid':detail['id'],'before_history':table_history(kind+'-before-history',T),'workers':[]};out['pairs'].append(pair);save();clients=[BoundedReads(O/('lane-'+kind+'-'+str(i)),socket_timeout=60) for i in range(2)]
   barrier=threading.Barrier(2,timeout=30)
   def work(i):
    client=clients[i];text=json.dumps({'owner':i if kind=='conflicting' else 0,'fixture':'r365'},sort_keys=True,separators=(',',':'));params={'id':'r365-race','text':text,'digest':hashlib.sha256(text.encode()).hexdigest()};q=f"MERGE INTO {T} t USING (SELECT :id publication_id,:text descriptor_json,:digest descriptor_sha256) s ON t.publication_id=s.publication_id WHEN MATCHED AND (t.descriptor_json<>s.descriptor_json OR t.descriptor_sha256<>s.descriptor_sha256) THEN UPDATE SET descriptor_json=CAST(raise_error('ASHLAR_PUBLICATION_ID_CONFLICT') AS STRING) WHEN NOT MATCHED THEN INSERT (publication_id,descriptor_json,descriptor_sha256) VALUES(s.publication_id,s.descriptor_json,s.descriptor_sha256)"
    try:
     barrier.wait();began=time.monotonic();client.sql('race-merge',q,parameters=params);return {'lane':client.out.name,'state':'SUCCEEDED','caller_s':time.monotonic()-began,'parameters':params,'statement_id':client.records[-1]['statement_id']}
    except RuntimeError:
     r=client.records[-1];assert r['response']['status']['state']=='FAILED';return {'lane':client.out.name,'state':'FAILED','caller_s':time.monotonic()-began,'parameters':params,'statement_id':r['statement_id'],'error':r['response']['status']['error']}
    finally:client.close()
   with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:pair['workers']=list(pool.map(work,range(2)))
   for client in clients:c.records.extend(client.records)
   pair['rows']=sql(kind+'-rows',f'SELECT publication_id,descriptor_json,descriptor_sha256 FROM {T} ORDER BY publication_id,descriptor_json');pair['after_history']=table_history(kind+'-after-history',T);race=[r for r in pair['rows'] if r[0]=='r365-race'];pair['race_row_count']=len(race);pair['distinct_race_descriptors']=len({r[1] for r in race});assert race and all(r[1:] in [[w['parameters']['text'],w['parameters']['digest']] for w in pair['workers']] for r in race);save()
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   history=c.history();native={q['query_id']:q for q in history}
   if set(native)=={r['statement_id'] for r in c.records} and all(q.get('is_final') for q in native.values()):break
   if i==19:raise RuntimeError('Native history pending; inspect same IDs')
   time.sleep(2)
  for pair in out['pairs']:
   q0,q1=[native[w['statement_id']] for w in pair['workers']];pair['native_statement_overlap_ms']=max(0,min(q0['execution_end_time_ms'],q1['execution_end_time_ms'])-max(q0['query_start_time_ms'],q1['query_start_time_ms']));pair['qualification']='Native statement windows include planning/queue; overlap does not prove simultaneous absent reads or general conflict behavior.'
  out['costs']={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in out['bounds'] if k!='wall_s'};out['wall_s']=time.monotonic()-start;assert all(v<=out['bounds'][k] for k,v in out['costs'].items()) and out['wall_s']<300;out['state']='Both bounded creation races terminal; inspect observed uniqueness outcomes';out['qualification']='Two serially staged pairs, two simultaneous local clients per pair, private three-column clones. No production writer authority, full-role graph publication, fresh-ID global uniqueness proof or scale admission.';(O/'all-statements.json').write_text(json.dumps(c.records,indent=2)+'\n');save();print(json.dumps({'state':out['state'],'pairs':[{k:p[k] for k in ['kind','race_row_count','distinct_race_descriptors','native_statement_overlap_ms']} for p in out['pairs']],'costs':out['costs'],'wall_s':out['wall_s']},indent=2))
 except Exception as e:out.update(state='Stopped; inspect same handles',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()

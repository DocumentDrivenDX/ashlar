"""Bounded synthetic publication benchmark; owned tables, no warehouse changes.
600k entities represent one minute at 10k/s, not a sustained-rate claim.
"""
import argparse,json,subprocess,time,uuid
from pathlib import Path
from urllib.parse import urlencode
BASE=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--schema');p.add_argument('--resume-batch',type=int,default=1);a=p.parse_args()
schema=a.schema or 'ashlar_ingest_'+uuid.uuid4().hex[:6]
assert schema.replace('_','').isalnum()
F='client_dev.'+schema;out=BASE/'out/native'/schema;out.mkdir(parents=True,exist_ok=True)
records=[json.loads(x) for x in (out/'statements.jsonl').read_text().splitlines()] if (out/'statements.jsonl').exists() else []
def api(method,path,body=None):
 cmd=['databricks','api',method,path,'--profile','aidev-cus']
 if body is not None:
  req=out/'request.json';req.write_text(json.dumps(body));cmd+=['--json','@'+str(req)]
 r=subprocess.run(cmd,text=True,capture_output=True,timeout=45)
 if r.returncode:raise RuntimeError(r.stderr)
 return json.loads(r.stdout)
def sql(label,statement):
 for r in records:
  if r['label']==label and r['sql']==statement and r['response']['status']['state']=='SUCCEEDED':return r['response'].get('result',{}).get('data_array',[])
 start=time.perf_counter()
 r=api('post','/api/2.0/sql/statements',{'warehouse_id':'2439e1f2e37ac563','statement':statement,'wait_timeout':'10s','on_wait_timeout':'CONTINUE','disposition':'INLINE','format':'JSON_ARRAY','row_limit':1000})
 deadline=time.monotonic()+180
 while r['status']['state'] in ('PENDING','RUNNING'):
  if time.monotonic()>deadline:
   api('post','/api/2.0/sql/statements/'+r['statement_id']+'/cancel',{});raise RuntimeError('Canceled deadline '+r['statement_id'])
  time.sleep(2);r=api('get','/api/2.0/sql/statements/'+r['statement_id'])
 rec={'label':label,'sql':statement,'statement_id':r['statement_id'],'wall_ms':(time.perf_counter()-start)*1000,'response':r};records.append(rec)
 with (out/'statements.jsonl').open('a') as h:h.write(json.dumps(rec)+'\n')
 print(label,r['status']['state'],round(rec['wall_ms']),flush=True)
 if r['status']['state']!='SUCCEEDED':raise RuntimeError(json.dumps(r['status']))
 return r.get('result',{}).get('data_array',[])
sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar authorized synthetic ingest spike'")
sql('environment','SELECT current_version()')
# Clone only synthetic existing data. Fixed source version retained in clone history.
sql('seed',f'CREATE TABLE {F}.objects DEEP CLONE client_dev.ashlar_spike_20261005_9586d0.type_a_l')
sql('projection',f'CREATE TABLE {F}.serving USING DELTA CLUSTER BY (id) AS SELECT id,props_json,retained_json FROM {F}.objects')
sql('journal',f'CREATE TABLE {F}.journal (batch BIGINT,id BIGINT,property_id BIGINT,old_present BOOLEAN,old_json STRING,new_present BOOLEAN,new_json STRING) USING DELTA CLUSTER BY (batch,id)')
sql('manifest',f'CREATE TABLE {F}.manifest (batch BIGINT,versions STRING,entities BIGINT,recorded_at TIMESTAMP) USING DELTA')
results=json.loads((out/'ingest-summary.json').read_text())['batches'] if a.resume_batch>1 else []
for batch,n in enumerate([10000,100000,600000],1):
 if batch<a.resume_batch:continue
 # Unique SHA2 blocks yield a deterministic ~2KB high-entropy string per object.
 payload="concat("+','.join(f"sha2(concat(cast(id as string),':{batch}:{k}'),256)" for k in range(32))+')'
 sql('stage-'+str(batch),f"CREATE TABLE {F}.stage_{batch} USING DELTA AS SELECT *,to_json(named_struct('101',group_value,'102',rank_value,'103',{payload})) new_props FROM {F}.objects WHERE id<={n}")
 start=time.perf_counter()
 sql('merge-'+str(batch),f'MERGE INTO {F}.objects t USING {F}.stage_{batch} s ON t.id=s.id WHEN MATCHED THEN UPDATE SET t.props_json=s.new_props')
 sql('journal-'+str(batch),f"INSERT INTO {F}.journal SELECT {batch},id,103,true,substring(to_json(array(get_json_object(props_json,'$.103'))),2,length(to_json(array(get_json_object(props_json,'$.103'))))-2),true,substring(to_json(array(get_json_object(new_props,'$.103'))),2,length(to_json(array(get_json_object(new_props,'$.103'))))-2) FROM {F}.stage_{batch}")
 sql('serving-'+str(batch),f'MERGE INTO {F}.serving t USING {F}.stage_{batch} s ON t.id=s.id WHEN MATCHED THEN UPDATE SET t.props_json=s.new_props')
 versions={}
 for table in ['objects','serving','journal']:
  versions[table]=int(sql('version-'+table+'-'+str(batch),f'DESCRIBE HISTORY {F}.{table} LIMIT 1')[0][0])
 # Full changed-set validation is included in measured publication cost.
 assert sql('parity-'+str(batch),f'SELECT count(*) FROM {F}.stage_{batch} s JOIN {F}.objects o ON s.id=o.id JOIN {F}.serving p ON s.id=p.id WHERE o.props_json<>s.new_props OR p.props_json<>s.new_props OR o.retained_json<>s.retained_json OR p.retained_json<>s.retained_json')==[['0']]
 assert sql('journal-count-'+str(batch),f'SELECT count(*) FROM {F}.journal WHERE batch={batch}')==[[str(n)]]
 assert sql('journal-exact-'+str(batch),f"SELECT count(*) FROM {F}.journal j JOIN {F}.stage_{batch} s ON j.id=s.id WHERE j.batch={batch} AND (from_json(concat('[',j.old_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.props_json,'$.103') OR from_json(concat('[',j.new_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.new_props,'$.103'))")==[['0']]
 v=json.dumps(versions,separators=(',',':'))
 sql('publish-'+str(batch),f"INSERT INTO {F}.manifest VALUES ({batch},'{v}',{n},current_timestamp())")
 elapsed=(time.perf_counter()-start)*1000
 results.append({'batch':batch,'entities':n,'publication_wall_ms':elapsed,'effective_entities_per_s':n/(elapsed/1000),'versions':versions,'includes':'merge, journal, projection, version discovery, changed-set validation, manifest; stage construction excluded'})
 for rep in range(10):
  key=1+(rep*7919)%n
  assert sql(f'read-{batch}-{rep}',f"SELECT id,length(props_json),uuid() FROM {F}.serving VERSION AS OF {versions['serving']} WHERE id={key}")[0][0]==str(key)
 (out/'ingest-summary.json').write_text(json.dumps({'schema':F,'state':'running','batches':results,'scope':'serial batch screening, no sustained arrival, no concurrent clients, no p95 freshness claim'},indent=2)+'\n')
ids={r['statement_id'] for r in records};history=[];token=None
for _ in range(5):
 params={'max_results':1000,'include_metrics':'true'}
 if token:params['page_token']=token
 response=api('get','/api/2.0/sql/history/queries?'+urlencode(params));history.extend(q for q in response.get('res',[]) if q['query_id'] in ids)
 if not response.get('has_next_page'):break
 token=response['next_page_token']
(out/'query-history.json').write_text(json.dumps(history,indent=2)+'\n')
(out/'ingest-summary.json').write_text(json.dumps({'schema':F,'state':'completed','batches':results,'statement_count':len(records),'history_count':len(history),'scope':'serial batch screening; no sustained arrival, concurrent clients or p95 freshness claim'},indent=2)+'\n')
print(json.dumps(results),flush=True)

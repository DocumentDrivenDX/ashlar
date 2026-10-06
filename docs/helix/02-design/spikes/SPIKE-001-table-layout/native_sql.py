"""Execute bounded synthetic Delta screening through the active Databricks CLI.
No credentials are read or emitted; CLI handles authentication internally.
"""
import argparse, hashlib, json, math, subprocess, time, uuid
from pathlib import Path
from urllib.parse import urlencode
BASE=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--rows',type=int,default=10000);p.add_argument('--repeats',type=int,default=20);p.add_argument('--schema');p.add_argument('--profile',default='aidev-cus');args=p.parse_args()
if not 10000<=args.rows<=1000000:raise ValueError('Bounded first-pass rows/type: 10000 to 1000000')
schema=args.schema or 'ashlar_spike_20261005_'+uuid.uuid4().hex[:6]
if not schema.replace('_','').isalnum():raise ValueError('Invalid isolated schema')
F='client_dev.'+schema;OUT=BASE/'out/native'/schema;OUT.mkdir(parents=True,exist_ok=True)
meta={'workspace':'dbw-aidev-cus','warehouse':'2439e1f2e37ac563','catalog':'client_dev','schema':schema,'rows_per_type':args.rows,'repeats':args.repeats,'start_ms':int(time.time()*1000),'notes':['CLI/REST wall timing includes client process/auth/network; query history separates engine/compile/queue','uuid() in read results prevents result-cache reuse; metrics verify this','No warehouse configuration changed; disk cache not cleared; no cold claim']}
if (OUT/'run.json').exists():
 previous=json.loads((OUT/'run.json').read_text());assert previous['rows_per_type']==args.rows
 meta['start_ms']=previous['start_ms']
(OUT/'run.json').write_text(json.dumps(meta,indent=2)+'\n')
records=[json.loads(line) for line in (OUT/'statements.jsonl').read_text().splitlines()] if (OUT/'statements.jsonl').exists() else []
def cli(method,path,body=None):
 command=['databricks','api',method,path,'--profile',args.profile]
 if body is not None:
  req=OUT/'request.json';req.write_text(json.dumps(body));command+=['--json','@'+str(req)]
 done=subprocess.run(command,text=True,capture_output=True,timeout=45)
 if done.returncode:raise RuntimeError(done.stderr)
 return json.loads(done.stdout)
latest=None
def execute(label,sql):
 global latest
 for prior in reversed(records):
  if prior['label']==label and prior['sql']==sql and prior['response']['status']['state']=='SUCCEEDED':
   latest=prior
   return prior['response'].get('result',{}).get('data_array',[])
 start=time.perf_counter();result=cli('post','/api/2.0/sql/statements',{'warehouse_id':meta['warehouse'],'statement':'/* ashlar '+schema+' '+label+' */ '+sql,'wait_timeout':'10s','on_wait_timeout':'CONTINUE','disposition':'INLINE','format':'JSON_ARRAY','row_limit':1000})
 sid=result['statement_id']; deadline=time.monotonic()+180
 while result['status']['state'] in ('PENDING','RUNNING'):
  print(label,result['status']['state'],sid,flush=True)
  if time.monotonic()>deadline:
   cli('post','/api/2.0/sql/statements/'+sid+'/cancel',{});raise RuntimeError('Statement deadline; canceled '+sid)
  time.sleep(2);result=cli('get','/api/2.0/sql/statements/'+sid)
 wall=(time.perf_counter()-start)*1000
 rec={'label':label,'sql':sql,'statement_id':sid,'wall_ms':wall,'response':result};records.append(rec);latest=rec
 with (OUT/'statements.jsonl').open('a') as handle:handle.write(json.dumps(rec)+'\n')
 if result['status']['state']!='SUCCEEDED':raise RuntimeError(json.dumps(result['status']))
 if result.get('manifest',{}).get('truncated'):raise RuntimeError('Unexpected truncated response')
 return result.get('result',{}).get('data_array',[])
n=args.rows
execute('create-schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar isolated synthetic graph layout spike; owner-authorized 2026-10-05'")
execute('environment','SELECT current_version()')
base=f"""SELECT 'pilot' source_system,t type_id,i id,
 to_json(named_struct('101',concat('g',cast(i%100 as string)), '102',i%1000,'103',repeat('x',512))) props_json,
 '{{"future":{{"preserve":true}}}}' retained_json
 FROM range(1,4) tt(t) CROSS JOIN range(1,{n+1}) ii(i)"""
for name,layout in [('bag_l','CLUSTER BY (type_id,id)'),('bag_z','PARTITIONED BY (type_id)')]:
 stats='id' if name.endswith('_z') else 'type_id,id'
 execute('create-'+name,f"CREATE TABLE {F}.{name} USING DELTA {layout} TBLPROPERTIES ('delta.dataSkippingStatsColumns'='{stats}') AS {base}")
 execute('optimize-'+name,f'OPTIMIZE {F}.{name}'+(' ZORDER BY (id)' if name.endswith('_z') else ''))
execute('create-promoted',f"""CREATE TABLE {F}.promoted_l USING DELTA CLUSTER BY (type_id,id)
 TBLPROPERTIES ('delta.dataSkippingStatsColumns'='type_id,id,group_value,rank_value') AS
 SELECT *,get_json_object(props_json,'$.101') group_value,cast(get_json_object(props_json,'$.102') AS BIGINT) rank_value FROM {F}.bag_l""")
execute('optimize-promoted',f'OPTIMIZE {F}.promoted_l')
execute('create-typed',f"CREATE TABLE {F}.type_a_l USING DELTA CLUSTER BY (id,group_value) TBLPROPERTIES ('delta.dataSkippingStatsColumns'='id,group_value,rank_value') AS SELECT * FROM {F}.promoted_l WHERE type_id=1")
execute('optimize-typed',f'OPTIMIZE {F}.type_a_l')
execute('create-edges',f"""CREATE TABLE {F}.edges_l USING DELTA CLUSTER BY (rel_type_id,source_id,target_id)
 TBLPROPERTIES ('delta.dataSkippingStatsColumns'='rel_type_id,source_id,target_id') AS
 SELECT rel rel_type_id,(rel-1)*{n*4}+i*4+k id,rel source_type,i source_id,rel+1 target_type,
 CASE WHEN i%100=0 THEN 1 ELSE ((i+k-1)%{n})+1 END target_id,k/10.0 score
 FROM range(1,3) r(rel) CROSS JOIN range(1,{n+1}) a(i) CROSS JOIN range(0,4) b(k)""")
execute('insert-hub',f'INSERT INTO {F}.edges_l SELECT 1,{n*20}+i,1,1,2,i,0.9 FROM range(1,{n+1}) t(i)')
execute('optimize-edges',f'OPTIMIZE {F}.edges_l')
for name,rel in [('edge_ab_l',1),('edge_bc_l',2)]:
 execute('create-'+name,f'CREATE TABLE {F}.{name} USING DELTA CLUSTER BY (source_id,target_id) AS SELECT * FROM {F}.edges_l WHERE rel_type_id={rel}')
 execute('optimize-'+name,f'OPTIMIZE {F}.{name}')
execute('create-reverse',f'CREATE TABLE {F}.edge_ab_reverse_l USING DELTA CLUSTER BY (target_id) AS SELECT id,source_id,target_id FROM {F}.edge_ab_l')
execute('optimize-reverse',f'OPTIMIZE {F}.edge_ab_reverse_l')
assert execute('object-count',f'SELECT count(*) FROM {F}.bag_l')==[[str(3*n)]]
assert execute('edge-count',f'SELECT count(*) FROM {F}.edges_l')==[[str(9*n)]]
# Validate endpoints, distinct edge identity, exact null/missing carriers natively.
assert execute('edge-duplicates',f'SELECT count(*)-count(DISTINCT id) FROM {F}.edges_l')==[['0']]
assert execute('bad-endpoints',f'SELECT count(*) FROM {F}.edges_l e LEFT ANTI JOIN {F}.bag_l v ON e.source_type=v.type_id AND e.source_id=v.id')==[['0']]
assert execute('bad-targets',f'SELECT count(*) FROM {F}.edges_l e LEFT ANTI JOIN {F}.bag_l v ON e.target_type=v.type_id AND e.target_id=v.id')==[['0']]
carriers=execute('exact-carriers',"SELECT '{\"integer\":9223372036854775807,\"decimal\":12345678901234567890.123456789,\"null\":null,\"future\":{\"x\":true}}' AS carrier")
assert '12345678901234567890.123456789' in carriers[0][0]
expected={};measurements=[]
for table in ['bag_l','bag_z','promoted_l','type_a_l']:
 for shape in ['lookup','list','count']:
  for rep in range(args.repeats+1):
   key=2+(rep*7919)%(n-1)
   if shape=='lookup':sql=f"SELECT id,props_json,retained_json,uuid() benchmark_nonce FROM {F}.{table} WHERE source_system='pilot' AND type_id=1 AND id={key}"
   elif shape=='list':
    group="get_json_object(props_json,'$.101')" if table.startswith('bag') else 'group_value'
    sql=f"SELECT id,uuid() benchmark_nonce FROM {F}.{table} WHERE type_id=1 AND {group}='g42' ORDER BY id LIMIT 100"
   else:
    group="get_json_object(props_json,'$.101')" if table.startswith('bag') else 'group_value'
    sql=f"SELECT {group} grp,count(*) n,uuid() benchmark_nonce FROM {F}.{table} WHERE type_id=1 GROUP BY {group} ORDER BY grp"
   label=f'measure-{table}-{shape}-{rep}';rows=execute(label,sql);clean=[row[:-1] for row in rows]
   assert expected.setdefault((shape,key),clean)==clean,(table,shape,key,'Differential mismatch')
   if shape=='lookup':assert clean==[[str(key),json.dumps({'101':'g'+str(key%100),'102':key%1000,'103':'x'*512},separators=(',',':')),'{"future":{"preserve":true}}']]
   if rep:measurements.append({'label':label,'table':table,'shape':shape,'statement_id':latest['statement_id'],'wall_ms':latest['wall_ms']})
  print('Measured',table,shape,flush=True)
for table in ['edges_l','edge_ab_l','edge_ab_reverse_l']:
 filter_rel=' AND rel_type_id=1' if table=='edges_l' else ''
 rows=execute('reverse-'+table,f'SELECT id,source_id FROM {F}.{table} WHERE target_id=1{filter_rel} ORDER BY id LIMIT 100')
 assert expected.setdefault('reverse',rows)==rows
for name,e,f in [('shared','edges_l','edges_l'),('typed','edge_ab_l','edge_bc_l')]:
 rows=execute('two-hop-'+name,f'SELECT e.id,f.id FROM {F}.{e} e JOIN {F}.{f} f ON e.target_id=f.source_id AND e.target_type=f.source_type WHERE e.rel_type_id=1 AND e.source_id=123 AND f.rel_type_id=2 AND e.score>=0.1 AND f.score>=0.1 ORDER BY e.id,f.id LIMIT 100')
 assert expected.setdefault('two-hop',rows)==rows
for table in ['bag_l','bag_z','promoted_l','type_a_l','edges_l','edge_ab_l','edge_bc_l','edge_ab_reverse_l']:
 execute('detail-'+table,f'DESCRIBE DETAIL {F}.{table}')
 execute('history-'+table,f'DESCRIBE HISTORY {F}.{table} LIMIT 3')
# Retain only this run's statement IDs from history; unrelated history is not saved or printed.
ids={x['statement_id'] for x in records};history=[];token=None
for attempt in range(3):
 filter_by={'warehouse_ids':[meta['warehouse']],'user_ids':[3755468182769617],'query_start_time_range':{'start_time_ms':meta['start_ms']}}
 params={'max_results':1000,'include_metrics':'true'}
 if token:params['page_token']=token
 response=cli('get','/api/2.0/sql/history/queries?'+urlencode(params))
 history.extend(q for q in response.get('res',[]) if q['query_id'] in ids)
 if not response.get('has_next_page'):break
 token=response['next_page_token']
(OUT/'query-history.json').write_text(json.dumps(history,indent=2)+'\n')
metrics={x['query_id']:x.get('metrics',{}) for x in history}
for x in measurements:x['metrics']=metrics.get(x['statement_id'],{})
(OUT/'measurements.json').write_text(json.dumps(measurements,indent=2)+'\n')
meta.update(end_ms=int(time.time()*1000),state='completed',statement_count=len(records),history_count=len(history),measured_queries=len(measurements))
(OUT/'run.json').write_text(json.dumps(meta,indent=2)+'\n')
print('Completed',schema,'statements',len(records),'measurements',len(measurements),'history',len(history),flush=True)

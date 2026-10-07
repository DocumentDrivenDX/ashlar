"""Two persistent clients, ten synchronized LC point reads each; no source scan."""
import json,time,math,threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_lc_concurrent_r183'
assert not O.exists(),'Inspect prior IDs; no blind replay'
O.mkdir(parents=True)
u=json.loads((B/'out/native/ashlar_bucket_update_r176/audited-summary.json').read_text())['owned']['lc'];T=u['table']
old=[json.loads(x) for x in (B/'out/native/ashlar_bucket_post_update_reads_r181/statements.jsonl').read_text().splitlines()]
rows=next(x for x in old if x['label']=='oracle')['response']['result']['data_array'];assert len(rows)==30
clients=[BoundedReads(O/('client'+str(i))) for i in range(2)]
for c in clients:
 c.sql('timeout','SET STATEMENT_TIMEOUT=15')
 a=c.sql('detail','DESCRIBE DETAIL '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,a[0]))['id']==u['id']
 assert c.sql('version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='1'
barrier=threading.Barrier(2);histories=[{},{}];budget=10000000000
query=f"SELECT {','.join(COLS)} FROM {T} VERSION AS OF 1 WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
def run(i,k):
 c=clients[i];row=rows[2*k+i]
 barrier.wait(timeout=15)
 assert c.sql('point-'+str(k),query,parameters={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]})==[row]
 c.cursor.close();c.cursor=c.connection.cursor()
 barrier.wait(timeout=15)
 assert c.sql('floor-'+str(k),'SELECT 1')==[['1']]
 c.cursor.close();c.cursor=c.connection.cursor()
for k in range(10):
 used=sum(v['metrics'].get('read_bytes',0) for h in histories for v in h.values())
 assert budget-used>=1000000000,'Stop before next pair exceeds conservative 1GB reserve'
 with ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(lambda i:run(i,k),range(2)))
 for i,c in enumerate(clients):
  for attempt in range(6):
   histories[i]={v['query_id']:v for v in c.history()}
   if all(r['statement_id'] in histories[i] and histories[i][r['statement_id']]['is_final'] for r in c.records):break
   time.sleep(2)
  assert all(histories[i][r['statement_id']]['is_final'] and histories[i][r['statement_id']]['status']=='FINISHED' for r in c.records)
 print('Completed synchronized round',k,'read bytes',sum(v['metrics'].get('read_bytes',0) for h in histories for v in h.values()),flush=True)
for c in clients:c.close()
p95=lambda a:sorted(a)[math.ceil(.95*len(a))-1]
summary={'table':T,'id':u['id'],'version':1,'concurrency':2,'rounds':10,'reads':{}}
for kind in ['point','floor']:
 a=[(r,histories[i][r['statement_id']]) for i,c in enumerate(clients) for r in c.records if r['label'].startswith(kind+'-')];assert len(a)==20
 assert all(not h['metrics'].get('result_from_cache') for r,h in a)
 summary['reads'][kind]={'caller_p95_ms':p95([r['wall_ms'] for r,h in a]),**{k+'_p95':p95([h['metrics'].get(k,0) for r,h in a]) for k in ['execution_time_ms','compilation_time_ms','waiting_at_capacity_duration_ms','read_bytes','read_files_count']},'remote_queries':sum(h['metrics'].get('read_remote_bytes',0)>0 for r,h in a),'caller_minus_server_p95_ms':p95([r['wall_ms']-h['metrics'].get('total_time_ms',0) for r,h in a])}
summary['costs']={k:sum(h['metrics'].get(k,0) for d in histories for h in d.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
assert summary['costs']['read_bytes']<=budget and summary['costs']['write_remote_bytes']==0
summary['qualification']='Two independent persistent SQL clients on existing warehouse,20 distinct updated stage identities,all20 fields exact. Synchronized pairs; telemetry polling outside timed reads introduces idle gaps. SELECT1 pairs measure caller/server residual, not network alone. Prior scans may warm files; not controlled cold or steady saturation, not sustained publication or billion admission.'
summary['state']='Twenty concurrent exact point reads and twenty SELECT1 controls passed within budget'
(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))

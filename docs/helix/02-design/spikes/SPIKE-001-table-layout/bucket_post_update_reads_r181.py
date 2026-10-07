"""Matched full-carrier reads: canonical E23 versus full20M owned bucket4."""
import json,math,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_post_update_reads_r181'
assert not (O/'statements.jsonl').exists(),'Inspect existing IDs; do not rerun'
s=json.loads((B/'out/native/ashlar_bucket_final_parity_r178/audited-summary.json').read_text());assert s['state']=='All four final 20M exact-value ranges passed; cost admission failed'
u=json.loads((B/'out/native/ashlar_bucket_update_r176/audited-summary.json').read_text()); assert json.loads((B/'out/native/ashlar_bucket_lc_custody_r179/summary.json').read_text())['version']==1; E=u['owned']['lc']['table'];T=s['table'];F='client_dev.ashlar_entropy_20261006_r86';c=BoundedReads(O)
c.sql('timeout','SET STATEMENT_TIMEOUT=30')
rows=c.sql('detail','DESCRIBE DETAIL '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];detail=dict(zip(names,rows[0]));assert detail['id']==s['id']
assert c.sql('version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='9'
oracle=c.sql('oracle',f"SELECT {','.join(COLS)} FROM {F}.bucket_stage_r176 VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 30");assert len(oracle)==30
for i,row in enumerate(oracle):
 for name in (('lc','part') if i%2==0 else ('part','lc')):
  table,version=(E,1) if name=='lc' else (T,9)
  q=f"SELECT {','.join(COLS)} FROM {table} VERSION AS OF {version} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
  params={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]}
  if name=='part':q+=' AND lookup_bucket=CAST(:bucket AS INT)';params['bucket']=int(row[16][:15],16)%64
  assert c.sql(name+'-read-'+str(i),q,parameters=params)==[row]
assert c.sql('final-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='9'
c.close()
for attempt in range(12):
 h={q['query_id']:q for q in c.history()}
 if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
 if attempt<11:time.sleep(5)
assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records)
def p95(a):return sorted(a)[math.ceil(.95*len(a))-1]
reads={}
for name in ('lc','part'):
 rs=[r for r in c.records if r['label'].startswith(name+'-read-')];assert len(rs)==30 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
 reads[name]={'caller_p95_ms':p95([r['wall_ms'] for r in rs]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs]),'compile_p95_ms':p95([h[r['statement_id']]['metrics']['compilation_time_ms'] for r in rs]),'files_p95':p95([h[r['statement_id']]['metrics']['read_files_count'] for r in rs]),'bytes_p95':p95([h[r['statement_id']]['metrics']['read_bytes'] for r in rs]),'remote_queries':sum(h[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in rs)}
costs={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ('read_bytes','write_remote_bytes','spill_to_disk_bytes')};assert costs['read_bytes']<=15000000000 and costs['write_remote_bytes']==0
(O/'summary.json').write_text(json.dumps({'state':'60 matched full-carrier reads exact and finalized','table':T,'id':s['id'],'version':9,'source':E,'source_version':1,'detail':detail,'reads':reads,'costs':costs,'oracle':F+'.bucket_stage_r176','oracle_version':0,'qualification':'Post-update LC1 versus bucket9. 30 same affected SHA-ranked keys per layout; alternation, cache false, large-token entity_version16/r176-b1. LC clone has existing DVs/hot files; bucket has full ZORDER and mixed COW/DV update. Operational comparison, not causal partition isolation. Prior validation may warm files; no controlled cold, sustained publication or billion admission.'},indent=2)+'\n');print(json.dumps({'reads':reads,'costs':costs},indent=2))

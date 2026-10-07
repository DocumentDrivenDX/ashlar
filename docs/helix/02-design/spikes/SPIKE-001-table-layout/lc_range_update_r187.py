"""Owned full20M LC clone: same100k stage, range-input hint, exact/custody/read proof."""
import json,time,math
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS,PropertyApply
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_lc_range_update_r187';assert not O.exists(),'Inspect existing native IDs; never replay'
old=json.loads((B/'out/native/ashlar_bucket_update_r176/audited-summary.json').read_text());E=old['source'];U=old['stage'];F=E.rsplit('.',1)[0];T=F+'.lc_range_r187'
c=BoundedReads(O);deadline=time.monotonic()+600;state={'table':T,'source':E,'source_version':23,'stage':U,'stage_version':0,'state':'In progress, unpublished'}
def save():(O/'checkpoint.json').write_text(json.dumps(state,indent=2)+'\n')
def sql(label,q,parameters=None):
 assert time.monotonic()<deadline,'Controller admission deadline'
 return c.sql(label,q,parameters=parameters)
def detail(label,t):
 a=sql(label,'DESCRIBE DETAIL '+t);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(names,a[0]))
def telemetry(reserve=0):
 c.cursor.close();c.cursor=c.connection.cursor()
 for attempt in range(12):
  h={x['query_id']:x for x in c.history()}
  if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
  if attempt<11:time.sleep(2)
 assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records)
 state['costs']={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save()
 assert state['costs']['read_bytes']+reserve<=25000000000 and state['costs']['write_remote_bytes']<=2000000000,'Stop further admission: budget exceeded'
 return h
try:
 sql('timeout','SET STATEMENT_TIMEOUT=90');assert sql('absence',f"SHOW TABLES IN {F} LIKE 'lc_range_r187'")==[]
 state['source_id']=detail('source-detail',E)['id'];d=detail('stage-detail',U);assert d['id']==old['stage_id'];state['stage_id']=d['id'];assert sql('stage-version','DESCRIBE HISTORY '+U+' LIMIT 1')[0][0]=='0'
 assert sql('stage-membership',f'SELECT count(*),count(DISTINCT id) FROM {U} VERSION AS OF 0')==[['100000','100000']]
 telemetry(8000000000)
 sql('clone',f'CREATE TABLE {T} SHALLOW CLONE {E} VERSION AS OF 23');state['id']=detail('clone-detail',T)['id'];assert sql('clone-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='0';save()
 q=PropertyApply(T,U,0,15,'r176-b1','r139-b1','bucket-r176',9007199254742501,eligibility_placement='on');apply=q.apply().replace('USING (SELECT ','USING (SELECT /*+ REPARTITION_BY_RANGE(6,lookup_hash) */ ')
 telemetry(8000000000);sql('apply',apply);state['apply_query_id']=c.records[-1]['statement_id'];assert sql('post-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='1';state['version']=1;save();print('Range-input MERGE committed at1',flush=True)
 a=sql('history','DESCRIBE HISTORY '+T);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];history=[dict(zip(names,r)) for r in a];assert [int(x['version']) for x in history]==[1,0];assert history[0]['queryHistoryStatementId']==state['apply_query_id'];m=json.loads(history[0]['operationMetrics']);assert m['numTargetRowsUpdated']=='100000' and m['numTargetRowsCopied']=='0' and m['numTargetRowsInserted']=='0' and m['numTargetRowsDeleted']=='0';state['history']=history
 assert json.loads(history[1]['operationParameters'])['sourceVersion']=='23'
 telemetry(8000000000);assert sql('exact-output',q.output(1))==[['0']]
 def untouched(v):return f"SELECT b.source_system,b.rel_type_id,b.id,b._metadata.file_path,b._metadata.row_index FROM {T} VERSION AS OF {v} b LEFT ANTI JOIN (SELECT source_system,rel_type_id,id FROM {U} VERSION AS OF 0) k ON b.source_system=k.source_system AND b.rel_type_id=k.rel_type_id AND b.id=k.id"
 before=untouched(0);after=untouched(1)
 assert sql('untouched-count',f'SELECT count(*) FROM ({after})')==[['19900000']]
 telemetry(4000000000);assert sql('untouched-custody',f'SELECT count(*) FROM (({before} EXCEPT ALL {after}) UNION ALL ({after} EXCEPT ALL {before}))')==[['0']]
 assert sql('identities',f'SELECT count(*),count(DISTINCT id) FROM {T} VERSION AS OF 1')==[['20000000','20000000']]
 telemetry(1000000000)
 files=sql('hot-ranges',f"SELECT _metadata.file_path,max(_metadata.file_size),count(*),min(lookup_hash),max(lookup_hash) FROM {T} VERSION AS OF 1 WHERE entity_version=16 AND apply_batch_id='r176-b1' GROUP BY _metadata.file_path");assert sum(int(x[2]) for x in files)==100000;state['hot_files']=files;save();print('Exact output,19.9M custody,20M IDs passed; hot files',len(files),flush=True)
 prior=[json.loads(x) for x in (B/'out/native/ashlar_bucket_post_update_reads_r181/statements.jsonl').read_text().splitlines()];rows=next(x for x in prior if x['label']=='oracle')['response']['result']['data_array']
 for i,row in enumerate(rows[:20]):
  telemetry(1000000000)
  query=f"SELECT {','.join(COLS)} FROM {T} VERSION AS OF 1 WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
  assert sql('read-'+str(i),query,parameters={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]})==[row]
 assert detail('final-detail',T)['id']==state['id'];assert sql('final-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='1'
 pins=sql('publication',f"SELECT table_versions_json FROM {F}.publication_manifest_r89 WHERE publication_id='r139-b1'");state['publication_vector']=json.loads(pins[0][0]);assert state['publication_vector'][E]==23
 h=telemetry();state['apply_metrics']={'caller_ms':next(r['wall_ms'] for r in c.records if r['statement_id']==state['apply_query_id']),'metrics':h[state['apply_query_id']]['metrics']}
 rs=[r for r in c.records if r['label'].startswith('read-')];assert len(rs)==20 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs);p95=lambda a:sorted(a)[math.ceil(.95*len(a))-1]
 state['reads']={'caller_p95_ms':p95([r['wall_ms'] for r in rs]),**{k+'_p95':p95([h[r['statement_id']]['metrics'].get(k,0) for r in rs]) for k in ['execution_time_ms','read_bytes','read_files_count']},'remote_queries':sum(h[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in rs)}
 state['state']='Range-input update exact;19.9M custody and20M identity plus20 singleton reads passed'
 state['qualification']='Owned shallow clone of E23 and immutable independently proved stage0. Same eligibility/hash/full native tuple, only source hint differs.20-field changed-output proof plus immutable untouched-file custody in closed zero-copy lineage. No fresh20M wide scan; no canonical mutation/publication or producer authority. Sequential control histories and metadata polling can affect cache/timing; no causal SLO/cold/sustained/billion admission.';save();print(json.dumps({'reads':state['reads'],'costs':state['costs'],'apply':state['apply_metrics']},indent=2))
except Exception as e:state['state']='Stopped; inspect same write IDs and owned versions before any new admission';state['error']=str(e);save();raise
finally:c.close()

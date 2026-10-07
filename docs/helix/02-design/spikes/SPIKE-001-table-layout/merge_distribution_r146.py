"""Paired actual update-only MERGEs on original E18 shallow clones."""
import json,subprocess,sys,time
from pathlib import Path
from driver_sql import DriverClient
from property_apply_queries import PropertyApply,COLS,lit
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_merge_pruning_r146'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';S=F+'.schedule_r139_1'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
results={}
for mode in ('control','candidate'):
 A=F+'.edge_merge_'+mode+'_r146'
 assert c.sql(mode+'-absence',f"SHOW TABLES IN {F} LIKE 'edge_merge_{mode}_r146'")==[]
 c.sql(mode+'-clone',f'CREATE TABLE {A} SHALLOW CLONE {E} VERSION AS OF 22')
 old=int(c.sql(mode+'-old',f'DESCRIBE HISTORY {A} LIMIT 1')[0][0])
 q=PropertyApply(A,S,old,14,'r139-b1','r133-b1','schedule-r139',9007199254741039,eligibility_placement='on')
 assert c.sql(mode+'-membership',q.membership())==[['100000','100000']]
 assert c.sql(mode+'-intended',q.intended())==[['0']]
 query=q.apply()
 if mode=='candidate':
  query=query.replace('USING (SELECT ', 'USING (SELECT /*+ REPARTITION_BY_RANGE(16,lookup_hash) */ ')
 c.sql(mode+'-merge',query)
 new=int(c.sql(mode+'-new',f'DESCRIBE HISTORY {A} LIMIT 1')[0][0])
 assert new==old+1
 assert c.sql(mode+'-output',q.output(new))==[['0']]
 assert c.sql(mode+'-identities',q.identities(new))==[['20000000','20000000','100000']]
 # Compare every untouched logical key and immutable physical row, not wide payloads.
 fields='b.source_system,b.rel_type_id,b.id,b._metadata.file_path,b._metadata.row_index'
 def untouched(version):
  return f'''SELECT {fields} FROM {A} VERSION AS OF {version} b LEFT ANTI JOIN {S} VERSION AS OF 0 s
   ON b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id'''
 assert c.sql(mode+'-untouched',f'SELECT count(*) FROM (({untouched(old)} EXCEPT ALL {untouched(new)}) UNION ALL ({untouched(new)} EXCEPT ALL {untouched(old)}))')==[['0']]
 files=c.sql(mode+'-file-ranges',f"SELECT _metadata.file_path,count(*),min(lookup_hash),max(lookup_hash) FROM {A} VERSION AS OF {new} WHERE entity_version=15 AND apply_batch_id='r139-b1' GROUP BY _metadata.file_path ORDER BY min(lookup_hash)")
 (O/(mode+'-file-ranges.json')).write_text(json.dumps(files,indent=2)+'\n')
 assert new==1
 read_out=O/(mode+'-read-lane');read_out.mkdir(parents=True,exist_ok=True)
 read_start=time.monotonic()
 with (read_out/'worker-output.txt').open('w') as log:
  process=subprocess.Popen([sys.executable,str(B/'merge_distribution_reads_r146.py'),mode],stdout=log,stderr=log)
  try:code=process.wait(timeout=60)
  except subprocess.TimeoutExpired:
   process.terminate()
   try:code=process.wait(timeout=5)
   except subprocess.TimeoutExpired:process.kill();code=process.wait()
  (read_out/'process-bound.json').write_text(json.dumps({'pid':process.pid,'exit_code':code,'worker_wall_s':time.monotonic()-read_start,'max_worker_wall_s':60},indent=2)+'\n')
  assert code==0,'Inspect correlated same-query history; no restart'
 rows=c.sql(mode+'-history',f'DESCRIBE HISTORY {A}')
 names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 history=[dict(zip(names,row)) for row in rows]
 (O/(mode+'-delta-history.json')).write_text(json.dumps(history,indent=2)+'\n')
 commit=next(r for r in history if int(r['version'])==new)
 metrics=json.loads(commit['operationMetrics'])
 assert commit['operation']=='MERGE'
 assert int(metrics['numSourceRows'])==100000 and int(metrics['numTargetRowsUpdated'])==100000
 assert all(int(metrics[k])==0 for k in ('numTargetRowsInserted','numTargetRowsDeleted','numTargetRowsCopied'))
 results[mode]={'table':A,'old_version':old,'new_version':new,'merge_metrics':metrics}
(O/'summary.json').write_text(json.dumps({'state':'Both actual MERGEs and full-carrier/identity/untouched custody checks passed; final query metrics pending','results':results,'qualification':'One sequential pair, synthetic property105 hot set. Both inherit E22 skipping statistics and use ON eligibility. Candidate adds only a16-way hash-range source repartition hint; no guarantee that optimizer retains it or writer preserves source ordering. No insert clauses. Full affected20-field values exact; unchanged19.9M row custody relies on stable schema and immutable Delta files. Canonical E23 and publication untouched. No publication p95 or production mixed-revision claim.'},indent=2)+'\n')
c.history();c.close();print('Actual paired MERGEs and preservation checks passed')

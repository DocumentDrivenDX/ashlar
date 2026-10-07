"""Paired actual update-only MERGEs on original E18 shallow clones."""
import json
from pathlib import Path
from driver_sql import DriverClient
from property_apply_queries import PropertyApply,COLS,lit
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_merge_pruning_r131'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';S=F+'.schedule_r128_1'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
results={}
for mode in ('control','candidate'):
 A=F+'.edge_merge_'+mode+'_r131'
 assert c.sql(mode+'-absence',f"SHOW TABLES IN {F} LIKE 'edge_merge_{mode}_r131'")==[]
 c.sql(mode+'-clone',f'CREATE TABLE {A} SHALLOW CLONE {E} VERSION AS OF 18')
 if mode=='candidate':
  c.sql(mode+'-stats',f"ALTER TABLE {A} SET TBLPROPERTIES ('delta.dataSkippingStatsColumns'='lookup_hash,source_system,rel_type_id,id,entity_version,apply_batch_id')")
  c.sql(mode+'-analyze',f'ANALYZE TABLE {A} COMPUTE DELTA STATISTICS')
 old=int(c.sql(mode+'-old',f'DESCRIBE HISTORY {A} LIMIT 1')[0][0])
 q=PropertyApply(A,S,old,12,'r128-b1','r123-b1','schedule-r128',9007199254741035)
 assert c.sql(mode+'-membership',q.membership())==[['100000','100000']]
 assert c.sql(mode+'-intended',q.intended())==[['0']]
 query=q.apply()
 if mode=='candidate':
  query=query.replace('WHEN MATCHED AND t.entity_version=', 'AND t.entity_version=').replace(' THEN UPDATE SET *',' WHEN MATCHED THEN UPDATE SET *')
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
(O/'summary.json').write_text(json.dumps({'state':'Both actual MERGEs and full-carrier/identity/untouched custody checks passed; final query metrics pending','results':results,'qualification':'One sequential pair, synthetic property105 hot set. Candidate changes both skipping statistics and placement of prior-version/batch eligibility predicates. No insert clauses. Full affected20-field values exact; unchanged19.9M row custody relies on stable schema and immutable Delta files. Canonical E19 and publication untouched. No publication p95 or production mixed-revision claim.'},indent=2)+'\n')
c.history();c.close();print('Actual paired MERGEs and preservation checks passed')

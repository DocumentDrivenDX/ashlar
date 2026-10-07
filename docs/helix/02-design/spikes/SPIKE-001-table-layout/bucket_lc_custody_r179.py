"""Full19.9M untouched LC physical custody plus audited100k exact changed rows."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_lc_custody_r179'
assert not (O/'statements.jsonl').exists(),'Inspect prior native IDs'
s=json.loads((B/'out/native/ashlar_bucket_update_r176/audited-summary.json').read_text());x=s['owned']['lc'];T=x['table'];U=s['stage'];assert x['old_version']==0 and x['version']==1
c=BoundedReads(O);deadline=time.monotonic()+180
state={'table':T,'id':x['id'],'before_version':0,'version':1,'stage':U,'stage_id':s['stage_id'],'stage_version':0}
def sql(label,q):
 assert time.monotonic()<deadline,'Controller admission deadline'
 return c.sql(label,q)
sql('timeout','SET STATEMENT_TIMEOUT=90')
rows=sql('detail','DESCRIBE DETAIL '+T);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==x['id'];assert sql('version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='1'
rows=sql('history','DESCRIBE HISTORY '+T);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];history=[dict(zip(names,r)) for r in rows];assert [int(r['version']) for r in history]==[1,0];assert history[0]['operation']=='MERGE' and history[0]['queryHistoryStatementId']==x['apply_query_id'];m=json.loads(history[0]['operationMetrics']);assert m['numTargetRowsCopied']=='0' and m['numTargetRowsUpdated']=='100000' and m['numTargetRowsInserted']=='0' and m['numTargetRowsDeleted']=='0'
rows=sql('stage-detail','DESCRIBE DETAIL '+U);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==s['stage_id'];assert sql('stage-version','DESCRIBE HISTORY '+U+' LIMIT 1')[0][0]=='0'
def untouched(v):
 return f'''SELECT b.source_system,b.rel_type_id,b.id,b._metadata.file_path,b._metadata.row_index FROM {T} VERSION AS OF {v} b LEFT ANTI JOIN (SELECT source_system,rel_type_id,id FROM {U} VERSION AS OF 0) k ON b.source_system=k.source_system AND b.rel_type_id=k.rel_type_id AND b.id=k.id'''
before=untouched(0);after=untouched(1)
assert sql('untouched-count',f'SELECT count(*) FROM ({after})')==[['19900000']]
assert sql('untouched-custody',f'SELECT count(*) FROM (({before} EXCEPT ALL {after}) UNION ALL ({after} EXCEPT ALL {before}))')==[['0']]
assert sql('identities',f'SELECT count(*),count(DISTINCT id) FROM {T} VERSION AS OF 1')==[['20000000','20000000']]
c.close()
for attempt in range(12):
 h={q['query_id']:q for q in c.history()}
 if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
 if attempt<11:time.sleep(5)
assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records)
state['costs']={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ('read_bytes','write_remote_bytes','spill_to_disk_bytes')};assert state['costs']['read_bytes']<=15000000000 and state['costs']['write_remote_bytes']==0
state['history']=history;state['state']='LC full19.9M unchanged physical custody and20M unique IDs passed;100k exact outputs audited r177';state['qualification']='Every unchanged full native identity/filepath/row_index is equal under immutable Delta files, stable schema and closed owned clone0/MERGE1 lineage. Combines with r176 exact20-field changed outputs; not a fresh all20M wide payload scan. No equivalent physical custody claim for bucket copied rows; final wide bucket proof is separate. No publisher, source authority, cold/latency or1B/5B admission.'
(O/'summary.json').write_text(json.dumps(state,indent=2)+'\n');print(json.dumps({'state':state['state'],'costs':state['costs']},indent=2))

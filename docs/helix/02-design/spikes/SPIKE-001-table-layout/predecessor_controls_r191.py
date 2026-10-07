"""Native predecessor scope and corruption controls; no mutations."""
import json,time,dataclasses
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS,PropertyApply
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_predecessor_controls_r191';assert not O.exists()
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_queue_r189';U=F+'.schedule_r189_1';c=BoundedReads(O);c.sql('timeout','SET STATEMENT_TIMEOUT=60')
q=PropertyApply(E,U,0,15,'r189-b1','r139-b1','schedule-r189',9007199254741101,eligibility_placement='on',input_ranges=6,scope_predecessor=True)
assert c.sql('target-version','DESCRIBE HISTORY '+E+' LIMIT 1')[0][0]=='0';assert c.sql('stage-version','DESCRIBE HISTORY '+U+' LIMIT 1')[0][0]=='0';assert c.sql('membership',q.membership())==[['100000','100000']]
key=c.sql('key',f'SELECT min(id) FROM {U} VERSION AS OF 0')[0][0];assert key.isdecimal()
query=q.intended();results={}
for name,sql,n in [('baseline',query,0),('wrong-version',dataclasses.replace(q,previous_entity_version=14).intended(),100000),('wrong-predecessor',dataclasses.replace(q,predecessor='wrong-r191').intended(),100000),('missing-target',query.replace(f'FROM {E} VERSION AS OF 0 b',f'FROM (SELECT * FROM {E} VERSION AS OF 0 WHERE id<>{key}) b'),1)]:
 assert c.sql(name,sql)==[[str(n)]];results[name]=n
for name,col,expression in [('retained','retained_json',"concat(retained_json,' ')"),('endpoint','target_type','target_type+1'),('version','entity_version','entity_version+1')]:
 stage='(SELECT '+','.join((f'CASE WHEN id={key} THEN {expression} ELSE {col} END AS {col}') if x==col else x for x in list(COLS)+['old_json'])+f' FROM {U} VERSION AS OF 0)'
 assert c.sql('corrupt-'+name,query.replace(U+' VERSION AS OF 0',stage))==[['1']];results['corrupt-'+name]=1
c.close()
for attempt in range(10):
 h={x['query_id']:x for x in c.history()}
 if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
 time.sleep(2)
assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records)
costs={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert costs['read_bytes']<=10000000000 and costs['write_remote_bytes']==0
(O/'summary.json').write_text(json.dumps({'state':'Predecessor scope baseline and six native counterexamples passed','results':results,'costs':costs,'baseline_query_id':next(r['statement_id'] for r in c.records if r['label']=='baseline'),'qualification':'Owned100k stage; full outer source check retained, missing/ineligible targets and field corruption fail. Separate source membership100k unique remains required. Read-only derived views; no source authority, publisher throughput or billion claim.'},indent=2)+'\n');print(json.dumps({'results':results,'costs':costs},indent=2))

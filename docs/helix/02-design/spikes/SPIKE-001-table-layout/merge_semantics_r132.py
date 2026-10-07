"""Native mixed-eligibility control for predicate placement; no production writes."""
import json
from pathlib import Path
from driver_sql import DriverClient
from property_apply_queries import COLS,PropertyApply,eq
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_merge_semantics_r132'
assert not (O/'statements.jsonl').exists(),'Inspect saved IDs before repeating'
F='client_dev.ashlar_entropy_20261006_r86';S=F+'.merge_stage_r132'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=120')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('stage-absence',f"SHOW TABLES IN {F} LIKE 'merge_stage_r132'")==[]
base=F+'.schedule_r128_1'
expressions={x:'b.'+x for x in COLS}
expressions.update(id='k',lookup_hash="CASE WHEN k=6 THEN NULL WHEN k=8 THEN 'key7' ELSE concat('key',cast(k AS STRING)) END",props_json="concat('{\"case\":',cast(k AS STRING),',\"updated\":true}')")
c.sql('stage',f"CREATE TABLE {S} AS SELECT {','.join(v+' AS '+k for k,v in expressions.items())} FROM (SELECT * FROM {base} VERSION AS OF 0 LIMIT 1) b CROSS JOIN (SELECT explode(sequence(1,8)) k)")
tables={}
for mode in ('control','candidate'):
 A=F+'.merge_semantics_'+mode+'_r132';tables[mode]=A
 assert c.sql(mode+'-absence',f"SHOW TABLES IN {F} LIKE 'merge_semantics_{mode}_r132'")==[]
 initial={x:'s.'+x for x in COLS}
 initial.update(rel_type_id='CASE WHEN id=7 THEN s.rel_type_id+1 ELSE s.rel_type_id END',entity_version='CASE WHEN id=2 THEN 11 WHEN id=4 THEN NULL ELSE 12 END',apply_batch_id="CASE WHEN id=3 THEN 'other' WHEN id=5 THEN NULL ELSE 'r123-b1' END",props_json="concat('{\"case\":',cast(id AS STRING),',\"updated\":false}')")
 c.sql(mode+'-create',f"CREATE TABLE {A} AS SELECT {','.join(v+' AS '+k for k,v in initial.items())} FROM {S} VERSION AS OF 0 s WHERE id<=7")
 q=PropertyApply(A,S,0,12,'r128-b1','r123-b1','schedule-r128',9007199254741035)
 query=q.apply()
 if mode=='candidate':query=query.replace('WHEN MATCHED AND t.entity_version=', 'AND t.entity_version=').replace(' THEN UPDATE SET *',' WHEN MATCHED THEN UPDATE SET *')
 c.sql(mode+'-merge',query)
 # Exactly eligible id1 takes every source field. Ineligible ids2..7 retain every old field.
 tests=['NOT('+eq('a.'+col,'b.'+col,col)+')' for col in COLS]
 expected=f'SELECT {",".join(COLS)} FROM {S} VERSION AS OF 0 WHERE id=1 UNION ALL SELECT {",".join(COLS)} FROM {A} VERSION AS OF 0 WHERE id<>1'
 assert c.sql(mode+'-exact',f"SELECT count(*) FROM {A} VERSION AS OF 1 a FULL OUTER JOIN ({expected}) b ON a.id=b.id WHERE a.id IS NULL OR b.id IS NULL OR "+' OR '.join(tests))==[['0']]
 assert c.sql(mode+'-counts',f'SELECT count(*),count(DISTINCT id),count_if(entity_version=13) FROM {A} VERSION AS OF 1')==[['7','7','1']]
fields=','.join(COLS)
a=tables['control'];b=tables['candidate']
assert c.sql('paired-parity',f'SELECT count(*) FROM ((SELECT {fields} FROM {a} VERSION AS OF 1 EXCEPT ALL SELECT {fields} FROM {b} VERSION AS OF 1) UNION ALL (SELECT {fields} FROM {b} VERSION AS OF 1 EXCEPT ALL SELECT {fields} FROM {a} VERSION AS OF 1))')==[['0']]
(O/'summary.json').write_text(json.dumps({'state':'Both predicate placements pass exact20-field expected output and paired parity','cases':{'1':'eligible update','2':'stale version','3':'wrong predecessor','4':'null version','5':'null predecessor','6':'null lookup hash','7':'unchanged existing compound identity','8':'unmatched ID sharing lookup hash with7; no insertion'},'tables':tables,'source':S,'versions':{'source':0,'target_before':0,'target_after':1},'qualification':'Bounded eight-input SQL semantics control, unique stage IDs and no insert branches. Nullable synthetic fields test three-valued SQL logic beyond canonical NOT NULL constraints. Not general multiwriter/fencing or performance admission.'},indent=2)+'\n')
c.history();c.close();print('Native mixed eligibility and exact carrier controls passed')

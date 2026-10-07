"""Read-only bounded exact journal keyed comparison and corruption controls."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from journal_validation_queries import COLS,expected,actual,symmetric
from journal_keyed_queries import keyed
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_journal_validation_r154'
assert not (O/'statements.jsonl').exists(),'Inspect prior handles; no replay'
F='client_dev.ashlar_entropy_20261006_r86';S=F+'.schedule_r139_1';J=F+'.property_journal_r89'
c=BoundedReads(O);c.sql('timeout','SET STATEMENT_TIMEOUT=90')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
left,right=expected(S),actual(J,19,'r139-b1')
for i in range(3):
 for mode in (('control','keyed') if i%2==0 else ('keyed','control')):
  query=symmetric(left,right) if mode=='control' else keyed(left,right)
  assert c.sql(mode+'-'+str(i),query)==[['0']]
# Tiny typed native journal fixture. Unicode and lexical string changes must stay distinguishable.
base="""SELECT 's' source_system,'edge' entity_kind,cast(7 AS BIGINT) type_id,cast(9 AS BIGINT) id,
 cast(105 AS BIGINT) property_id,cast(16 AS BIGINT) entity_version,'set' operation,true old_present,
 '"é"' old_json,true new_present,'"new"' new_json,'r1' schema_revision,'feed' source_feed,
 'epoch' source_epoch,cast(NULL AS BIGINT) source_position,cast(0 AS BIGINT) event_ordinal,
 cast(NULL AS STRING) source_time_text,cast('2026-10-06 12:00:00.123456' AS TIMESTAMP) published_at,
 'batch' apply_batch_id,'{"xid":"9007199254741101","seq":"9"}' source_cursor_json,'delivery' source_delivery_id"""
assert c.sql('fixture-equal',keyed(base,base))==[['0']]
assert int(c.sql('equal-duplicate-bags-refused',keyed(base+' UNION ALL '+base,base+' UNION ALL '+base))[0][0])>0
cases={'duplicate':base+' UNION ALL '+base,'missing':'SELECT * FROM ('+base+') WHERE false'}
changes={'old-null':{'old_json':'cast(NULL AS STRING)'},'missing-flag':{'old_present':'false'},
 'unicode-lexical':{'old_json':"concat(char(34),decode(unhex('65cc81'),'UTF-8'),char(34))"},
 'new-value':{'new_json':"concat(char(34),'different',char(34))"},'cursor':{'source_cursor_json':"'{}'"},
 'origin':{'source_epoch':"'other'"},'timestamp':{'published_at':"cast('2026-10-06 12:00:00.123457' AS TIMESTAMP)"},
 'event-ordinal':{'event_ordinal':'cast(1 AS BIGINT)'},'property-id':{'property_id':'cast(106 AS BIGINT)'}}
for name,patch in changes.items():cases[name]='SELECT '+','.join(patch.get(col,col)+' AS '+col for col in COLS)+' FROM ('+base+')'
refused=[]
for name,bad in cases.items():
 a=c.sql('refusal-control-'+name,symmetric(base,bad));z=c.sql('refusal-keyed-'+name,keyed(base,bad))
 assert int(a[0][0])>0 and int(z[0][0])>0;refused.append(name)
report={'state':'Three exact journal pairs and11 typed native corruption refusals passed; final metrics pending',
 'stage':S,'stage_version':0,'journal':J,'journal_version':19,'batch':'r139-b1','refused':refused,
 'qualification':'100k existing unique-event scoped journal rows, exact all21 fields/UTF8 and duplicate detection included; no digest substitution.11 tiny typed native controls, not all production semantics. No table writes or publication/sustained admission.'}
(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n');c.history();c.close();print('Journal comparison and corruption controls passed')

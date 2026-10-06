"""Synthetic property carrier check, not a native Truss feed adapter."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent; N='client_dev.ashlar_layout_v02_20261006_r65'
out=B/'out/native/ashlar_property_journal_v02_20261006_r69';c=Client(out)
T=N+'.property_journal_r69'
assert c.sql('absent',f"SHOW TABLES IN {N} LIKE 'property_journal_r69'")==[]
c.sql('create',f'CREATE TABLE {T} USING DELTA AS SELECT * FROM {N}.property_journal VERSION AS OF 2 WHERE false')
events=[(False,None,True,'null'),(True,'null',True,'9007199254740993'),(True,'9007199254740993',True,'1.2300e+04'),(True,'1.2300e+04',False,None),(False,None,True,'"2026-10-06T12:00:00+05:30"')]
def lit(v):return 'NULL' if v is None else "'"+v.replace("'","''")+"'"
rows=[]
for ordinal,(oldp,old,newp,new) in enumerate(events,1):
 rows.append("('pilot','node',1,1,101,5,'property',%s,%s,%s,%s,'r1','property-fixture','e',1,%d,'opaque-source-time',current_timestamp(),'r69:1')"%(str(oldp).lower(),lit(old),str(newp).lower(),lit(new),ordinal))
c.sql('events',f'INSERT INTO {T} VALUES '+','.join(rows))
actual=c.sql('exact',f'SELECT event_ordinal,old_present,old_json,new_present,new_json FROM {T} ORDER BY event_ordinal')
expected=[[str(i),str(a).lower(),b,str(d).lower(),e] for i,(a,b,d,e) in enumerate(events,1)]
assert actual==expected,(actual,expected)
assert c.sql('carrier-invariants',f'SELECT count_if((NOT old_present AND old_json IS NOT NULL) OR (old_present AND old_json IS NULL) OR (NOT new_present AND new_json IS NOT NULL) OR (new_present AND new_json IS NULL)),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {T}')==[['0','5']]
version=int(c.sql('version',f'DESCRIBE HISTORY {T} LIMIT 1')[0][0])
(out/'summary.json').write_text(json.dumps({'state':'passed','table':T,'version':version,'events':5,'scope':'Synthetic property journal storage preserves absence/null, integer above 2^53, exponent/decimal token and timezone text. entity_version=5 is a fixture boundary marker, not inferred native property ordering. No state reconstruction, producer feed, atomic publisher or retention policy claim.'},indent=2)+'\n')
print('Five exact property-journal carriers passed',flush=True)

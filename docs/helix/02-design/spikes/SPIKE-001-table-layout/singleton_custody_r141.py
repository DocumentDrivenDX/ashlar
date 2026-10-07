"""Exact rewritten carriers and immutable untouched custody after bounded timeout."""
import json
from pathlib import Path
from driver_sql import DriverClient
from property_apply_queries import COLS,TEXTS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_singleton_custody_r141'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';A=F+'.edge_optimize_r140';S=F+'.schedule_r139_1'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=90')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert int(c.sql('clone-version','DESCRIBE HISTORY '+A+' LIMIT 1')[0][0])==2
fields=','.join(f"hex(encode({col},'UTF-8')) AS {col}" if col in TEXTS else col for col in COLS)
expected=f'SELECT {fields} FROM {S} VERSION AS OF 0'
actual=f"SELECT {fields} FROM {A} VERSION AS OF 2 WHERE entity_version=15 AND apply_batch_id='r139-b1'"
assert c.sql('rewritten-count',f"SELECT count(*),count(DISTINCT id) FROM {A} VERSION AS OF 2 WHERE entity_version=15 AND apply_batch_id='r139-b1'")==[['100000','100000']]
assert c.sql('rewritten-exact',f'SELECT count(*) FROM (({expected} EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL {expected}))')==[['0']]
def untouched(table,version):
 return f'''SELECT b.source_system,b.rel_type_id,b.id,b._metadata.file_path,b._metadata.row_index
 FROM {table} VERSION AS OF {version} b LEFT ANTI JOIN {S} VERSION AS OF 0 s
 ON b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id'''
before=untouched(E,23);after=untouched(A,2)
assert c.sql('untouched-custody',f'SELECT count(*) FROM (({before} EXCEPT ALL {after}) UNION ALL ({after} EXCEPT ALL {before}))')==[['0']]
assert c.sql('identities',f'SELECT count(*),count(DISTINCT id) FROM {A} VERSION AS OF 2')==[['20000000','20000000']]
rows=c.sql('history','DESCRIBE HISTORY '+A);names=[col['name'] for col in c.records[-1]['response']['manifest']['schema']['columns']]
(O/'delta-history.json').write_text(json.dumps([dict(zip(names,row)) for row in rows],indent=2)+'\n')
(O/'summary.json').write_text(json.dumps({'state':'Exact100k rewritten carriers, full19.9M untouched immutable row custody and20M global identities passed','table':A,'source':E,'source_version':23,'versions':{'before':0,'after':2},'qualification':'All20 rewritten fields compared with UTF8 byte normalization. Unchanged keys/filepaths/row indices prove custody under stable schema and immutable Delta-file semantics, not fresh wide payload equality. Original full20M digest query timed out180s and remains failed evidence. No canonical publication change.'},indent=2)+'\n')
c.history();c.close();print('Exact rewritten carriers and untouched custody passed')

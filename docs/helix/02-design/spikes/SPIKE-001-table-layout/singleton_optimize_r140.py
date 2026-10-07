"""Incremental physical maintenance screen on an isolated full-scale clone."""
import json
from pathlib import Path
from driver_sql import DriverClient
from property_apply_queries import COLS
from wire_json import encode
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_singleton_optimize_r140'
assert not (O/'statements.jsonl').exists(),'Inspect saved live handle before repeating'
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';A=F+'.edge_optimize_r140';S=F+'.schedule_r139_1'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert int(c.sql('source-version',f'DESCRIBE HISTORY {E} LIMIT 1')[0][0])==23
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'edge_optimize_r140'")==[]
c.sql('clone',f'CREATE TABLE {A} SHALLOW CLONE {E} VERSION AS OF 23')
rows=c.sql('oracle',f"SELECT {','.join(COLS)} FROM {S} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 30")
assert len(rows)==30
versions={}
for phase in ('before','after'):
 if phase=='after':c.sql('optimize',f'OPTIMIZE {A}')
 v=int(c.sql(phase+'-version',f'DESCRIBE HISTORY {A} LIMIT 1')[0][0]);versions[phase]=v
 c.sql(phase+'-detail','DESCRIBE DETAIL '+A)
 query=f"SELECT {','.join(COLS)} FROM {A} VERSION AS OF {v} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
 for i,row in enumerate(rows):
  assert c.sql(f'{phase}-read-{i}',query,parameters={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]},tag=False)==[row]
assert c.sql('identities',f'SELECT count(*),count(DISTINCT id) FROM {A} VERSION AS OF {versions["after"]}')==[['20000000','20000000']]
carrier=encode('named_struct('+','.join("'"+col+"',"+col for col in COLS)+')')
def fingerprints(table,version):return f'SELECT source_system,rel_type_id,id,sha2({carrier},256) carrier_digest FROM {table} VERSION AS OF {version}'
before=fingerprints(E,23);after=fingerprints(A,versions['after'])
assert c.sql('full-carrier-digests',f'SELECT count(*) FROM (({before} EXCEPT ALL {after}) UNION ALL ({after} EXCEPT ALL {before}))')==[['0']]
history=c.sql('history','DESCRIBE HISTORY '+A);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
(O/'delta-history.json').write_text(json.dumps([dict(zip(names,row)) for row in history],indent=2)+'\n')
(O/'summary.json').write_text(json.dumps({'state':'60 exact singleton reads, full20M keyed SHA256 carrier parity and global IDs passed; final metrics pending','table':A,'source':E,'source_version':23,'versions':versions,'qualification':'One isolated original-table clone, incremental OPTIMIZE only, existing compute/180s statement bounds.30 SHA-ranked affected keys each phase, not all degree/entropy/read distributions. Full20M keyed20-field wire SHA256 comparison assumes collision resistance and injective scoped wire encoding; not fresh wide-payload EXCEPT ALL. Published canonical vector unchanged.'},indent=2)+'\n')
c.history();c.close();print('Incremental maintenance and singleton preservation checks passed')

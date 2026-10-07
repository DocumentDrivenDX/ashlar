"""Isolated statistics-only clone; exact eligible-key read pruning screen."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_write_pruning_r130'
assert not (O/'statements.jsonl').exists(),'Inspect handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';A=F+'.edge_stats_r130';S=F+'.schedule_r128_1'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'edge_stats_r130'")==[]
c.sql('clone',f'CREATE TABLE {A} SHALLOW CLONE {E} VERSION AS OF 19')
c.sql('stats-columns',f"ALTER TABLE {A} SET TBLPROPERTIES ('delta.dataSkippingStatsColumns'='lookup_hash,source_system,rel_type_id,id,entity_version,apply_batch_id')")
c.sql('analyze',f'ANALYZE TABLE {A} COMPUTE DELTA STATISTICS')
v=int(c.sql('clone-version',f'DESCRIBE HISTORY {A} LIMIT 1')[0][0])
for i in range(3):
 for mode in (('original','stats') if i%2==0 else ('stats','original')):
  table=E if mode=='original' else A;version=19 if mode=='original' else v
  query=f'''SELECT count(*) FROM {table} VERSION AS OF {version} t JOIN {S} VERSION AS OF 0 s
 ON t.lookup_hash=s.lookup_hash AND t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id
 AND t.entity_version=13 AND t.apply_batch_id='r128-b1' '''
  assert c.sql(f'eligible-{mode}-{i}',query)==[['100000']]
# Statistics-only maintenance must retain every logical key and immutable physical row.
fields='source_system,rel_type_id,id,_metadata.file_path,_metadata.row_index'
assert c.sql('immutable-custody',f'SELECT count(*) FROM ((SELECT {fields} FROM {E} VERSION AS OF 19 EXCEPT ALL SELECT {fields} FROM {A} VERSION AS OF {v}) UNION ALL (SELECT {fields} FROM {A} VERSION AS OF {v} EXCEPT ALL SELECT {fields} FROM {E} VERSION AS OF 19))')==[['0']]
rows=c.sql('history',f'DESCRIBE HISTORY {A}');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];(O/'delta-history.json').write_text(json.dumps([dict(zip(names,row)) for row in rows],indent=2)+'\n')
(O/'summary.json').write_text(json.dumps({'state':'6 eligible-key counts and full20M immutable row custody passed; final metrics pending','table':A,'version':v,'source_table':E,'source_version':19,'qualification':'Same physical files, added entity_version/apply_batch_id skipping statistics on isolated clone only. Explicit eligible-key SELECT joins, not actual MERGE performance or production mixed-revision admission. Stable logical schema and Delta immutable-file semantics assumed; no payload rewrite.'},indent=2)+'\n');c.history();c.close();print('Eligible-key pruning and immutable custody passed')

"""Bounded scattered structural updates and OPTIMIZE on an isolated clone."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_maintenance_r118'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';A=F+'.adjacency_maintenance_r118';S=F+'.adjacency_filesize_r117_16'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'adjacency_maintenance_r118'")==[]
c.sql('clone',f'CREATE TABLE {A} SHALLOW CLONE {S} VERSION AS OF 3')
predicate='pmod(xxhash64(edge_id),200)=0'
columns='source_system,rel_type_id,edge_id,source_type,source_id,target_type,target_id,structural_version'
destination='pmod(target_id-1+10,4000000)+1'
expected=f"SELECT source_system,CASE WHEN {predicate} THEN rel_type_id+1000 ELSE rel_type_id END rel_type_id,edge_id,source_type,source_id,CASE WHEN {predicate} THEN pmod({destination},32)+1 ELSE target_type END target_type,CASE WHEN {predicate} THEN {destination} ELSE target_id END target_id,CASE WHEN {predicate} THEN 2 ELSE structural_version END structural_version FROM {S} VERSION AS OF 3"
changed=int(c.sql('expected-change-count',f'SELECT count(*) FROM {S} VERSION AS OF 3 WHERE {predicate}')[0][0]);assert 90000<changed<110000
versions={};physical={}
def check(phase):
 v=int(c.sql(phase+'-version',f'DESCRIBE HISTORY {A} LIMIT 1')[0][0]);versions[phase]=v
 oracle=f'SELECT {columns} FROM {S} VERSION AS OF 3' if phase=='baseline' else expected
 assert c.sql(phase+'-parity',f'SELECT count(*) FROM ((SELECT {columns} FROM {A} VERSION AS OF {v} EXCEPT ALL {oracle}) UNION ALL ({oracle} EXCEPT ALL SELECT {columns} FROM {A} VERSION AS OF {v}))')==[['0']]
 assert c.sql(phase+'-identity',f'SELECT count(*),count(DISTINCT edge_id) FROM {A} VERSION AS OF {v}')==[['20000000','20000000']]
 assert c.sql(phase+'-pairs',f'SELECT count(DISTINCT struct(source_system,rel_type_id,source_type,source_id,target_type,target_id)) FROM {A} VERSION AS OF {v}')==[['20000000']]
 assert c.sql(phase+'-closure',f'SELECT count(*) FROM {A} VERSION AS OF {v} a LEFT JOIN {F}.object_current VERSION AS OF 0 n ON a.source_system=n.source_system AND a.target_type=n.type_id AND a.target_id=n.id WHERE n.id IS NULL')==[['0']]
 physical[phase]=c.sql(phase+'-files',f'SELECT source_system,count(DISTINCT _metadata.file_path) FROM {A} VERSION AS OF {v} GROUP BY source_system ORDER BY source_system')
 for source,hub in [('pilot',32),('other',160)]:
  for rel,edge in [(9005,4000000),(9085,14000000),(9155,22000000)]:
   where=f"source_system='{source}' AND source_type=1 AND source_id={hub} AND (rel_type_id>{rel} OR (rel_type_id={rel} AND edge_id>{edge}))"
   wanted=c.sql(f'oracle-{phase}-{source}-{rel}',f'SELECT {columns} FROM ({oracle}) WHERE {where} ORDER BY rel_type_id,edge_id LIMIT 100')
   assert len(wanted)==100
   for i in range(3):
    assert c.sql(f'page-{phase}-{source}-{rel}-{i}',f'SELECT {columns} FROM {A} VERSION AS OF {v} WHERE {where} ORDER BY rel_type_id,edge_id LIMIT 100')==wanted
check('baseline')
c.sql('update',f"UPDATE {A} SET rel_type_id=rel_type_id+1000,target_id={destination},target_type=pmod({destination},32)+1,structural_version=2 WHERE {predicate}")
check('updated')
c.sql('optimize',f'OPTIMIZE {A} FULL')
check('optimized')
assert c.sql('source-unchanged',f'SELECT count(*) FROM {S} VERSION AS OF 3 WHERE structural_version<>1')==[['0']]
(O/'summary.json').write_text(json.dumps({'state':'full parity and54 exact full-shape pages passed; final audit pending','table':A,'source_table':S,'source_version':3,'versions':versions,'changed_edges':changed,'hub_files':physical,'qualification':'20M higher-entropy synthetic edges; isolated16MiB clone, hash-scattered valid destination+10 modulo4M moves, source preserved, disjoint new relationship labels+1000. Full unique pairs and typed target closure at each phase. Oracle projects pinned original, independent of mutated clone. No canonical publication, producer ingest, reverse layout, high-entropy capacity or billion admission. OPTIMIZE FULL; no source producer authority or catalog binding.'},indent=2)+'\n')
c.history();c.close();print('Completed maintenance parity and54 pages')

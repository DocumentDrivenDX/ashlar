"""Matched16/64MiB narrow adjacency maintenance comparison."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_filesize_r117'
assert not (O/'statements.jsonl').exists(),'Inspect handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';S=F+'.adjacency_entropy_r116'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'adjacency_filesize_r117_*'")==[]
tables={};versions={};details={};files={}
for mib in (16,64):
 key=str(mib);a=F+'.adjacency_filesize_r117_'+key;tables[key]=a
 c.sql('clone-'+key,f'CREATE TABLE {a} SHALLOW CLONE {S} VERSION AS OF 8')
 c.sql('target-'+key,f"ALTER TABLE {a} SET TBLPROPERTIES ('delta.targetFileSize'='{mib*1048576}')")
 c.sql('optimize-'+key,f'OPTIMIZE {a} FULL')
 v=int(c.sql('version-'+key,f'DESCRIBE HISTORY {a} LIMIT 1')[0][0]);versions[key]=v
 assert c.sql('parity-'+key,f'SELECT count(*) FROM ((SELECT * FROM {a} VERSION AS OF {v} EXCEPT ALL SELECT * FROM {S} VERSION AS OF 8) UNION ALL (SELECT * FROM {S} VERSION AS OF 8 EXCEPT ALL SELECT * FROM {a} VERSION AS OF {v}))')==[['0']]
 rows=c.sql('detail-'+key,f'DESCRIBE DETAIL {a}');cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];details[key]=dict(zip(cols,rows[0]))
 files[key]=c.sql('file-sizes-'+key,f'SELECT _metadata.file_path,min(_metadata.file_size),max(rel_type_id),min(rel_type_id),count(*) FROM {a} VERSION AS OF {v} GROUP BY _metadata.file_path ORDER BY _metadata.file_path')
 rows=c.sql('history-'+key,f'DESCRIBE HISTORY {a}');cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];(O/('delta-history-'+key+'.json')).write_text(json.dumps([dict(zip(cols,r)) for r in rows],indent=2)+'\n')
for source,hub in [('pilot',32),('other',160)]:
 for rel,edge in [(9000,4000000),(9080,14000000),(9150,22000000)]:
  where=f"source_system='{source}' AND source_type=1 AND source_id={hub} AND (rel_type_id>{rel} OR (rel_type_id={rel} AND edge_id>{edge}))"
  expected=c.sql(f'oracle-{source}-{rel}',f'SELECT * FROM {S} VERSION AS OF 8 WHERE {where} ORDER BY rel_type_id,edge_id LIMIT 100');assert len(expected)==100
  for i in range(3):
   for key in (('16','64') if i%2==0 else ('64','16')):
    assert c.sql(f'page-{key}-{source}-{rel}-{i}',f'SELECT * FROM {tables[key]} VERSION AS OF {versions[key]} WHERE {where} ORDER BY rel_type_id,edge_id LIMIT 100')==expected
(O/'summary.json').write_text(json.dumps({'state':'matched FULL maintenance parity and36 exact pages passed; final metrics pending','tables':tables,'versions':versions,'details':details,'files':files,'source':{'table':S,'version':8},'qualification':'Same20M eight-column higher-entropy synthetic graph, identical reference clustering, pinned source oracles, separate shallow clones and explicit OPTIMIZE FULL both targets. Actual target realization must be observed. No production distribution, ingestion publication, cold/concurrent SLO or billion admission.'},indent=2)+'\n')
c.history();c.close();print('Completed matched16/64MiB adjacency comparison')

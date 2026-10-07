"""20M exact r110 graph in eight append batches per layout; verify actual files."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_ordering_r111'
assert not (O/'statements.jsonl').exists(),'Inspect handles and tables before any repeated CTAS'
F='client_dev.ashlar_entropy_20261006_r86'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'adjacency_allhub_r111_*'")==[]
projection=f'SELECT * FROM {F}.adjacency_allhub_r110_endpoint VERSION AS OF 0'
tables={};versions={}
for mode,keys in [('endpoint','source_system,source_type,source_id'),('ordered','source_system,source_type,source_id,edge_id')]:
 a=F+'.adjacency_allhub_r111_'+mode;tables[mode]=a
 c.sql('create-'+mode,f"CREATE TABLE {a} USING DELTA CLUSTER BY ({keys}) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd','delta.autoOptimize.autoCompact'='false','delta.autoOptimize.optimizeWrite'='false') AS {projection} WHERE false")
 for batch in range(8):
  lower=4000000+batch*2500000;upper=lower+2500000
  c.sql(f'append-{mode}-{batch}',f'INSERT INTO {a} {projection} WHERE edge_id>{lower} AND edge_id<={upper}')
 version=int(c.sql('version-'+mode,f'DESCRIBE HISTORY {a} LIMIT 1')[0][0])
 versions[mode]=version
 files=c.sql('hub-file-count-'+mode,f'SELECT source_system,count(DISTINCT _metadata.file_path) FROM {a} VERSION AS OF {version} GROUP BY source_system ORDER BY source_system')
 assert len(files)==2 and all(int(row[1])>=2 for row in files), 'Intended multi-file hub not achieved: stop before query qualification'
 assert c.sql('parity-'+mode,f'SELECT count(*) FROM ((SELECT * FROM {a} VERSION AS OF {version} EXCEPT ALL {projection}) UNION ALL ({projection} EXCEPT ALL SELECT * FROM {a} VERSION AS OF {version}))')==[['0']]
a=tables['endpoint'];version=versions['endpoint']
assert c.sql('identity',f'SELECT count(*),count(DISTINCT edge_id),count(DISTINCT struct(source_system,rel_type_id,source_type,source_id,target_type,target_id)) FROM {a} VERSION AS OF {version}')==[['20000000','20000000','20000000']]
assert c.sql('closure',f'''SELECT count(*) FROM {a} VERSION AS OF {version} a LEFT JOIN {F}.object_current VERSION AS OF 0 s ON a.source_system=s.source_system AND a.source_type=s.type_id AND a.source_id=s.id
LEFT JOIN {F}.object_current VERSION AS OF 0 t ON a.source_system=t.source_system AND a.target_type=t.type_id AND a.target_id=t.id WHERE s.id IS NULL OR t.id IS NULL''')==[['0']]
for source,hub in [('pilot',32),('other',160)]:
 for cursor in (4000000,14000000,22000000,24000000):
  expected=[];ordinal=cursor-4000000+1
  while ordinal<=20000000 and len(expected)<100:
   if (ordinal%10!=0)==(source=='pilot'):
    expected.append([str(4000000+ordinal),str(9000+(ordinal%32+1)*5+(ordinal-1)//4000000),str(ordinal%32+1),str((ordinal-1)%4000000+1)])
   ordinal+=1
  for i in range(3):
   for mode in (('endpoint','ordered') if i%2==0 else ('ordered','endpoint')):
    rows=c.sql(f'page-{source}-{cursor}-{mode}-{i}',f"SELECT edge_id,rel_type_id,target_type,target_id FROM {tables[mode]} VERSION AS OF {versions[mode]} WHERE source_system='{source}' AND source_type=1 AND source_id={hub} AND edge_id>{cursor} ORDER BY edge_id LIMIT 100")
    assert rows==expected,(source,cursor,mode)
details={}
for mode,a in tables.items():
 rows=c.sql('detail-'+mode,'DESCRIBE DETAIL '+a);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];details[mode]=dict(zip(cols,rows[0]))
(O/'summary.json').write_text(json.dumps({'state':'batched paired 20M multi-file hub tables and 48 exact page checks passed; history audit pending','tables':tables,'versions':versions,'degrees':{'pilot32':18000000,'other160':2000000},'details':details,'qualification':'Identical seven-column synthetic all-hub graph. Full independent projection parity both tables; typed closure/global edge IDs/unique relation-endpoint pairs. Not canonical graph, publication/ingest or billion-scale admission. Three repetitions per query are descriptive samples.'},indent=2)+'\n')
c.history();c.close();print('Completed batched multi-file hub ordering comparison')

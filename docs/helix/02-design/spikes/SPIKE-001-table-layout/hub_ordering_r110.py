"""20M all-hub synthetic adjacency: paired physical clustering candidates."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_ordering_r110'
assert not (O/'statements.jsonl').exists(),'Inspect handles and tables before any repeated CTAS'
F='client_dev.ashlar_entropy_20261006_r86'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'adjacency_allhub_r110_*'")==[]
projection='''SELECT CASE WHEN pmod(ordinal,10)=0 THEN 'other' ELSE 'pilot' END source_system,
ordinal+4000000 edge_id,cast(9000+(pmod(ordinal,32)+1)*5+cast(floor((ordinal-1)/4000000) AS BIGINT) AS BIGINT) rel_type_id,
cast(1 AS BIGINT) source_type,cast(CASE WHEN pmod(ordinal,10)=0 THEN 160 ELSE 32 END AS BIGINT) source_id,
cast(pmod(ordinal,32)+1 AS BIGINT) target_type,pmod(ordinal-1,4000000)+1 target_id
FROM (SELECT id+1 ordinal FROM range(20000000))'''
tables={}
for mode,keys in [('endpoint','source_system,source_type,source_id'),('ordered','source_system,source_type,source_id,edge_id')]:
 a=F+'.adjacency_allhub_r110_'+mode;tables[mode]=a
 c.sql('create-'+mode,f"CREATE TABLE {a} USING DELTA CLUSTER BY ({keys}) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd') AS {projection}")
 assert c.sql('version-'+mode,f'DESCRIBE HISTORY {a} LIMIT 1')[0][0]=='0'
 assert c.sql('parity-'+mode,f'SELECT count(*) FROM ((SELECT * FROM {a} VERSION AS OF 0 EXCEPT ALL {projection}) UNION ALL ({projection} EXCEPT ALL SELECT * FROM {a} VERSION AS OF 0))')==[['0']]
a=tables['endpoint']
assert c.sql('identity',f'SELECT count(*),count(DISTINCT edge_id),count(DISTINCT struct(source_system,rel_type_id,source_type,source_id,target_type,target_id)) FROM {a} VERSION AS OF 0')==[['20000000','20000000','20000000']]
assert c.sql('closure',f'''SELECT count(*) FROM {a} VERSION AS OF 0 a LEFT JOIN {F}.object_current VERSION AS OF 0 s ON a.source_system=s.source_system AND a.source_type=s.type_id AND a.source_id=s.id
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
    rows=c.sql(f'page-{source}-{cursor}-{mode}-{i}',f"SELECT edge_id,rel_type_id,target_type,target_id FROM {tables[mode]} VERSION AS OF 0 WHERE source_system='{source}' AND source_type=1 AND source_id={hub} AND edge_id>{cursor} ORDER BY edge_id LIMIT 100")
    assert rows==expected,(source,cursor,mode)
details={}
for mode,a in tables.items():
 rows=c.sql('detail-'+mode,'DESCRIBE DETAIL '+a);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];details[mode]=dict(zip(cols,rows[0]))
(O/'summary.json').write_text(json.dumps({'state':'paired 20M hub tables and 48 exact page checks passed; history audit pending','tables':tables,'versions':{'endpoint':0,'ordered':0},'degrees':{'pilot32':18000000,'other160':2000000},'details':details,'qualification':'Identical seven-column synthetic all-hub graph. Full independent projection parity both tables; typed closure/global edge IDs/unique relation-endpoint pairs. Not canonical graph, publication/ingest or billion-scale admission. Three repetitions per query are descriptive samples.'},indent=2)+'\n')
c.history();c.close();print('Completed paired 20M hub ordering comparison')

"""Bounded physical key screen, identical100k full carriers; not scale admission."""
import json
from pathlib import Path
from driver_sql import DriverClient
from property_apply_queries import COLS,TEXTS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_key_screen_r147'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';S=F+'.schedule_r139_1'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=90')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
report={'state':'running bounded key screen','tables':{},'qualification':'100k affected hot-set full20-field carriers only, identical input and16MiB maintenance target/Zstd. One build/OPTIMIZE FULL each; not full20M layout, incremental MERGE/publication, source/type diversity or1B/5B qualification.'}
for mode,keys in [('hash',['lookup_hash']),('source_id',['source_system','id'])]:
 A=F+'.key_'+mode+'_r147'
 assert c.sql(mode+'-absence',f"SHOW TABLES IN {F} LIKE 'key_{mode}_r147'")==[]
 c.sql(mode+'-build',f"CREATE TABLE {A} USING DELTA CLUSTER BY ({','.join(keys)}) TBLPROPERTIES ('delta.parquet.compression.codec'='zstd','delta.targetFileSize'='16777216','delta.dataSkippingStatsColumns'='lookup_hash,source_system,rel_type_id,id') AS SELECT {','.join(COLS)} FROM {S} VERSION AS OF 0")
 c.sql(mode+'-optimize',f'OPTIMIZE {A} FULL')
 v=int(c.sql(mode+'-version','DESCRIBE HISTORY '+A+' LIMIT 1')[0][0])
 fields=','.join(f"hex(encode({col},'UTF-8')) AS {col}" if col in TEXTS else col for col in COLS)
 expected=f'SELECT {fields} FROM {S} VERSION AS OF 0';actual=f'SELECT {fields} FROM {A} VERSION AS OF {v}'
 assert c.sql(mode+'-exact',f'SELECT count(*) FROM (({expected} EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL {expected}))')==[['0']]
 assert c.sql(mode+'-identities',f'SELECT count(*),count(DISTINCT id) FROM {A} VERSION AS OF {v}')==[['100000','100000']]
 detail=c.sql(mode+'-detail','DESCRIBE DETAIL '+A);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 report['tables'][mode]={'table':A,'version':v,'keys':keys,'detail':dict(zip(names,detail[0]))}
 (O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
report['state']='Both key-screen tables pass exact100k full-carrier preservation/IDs; bounded read comparison pending'
(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n');c.history();c.close();print(report['state'])

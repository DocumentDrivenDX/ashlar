"""Fresh-file reads qualify as cold only when native metrics show remote bytes."""
import json,math
from pathlib import Path
from driver_sql import DriverClient as Client
BASE=Path(__file__).resolve().parent;schema='ashlar_cold_20261005_d2';F='client_dev.'+schema;out=BASE/'out/native'/schema;c=Client(out)
c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar synthetic fresh-file cold I/O probe'")
c.sql('environment','SELECT current_version()')
for name,size in [('liquid16',16777216),('liquid128',134217728)]:
 c.sql('create-'+name,f"CREATE TABLE {F}.{name} USING DELTA CLUSTER BY (id) TBLPROPERTIES ('delta.dataSkippingStatsColumns'='id','delta.targetFileSize'='{size}') AS SELECT * FROM client_dev.ashlar_ingest_20261005_a2.serving VERSION AS OF 3")
 c.sql('optimize-'+name,f'OPTIMIZE {F}.{name} FULL')
 c.sql('detail-'+name,f'DESCRIBE DETAIL {F}.{name}')
 # First data reads occur here; metadata queries above do not validate coldness.
 for rep in range(32):
  key=1+(rep*18749)%600000
  rows=c.sql(f'first-{name}-{rep}',f'SELECT id,props_json,retained_json FROM {F}.{name} WHERE id={key}')
  assert len(rows)==1 and rows[0][0]==str(key)
 print('first-touch complete',name,flush=True)
for name in ['liquid16','liquid128']:
 assert c.sql('parity-'+name,f'SELECT count(*) FROM {F}.{name} t FULL OUTER JOIN client_dev.ashlar_ingest_20261005_a2.serving VERSION AS OF 3 s ON t.id=s.id WHERE t.id IS NULL OR s.id IS NULL OR t.props_json<>s.props_json OR t.retained_json<>s.retained_json')==[['0']]
 for rep in range(32):
  key=1+(rep*18749)%600000
  rows=c.sql(f'warm-{name}-{rep}',f'SELECT id,props_json,retained_json FROM {F}.{name} WHERE id={key}')
  assert rows[0][0]==str(key)
 print('warm complete',name,flush=True)
c.history();print('Reads completed; refresh async metrics and summarize native remote I/O qualification',flush=True)

c.close()

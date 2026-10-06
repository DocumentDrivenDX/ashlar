"""Compare equal-carrier ordinary versus catalog-managed liquid-id tables."""
import json
from pathlib import Path
from driver_sql import DriverClient
BASE=Path(__file__).resolve().parent;schema='ashlar_catalog_lookup_20261005_g2';F='client_dev.'+schema;out=BASE/'out/native'/schema;c=DriverClient(out)
c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar isolated catalog-metadata singleton candidate'")
c.sql('catalog-table',f"CREATE TABLE {F}.objects USING DELTA CLUSTER BY (id) TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.dataSkippingStatsColumns'='source_system,type_id,id','delta.targetFileSize'='16777216') AS SELECT * FROM client_dev.ashlar_pruning_20261005_c1.liquid_16m")
c.sql('optimize',f'OPTIMIZE {F}.objects FULL')
assert c.sql('parity',f'SELECT count(*) FROM {F}.objects t FULL OUTER JOIN client_dev.ashlar_pruning_20261005_c1.liquid_16m s ON t.id=s.id WHERE t.id IS NULL OR s.id IS NULL OR t.props_json<>s.props_json OR t.retained_json<>s.retained_json')==[['0']]
c.sql('detail',f'DESCRIBE DETAIL {F}.objects')
for rep in range(51):
 key=1+(rep*104729)%600000;expected=None
 for name,table in [('ordinary','client_dev.ashlar_pruning_20261005_c1.liquid_16m'),('catalog',F+'.objects')]:
  rows=c.sql(f'{name}-{rep}',f"SELECT id,props_json,retained_json FROM {table} WHERE source_system='pilot' AND type_id=1 AND id={key}");assert len(rows)==1 and rows[0][0]==str(key)
  if expected is None:expected=rows
  else:assert expected==rows
 if rep%10==0:print('round',rep,flush=True)
c.history();c.close();print('Catalog lookup comparison complete',flush=True)

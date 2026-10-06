"""Bounded warm uncached-result persistent-client latency controls."""
import json,math,time
from importlib.metadata import version
from pathlib import Path
from persistent_sql import Client
BASE=Path(__file__).resolve().parent
schema='ashlar_floor_20261005_b1';F='client_dev.'+schema;out=BASE/'out/native'/schema;c=Client(out)
c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar synthetic latency control'")
c.sql('environment','SELECT current_version()')
c.sql('tiny',f"CREATE TABLE {F}.tiny USING DELTA AS SELECT cast(1 AS BIGINT) id,'{{\"101\":9223372036854775807}}' props_json")
measurements=[]
# Round-robin avoids attributing time drift to a sequential table run.
for rep in range(26):
 key=1+(rep*7919)%600000
 for name,statement in [
  ('no_table','SELECT 1,uuid()'),
  ('tiny',f'SELECT id,props_json,uuid() FROM {F}.tiny WHERE id=1'),
  ('typed_repetitive',f'SELECT id,props_json,uuid() FROM client_dev.ashlar_spike_20261005_9586d0.type_a_l WHERE id={key}'),
  ('typed_varied_pinned',f'SELECT id,props_json,uuid() FROM client_dev.ashlar_ingest_20261005_a2.serving VERSION AS OF 3 WHERE id={key}')]:
  rows=c.sql(f'{name}-{rep}',statement)
  assert len(rows)==1
  if name=='no_table':assert rows[0][0]=='1'
  elif name=='tiny':assert rows[0][1]=='{"101":9223372036854775807}'
  else:assert rows[0][0]==str(key)
  if rep:measurements.append(c.records[-1])
 print('round',rep,flush=True)
history=c.history()
print('Statements complete; refresh asynchronous metrics if needed and run summarize_floor.py',flush=True)

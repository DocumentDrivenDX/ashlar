"""Tiny isolated catalog-commit/atomic rollback probe; no workspace preview change."""
import json
from pathlib import Path
from persistent_sql import Client
BASE=Path(__file__).resolve().parent;schema='ashlar_atomic_20261005_g1';F='client_dev.'+schema;out=BASE/'out/native'/schema;c=Client(out)
summary={'schema':F,'scope':'two tiny synthetic tables; not publisher, performance or external-reader conformance'}
def save(): (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar isolated beta catalog commit availability and rollback probe'")
try:
 c.sql('create-objects',f"CREATE TABLE {F}.objects (id BIGINT,props_json STRING,retained_json STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
 c.sql('create-journal',f"CREATE TABLE {F}.journal (id BIGINT,old_json STRING,new_json STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
except RuntimeError:
 summary.update(state='unavailable',error=c.records[-1]['response']['status']);save();raise SystemExit(0)
for name in ['objects','journal']:
 rows=c.sql('detail-'+name,f'DESCRIBE DETAIL {F}.{name}');cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(cols,rows[0]));assert 'catalogManaged' in d['tableFeatures'];summary[name+'_features']=d['tableFeatures']
old='{"101":9223372036854775807,"102":12345678901234567890.123456789}'
new='{"101":9223372036854775807,"102":null}'
retained='{"future":{"keep":[null,true,"x"]}}'
c.sql('seed',f"INSERT INTO {F}.objects VALUES (1,'{old}','{retained}')")
try:
 c.sql('intentional-rollback',f"BEGIN ATOMIC UPDATE {F}.objects SET props_json='{new}' WHERE id=1; INSERT INTO {F}.journal VALUES (1,'{old}','{new}'); SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ashlar intentional rollback'; END")
 raise AssertionError('Intentional transaction unexpectedly succeeded')
except RuntimeError:
 error=c.records[-1]['response']['status'];assert 'ashlar intentional rollback' in json.dumps(error),error
assert c.sql('verify-rollback-object',f'SELECT props_json,retained_json FROM {F}.objects WHERE id=1')==[[old,retained]]
assert c.sql('verify-rollback-journal',f'SELECT count(*) FROM {F}.journal')==[['0']]
c.sql('atomic-success',f"BEGIN ATOMIC UPDATE {F}.objects SET props_json='{new}' WHERE id=1; INSERT INTO {F}.journal VALUES (1,'{old}','{new}'); END")
assert c.sql('verify-commit-object',f'SELECT props_json,retained_json FROM {F}.objects WHERE id=1')==[[new,retained]]
assert c.sql('verify-commit-journal',f'SELECT old_json,new_json FROM {F}.journal WHERE id=1')==[[old,new]]
summary.update(state='passed',intentional_multi_table_rollback=True,multi_table_commit=True,exact_stored_carriers=True);save();c.history()

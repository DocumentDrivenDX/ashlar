"""Typed role extraction from exact immutable synthetic change source."""
from mixed_changes_r228 import Changes
INTS={'type_id','rel_type_id','id','source_type','source_id','target_type','target_id','entity_version','root_id','source_position','property_id','event_ordinal'}
BOOLS={'old_present','new_present'};TIMES={'published_at','received_at'}
def sample(role):
 c=Changes();x=c.change(1)
 return x['after'] if role=='current' else x['raw'] if role=='source_record' else x['events'][0] if role=='property_journal' else c.change(0)['tombstone']
def schema(row):return 'STRUCT<'+','.join(k+':'+('BOOLEAN' if k in BOOLS else 'STRING') for k in row)+'>'
def fields_sql(row,prefix='r'):
 return ','.join(f"cast({prefix}.{k} AS {'BIGINT' if k in INTS else 'BOOLEAN' if k in BOOLS else 'TIMESTAMP' if k in TIMES else 'STRING'}) AS {k}" for k in row)
def extract(role,table,version=8):
 row=sample(role);key={'current':'after','source_record':'raw','property_journal':'events','tombstone':'tombstone'}[role];sch=schema(row)
 if role=='property_journal':return f"SELECT {fields_sql(row)} FROM (SELECT explode(from_json(get_json_object(change_json,'$.{key}'),'ARRAY<{sch}>')) r FROM {table} VERSION AS OF {version})"
 return f"SELECT {fields_sql(row)} FROM (SELECT from_json(get_json_object(change_json,'$.{key}'),'{sch}') r FROM {table} VERSION AS OF {version}) WHERE r IS NOT NULL"
def mutation_source(table,version=8):
 row=sample('current');return f"SELECT cast(get_json_object(change_json,'$.before.rel_type_id') AS BIGINT) rel_type_id,cast(get_json_object(change_json,'$.before.id') AS BIGINT) id,get_json_object(change_json,'$.before.source_system') source_system,get_json_object(change_json,'$.before.lookup_hash') lookup_hash,from_json(get_json_object(change_json,'$.after'),'{schema(row)}') after FROM {table} VERSION AS OF {version}"
def row_hash_sql(fields):
 parts=[]
 for f in fields:
  value=f'CAST({f} AS STRING)';parts.append(f"CASE WHEN {f} IS NULL THEN 'N;' ELSE concat('V',cast(length(encode({value},'UTF-8')) AS STRING),':',hex(encode({value},'UTF-8')),';') END")
 return 'sha2(concat('+','.join(parts)+'),256)'
def row_hash(row,fields):
 import hashlib
 encoded=''
 for f in fields:
  v=row[f]
  if v is None:encoded+='N;';continue
  v=v.replace('T',' ').removesuffix('Z') if f in TIMES else str(v).lower() if type(v) is bool else str(v);data=v.encode();encoded+='V'+str(len(data))+':'+data.hex().upper()+';'
 return hashlib.sha256(encoded.encode()).hexdigest()

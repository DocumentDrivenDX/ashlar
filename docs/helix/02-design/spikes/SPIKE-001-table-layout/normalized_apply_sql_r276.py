"""Bounded synthetic publisher SQL over pinned normalized Delta inputs.
No execution on import. Not a real Truss producer or concurrency fence.
"""
import re
from normalized_input_sql_r264 import role_row
from mixed_change_queries_r230 import schema,fields_sql,row_hash_sql

def pin(table,version):
 if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',table) or type(version) is not int or version<0:raise ValueError('Explicit three-part Delta table and nonnegative version required')
 return f'{table} VERSION AS OF {version}'

def predecessor(table,version):
 row=role_row('current_replacement')
 return f"SELECT {fields_sql(row)},input_json,delivery_id AS change_delivery_id FROM (SELECT input_json,delivery_id,from_json(get_json_object(payload_json,'$.before_carrier'),'{schema(row)}',map('mode','FAILFAST','allowSingleQuotes','false','allowNonNumericNumbers','false')) r FROM {pin(table,version)})"

def predecessor_check(raw_table,raw_version,edge_table,edge_version):
 fields=list(role_row('current_replacement'));lhs=row_hash_sql(['s.'+f for f in fields]);rhs=row_hash_sql(['b.'+f for f in fields])
 return f"SELECT count(*),count_if(b.id IS NULL OR {lhs} <> {rhs}) FROM ({predecessor(raw_table,raw_version)}) s LEFT JOIN {pin(edge_table,edge_version)} b ON b.lookup_hash=s.lookup_hash AND b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id"

def mutation_source(raw_table,raw_version,current_table,current_version):
 # Source stage must first prove all unique delivery links, original/known field
 # digests and raw SHA. Missing replacement denotes synthetic deletion only
 # after exact tombstone/current partition and operation checks by controller.
 fields=list(role_row('current_replacement'))
 values=','.join('a.'+f+' AS after_'+f for f in fields)
 return f"SELECT b.*,{values},a.id IS NULL AS is_delete FROM ({predecessor(raw_table,raw_version)}) b LEFT JOIN {pin(current_table,current_version)} a ON b.source_feed=a.source_feed AND b.source_epoch=a.source_epoch AND b.change_delivery_id=a.source_delivery_id"

def current_merge(target,raw_table,raw_version,current_table,current_version):
 pin(target,0);fields=list(role_row('current_replacement'));source=mutation_source(raw_table,raw_version,current_table,current_version)
 assignments=','.join(f'b.{f}=s.after_{f}' for f in fields)
 return f"MERGE INTO {target} b USING ({source}) s ON b.lookup_hash=s.lookup_hash AND b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id AND b.entity_version=1 AND b.apply_batch_id='mixed-bootstrap' WHEN MATCHED AND s.is_delete THEN DELETE WHEN MATCHED THEN UPDATE SET {assignments}"

def adjacency_merge(target,raw_table,raw_version,current_table,current_version):
 pin(target,0);source=mutation_source(raw_table,raw_version,current_table,current_version)
 return f"MERGE INTO {target} b USING ({source}) s ON b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id AND b.entity_version=1 WHEN MATCHED AND s.is_delete THEN DELETE WHEN MATCHED THEN UPDATE SET b.entity_version=s.after_entity_version"

def append(target,role,input_table,input_version):
 pin(target,0)
 if role not in ('source_record','property_journal','tombstone'):raise ValueError('Append-only normalized role required')
 fields=','.join(role_row(role))
 return f'INSERT INTO {target} ({fields}) SELECT {fields} FROM {pin(input_table,input_version)}'

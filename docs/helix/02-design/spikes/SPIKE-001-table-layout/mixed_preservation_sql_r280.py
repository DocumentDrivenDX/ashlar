"""Complete intermediate publisher preservation queries; no native execution."""
import re
from normalized_apply_sql_r276 import pin,predecessor
from mixed_change_queries_r230 import row_hash_sql
from pure_group_blocks_r254 import pure_bucket_sql
from mixed_grouped_pruning_r253 import pruned_mixed_query

def checked_fields(fields):
 if not fields or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',f) for f in fields):raise ValueError('Explicit simple known fields required')
 return fields

def unchanged_groups(table,version,fields,raw_table,raw_version,first,groups=100):
 checked_fields(fields)
 if type(first) is not int or first not in (0,100,200,300) or groups!=100:raise ValueError('Four exhaustive intermediate blocks required')
 bucket=re.sub(r'\bid\b','e.id',pure_bucket_sql(8000000,40000000))
 digest=row_hash_sql(['e.'+f for f in fields]);selected=predecessor(raw_table,raw_version)
 low=8000000+first*100000;high=8000000+(first+groups)*100000
 return f"SELECT bucket,count(*),sha2(concat_ws('',sort_array(collect_list(row_digest))),256) FROM (SELECT {bucket} bucket,{digest} row_digest FROM {pin(table,version)} e LEFT ANTI JOIN ({selected}) s ON e.source_system=s.source_system AND e.rel_type_id=s.rel_type_id AND e.id=s.id WHERE e.id>{low} AND e.id<={high}) GROUP BY bucket ORDER BY bucket"

def changed_digest(table,version,fields,raw_table,raw_version):
 checked_fields(fields);digest=row_hash_sql(['e.'+f for f in fields])
 return f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({digest}))),256) FROM {pin(table,version)} e INNER JOIN ({predecessor(raw_table,raw_version)}) s ON e.source_system=s.source_system AND e.rel_type_id=s.rel_type_id AND e.id=s.id"

def bootstrap_groups(table,version,role,fields,first):
 checked_fields(fields)
 if role not in ('source_record','property_journal') or first not in (0,100,200,300,400):raise ValueError('Exhaustive bootstrap role blocks required')
 original=pin(table,version);q=pruned_mixed_query(table,version,role,fields,8000000,40000000,100000,first,100)
 assert original in q
 return q.replace(original,f"(SELECT * FROM {original} WHERE apply_batch_id='mixed-bootstrap')")

def added_digest(table,version,fields,tombstone=False):
 checked_fields(fields);scope='' if tombstone else " WHERE apply_batch_id='mixed-change/1'"
 return f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(fields)}))),256) FROM {pin(table,version)}{scope}"

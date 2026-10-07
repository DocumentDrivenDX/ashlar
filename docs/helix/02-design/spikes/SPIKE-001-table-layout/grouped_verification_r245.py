"""Single-scan synthetic range digests; preserves invalid membership as bucket-1."""
import re
from mixed_change_queries_r230 import row_hash_sql
from range_verification_r243 import digest_query

def bucket_sql(role,kind,nodes,end,width):
 if role not in ('object_current','edge_current','source_record','property_journal','adjacency_forward') or kind not in ('node','edge') or any(type(x) is not int for x in [nodes,end,width]) or not nodes>=5 or not 0<end<=10000000 or not 0<width<=100000 or (end+width-1)//width>100:raise ValueError('Invalid bounded grouping profile')
 if role=='source_record':
  ordinal="try_cast(element_at(split(delivery_id,':'),2) AS BIGINT)";valid=f"delivery_id RLIKE '^{kind}:(0|[1-9][0-9]*):1$'"
 else:
  ordinal=f'(id-{1 if kind=="node" else nodes+1})';valid=f"entity_kind='{kind}'" if role=='property_journal' else 'true'
 # COALESCE prevents null invalid membership disappearing from the result.
 return f"CASE WHEN coalesce(({valid}) AND {ordinal}>=0 AND {ordinal}<{end},false) THEN cast({ordinal} DIV {width} AS BIGINT) ELSE cast(-1 AS BIGINT) END"

def grouped_query(table,version,role,fields,kind,nodes,end,width=100000):
 if not fields or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',f) for f in fields):raise ValueError('Invalid field list')
 digest_query(table,version,role,fields,kind,nodes,0,end)
 bucket=bucket_sql(role,kind,nodes,end,width)
 return f"SELECT bucket,count(*),sha2(concat_ws('',sort_array(collect_list(row_digest))),256) FROM (SELECT {bucket} AS bucket,{row_hash_sql(fields)} AS row_digest FROM {table} VERSION AS OF {version}) GROUP BY bucket ORDER BY bucket"

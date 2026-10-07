"""Capped current-edge/forward blocks; complete count and every block required."""
import re
from mixed_change_queries_r230 import row_hash_sql

def pure_bucket_sql(nodes,end,width=100000):
 if any(type(v) is not int for v in [nodes,end,width]) or not 5<=nodes<=10000000 or not 0<end<=50000000 or not 0<width<=100000:raise ValueError('Invalid intermediate edge prefix')
 ordinal=f'(cast(id AS DECIMAL(38,0))-{nodes+1})'
 return f"CASE WHEN coalesce({ordinal}>=0 AND {ordinal}<{end},false) THEN cast({ordinal} DIV {width} AS BIGINT) ELSE cast(-1 AS BIGINT) END"

def pure_grouped_block(table,version,role,fields,nodes,end,width=100000,first=0,groups=100):
 if role not in ('edge_current','adjacency_forward') or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',table) or type(version) is not int or version<0 or not fields or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',v) for v in fields):raise ValueError('Invalid pinned edge role')
 bucket=pure_bucket_sql(nodes,end,width);total=(end+width-1)//width
 if any(type(v) is not int for v in [first,groups]) or not 0<=first<total or not 1<=groups<=100:raise ValueError('Invalid capped block')
 low=first*width;high=min(end,(first+groups)*width)
 return f"SELECT bucket,count(*),sha2(concat_ws('',sort_array(collect_list(row_digest))),256) FROM (SELECT {bucket} bucket,{row_hash_sql(fields)} row_digest FROM {table} VERSION AS OF {version} WHERE id>{nodes+low} AND id<={nodes+high}) GROUP BY bucket ORDER BY bucket"

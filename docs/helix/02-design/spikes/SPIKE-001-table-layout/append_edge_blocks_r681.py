"""Bounded append-role digests plus whole-table coverage, not hidden filtering."""
import re
from mixed_change_queries_r230 import row_hash_sql

def shape(table,version,role,extent_end):
 if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',table) or type(version) is not int or version<0:
  raise ValueError('Validated owned table/pin required')
 if role not in ['edge_current','source_record','property_journal','adjacency_forward'] or type(extent_end) is not int or not 40000000<extent_end<=80000000:
  raise ValueError('Explicit append profile extent required')
 ordinal="try_cast(element_at(split(delivery_id,':'),2) AS BIGINT)" if role=='source_record' else '(id-16000001)'
 valid="delivery_id RLIKE '^edge:(0|[1-9][0-9]*):1$'" if role=='source_record' else "entity_kind='edge'" if role=='property_journal' else 'true'
 member=f"coalesce(({valid}) AND {ordinal}>=40000000 AND {ordinal}<{extent_end},false)"
 return ordinal,member

def coverage_query(table,version,role,extent_end):
 _,member=shape(table,version,role,extent_end)
 return f'SELECT count(*),count_if(NOT ({member})) FROM {table} VERSION AS OF {version}'

def block_query(table,version,role,fields,start,end,extent_end):
 ordinal,member=shape(table,version,role,extent_end)
 if any(type(x) is not int for x in [start,end]) or not 40000000<=start<end<=extent_end or start%100000 or end%100000 or end-start>4000000:
  raise ValueError('Aligned block of at most4M entities required')
 if not fields or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',f) for f in fields):raise ValueError('Complete field list required')
 # The direct delivery range can prune raw/journal files; coverage count and
 # sum-of-block row counts must also match so corrupt delivery cannot vanish.
 delivery='delivery_id' if role=='source_record' else 'source_delivery_id' if role=='property_journal' else None
 prune=f" AND {delivery}>='edge:{start}:1' AND {delivery}<='edge:{end-1}:1'" if delivery else ''
 direct=f' AND id>{16000000+start} AND id<={16000000+end}' if role in ['edge_current','adjacency_forward'] else ''
 predicate=f'({member}) AND {ordinal}>={start} AND {ordinal}<{end}'+prune+direct
 bucket=f'cast(({ordinal}-40000000) DIV 100000 AS BIGINT)'
 return f"SELECT bucket,count(*),sha2(concat_ws('',sort_array(collect_list(row_digest))),256) FROM (SELECT {bucket} bucket,{row_hash_sql(fields)} row_digest FROM {table} VERSION AS OF {version} WHERE {predicate}) GROUP BY bucket ORDER BY bucket"

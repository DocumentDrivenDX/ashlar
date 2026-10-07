"""Bounded synthetic-bootstrap field oracles; never general source reconstruction."""
import hashlib,re,collections
from mixed_change_queries_r230 import row_hash,row_hash_sql

def oracle_chunks(workload,kind,end,width=100000,start=0):
 if type(start) is not int or start<0 or start>=end or start%width!=0 or type(end) is not int or type(width) is not int or not 0<end<= (workload.nodes if kind=='node' else workload.edges) or not 0<width<=100000:raise ValueError('Invalid bounded prefix')
 for start in range(start,end,width):
  stop=min(end,start+width);hashes=collections.defaultdict(list);fields={}
  for ordinal in range(start,stop):
   for role,row in workload.roles(kind,ordinal):
    fields.setdefault(role,list(row));hashes[role].append(row_hash(row,fields[role]))
  yield {'start':start,'end':stop,'roles':{role:{'fields':fields[role],'rows':len(values),'digest':hashlib.sha256(''.join(sorted(values)).encode()).hexdigest()} for role,values in hashes.items()}}

def digest_query(table,version,role,fields,kind,nodes,start,end):
 if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',table):raise ValueError('Invalid owned table identifier')
 if type(version) is not int or version<0 or kind not in ('node','edge') or not 0<=start<end:raise ValueError('Invalid pinned range')
 if role=='source_record':
  ordinal="try_cast(element_at(split(delivery_id,':'),2) AS BIGINT)"
  predicate=f"delivery_id RLIKE '^{kind}:(0|[1-9][0-9]*):1$' AND {ordinal}>={start} AND {ordinal}<{end}"
 else:
  offset=0 if kind=='node' else nodes;predicate=f'id>{offset+start} AND id<={offset+end}'
  if role=='property_journal':predicate+=f" AND entity_kind='{kind}'"
 return f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(fields)}))),256) FROM {table} VERSION AS OF {version} WHERE {predicate}"

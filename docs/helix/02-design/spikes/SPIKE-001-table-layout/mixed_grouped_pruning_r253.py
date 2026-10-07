"""Equivalent pure-edge block predicates; full counts/all blocks still required."""
from mixed_grouped_verification_r249 import mixed_grouped_query

def pruned_mixed_query(table,version,role,fields,nodes,edges,width=100000,first=0,groups=100):
 q=mixed_grouped_query(table,version,role,fields,nodes,edges,width,first,groups);ng=(nodes+width-1)//width
 if first<ng:return q
 low=(first-ng)*width;high=min(edges,(first+groups-ng)*width)
 if role=='property_journal':predicate=f"entity_kind='edge' AND id>{nodes+low} AND id<={nodes+high}"
 else:
  ordinal="try_cast(element_at(split(delivery_id,':'),2) AS BIGINT)";predicate=f"delivery_id LIKE 'edge:%' AND {ordinal}>={low} AND {ordinal}<{high}"
 needle=f'FROM {table} VERSION AS OF {version})';assert q.count(needle)==1
 return q.replace(needle,f'FROM {table} VERSION AS OF {version} WHERE {predicate})')

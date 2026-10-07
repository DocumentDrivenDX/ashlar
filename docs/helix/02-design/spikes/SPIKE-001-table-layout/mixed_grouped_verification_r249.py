"""Mixed synthetic raw/history verification in capped group blocks with full coverage."""
import re,math
from mixed_change_queries_r230 import row_hash_sql

def mixed_bucket_sql(role,nodes,edges,width=100000):
 if role not in ('source_record','property_journal') or any(type(x) is not int for x in [nodes,edges,width]) or not 5<=nodes<=10000000 or not 0<=edges<=50000000 or not 0<width<=100000:raise ValueError('Invalid mixed spike prefix')
 node_groups=(nodes+width-1)//width
 if role=='source_record':
  ordinal="try_cast(element_at(split(delivery_id,':'),2) AS BIGINT)"
  node=f"delivery_id RLIKE '^node:(0|[1-9][0-9]*):1$'";edge=f"delivery_id RLIKE '^edge:(0|[1-9][0-9]*):1$'";no=eo=ordinal
 else:
  node="entity_kind='node'";edge="entity_kind='edge'";no='(cast(id AS DECIMAL(38,0))-1)';eo=f'(cast(id AS DECIMAL(38,0))-{nodes+1})'
 return f"CASE WHEN coalesce(({node}) AND {no}>=0 AND {no}<{nodes},false) THEN cast({no} DIV {width} AS BIGINT) WHEN coalesce(({edge}) AND {eo}>=0 AND {eo}<{edges},false) THEN cast({eo} DIV {width} AS BIGINT)+{node_groups} ELSE cast(-1 AS BIGINT) END"

def mixed_grouped_query(table,version,role,fields,nodes,edges,width=100000,first=0,groups=100):
 if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',table) or type(version) is not int or version<0 or not fields or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',f) for f in fields):raise ValueError('Invalid pinned input')
 bucket=mixed_bucket_sql(role,nodes,edges,width);total=(nodes+width-1)//width+(edges+width-1)//width
 if any(type(x) is not int for x in [first,groups]) or not 0<=first<total or not 1<=groups<=100:raise ValueError('Invalid group block')
 return f"SELECT bucket,count(*),sha2(concat_ws('',sort_array(collect_list(row_digest))),256) FROM (SELECT {bucket} AS bucket,{row_hash_sql(fields)} AS row_digest FROM {table} VERSION AS OF {version}) WHERE bucket>={first} AND bucket<{min(total,first+groups)} GROUP BY bucket ORDER BY bucket"

def expected_mixed(node_oracle,edge_oracle,role):
 # Caller must supply complete contiguous prefixes; offsets include partial final node group.
 return [[str(i),str(c['roles'][role]['rows']),c['roles'][role]['digest']] for i,c in enumerate(node_oracle+edge_oracle)]

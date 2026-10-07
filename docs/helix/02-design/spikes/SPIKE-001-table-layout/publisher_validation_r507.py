"""Reusable private post-commit validation stage; no writes/clients on import.
Caller supplies independently qualified oracles and exact committed version/UUID
custody, owns deadlines/history and closing custody before publishing. Not a fence.
"""
from publisher_content_r503 import Check,collect
from normalized_apply_sql_r276 import pin
from mixed_change_queries_r230 import row_hash_sql

def plan(tables,inputs,source):
 checks=[]
 def cdf(role):
  t=tables[role];v=t['version']
  if role in ['edge_current','adjacency_forward']:
   e=source['cdf']['roles'][role];want=tuple((kind,str(x['rows']),str(v),str(v),x['digest']) for kind,x in sorted(e['images'].items()))
  else:
   e=source['inputs']['checks'][role];want=(('insert',str(e['rows']),str(v),str(v),e['all_known_field_digest']),)
  q=f"SELECT _change_type,count(*),min(_commit_version),max(_commit_version),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(e['fields'])}))),256) FROM table_changes('{t['table']}',{v},{v}) GROUP BY _change_type ORDER BY _change_type"
  return Check('cdf-'+role,q,want)
 edge=pin(tables['edge_current']['table'],tables['edge_current']['version']);node=pin(tables['object_current']['table'],tables['object_current']['version']);tomb=pin(tables['tombstone']['table'],tables['tombstone']['version']);count=str(source['budget']['expected_final_rows']['edge_current'])
 closure=Check('typed-endpoints',f'SELECT count(*),count_if(s.id IS NULL OR t.id IS NULL) FROM {edge} e LEFT JOIN {node} s ON e.source_system=s.source_system AND e.source_type=s.type_id AND e.source_id=s.id LEFT JOIN {node} t ON e.source_system=t.source_system AND e.target_type=t.type_id AND e.target_id=t.id',((count,'0'),))
 # Keep the experimentally measured four-check first cohort together.
 checks=[cdf('source_record'),cdf('property_journal'),cdf('edge_current'),closure,cdf('adjacency_forward')]
 t=tables['tombstone'];s=inputs['tombstone'];e=source['tomb'];q=f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(['t.'+f for f in e['fields']])}))),256) FROM {pin(t['table'],t['version'])} t INNER JOIN {pin(s['table'],s['version'])} s ON t.source_feed=s.source_feed AND t.source_epoch=s.source_epoch AND t.source_delivery_id=s.source_delivery_id"
 checks.append(Check('canonical-tombstone',q,((str(source['inputs']['checks']['tombstone']['rows']),e['digest']),)))
 for role,t in tables.items():checks.append(Check('final-count-'+role,'SELECT count(*) FROM '+pin(t['table'],t['version']),((str(source['budget']['expected_final_rows'][role]),),)))
 checks.append(Check('unique-edges',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {edge}',((count,count),)))
 checks.append(Check('deleted-absent',f'SELECT count(*) FROM {edge} e INNER JOIN {tomb} t ON e.source_system=t.source_system AND e.rel_type_id=t.type_id AND e.id=t.id',(('0',),)))
 raw=pin(tables['source_record']['table'],tables['source_record']['version']);checks.append(Check('schema-revisions',f'SELECT DISTINCT source_feed,schema_revision FROM {raw}',tuple(tuple(x) for x in source['revision_rows'])))
 if len({c.label for c in checks})!=len(checks):raise ValueError('Duplicate validation labels')
 return checks

def validate(workers,tables,inputs,source,checkpoint):
 if len(workers)!=4:raise ValueError('Four independently owned read workers required')
 checks=plan(tables,inputs,source);accepted={}
 for offset in range(0,len(checks),4):
  batch=checks[offset:offset+4];result=collect(workers[:len(batch)],batch)
  accepted.update(result);checkpoint(offset,result)
 if set(accepted)!={c.label for c in checks}:raise ValueError('Incomplete post-commit validation')
 return accepted

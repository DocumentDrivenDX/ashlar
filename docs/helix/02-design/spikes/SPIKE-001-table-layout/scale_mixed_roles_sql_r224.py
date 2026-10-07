"""Complete native bootstrap roles; lexical source semantics remain synthetic."""
from scale_mixed_sql_r222 import carrier_sql,literal
from scale_mixed_r219 import Workload,SEED,bags
from mixed_history_r215 import tokens

def role_sql(role,kind,nodes,edges,start,end,*,_carrier_query=None):
 carrier=carrier_sql(kind,nodes,edges,start,end)
 if _carrier_query is not None:carrier=_carrier_query
 w=Workload(nodes,edges);fields=list(w.carrier(kind,start));typ='type_id' if kind=='node' else 'rel_type_id'
 if role==('object_current' if kind=='node' else 'edge_current'):return carrier
 if role=='adjacency_forward':
  if kind!='edge':raise ValueError('Adjacency requires edge range')
  return 'SELECT '+','.join(['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','entity_version'])+' FROM ('+carrier+')'
 if role=='source_record':
  # Original Python carrier numeric transport fields are decimal strings in raw.
  pairs=','.join("'"+f+"',"+(literal('2026-10-07T00:00:00Z') if f=='published_at' else 'cast('+f+' AS STRING)') for f in fields)
  payload=f"to_json(named_struct('profile','{SEED}','kind','{kind}','carrier',named_struct({pairs}),'unknown_envelope',named_struct('keep',true)),map('ignoreNullFields','false'))"
  return f"WITH c AS ({carrier}),p AS (SELECT *,{payload} payload_json FROM c) SELECT source_feed,source_epoch,source_delivery_id delivery_id,'synthetic-full-carrier' record_kind,source_cursor_json,payload_json,sha2(payload_json,256) payload_digest,schema_revision,published_at received_at,apply_batch_id FROM p"
 if role!='property_journal':raise ValueError('Unknown native role')
 branches=[]
 for shape in range(8):
  entries=[]
  for key,value in sorted(tokens(bags(kind,shape)[0]).items(),key=lambda x:int(x[0])):
   token="concat('\"',get_json_object(props_json,'$.901'),'\"')" if key=='901' else literal(value)
   entries.append(f"named_struct('property_id',cast({key} AS BIGINT),'new_json',{token})")
  branches.append('WHEN '+str(shape)+' THEN array('+','.join(entries)+')')
 case='CASE pmod(cast(id AS BIGINT)-'+('1' if kind=='node' else str(nodes+1))+',8) '+' '.join(branches)+' END'
 return f"""WITH c AS ({carrier}),events AS (SELECT c.*,event_ordinal,event FROM c LATERAL VIEW posexplode({case}) e AS event_ordinal,event)
 SELECT source_system,'{kind}' entity_kind,{typ} type_id,id,event.property_id property_id,entity_version,'set' operation,false old_present,cast(NULL AS STRING) old_json,true new_present,event.new_json new_json,schema_revision,source_feed,source_epoch,source_position,cast(event_ordinal AS BIGINT) event_ordinal,cast(NULL AS STRING) source_time_text,published_at,apply_batch_id,source_cursor_json,source_delivery_id FROM events"""


def role_from_pinned_carrier(role,kind,nodes,edges,start,end,table,version):
 import re
 if not isinstance(table,str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',table) or type(version) is not int or version<0:raise ValueError('Validated native table/version required')
 carrier_sql(kind,nodes,edges,start,end)
 offset=0 if kind=='node' else nodes
 q=f'SELECT * FROM {table} VERSION AS OF {version} WHERE id>{offset+start} AND id<={offset+end}'
 return role_sql(role,kind,nodes,edges,start,end,_carrier_query=q)

"""Native range carrier generator matching scale_mixed_r219 lexical recipe."""
from scale_mixed_r219 import Workload,SEED,bags

def literal(text):return "decode(unhex('"+text.encode().hex()+"'),'UTF-8')"
def carrier_sql(kind,nodes,edges,start,end):
 Workload(nodes,edges)
 limit=nodes if kind=='node' else edges
 if kind not in ['node','edge'] or any(type(x) is not int for x in [start,end]) or not 0<=start<end<=limit:raise ValueError('Valid bounded native range required')
 # Bound arithmetic intermediates explicitly; planning scale fits comfortably.
 if (end-1)*13007+1>9223372036854775807 or (end-1)*104729>9223372036854775807:raise ValueError('Endpoint multiplier would overflow BIGINT')
 bases=[bags(kind,i)[0].rsplit(',"901":',1)[0]+'}' if i%8 else '{}' for i in range(8)]
 # Each base is original lexical content; large numerics never parsed/coerced.
 base='CASE pmod(ordinal,8) '+' '.join('WHEN '+str(i)+' THEN '+literal(x) for i,x in enumerate(bases))+' END'
 b=(nodes+4)//5;acount=nodes-b
 def endpoint(index):
  k=f'pmod(({index}),{acount})'
  return f"CASE WHEN source_system='pilot-b' THEN 5*pmod(({index}),{b}) ELSE ({k} DIV 4)*5+1+pmod({k},4) END"
 a=endpoint('CASE WHEN pmod(ordinal,5)=0 THEN 0 ELSE ordinal*104729 END');z=endpoint('ordinal*13007+1')
 typ='type_id' if kind=='node' else 'rel_type_id'
 fields=("cast(1+pmod(ordinal,4) AS BIGINT) type_id,cast(ordinal+1 AS BIGINT) id,to_json(named_struct('synthetic_native_tuple',array(cast(1+pmod(ordinal,4) AS STRING),cast(ordinal+1 AS STRING)))) logical_key_json,cast(NULL AS BIGINT) root_id" if kind=='node' else f"cast(7+pmod(ordinal,3) AS BIGINT) rel_type_id,cast({nodes}+ordinal+1 AS BIGINT) id,cast(1+pmod(a,4) AS BIGINT) source_type,cast(a+1 AS BIGINT) source_id,cast(1+pmod(z,4) AS BIGINT) target_type,cast(z+1 AS BIGINT) target_id,cast(NULL AS STRING) order_key")
 return f"""WITH ord AS (SELECT id ordinal FROM range({start},{end})),
 widths AS (SELECT *,CASE WHEN pmod(ordinal,5)=0 THEN 'pilot-b' ELSE 'pilot-a' END source_system,element_at(array(64,256,1024,4096),cast(pmod(ordinal DIV 8,4)+1 AS INT)) width FROM ord),
 endpoints AS (SELECT *,{a} a0,{z} z0 FROM widths),
 opaque_rows AS (SELECT *,a0 a,CASE WHEN pmod(ordinal,97)=0 THEN a0 ELSE z0 END z,concat_ws('',transform(sequence(0,cast(width DIV 64-1 AS INT)),j->sha2(concat('{SEED}:{kind}:',cast(ordinal AS STRING),':',cast(j AS STRING)),256))) opaque,{base} base FROM endpoints),
 carriers AS (SELECT source_system,{fields},'synthetic-mixed/1' schema_revision,cast(1 AS BIGINT) entity_version,
 concat(substring(base,1,length(base)-1),CASE WHEN base='{{}}' THEN '' ELSE ',' END,'"901":"',opaque,'"}}') props_json,
 to_json(named_struct('future',named_struct('kind','{kind}','ordinal',cast(ordinal AS STRING),'opaque',substring(opaque,1,64),'unknown',array(cast(NULL AS BOOLEAN),true))),map('ignoreNullFields','false')) retained_json,
 'synthetic-scale-mixed' source_feed,'{SEED}' source_epoch,cast(NULL AS BIGINT) source_position,cast('2026-10-07T00:00:00Z' AS TIMESTAMP) published_at,'mixed-bootstrap' apply_batch_id,
 to_json(named_struct('xid','9223372036854775808','seq',cast(ordinal AS STRING))) source_cursor_json,concat('{kind}:',cast(ordinal AS STRING),':1') source_delivery_id FROM opaque_rows)
 SELECT *,sha2(to_json(named_struct('source_system',source_system,'{typ}',{typ},'id',id)),256) lookup_hash FROM carriers"""

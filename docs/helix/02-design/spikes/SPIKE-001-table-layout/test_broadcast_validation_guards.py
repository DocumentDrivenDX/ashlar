"""Native adversarial controls against the actual fused guard expression.
Two-row virtual fixture only; no database mutations or full publisher proof.
"""
import ast,json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent
# Load only the pure guard-building assignments, never execute the harness.
tree=ast.parse((B/'native_broadcast_validation.py').read_text());env={}
for name in ['cols','unchanged','post']:
 node=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in n.targets))
 exec(compile(ast.Module(body=[node],type_ignores=[]),'guard-expression','exec'),env)
post=env['post'];c=DriverClient(B/'out/native/ashlar_broadcast_guard_controls_20261005_n3')
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
s="SELECT 'pilot' source_system,cast(1 AS BIGINT) type_id,id,to_json(array(id)) logical_key_json,'r1' schema_revision,cast(0 AS BIGINT) entity_version,'{\"103\":\"old\"}' props_json,'{\"future\":true}' retained_json,cast(NULL AS BIGINT) root_id,'S' source_feed,'e' source_epoch,cast(0 AS BIGINT) source_position,current_timestamp() published_at,'{\"103\":\"new\"}' new_props FROM VALUES (1L),(2L) AS ids(id)"
o="SELECT source_system,type_id,id,logical_key_json,schema_revision,cast(3 AS BIGINT) entity_version,new_props props_json,retained_json,root_id,source_feed,source_epoch,cast(3 AS BIGINT) source_position,published_at FROM s"
j="SELECT source_system,'object' entity_kind,type_id,id,cast(103 AS BIGINT) property_id,cast(3 AS BIGINT) entity_version,'update' operation,true old_present,'\"old\"' old_json,true new_present,'\"new\"' new_json,'r1' schema_revision,'S' source_feed,'e' source_epoch,cast(3 AS BIGINT) source_position,id event_ordinal,'synthetic-batch:3' source_time_text FROM s"
controls=[('balanced-missing-duplicate',o+' WHERE id=1 UNION ALL '+o+' WHERE id=1',j,True),('valid',o,j,False),('missing-canonical',o+' WHERE id=1',j,True),('missing-journal',o,j+' WHERE id=1',True),('duplicate-canonical',o+' UNION ALL '+o+' WHERE id=1',j,True),('duplicate-journal',o,j+' UNION ALL '+j+' WHERE id=1',True),('retained-loss',o.replace('retained_json,root_id',"'{}' retained_json,root_id"),j,True),('bad-old-token',o,j.replace("'\"old\"' old_json","'\"wrong\"' old_json"),True),('bad-new-token',o,j.replace("'\"new\"' new_json","'\"wrong\"' new_json"),True),('null-token',o,j.replace("'\"old\"' old_json","cast(NULL AS STRING) old_json"),True),('wrong-presence',o,j.replace('true old_present','false old_present'),True),('wrong-property',o,j.replace('cast(103 AS BIGINT)','cast(999 AS BIGINT)'),True)]
for label,oc,jc,expected in controls:
 rows=c.sql(label,f"WITH s AS ({s}),o AS ({oc}),j AS ({jc}) SELECT count(*)<>2 OR count(DISTINCT struct(s.source_system,s.type_id,s.id))<>2 OR count_if({post})<>0 invalid FROM s JOIN o ON s.source_system=o.source_system AND s.type_id=o.type_id AND s.id=o.id LEFT JOIN j ON j.source_system=s.source_system AND j.entity_kind='object' AND j.type_id=s.type_id AND j.id=s.id AND j.source_feed='S' AND j.source_epoch='e' AND j.source_position=3")
 assert len(rows)==1 and rows[0][0].lower()==str(expected).lower(),(label,rows)
c.history();c.close();print('12 native broadcast guard controls passed',flush=True)

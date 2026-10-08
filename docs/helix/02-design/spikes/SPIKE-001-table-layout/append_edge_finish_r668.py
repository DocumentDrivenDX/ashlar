"""Finish only pending read-only checks after same-handle interruption recovery."""
import json,time
from append_edge_growth_r665 import BASE,OUT
from append_edge_digest_r658 import grouped_query
from persistent_sql import Client
from publication_history import collect_history,HistoryPending

s=json.loads((OUT/'summary.json').read_text())
assert set(s['checks'])=={'edge_current','source_record'}
c=Client(OUT,observation_timeout=240,cancel_after=180)
c.records=[json.loads(x) for x in (OUT/'statements.jsonl').read_text().splitlines()]
start=time.monotonic()
oracle=json.loads((BASE/'out/append-edge-oracle-r656.json').read_text())
def save():
 s['recovery_wall_s']=time.monotonic()-start
 (OUT/'summary.json').write_text(json.dumps(s,indent=2)+'\n')
def closing(role):
 t=s['tables'][role]
 rows=c.sql('closing-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1')
 names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 head=dict(zip(names,rows[0]));assert int(head['version'])==s['versions'][role]
 rows=c.sql('closing-detail-'+role,'DESCRIBE DETAIL '+t['table'])
 names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 detail=dict(zip(names,rows[0]));assert detail['id']==t['id']
 t['closing_head']=head;t['closing_detail']=detail;save()
closing('source_record')
for role in ['property_journal','adjacency_forward']:
 t=s['tables'][role];fields=oracle['chunks'][0]['roles'][role]['fields']
 rows=c.sql('complete-digest-'+role,grouped_query(t['table'],s['versions'][role],role,fields))
 expected=[[str(i),str(ch['roles'][role]['rows']),ch['roles'][role]['digest']] for i,ch in enumerate(oracle['chunks'])]
 assert rows==expected
 s['checks'][role]={'version':s['versions'][role],'groups':rows,'rows':sum(int(r[1]) for r in rows),'all_fields':True,'invalid_membership':0};save();closing(role)
old='client_dev.ashlar_entropy_20261006_r86.growth_object_current_r236';new='client_dev.ashlar_entropy_20261006_r86.append_node_object_current_r643'
for label,table,ident in [('old',old,'ea6b0cbe-28fc-426c-a52b-15844ef14e91'),('new',new,'67cb3fd1-5ec4-4f5e-be0f-bab0e9521b50')]:
 rows=c.sql('node-identity-'+label,'DESCRIBE DETAIL '+table)
 names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 assert dict(zip(names,rows[0]))['id']==ident
e=s['tables']['edge_current']['table'];v=s['versions']['edge_current']
nodes=f'SELECT source_system,type_id,id FROM {old} VERSION AS OF 6 UNION ALL SELECT source_system,type_id,id FROM {new} VERSION AS OF 15'
endpoints=f'SELECT source_system,source_type type_id,source_id id FROM {e} VERSION AS OF {v} UNION ALL SELECT source_system,target_type type_id,target_id id FROM {e} VERSION AS OF {v}'
q=f'WITH nodes AS ({nodes}),endpoints AS ({endpoints}),missing AS (SELECT e.* FROM endpoints e LEFT ANTI JOIN nodes n ON e.source_system=n.source_system AND e.type_id=n.type_id AND e.id=n.id) SELECT (SELECT count(*) FROM endpoints),(SELECT count(*) FROM missing),(SELECT count_if(source_system IS NULL OR type_id IS NULL OR id IS NULL) FROM endpoints)'
rows=c.sql('complete-endpoint-closure',q);assert rows==[['16000000','0','0']]
s['endpoint_closure']={'pins':{'old_nodes':6,'new_nodes':15,'edges':v},'rows':rows}
for attempt in range(12):
 try:h=collect_history(c.w,c.records,OUT/'shared-history.json');break
 except HistoryPending:
  if attempt==11:raise
  time.sleep(2)
s['costs']={k:sum(q['metrics'][k] for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
assert all(s['costs'][k]<=s['bounds'][k] for k in s['costs'])
assert time.monotonic()-start<600
s['state']='Complete independent first8M native new-edge extent parity'
s['recovery_qualification']='Source response recovered from original terminal handle without replay. Pending validations completed in separate read-only phase. Original wall_s remains partial observation; full uninterrupted controller wall time unqualified.'
save();(OUT/'live-statement.json').unlink(missing_ok=True)
print(json.dumps({'state':s['state'],'costs':s['costs'],'recovery_wall_s':s['recovery_wall_s']},indent=2))

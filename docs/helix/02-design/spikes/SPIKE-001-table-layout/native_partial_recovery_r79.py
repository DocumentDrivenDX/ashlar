"""Tiny serialized edge/forward-adjacency repair; no source or fencing claim."""
import hashlib,json,time,sys
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;N='client_dev.ashlar_layout_v03_20261006_r73'
resume='--resume' in sys.argv
O=B/('out/native/ashlar_partial_recovery_20261006_r79_resume' if resume else 'out/native/ashlar_partial_recovery_20261006_r79');c=Client(O);start=time.time()
E=N+'.partial_edges_r79';A=N+'.partial_adjacency_r79';S=N+'.partial_stage_r79'
R=N+'.partial_binding_r79';M=N+'.partial_manifest_r79'
def lit(x):return "decode(unhex('"+x.encode().hex()+"'),'UTF-8')"
def ver(client,t,label):return int(client.sql(label,f'DESCRIBE HISTORY {t} LIMIT 1')[0][0])
def rows(client,t,v,label):return client.sql(label,f'SELECT * FROM {t} VERSION AS OF {v} ORDER BY source_system,rel_type_id,'+('edge_id' if t==A else 'id'))
def mismatch(client,ev,av,label):
 return client.sql(label,f'''SELECT count(*) FROM (
 (SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id FROM {E} VERSION AS OF {ev}
 EXCEPT ALL SELECT source_system,rel_type_id,edge_id,source_type,source_id,target_type,target_id FROM {A} VERSION AS OF {av})
 UNION ALL
 (SELECT source_system,rel_type_id,edge_id,source_type,source_id,target_type,target_id FROM {A} VERSION AS OF {av}
 EXCEPT ALL SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id FROM {E} VERSION AS OF {ev}))''')
prior=json.loads((B/'out/native/ashlar_layout_v03_references_20261006_r74/summary.json').read_text());assert prior['state']=='passed'
if resume:
 previous=[json.loads(x) for x in (B/'out/native/ashlar_partial_recovery_20261006_r79/statements.jsonl').read_text().splitlines()]
 assert previous[-1]['label']=='retained-edge-stage' and previous[-1]['response']['status']['state']=='FAILED'
 names={r[1] for r in c.sql('resume-inventory',f"SHOW TABLES IN {N} LIKE 'partial_*_r79'")}
 assert names=={E.split('.')[-1],A.split('.')[-1],M.split('.')[-1]},names
 assert ver(c,E,'resume-edge-version')==ver(c,A,'resume-adjacency-version')==0
else:
 assert c.sql('absent-private-tables',f"SHOW TABLES IN {N} LIKE 'partial_*_r79'")==[]
 c.sql('edge-copy',f'CREATE TABLE {E} USING DELTA AS SELECT * FROM {N}.edge_current VERSION AS OF 1')
 c.sql('adjacency-copy',f'CREATE TABLE {A} USING DELTA AS SELECT * FROM {N}.adjacency_forward VERSION AS OF 1')
old_e=rows(c,E,0,'old-edges');old_a=rows(c,A,0,'old-adjacency');assert len(old_e)==len(old_a)==3
assert mismatch(c,0,0,'baseline-structure')==[['0']]
old_vector=json.dumps({E:0,A:0,N+'.object_current':7},sort_keys=True,separators=(',',':'))
if not resume:c.sql('old-manifest',f"CREATE TABLE {M} USING DELTA AS SELECT 'old' publication_id,{lit(old_vector)} vector_json,'none' binding_json")
# Immutable complete intended edge stage; only one endpoint tuple changes.
c.sql('retained-edge-stage',f'''CREATE TABLE {S} USING DELTA AS SELECT source_system,rel_type_id,id,source_type,source_id,
CASE WHEN rel_type_id=7 AND id=1 THEN cast(1 AS BIGINT) ELSE target_type END target_type,
target_id,schema_revision,entity_version,props_json,retained_json,order_key,
source_feed,source_epoch,source_position,published_at,lookup_hash,apply_batch_id,
source_cursor_json,source_delivery_id FROM {E} VERSION AS OF 0''')
expected=rows(c,S,0,'staged-edges');wanted=[x.copy() for x in old_e]
for x in wanted:
 if x[1:3]==['7','1']:x[5]='1'
assert expected==wanted and expected!=old_e
stage_hash=hashlib.sha256(json.dumps(expected,separators=(',',':')).encode()).hexdigest()
intent={'profile':'serialized-edge-forward-recovery-spike/1','batch':'r79','previous_publication':'old','stage':{'table':S,'version':0,'ordered_row_json_sha256':stage_hash},'old_vector':json.loads(old_vector),'coverage':'edges and forward adjacency only; no degree or reverse-read support','source_boundary':'synthetic endpoint change; no source-feed interpretation'}
intent_json=json.dumps(intent,sort_keys=True,separators=(',',':'))
c.sql('durable-intent',f"CREATE TABLE {R} USING DELTA AS SELECT 'r79' batch_id,{lit(intent_json)} intent_json,cast(NULL AS STRING) output_vector_json")
c.sql('apply-edge-only',f'MERGE INTO {E} t USING (SELECT * FROM {S} VERSION AS OF 0) s ON t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id WHEN MATCHED THEN UPDATE SET *')
ev=ver(c,E,'partial-edge-version');assert rows(c,E,ev,'partial-edge-parity')==expected
assert mismatch(c,ev,0,'detect-unrepaired-adjacency')==[['2']]
assert c.sql('old-only-during-partial',f'SELECT publication_id,vector_json FROM {M}')==[['old',old_vector]]
assert rows(c,E,0,'old-pinned-edges-during-partial')==old_e
assert rows(c,A,0,'old-pinned-adjacency-during-partial')==old_a
# Fresh client resolves retained intent and repairs only the outstanding projection.
d=Client(O/'recovered-client');assert d.sql('recover-intent',f'SELECT intent_json,output_vector_json FROM {R}')==[[intent_json,None]]
assert rows(d,S,0,'recover-stage')==expected
projection=f'''SELECT source_system,rel_type_id,id edge_id,source_type,source_id,target_type,target_id,
 CASE WHEN rel_type_id=7 AND id=1 THEN cast(2 AS BIGINT) ELSE cast(1 AS BIGINT) END structural_version
 FROM {S} VERSION AS OF 0'''
d.sql('repair-adjacency',f'MERGE INTO {A} t USING ({projection}) s ON t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.edge_id=s.edge_id WHEN MATCHED THEN UPDATE SET *')
av=ver(d,A,'repaired-adjacency-version');assert mismatch(d,ev,av,'repaired-structure')==[['0']]
new_a=rows(d,A,av,'repaired-adjacency-rows');wanted_a=[x.copy() for x in old_a]
for x in wanted_a:
 if x[1:3]==['7','1']:x[5]='1';x[7]='2'
assert new_a==wanted_a
# Pinned object snapshot contains every endpoint in the repaired edge snapshot.
assert d.sql('endpoint-closure',f'''SELECT count(*) FROM {E} VERSION AS OF {ev} e
 LEFT ANTI JOIN {N}.object_current VERSION AS OF 7 n ON e.source_system=n.source_system AND e.target_type=n.type_id AND e.target_id=n.id''')==[['0']]
vector=json.dumps({E:ev,A:av,N+'.object_current':7},sort_keys=True,separators=(',',':'))
d.sql('complete-durable-binding',f'UPDATE {R} SET output_vector_json={lit(vector)}')
assert d.sql('verify-binding',f'SELECT intent_json,output_vector_json FROM {R}')==[[intent_json,vector]]
d.sql('publish-repaired-vector',f"INSERT INTO {M} SELECT 'new',{lit(vector)},{lit(intent_json)}")
assert d.sql('both-manifests',f'SELECT publication_id,vector_json FROM {M} ORDER BY publication_id')==[['new',vector],['old',old_vector]]
assert ver(d,E,'no-edge-reapply')==ev
assert rows(d,E,0,'old-edge-after-recovery')==old_e
assert rows(d,A,0,'old-adjacency-after-recovery')==old_a
summary={'state':'passed','intent':intent,'versions':{'edges':ev,'forward_adjacency':av},'tables':{'edge':E,'adjacency':A,'stage':S,'binding':R,'manifest':M},'rows':3,'checks':['full-row intended edge change only','unrepaired projection produces two EXCEPT ALL differences','old publication retained through partial commit','fresh-client projection repair and exact unchanged-field parity','target endpoint closure in pinned objects','durable complete vector before publication','no canonical edge reapply','old snapshots unchanged after recovery'],'elapsed_seconds':time.time()-start,'cost':'Three rows per edge/adjacency/stage table, one binding and two manifests on existing authorized warehouse; billing dollars unavailable; no large scan/new compute.','scope':'Intentional statement-boundary interruption and fresh client; serialized writer only. Synthetic endpoint change does not implement source event/version semantics. No degree/reverse coverage, journal/raw update, real Truss feed, acknowledgement, concurrency fencing, ambiguous submission recovery, whole-graph atomicity, latency or scale claim.'}
(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('Partial edge/adjacency native recovery passed',flush=True)

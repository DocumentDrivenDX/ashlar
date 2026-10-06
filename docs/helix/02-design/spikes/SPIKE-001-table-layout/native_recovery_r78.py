"""Serialized, single-object-table recovery spike. No producer/fencing claim."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent
N='client_dev.ashlar_layout_v03_20261006_r73'
O=B/'out/native/ashlar_serialized_recovery_20261006_r78'
c=Client(O); started=time.time()
T=N+'.recovery_objects_r78'; S=N+'.recovery_stage_r78'
R=N+'.recovery_binding_r78'; M=N+'.recovery_manifest_r78'
def literal(value):
 return "decode(unhex('"+value.encode().hex()+"'),'UTF-8')"
def version(client,table,label):
 return int(client.sql(label,f'DESCRIBE HISTORY {table} LIMIT 1')[0][0])
def rows(client,table,v,label):
 return client.sql(label,f'SELECT * FROM {table} VERSION AS OF {v} ORDER BY source_system,type_id,id')
prior=json.loads((B/'out/native/ashlar_layout_v03_references_20261006_r74/summary.json').read_text())
v=prior['versions'][N+'.object_current']; assert prior['state']=='passed'
assert c.sql('absent-private-tables',f"SHOW TABLES IN {N} LIKE 'recovery_*_r78'")==[]
c.sql('retained-stage',f'CREATE TABLE {S} USING DELTA AS SELECT * FROM {N}.object_current VERSION AS OF {v}')
c.sql('working-copy',f'CREATE TABLE {T} USING DELTA AS SELECT * FROM {S} VERSION AS OF 0')
before=rows(c,T,0,'baseline'); assert len(before)==3
old_vector=json.dumps({T:0},sort_keys=True,separators=(',',':'))
c.sql('old-publication',f"CREATE TABLE {M} USING DELTA AS SELECT 'old' publication_id,{literal(old_vector)} vector_json,'none' batch_id,'none' binding_json")
# Retained stage fixes the intended output once, before applying it.
props='{"101":null,"102":9007199254740993,"103":"recovered"}'
c.sql('prepare-change',f"UPDATE {S} SET props_json={literal(props)},apply_batch_id='r78' WHERE type_id=1 AND id=1")
sv=version(c,S,'stage-version'); expected=rows(c,S,sv,'expected-output')
wanted=[row.copy() for row in before]
for row in wanted:
 if row[1:3]==['1','1']:
  row[6]=props; row[14]='r78'
assert expected==wanted and expected!=before
stage_digest=hashlib.sha256(json.dumps(expected,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
c.sql('apply-once',f'MERGE INTO {T} t USING (SELECT * FROM {S} VERSION AS OF {sv}) s ON t.source_system=s.source_system AND t.type_id=s.type_id AND t.id=s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *')
tv=version(c,T,'output-version'); assert rows(c,T,tv,'output-parity')==expected
assert rows(c,T,0,'prior-pinned-during-interruption')==before
assert c.sql('no-new-manifest-yet',f'SELECT publication_id,vector_json FROM {M}')==[['old',old_vector]]
vector=json.dumps({T:tv},sort_keys=True,separators=(',',':'))
binding={'profile':'serialized-object-recovery-spike/1','batch':'r78','previous_publication':'old','stage':{'table':S,'version':sv,'row_count':3,'ordered_row_json_sha256':stage_digest},'output_versions':{T:tv},'source_boundary':'synthetic stage only; no native producer cursor'}
encoded=json.dumps(binding,sort_keys=True,separators=(',',':'))
c.sql('durable-supplemental-binding',f"CREATE TABLE {R} USING DELTA AS SELECT 'r78' batch_id,{literal(encoded)} binding_json")
# New client loses all local query handles. Recover solely from durable binding.
d=Client(O/'recovered-client')
receipt=d.sql('read-durable-binding',f'SELECT batch_id,binding_json FROM {R}'); assert receipt==[['r78',encoded]]
recovered=json.loads(receipt[0][1]); assert recovered==binding
assert rows(d,S,sv,'retained-stage-recovery')==expected
assert rows(d,T,tv,'committed-output-recovery')==expected
publish=f'''MERGE INTO {M} t USING (SELECT 'new' publication_id,{literal(vector)} vector_json,'r78' batch_id,{literal(encoded)} binding_json) s ON t.publication_id=s.publication_id
WHEN MATCHED AND (t.vector_json IS DISTINCT FROM s.vector_json OR t.batch_id IS DISTINCT FROM s.batch_id OR t.binding_json IS DISTINCT FROM s.binding_json) THEN UPDATE SET binding_json=cast(raise_error('PUBLICATION_CONFLICT') AS STRING)
WHEN NOT MATCHED THEN INSERT *'''
d.sql('publish-recovered-output',publish)
first=d.sql('published-descriptors',f'SELECT * FROM {M} ORDER BY publication_id'); assert len(first)==2
# Lost acknowledgement: same durable input returns same descriptor without apply.
d.sql('lost-ack-publication-retry',publish)
assert d.sql('retry-descriptors',f'SELECT * FROM {M} ORDER BY publication_id')==first
assert version(d,T,'no-data-reapply')==tv
assert rows(d,T,0,'old-reader-after-recovery')==before
assert rows(d,T,tv,'new-reader-after-recovery')==expected
summary={'state':'passed','profile':binding['profile'],'tables':{'stage':S,'output':T,'binding':R,'manifest':M},'versions':{'stage':sv,'output':tv},'rows':3,'binding':binding,'checks':['old pinned rows survive unpublished output commit','new client resolves retained stage and committed output from durable binding','publication retry after simulated lost acknowledgement retains exact descriptors','no output reapply/version advance','old and recovered readers retain exact full-row parity'],'elapsed_seconds':time.time()-started,'cost':'Existing authorized warehouse only; three object rows in stage/output plus one binding and two manifests; billing dollars unavailable. No large scans or new compute.','scope':'Injected interruption is an intentional stop between SQL statements plus a fresh client; no process kill, ambiguous write outcome, real source acknowledgement, concurrent writer/fencing, graph-wide atomicity, production receipt schema, latency/ingest/scale admission or Truss trusted proof claim.'}
(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print('Serialized native recovery checks passed',flush=True)

"""Small isolated failed raw lane after overlapping native writes; no mutation retry."""
import json,time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS,PropertyApply,lit
from raw_validation_queries import origin_sql,verified_origin,raw_validation
from journal_validation_queries import expected,symmetric
from wire_json import encode
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_failure_r156'
assert not (O/'statements.jsonl').exists(),'Inspect prior handles; no replay'
F='client_dev.ashlar_entropy_20261006_r86';E,R,J,M,S=[F+'.'+n+'_failure_r156' for n in ('edge','raw','journal','manifest','stage')]
c=BoundedReads(O);a=BoundedReads(O/'apply-lane');j=BoundedReads(O/'journal-lane')
for client,name in [(c,'main'),(a,'apply'),(j,'journal')]:client.sql(name+'-timeout','SET STATEMENT_TIMEOUT=60')
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE '*failure_r156'")==[]
# Complete carriers in a bounded subset, not a full graph/profile admission.
c.sql('create-edge',f"CREATE TABLE {E} USING DELTA AS SELECT {','.join(COLS)} FROM {F}.schedule_r139_1 VERSION AS OF 0 ORDER BY id LIMIT 1000")
c.sql('create-raw',f'CREATE TABLE {R} USING DELTA AS SELECT r.* FROM {F}.source_record_r89 VERSION AS OF 17 r INNER JOIN {E} VERSION AS OF 0 e ON r.source_feed=e.source_feed AND r.source_epoch=e.source_epoch AND r.delivery_id=e.source_delivery_id')
c.sql('create-journal',f'CREATE TABLE {J} USING DELTA AS SELECT r.* FROM {F}.property_journal_r89 VERSION AS OF 19 r INNER JOIN {E} VERSION AS OF 0 e ON r.source_feed=e.source_feed AND r.source_epoch=e.source_epoch AND r.source_delivery_id=e.source_delivery_id')
c.sql('create-manifest',f'CREATE TABLE {M} USING DELTA AS SELECT * FROM {F}.publication_manifest_r89 WHERE false')
old_vector={E:0,R:0,J:0,F+'.object_current':0}
old_values=['r156-base','ashlar-delta/0.3-synthetic-failure-slice',json.dumps(old_vector,sort_keys=True),'{}','{}','{"scope":"1000-edge failure control, no full graph admission"}']
c.sql('old-publication',f'INSERT INTO {M} SELECT '+','.join(map(lit,old_values))+',current_timestamp()')
old_manifest=c.sql('old-manifest',f'SELECT * FROM {M} ORDER BY publication_id')
overrides={'entity_version':'entity_version+1','props_json':"replace(props_json,concat('\"105\":\"',old_opaque,'\"'),concat('\"105\":\"',new_opaque,'\"'))",'source_epoch':"'failure-r156'",'apply_batch_id':"'r156-b1'",'published_at':'current_timestamp()', 'source_delivery_id':"concat('r156-b1:edge:',cast(id AS STRING))", 'source_cursor_json':"concat('{\"xid\":\"9007199254741201\",\"seq\":\"',cast(id AS STRING),'\"}')"}
c.sql('create-stage',f"CREATE TABLE {S} USING DELTA AS SELECT "+','.join(overrides.get(col,col)+' AS '+col for col in COLS)+",concat(char(34),old_opaque,char(34)) old_json FROM (SELECT *,get_json_object(props_json,'$.105') old_opaque,sha2(concat('r156:',cast(id AS STRING)),256) new_opaque FROM "+E+' VERSION AS OF 0)')
q=PropertyApply(E,S,0,15,'r156-b1','r139-b1','failure-r156',9007199254741201,eligibility_placement='on')
assert c.sql('stage-membership',q.membership())==[['1000','1000']]
assert c.sql('intended',q.intended())==[['0']]
c.sql('origin',origin_sql(S));proof=verified_origin(S,1000,c.records[-1])
wire=encode('named_struct('+','.join("'"+col+"',"+col for col in list(COLS)+['old_json'])+')')
# Deliberately self-consistent digest of corrupted payload: exact comparison must still fail.
raw_insert=f"INSERT INTO {R} SELECT source_feed,source_epoch,source_delivery_id,'synthetic-scheduled-wide-edge',source_cursor_json,corrupt,sha2(corrupt,256),schema_revision,current_timestamp(),apply_batch_id FROM (SELECT *,concat({wire},' ') corrupt FROM {S} VERSION AS OF 0)"
with ThreadPoolExecutor(max_workers=3) as pool:
 futures=[pool.submit(a.sql,'apply',q.apply()),pool.submit(c.sql,'corrupt-raw',raw_insert),pool.submit(j.sql,'journal',f'INSERT INTO {J} '+expected(S))]
 for f in futures:f.result()
v=int(c.sql('new-edge-version','DESCRIBE HISTORY '+E+' LIMIT 1')[0][0]);rv=int(c.sql('new-raw-version','DESCRIBE HISTORY '+R+' LIMIT 1')[0][0]);jv=int(c.sql('new-journal-version','DESCRIBE HISTORY '+J+' LIMIT 1')[0][0])
assert c.sql('current-exact',q.output(v))==[['0']]
assert c.sql('journal-exact',symmetric(expected(S),f"SELECT * FROM {J} VERSION AS OF {jv} WHERE apply_batch_id='r156-b1'"))==[['0']]
errors=c.sql('raw-refusal',raw_validation(S,R,rv,'r156-b1',proof));assert errors==[['1000']]
# Same controller condition as publication harness: nonzero validation must stop before INSERT.
blocked=False
try:
 assert errors==[['0']], 'Raw validation refusal: new manifest must not be submitted'
 c.sql('publish-new',f'INSERT INTO {M} SELECT '+','.join(map(lit,['r156-b1',old_values[1],json.dumps({E:v,R:rv,J:jv}),'{}','{}','{}']))+',current_timestamp()')
except AssertionError:blocked=True
assert blocked and not any(r['label']=='publish-new' for r in c.records)
assert c.sql('manifest-unchanged',f'SELECT * FROM {M} ORDER BY publication_id')==old_manifest
assert c.sql('old-pinned-carriers',f"SELECT count(*),count(DISTINCT id) FROM {E} VERSION AS OF 0 WHERE entity_version=15 AND apply_batch_id='r139-b1'")==[['1000','1000']]
assert c.sql('unpublished-carriers',f"SELECT count(*),count(DISTINCT id) FROM {E} VERSION AS OF {v} WHERE entity_version=16 AND apply_batch_id='r156-b1'")==[['1000','1000']]
assert c.sql('canonical-anchor',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0]=='23'
owned={}
for i,t in enumerate([E,R,J,M,S]):
 rows=c.sql('detail-'+str(i),'DESCRIBE DETAIL '+t);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 owned[t]={'id':dict(zip(names,rows[0]))['id'],'version':int(c.sql('version-'+str(i),'DESCRIBE HISTORY '+t+' LIMIT 1')[0][0])}
report={'state':'1000 corrupt raw records refused; current/journal exact, old manifest unchanged and pinned rows retained', 'owned_identity':owned,'old_vector':old_vector,'unpublished_versions':{E:v,R:rv,J:jv},'refused_rows':1000,'qualification':'Synthetic1000-edge property slice; validation failure after successful independent writes, not process crash, transport ambiguity or durable recovery/fencing admission. Source record digest is self-consistent but exact bytes differ, so publication is refused. Unpublished versions retained for inspection then owned cleanup; no mutation retry.'}
(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
for client in (c,a,j):client.history();client.close()
print('Failed raw lane refused; old publication and pinned rows retained')

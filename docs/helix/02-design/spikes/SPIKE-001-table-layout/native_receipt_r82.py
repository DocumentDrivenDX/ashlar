"""Serialized receipt storage/replay only; no source authority claim."""
import json,hashlib,time
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_receipt_20261006_r82';c=Client(O);start=time.time()
N='client_dev.ashlar_layout_v03_20261006_r73';T=N+'.publication_receipt_r82';S=N+'.publication_receipt_stage_r82'
assert c.sql('absent-private-tables',f"SHOW TABLES IN {N} LIKE 'publication_receipt*_r82'")==[]
ddl=(B/'sql/publication-receipt-candidate.sql').read_text().split('CREATE TABLE publication_receipt (',1)[1].split(';',1)[0]
c.sql('receipt-ddl','CREATE TABLE '+T+' ('+ddl)
base={'profile_version':'ashlar-publication-receipt/0.1','stream':'synthetic','apply_batch_id':'r82','predecessor_publication_id':'fixture-old','authority_context':{'profile':'serialized-fixture/1','worker_generation':'18446744073709551615'},'source_boundaries':[{'profile':'synthetic-only/1','epoch':'e','cursor':{'xid':'9223372036854775808','seq':'1'},'completeness':'fixture assertion only'}],'input_table_versions':{'fixture_stage':0},'input_digest':'synthetic-placeholder; not a validated stage digest'}
rows=[]
for phase in ['intent','complete']:
 payload={**base,'phase':phase}
 if phase=='complete':payload.update({'output_table_versions':{'fixture_output':1},'validation_report':{'scope':'synthetic storage only; no real input/output membership validation'}})
 encoded=json.dumps(payload,separators=(',',':'));rows.append({'stream':'synthetic','apply_batch_id':'r82','phase':phase,'profile_version':base['profile_version'],'receipt_json':encoded,'receipt_digest':hashlib.sha256(encoded.encode()).hexdigest(),'recorded_at':'2026-10-06T00:00:00Z'})
c.sql('stage-ddl','CREATE TABLE '+S+' ('+ddl)
schema='ARRAY<STRUCT<stream:STRING,apply_batch_id:STRING,phase:STRING,profile_version:STRING,receipt_json:STRING,receipt_digest:STRING,recorded_at:STRING>>'
c.sql('stage-load',f"INSERT INTO {S} SELECT r.stream,r.apply_batch_id,r.phase,r.profile_version,r.receipt_json,r.receipt_digest,cast(r.recorded_at AS TIMESTAMP) FROM (SELECT explode(from_json(:payload,'{schema}')) r)",parameters=[{'name':'payload','type':'STRING','value':json.dumps(rows)}])
assert c.sql('stage-keys-digest',f'SELECT count(*),count(DISTINCT struct(stream,apply_batch_id,phase)),count_if(receipt_digest IS DISTINCT FROM sha2(receipt_json,256)) FROM {S}')==[['2','2','0']]
merge=f'''MERGE INTO {T} t USING {S} s ON t.stream=s.stream AND t.apply_batch_id=s.apply_batch_id AND t.phase=s.phase
WHEN MATCHED AND (t.profile_version IS DISTINCT FROM s.profile_version OR t.receipt_json IS DISTINCT FROM s.receipt_json OR t.receipt_digest IS DISTINCT FROM s.receipt_digest) THEN UPDATE SET receipt_json=cast(raise_error('RECEIPT_CONFLICT') AS STRING)
WHEN NOT MATCHED THEN INSERT *'''
c.sql('initial-append',merge)
fields='stream,apply_batch_id,phase,profile_version,receipt_json,receipt_digest,cast(unix_micros(recorded_at) AS STRING)'
before=c.sql('initial-parity',f'SELECT {fields} FROM {T} ORDER BY phase')
expected=[[r['stream'],r['apply_batch_id'],r['phase'],r['profile_version'],r['receipt_json'],r['receipt_digest'],'1791244800000000'] for r in sorted(rows,key=lambda r:r['phase'])]
# Timestamp instant independently calculated, avoiding a hardcoded epoch assumption.
import datetime
for r in expected:r[-1]=str(int(datetime.datetime(2026,10,6,tzinfo=datetime.timezone.utc).timestamp()*1000000))
assert before==expected
c.sql('identical-replay',merge);assert c.sql('retry-parity',f'SELECT {fields} FROM {T} ORDER BY phase')==before
c.sql('valid-digest-conflict',f"UPDATE {S} SET receipt_json=concat(receipt_json,' '),receipt_digest=sha2(concat(receipt_json,' '),256) WHERE phase='complete'")
try:
 c.sql('conflict-refusal',merge);raise AssertionError('Conflict accepted')
except RuntimeError:
 assert c.records[-1]['response']['status']['state']=='FAILED' and 'RECEIPT_CONFLICT' in json.dumps(c.records[-1]['response']['status'])
assert c.sql('conflict-parity',f'SELECT {fields} FROM {T} ORDER BY phase')==before
version=int(c.sql('actual-version',f'DESCRIBE HISTORY {T} LIMIT 1')[0][0])
summary={'state':'passed','table':T,'version':version,'records':2,'checks':['candidate seven-column native DDL','exact intent/complete payload and unsigned integer text','unique fixture keys and native digest checks','identical retry preserves all fields','byte-different valid-digest retry fails with complete target parity'],'elapsed_seconds':time.time()-start,'cost':'Two receipt and two stage rows on existing authorized warehouse; no new compute; billing dollars unavailable.','scope':'Serialized single-table receipt carrier and replay/refusal only. Input digest and boundary validation are synthetic placeholders. No source membership/authority, phase-link validation, stale-owner prevention, concurrent uniqueness, native manifest installation, performance or scale claim.'}
(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('Native receipt storage/replay checks passed',flush=True)

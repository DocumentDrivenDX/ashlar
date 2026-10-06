"""Synthetic source-envelope preservation; not a Truss transport implementation."""
import base64,hashlib,json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;N='client_dev.ashlar_layout_v02_20261006_r65'
out=B/'out/native/ashlar_source_record_20261006_r71_resume';c=Client(out);T=N+'.source_record_r71'
assert c.sql('empty-recovery',f'SELECT count(*) FROM {T}')==[['0']]
assert c.sql('create-only-recovery',f'DESCRIBE HISTORY {T} LIMIT 1')[0][0]=='0'
ddl='\n'.join(x for x in (B/'sql/source-record-candidate.sql').read_text().splitlines() if not x.strip().startswith('--'))
# Prior CREATE succeeded; failed INSERT was terminal with no rows.
# Exact transport text deliberately retains native JSON tokens and unknown origin.
payloads=[('change','{"entity_kind":"o","entity_id":"1","entity_type":"1","ver":"2","op":"update","prop_id":"101","old_present":true,"old_value":null,"new_present":true,"new_value":9007199254740993,"rev":"3","origin":{"db_role":"app_w","x-host":{"decimal":1.2300e+04}},"xid":"18446744073709551614","seq":"7"}'),('revision','{"rev":"3","documents":[{"document_base64":"eyJ4IjoxfQo=","content_sha256":"'+hashlib.sha256(b'{"x":1}\n').hexdigest()+'","umf_version":"uninterpreted-fixture"}],"origin":{"actor":"fixture"}}'),('source','{"entity_kind":"o","entity_id":"1","load_id":"load-1","source":{"author":"fixture","x-future":[null,"x"]}}'),('reservation','{"entity_kind":"o","entity_type":"1","key_num":"2","key_text":"[\\"a\\",\\"b\\"]","entity_id":"1","ver":"3"}'),('x-future','{"unknown":{"null":null,"decimal":0.00000000000000000001,"timestamp":"2026-10-06T12:00:00+05:30"}}')]
def lit(s):return "decode(unhex('"+s.encode("utf8").hex()+"'),'UTF-8')"
expected=[];values=[]
for ordinal,(kind,payload) in enumerate(payloads,1):
 json.loads(payload) # Validity only; never render decoded numeric values.
 cursor=json.dumps({'xid':'18446744073709551614','seq':str(ordinal)},separators=(',',':'))
 delivery='fixture-raw/'+str(ordinal) # Synthetic durable IDs, not native derivation.
 digest=hashlib.sha256(payload.encode('utf8')).hexdigest()
 expected.append([delivery,kind,cursor,payload,digest])
 values.append('('+','.join([lit('raw-fixture'),lit('e'),lit(delivery),lit(kind),lit(cursor),lit(payload),lit(digest),lit('3'),'current_timestamp()',lit('r71:1')])+')')
c.sql('append',f'INSERT INTO {T} '+ ' UNION ALL '.join('SELECT '+v[1:-1] for v in values))
actual=c.sql('exact',f'SELECT delivery_id,record_kind,source_cursor_json,payload_json,payload_digest FROM {T} ORDER BY delivery_id')
assert actual==expected
assert c.sql('digest',f'SELECT count_if(payload_digest IS DISTINCT FROM sha2(payload_json,256)),count(DISTINCT struct(source_feed,source_epoch,delivery_id)) FROM {T}')==[['0','5']]
# Read-only retry/conflict classification. Does not implement an atomic writer.
for label,payload,want in [('replay',payloads[0][1],'same'),('conflict',payloads[0][1]+' ','conflict')]:
 assert c.sql(label,f"SELECT CASE WHEN payload_json={lit(payload)} THEN 'same' ELSE 'conflict' END FROM {T} WHERE delivery_id='fixture-raw/1'")==[[want]]
revision=json.loads(actual[1][3])['documents'][0]
assert hashlib.sha256(base64.b64decode(revision['document_base64'],validate=True)).hexdigest()==revision['content_sha256']
version=int(c.sql('version',f'DESCRIBE HISTORY {T} LIMIT 1')[0][0])
(out/'fixture.json').write_text(json.dumps({'records':expected,'encoding':'synthetic raw envelope v1; UTF-8 exact transport JSON; decimal-string cursors; explicit presence flags; revision bytes base64','delivery_ids':'synthetic fixture IDs, native derivation undefined'},indent=2)+'\n')
(out/'summary.json').write_text(json.dumps({'state':'passed','table':T,'version':version,'records':5,'scope':'Synthetic raw-envelope exact storage and SHA256 checks for change/revision/source/reservation/unknown kind, unsigned-range cursor text, unknown origin, exact JSON values and base64 revision bytes. Read-only replay/conflict classification only. No atomic append/refusal, native transport, safe-watermark ordering, producer or UMF interpretation claim.'},indent=2)+'\n')
print('Five source-record kinds passed exact native storage',flush=True)

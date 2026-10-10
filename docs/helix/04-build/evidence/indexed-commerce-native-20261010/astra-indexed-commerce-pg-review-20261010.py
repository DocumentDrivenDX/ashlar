from pathlib import Path
import json,hashlib
from local_outbox_connection import connect
from postgres_transactions import Session
from protected_outbox_ack import AckScope,receipt_bytes
P=Path('/private/tmp/ashlar-indexed-commerce-publication-20261010-a');Q=Path('/private/tmp/ashlar-indexed-commerce-queries-20261010-b');p=json.loads((P/'report.json').read_bytes());q=json.loads((Q/'report.json').read_bytes());scope=AckScope(**p['protected_ack_scope']);role=scope.service_schema.replace('pipeline_','operator_');con=connect(role)
try:
 s=Session(con);binding,=s.query('SELECT "'+scope.service_schema+'".scope_binding(CAST(:scope AS uuid)) AS binding',{'scope':scope.scope_id}).rows;binding=binding['binding'];assert binding['source_schema']==p['source_schema']and binding['source_signature_sha256']==p['source_signature_sha256']
 obs,=s.query('SELECT * FROM "'+scope.service_schema+'".observe(CAST(:scope AS uuid),CAST(:position AS bigint))',{'scope':scope.scope_id,'position':'1'}).rows;assert obs==p['protected_ack_observation']==q['relationship']['closed_interval']['closing_ack']['observation']
 identity,=s.query('SELECT current_user,session_user,current_database() AS database,CAST(inet_server_addr() AS TEXT) AS server_address,CAST(inet_server_port() AS TEXT) AS server_port,CAST(pg_backend_pid() AS TEXT) AS backend_pid',{}).rows;assert identity['current_user']==identity['session_user']==role and identity['database']=='truss_e2e'
 expected=receipt_bytes(scope,bytes.fromhex(obs['request_hex']),bytes.fromhex(obs['manifest_hex']))[-1].hex();assert obs['receipt_hex']==expected
 con.rollback()
finally:con.close()
sha=lambda b:hashlib.sha256(b).hexdigest()
result={'scope':'Fresh ordinary-session read-only observation after native query completion; no data mutation or engine rerun','source_signature_sha256':p['source_signature_sha256'],'source_schema':p['source_schema'],'ack_scope':p['protected_ack_scope'],'session':identity,'position':obs['position'],'original_observation_equal':True,'request_sha256':sha(bytes.fromhex(obs['request_hex'])),'manifest_sha256':sha(bytes.fromhex(obs['manifest_hex'])),'receipt_sha256':sha(bytes.fromhex(obs['receipt_hex'])),'connection_rolled_back_and_closed':True}
out=Path('/private/tmp/astra-indexed-commerce-pg-review-20261010.json');out.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'receipt':str(out),'sha256':sha(out.read_bytes()),'ordinary_role':role,'position':obs['position']}))

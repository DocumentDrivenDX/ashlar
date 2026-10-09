"""Closed private-local immutable export custody; hashes require trusted host input.

Correspondence checks do not authorize a source, mint a lease, or establish future
native availability. A separately admitted host must supply both trusted digests.
"""
import hashlib,json,re
from local_delta_custody import encoded
from protected_outbox_ack import AckScope,receipt_bytes
from ashlar.source_checkpoint import validate_checkpoint_request
PROFILE='ashlar-private-local-graph-custody/0.1'
ENCODING='python-json-sorted-utf8-no-newline/0.1'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def _pairs(pairs):
 result={}
 for k,v in pairs:
  if k in result:raise ValueError('Duplicate custody JSON member')
  result[k]=v
 return result
def _decode(raw):
 if type(raw)is not bytes or not 0<len(raw)<=16777216:raise ValueError('Bounded exact custody bytes required')
 return json.loads(raw.decode('utf-8'),object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite custody value')))
def _closed(value,keys):
 if type(value)is not dict or set(value)!=set(keys):raise ValueError('Closed original custody inventory required')
def _digest(value):
 if type(value)is not str or not re.fullmatch('[0-9a-f]{64}',value):raise ValueError('Exact SHA256 text required')
def _hex(value):
 if type(value)is not str or not value or len(value)>8388608 or len(value)%2 or not re.fullmatch('[0-9a-f]+',value):raise ValueError('Bounded canonical original hex required')
 return bytes.fromhex(value)
def _session(value):
 _closed(value,('current_user','session_user','database','server_address','server_port','backend_pid','source_schema','source_signature_sha256','connection_route'))
 if any(type(value[k])is not str or not value[k]or '\x00'in value[k]for k in ('current_user','session_user','database','source_schema')):raise ValueError('Original ordinary PostgreSQL identities required')
 if value['connection_route']!='private-local-postgresql' or type(value['backend_pid'])is not str or not re.fullmatch('[1-9][0-9]*',value['backend_pid']):raise ValueError('Actual private native PostgreSQL session required')
 for k in ('server_address','server_port'):
  if value[k]is not None and(type(value[k])is not str or not value[k]or '\x00'in value[k]):raise ValueError('Original native socket/address identity required')
 _digest(value['source_signature_sha256'])
def admit_private_custody(release,payload,trusted_receipt_sha256,trusted_release_sha256):
 """Verify structurally bound immutable bytes under explicit trusted-host digests."""
 _digest(trusted_receipt_sha256);_digest(trusted_release_sha256)
 if type(payload)is not bytes or sha(payload)!=trusted_receipt_sha256:raise ValueError('Trusted private custody receipt digest mismatch')
 value=_decode(payload);_closed(value,('format','encoding','releaseSha256','manifestSha256','originalManifest','snapshots','roles','sourceAdmissionSha256','ack','interval','scope'))
 if (encoded(value)+'\n').encode()!=payload:raise ValueError('Canonical original custody receipt bytes required')
 if value['format']!=PROFILE or value['encoding']!=ENCODING or value['scope']!='immutable-export-bytes-only':raise ValueError('Explicit private export profile/scope required')
 if value['releaseSha256']!=trusted_release_sha256:raise ValueError('Receipt binds another immutable release')
 manifest=release['publication'];report=json.loads(manifest['validation_report_json'],object_pairs_hook=_pairs)
 if type(report)is not dict:raise ValueError('Original validation inventory required')
 if 'retention'in report:
  if type(report['retention'])is not dict or 'targets'in report['retention']:raise ValueError('Private profile cannot rescue original retention inventory')
 if value['originalManifest']!=manifest or type(manifest)is not dict or any(type(v)is not str for v in manifest.values()):raise ValueError('Every exact original manifest string must match')
 _digest(value['manifestSha256']);_digest(value['sourceAdmissionSha256'])
 if value['manifestSha256']!=sha(encoded(manifest).encode()):raise ValueError('Exact original manifest encoding/digest differs')
 if value['snapshots']!=release['snapshots']or value['roles']!=release['roles']:raise ValueError('Original full UUID/version/role vector differs')
 versions=json.loads(manifest['table_versions_json'],object_pairs_hook=_pairs)
 if type(value['snapshots'])is not dict or set(value['snapshots'])!=set(versions):raise ValueError('Complete original snapshot inventory required')
 for table,snapshot in value['snapshots'].items():
  _closed(snapshot,('uuid','version'))
  if type(snapshot['version'])is not int or type(versions[table])is not int or snapshot['version']!=versions[table]:raise ValueError('Original exact snapshot versions differ')
 source=report.get('source_admission')
 if type(source)is not dict or value['sourceAdmissionSha256']!=sha(encoded(source).encode()):raise ValueError('Original source admission custody differs')
 interval=value['interval'];_closed(interval,('wholeVectorHeld','closingSucceeded'))
 if interval['wholeVectorHeld']is not True or interval['closingSucceeded']is not True:raise ValueError('All original contexts must close successfully')
 ack=value['ack'];_closed(ack,('scope','requestHex','manifestHex','opening','closing'))
 _closed(ack['scope'],('service_schema','scope_id','installation_id','consumer','feed','epoch'));scope=AckScope(**ack['scope'])
 request_raw=_hex(ack['requestHex']);manifest_raw=_hex(ack['manifestHex']);request,ack_manifest,checkpoint,receipt=receipt_bytes(scope,request_raw,manifest_raw)
 validate_checkpoint_request(request)
 if manifest_raw!=encoded(manifest).encode()or ack_manifest!=manifest:raise ValueError('Protected ACK binds another original manifest')
 if json.loads(manifest['source_progress_json'],object_pairs_hook=_pairs).get(scope.feed)!=checkpoint or json.loads(manifest['schema_revisions_json'],object_pairs_hook=_pairs)!=json.loads(request['schema_revisions_json'],object_pairs_hook=_pairs)or report.get('request_digest')!=request['request_digest']:raise ValueError('Original published request/source progress differs')
 sessions=[]
 for phase in ('opening','closing'):
  observed=ack[phase];_closed(observed,('session','observation'));_session(observed['session']);sessions.append(observed['session'])
  row=observed['observation'];_closed(row,('position','request_hex','manifest_hex','receipt_hex'))
  position=row['position']
  if type(position)is not str or len(position)>19 or not re.fullmatch('0|[1-9][0-9]*',position)or not int(checkpoint['position'])<=int(position)<2**63:raise ValueError('Actual native ACK head does not include original published group')
  if row!={'position':position,'request_hex':ack['requestHex'],'manifest_hex':ack['manifestHex'],'receipt_hex':receipt.hex()}:raise ValueError('Actual opening/closing protected ACK custody differs')
 if sessions[0]!=sessions[1]:raise ValueError('Original held ordinary PostgreSQL session changed')
 return value

def build_private_custody(release,source_admission,ack_capture):
 """Called only after the trusted host's complete interval exits successfully."""
 value=json.loads(release.payload);manifest=value['publication']
 receipt={'format':PROFILE,'encoding':ENCODING,'releaseSha256':release.sha256,'manifestSha256':sha(encoded(manifest).encode()),'originalManifest':manifest,'snapshots':value['snapshots'],'roles':value['roles'],'sourceAdmissionSha256':sha(encoded(source_admission).encode()),'ack':ack_capture,'interval':{'wholeVectorHeld':True,'closingSucceeded':True},'scope':'immutable-export-bytes-only'}
 payload=(encoded(receipt)+'\n').encode();admit_private_custody(value,payload,sha(payload),release.sha256)
 return payload

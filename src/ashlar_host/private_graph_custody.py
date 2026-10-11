"""Closed private-local immutable export custody; hashes require trusted host input.

Correspondence checks do not authorize a source, mint a lease, or establish future
native availability. A separately admitted host must supply both trusted digests.
"""
import hashlib,json,re
from .delta_custody import encoded
from .ack import AckScope,receipt_bytes
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
def _integer(text):
 if len(text)>20:raise ValueError('Bounded custody integer required')
 return int(text)
def _decode(raw):
 if type(raw)is not bytes or not 0<len(raw)<=16777216:raise ValueError('Bounded exact custody bytes required')
 return json.loads(raw.decode('utf-8'),object_pairs_hook=_pairs,parse_int=_integer,parse_float=lambda _:(_ for _ in ()).throw(ValueError('Floating custody value')),parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite custody value')))
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
 release=_owned_json(release)
 _digest(trusted_receipt_sha256);_digest(trusted_release_sha256)
 if type(payload)is not bytes or sha(payload)!=trusted_receipt_sha256:raise ValueError('Trusted private custody receipt digest mismatch')
 value=_decode(payload);_closed(value,('format','encoding','releaseSha256','manifestSha256','originalManifest','snapshots','roles','sourceAdmissionSha256','ack','interval','scope'))
 if (encoded(value)+'\n').encode()!=payload:raise ValueError('Canonical original custody receipt bytes required')
 if value['format']!=PROFILE or value['encoding']!=ENCODING or value['scope']!='immutable-export-bytes-only':raise ValueError('Explicit private export profile/scope required')
 if value['releaseSha256']!=trusted_release_sha256:raise ValueError('Receipt binds another immutable release')
 manifest=release['publication'];report=json.loads(manifest['validation_report_json'],object_pairs_hook=_pairs,parse_int=_integer,parse_float=lambda _:(_ for _ in ()).throw(ValueError('Floating original value')),parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite original value')))
 if type(report)is not dict:raise ValueError('Original validation inventory required')
 if 'retention'in report:
  if type(report['retention'])is not dict or 'targets'in report['retention']:raise ValueError('Private profile cannot rescue original retention inventory')
 if value['originalManifest']!=manifest or type(manifest)is not dict or any(type(v)is not str for v in manifest.values()):raise ValueError('Every exact original manifest string must match')
 _digest(value['manifestSha256']);_digest(value['sourceAdmissionSha256'])
 if value['manifestSha256']!=sha(encoded(manifest).encode()):raise ValueError('Exact original manifest encoding/digest differs')
 if value['snapshots']!=release['snapshots']or value['roles']!=release['roles']:raise ValueError('Original full UUID/version/role vector differs')
 versions=json.loads(manifest['table_versions_json'],object_pairs_hook=_pairs,parse_int=_integer,parse_float=lambda _:(_ for _ in ()).throw(ValueError('Floating original value')),parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite original value')))
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
 if json.loads(manifest['source_progress_json'],object_pairs_hook=_pairs,parse_int=_integer,parse_float=lambda _:(_ for _ in ()).throw(ValueError('Floating original value')),parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite original value'))).get(scope.feed)!=checkpoint or json.loads(manifest['schema_revisions_json'],object_pairs_hook=_pairs,parse_int=_integer,parse_float=lambda _:(_ for _ in ()).throw(ValueError('Floating original value')),parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite original value')))!=json.loads(request['schema_revisions_json'],object_pairs_hook=_pairs,parse_int=_integer,parse_float=lambda _:(_ for _ in ()).throw(ValueError('Floating original value')),parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite original value')))or report.get('request_digest')!=request['request_digest']:raise ValueError('Original published request/source progress differs')
 sessions=[]
 for phase in ('opening','closing'):
  observed=ack[phase];_closed(observed,('session','observation'));_session(observed['session']);sessions.append(observed['session'])
  row=observed['observation'];_closed(row,('position','request_hex','manifest_hex','receipt_hex'))
  position=row['position']
  if type(position)is not str or len(position)>19 or not re.fullmatch('0|[1-9][0-9]*',position)or not int(checkpoint['position'])<=int(position)<2**63:raise ValueError('Actual native ACK head does not include original published group')
  if row!={'position':position,'request_hex':ack['requestHex'],'manifest_hex':ack['manifestHex'],'receipt_hex':receipt.hex()}:raise ValueError('Actual opening/closing protected ACK custody differs')
 if sessions[0]!=sessions[1]:raise ValueError('Original held ordinary PostgreSQL session changed')
 return value


def _owned_json(value):
 """Bounded snapshot of passive carriers, including direct public callers."""
 budget=[100000,16777216]
 def copy(item,depth):
  budget[0]-=1
  if depth>64 or budget[0]<0:raise ValueError('Bounded original JSON structure required')
  if item is None or type(item)is bool:return item
  if type(item)is int:
   if not -(2**63)<=item<2**63:raise ValueError('Bounded original integer required')
   return item
  if type(item)is str:
   budget[1]-=len(item.encode('utf-8'))
   if budget[1]<0:raise ValueError('Bounded original text required')
   return item
  if type(item)is list:return [copy(v,depth+1)for v in item]
  if type(item)is dict:
   if any(type(k)is not str for k in item):raise ValueError('Exact JSON member names required')
   return {copy(k,depth+1):copy(v,depth+1)for k,v in item.items()}
  raise ValueError('Exact passive original JSON carriers required')
 return copy(value,0)

from ashlar.graph_release import NODE_INTS, NODE_TEXT, EDGE_INTS, EDGE_TEXT, validate_release_carrier
FORMAT="ashlar-graph-release/0.1"
_object=_pairs
def _release_lineage(value):
    if type(value) is not dict or set(value)!={'format','publication','snapshots','roles','nodes','edges','mapping'}:
        raise ValueError('Closed complete release envelope required')
    publication=value['publication'];snapshots=value['snapshots'];roles=value['roles'];mapping=value['mapping']
    required={'publication_id','profile_version','recorded_at','table_versions_json','schema_revisions_json','source_progress_json','validation_report_json'}
    if type(publication) is not dict or not required.issubset(publication):raise ValueError('Original manifest fields required')
    for name in required:
        text=publication[name]
        if type(text) is not str or not text or '\x00' in text:raise ValueError('Exact manifest text required')
        text.encode('utf-8')
    if publication['profile_version']!='ashlar-delta/0.3':raise ValueError('Qualified canonical source profile required')
    decoded={}
    for name in ['table_versions_json','schema_revisions_json','source_progress_json','validation_report_json']:
        decoded[name]=json.loads(publication[name],object_pairs_hook=_pairs,parse_int=_integer,parse_float=lambda _:(_ for _ in ()).throw(ValueError('Floating original value')),parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite original value')))
        if type(decoded[name]) is not dict:raise ValueError('Original manifest object carrier required')
    versions=decoded['table_versions_json']
    if type(snapshots) is not dict or not snapshots or set(snapshots)!=set(versions):
        raise ValueError('Complete original manifest snapshot inventory required')
    for table,snapshot in snapshots.items():
        if type(table) is not str or not table or type(snapshot) is not dict or set(snapshot)!={'uuid','version'}:
            raise ValueError('Original table snapshot identity required')
        if type(snapshot['uuid']) is not str or not re.fullmatch('[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}',snapshot['uuid']):
            raise ValueError('Native table UUID required')
        if type(snapshot['version']) is not int or not 0<=snapshot['version']<2**63 or type(versions[table]) is not int or versions[table]!=snapshot['version']:
            raise ValueError('Original manifest snapshot version mismatch')
    if type(roles) is not dict or set(roles)!={'nodes','edges'} or any(type(t) is not str or t not in snapshots for t in roles.values()) or roles['nodes']==roles['edges']:
        raise ValueError('Distinct original snapshot graph roles required')
    expected={'reversibleIdentity':True,'independentEdges':True,'isolatedNodes':True,'exactCanonicalText':True,'selectedScalarPromotion':False,'nativeReleaseMaterialization':False,'engineExecution':False}
    if type(mapping) is not dict or set(mapping)!={'identity','properties','residuals','capabilities','losses','engineSupport'}:
        raise ValueError('Complete mapping capability/loss inventory required')
    if mapping['identity']!='ashlar-key/1' or type(mapping['properties']) is not str or not mapping['properties'] or type(mapping['residuals']) is not list or not mapping['residuals'] or any(type(t) is not str or not t for t in mapping['residuals']):
        raise ValueError('Named exact carrier mapping required')
    capabilities=mapping['capabilities']
    if type(capabilities) is not dict or capabilities!=expected or any(type(v) is not bool for v in capabilities.values()) or mapping['losses']!=[] or mapping['engineSupport']!=[]:
        raise ValueError('Supported lossless unexecuted release capability inventory required')

def load_private_graph_release(payload, trusted_sha256, custody_payload, trusted_custody_sha256):
    if type(payload) is not bytes or len(payload)>16*1024*1024 or type(trusted_sha256) is not str or not re.fullmatch('[0-9a-f]{64}',trusted_sha256) or hashlib.sha256(payload).hexdigest()!=trusted_sha256:
        raise ValueError('Trusted release byte digest mismatch')
    value=json.loads(payload.decode('utf-8'),object_pairs_hook=_pairs,parse_int=_integer,parse_float=lambda _: (_ for _ in ()).throw(ValueError("Floating release value")),
                    parse_constant=lambda x: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))
    _release_lineage(value)
    admit_private_custody(value,custody_payload,trusted_custody_sha256,trusted_sha256)
    if value.get('format')!=FORMAT or value.get('mapping',{}).get('identity')!='ashlar-key/1':
        raise ValueError('Unsupported named release/identity encoding')
    if (json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()!=payload:
        raise ValueError('Noncanonical exact release encoding')
    for kind,role in [('node','nodes'),('edge','edges')]:
        rows=value.get(role)
        if type(rows) is not list or len(rows)>10000:raise ValueError('Bounded row inventory required')
        names=(NODE_INTS+NODE_TEXT if kind=='node' else EDGE_INTS+EDGE_TEXT)+('published_at',)
        for row in rows:
            if type(row) is not dict:raise ValueError('Object carrier required')
            original={name:row[name] for name in names if name in row}
            if validate_release_carrier(original,kind)!=row:raise ValueError('Closed exact graph carrier mismatch')
        if len({row['graph_id'] for row in rows})!=len(rows):raise ValueError('Duplicate graph identity')
    nodes={row['graph_id'] for row in value['nodes']}
    if any(row['src'] not in nodes or row['dst'] not in nodes for row in value['edges']):
        raise ValueError('Dangling endpoint')
    return value


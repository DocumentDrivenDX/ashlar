"""Strict canonical public graph-release carrier admission, without engine authority.

Byte and manifest correspondence cannot establish current source/read authority;
preparation and activation must independently admit the original release.
Private-custody profiles require their separate owner and are not accepted here.
"""
import hashlib
import json
import re
from ashlar.graph_release import NODE_INTS, NODE_TEXT, EDGE_INTS, EDGE_TEXT, validate_release_carrier

FORMAT='ashlar-graph-release/0.1'

def _object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('Duplicate JSON member')
        result[key]=value
    return result

def _lineage(value,*,private_custody_admitted=False):
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
        decoded[name]=json.loads(publication[name],object_pairs_hook=_object)
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
    if not private_custody_admitted:
        retention=decoded['validation_report_json'].get('retention',{}).get('targets')
        if type(retention) is not dict or set(retention)!=set(snapshots):
            raise ValueError('Original manifest UUID/version custody inventory required')
        for table,snapshot in snapshots.items():
            target=retention[table]
            if type(target) is not dict or target.get('uuid')!=snapshot['uuid'] or type(target.get('version')) is not int or target['version']!=snapshot['version']:
                raise ValueError('Original manifest UUID/version custody differs')
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

def load_graph_release(payload, trusted_sha256):
    if type(payload) is not bytes or len(payload)>16*1024*1024 or type(trusted_sha256) is not str or not re.fullmatch('[0-9a-f]{64}',trusted_sha256) or hashlib.sha256(payload).hexdigest()!=trusted_sha256:
        raise ValueError('Trusted release byte digest mismatch')
    value=json.loads(payload.decode('utf-8'),object_pairs_hook=_object,
                    parse_constant=lambda x: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))
    _lineage(value)
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


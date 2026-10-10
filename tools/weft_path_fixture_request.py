"""Requests for the separately authored fixture; no publication or compiler authority.

The caller owns actual publication resolution and the public receipt verifier.
This pure builder never opens a provider, executes SQL or registers aliases.
"""
import copy
import hashlib
import json
import re
from weft_path_plan import BACKEND

LAYOUT_SHA = 'ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e'
INPUT_HASHES = {'model': '5a6c6de5abb1918e71807c0bc9f5e3ddd31c1aaf5341f1772da0f97ac083148c', 'graph': 'cd5d72bfe58cb532c6a17c2df9cb45f56086cf8126e9830bb7a335cbf098d915', 'registry': 'c29037617cca4e7d7aa29cb1768036244396db7ca2f1a47445c6dbb31b14ee97'}


def decode_public_receipt(raw: bytes) -> dict:
    """Bound receipt numeric work before conversion; logical wide tokens are text."""
    if type(raw) is not bytes or not 0 < len(raw) <= 4_000_000:
        raise ValueError('Bounded public receipt bytes required')
    def integer(token):
        if len(token.lstrip('-')) > 20:raise ValueError('Receipt integer capacity exceeded')
        return int(token)
    def refused(token):raise ValueError('Receipt fractional/nonfinite atom refused')
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate receipt member')
            out[key]=value
        return out
    return json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refused,parse_constant=refused)


def authored_path_request(sql: str, *, model_bytes: bytes, graph_bytes: bytes,
                          development_registry_bytes: bytes, public_receipt_bytes: bytes,
                          verify_public_receipt, publication: dict, aliases: dict) -> dict:
    """Bind exact authored input to caller-supplied complete immutable pin metadata."""
    inputs = (model_bytes, graph_bytes, development_registry_bytes, public_receipt_bytes)
    if any(type(b) is not bytes or not 0 < len(b) <= 4_000_000 for b in inputs):
        raise ValueError('Bounded exact input bytes required')
    for name, raw in zip(('model', 'graph', 'registry'), inputs):
        if hashlib.sha256(raw).hexdigest() != INPUT_HASHES[name]:
            raise ValueError('Exact separately authored input required')
    if type(sql) is not str or not sql or len(sql.encode()) > 65536 or not callable(verify_public_receipt):
        raise ValueError('Explicit query and public verification port required')
    model, graph, registry = map(json.loads, inputs[:3])
    public = decode_public_receipt(public_receipt_bytes)
    publication, aliases = copy.deepcopy(publication), copy.deepcopy(aliases)
    if public.get('sourceSha256') != INPUT_HASHES['model'] or public.get('graphSha256') != INPUT_HASHES['graph']:
        raise ValueError('Public source correspondence refused')
    if public.get('umfRevision') != 'c7c95e1c4ea5b72541f47fa0350ca467ff02f395' or public.get('profile') != 'ashlar-authored-path-public-dataset/0.1':
        raise ValueError('Exact public producer profile required')
    receipt = public.get('receipt', {})
    if (receipt.get('source') != model or receipt.get('datasetValidation', {}).get('valid') is not True
            or receipt.get('datasetValidation', {}).get('complete') is not True
            or [len(receipt.get(k, [])) for k in ('records', 'keys', 'relationships')] != [10, 10, 10]):
        raise ValueError('Complete original public dataset receipt required')
    if verify_public_receipt(model_bytes, graph_bytes, public_receipt_bytes) is not None:
        raise ValueError('Public verification must complete without a result')
    if type(publication) is not dict or set(publication) != {'id', 'manifestUuid', 'tables'}:
        raise ValueError('Complete explicit publication metadata required')
    uuid = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
    if any(type(publication[k]) is not str or not uuid.fullmatch(publication[k]) for k in ('id', 'manifestUuid')):
        raise ValueError('Explicit publication identities required')
    tables = publication['tables']
    roles = {'object_current', 'edge_current', 'tombstone', 'whole_source_history'}
    if type(tables) is not list or len(tables) != 4 or type(aliases) is not dict or set(aliases) != roles:
        raise ValueError('Complete canonical table and alias inventory required')
    seen, names = set(), set()
    for table in tables:
        if (type(table) is not dict or set(table) != {'name', 'uuid', 'version'}
                or type(table['name']) is not list or len(table['name']) != 3
                or any(type(n) is not str or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', n) for n in table['name'])
                or table['name'][-1] not in roles or table['name'][-1] in seen
                or type(table['uuid']) is not str or not uuid.fullmatch(table['uuid'])
                or type(table['version']) is not int or not 0 <= table['version'] <= 9223372036854775807
                or aliases[table['name'][-1]] != table['name']):
            raise ValueError('Exact canonical table pin correspondence required')
        seen.add(table['name'][-1]); names.add(tuple(table['name']))
    if len(names) != 4 or len({t['uuid'] for t in tables}) != 4:
        raise ValueError('Injective table inventory required')
    revision = INPUT_HASHES['model']
    pin = {'documentId': model['id'], 'revision': revision, 'sha256': revision, 'umfVersion': '0.8.0'}
    identity = lambda element: {'documentId': model['id'], 'module': 'paths', 'element': element, 'revision': revision}
    object_index = next(i for i,t in enumerate(tables) if t['name'][-1] == 'object_current')
    edge_index = next(i for i,t in enumerate(tables) if t['name'][-1] == 'edge_current')
    records = []
    for record in model['modules'][0]['elements']:
        if record['kind'] != 'record': continue
        records.append({'kind':'object', 'logical':identity(record['id']), 'sourceSystem':registry['sourceSystem'],
                        'typeId':str(registry['types'][record['id']]), 'table':object_index, 'schemaRevision':revision,
                        'properties':[{'logical':identity(f['element']), 'home':{'kind':'props','propertyId':str(registry['properties'][f['element']])}} for f in record['members']]})
    relationships = [{'logical':{'documentId':model['id'],'module':'paths','relationship':r['id'],'revision':revision},
                      'acceptedDefinition':r,'table':edge_index,'kind':'edge','sourceSystem':registry['sourceSystem'],
                      'typeId':str(registry['relationships'][r['id']]),'schemaRevision':revision,
                      'source':identity(r['source'][0]['element']),'target':identity(r['target'][0]['element'])}
                     for r in model['modules'][0]['relationships']]
    binding = {'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3',
               'layoutSha256':LAYOUT_SHA,'modelPins':[pin],'publication':publication,'records':records,'relationships':relationships}
    raw = json.dumps(binding,sort_keys=True,separators=(',',':'),ensure_ascii=False)
    return {'interfaceVersion':'weft-compile/0.4.0','dialect':'weft-sql/0.4.0','sql':sql,
            'modules':[{'documentJson':model_bytes.decode(),'pin':pin,'selectedModuleIds':['paths']}],
            'target':{**{k:BACKEND[k] for k in ('backendId','backendVersion','targetProfile')},
                      'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()},'options':{'allowCandidate':True}}

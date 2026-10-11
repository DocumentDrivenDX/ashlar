"""Owned original commerce whole-source and native-projection correspondence.

The caller must independently hold original public producer and publication
custody. This port retains complete selected source bytes; receipts do not grant
source, reader, or native authority. No compiler/SDK/process imports or effects.
"""
from collections import Counter
import hashlib
import json
from .config import HostError
from .source_identity import SOURCE_SHA

UMF_REVISION = 'c7c95e1c4ea5b72541f47fa0350ca467ff02f395'

def encode_commerce_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)



def encode_bounded_commerce_json(value, maximum):
    """Exact ensure-ascii byte preflight before whole serialization."""
    remaining=maximum
    def charge(amount):
        nonlocal remaining
        remaining-=amount
        if remaining<0:raise HostError('paths-retained-bound')
    def string(text):
        charge(2)
        for char in text:
            n=ord(char)
            if 0xD800<=n<=0xDFFF:raise HostError('paths-json-refused')
            charge(2 if char in '\"\\\b\f\n\r\t' else 6 if n<32 or n>=127 and n<=65535 else 12 if n>65535 else 1)
    def visit(item,depth=0):
        if depth>64:raise HostError('paths-retained-bound')
        if type(item) is str:string(item)
        elif item is None:charge(4)
        elif type(item) is bool:charge(4 if item else 5)
        elif type(item) is int:
            if abs(item)>=10**20:raise HostError('paths-json-refused')
            charge(len(str(item)))
        elif type(item) in (tuple,list):
            charge(2+max(0,len(item)-1))
            for entry in item:visit(entry,depth+1)
        elif type(item) is dict:
            charge(2+max(0,len(item)-1))
            for key,entry in item.items():
                if type(key) is not str:raise HostError('paths-json-refused')
                string(key);charge(1);visit(entry,depth+1)
        else:raise HostError('paths-json-refused')
    visit(value)
    return encode_commerce_json(value)

def parse_commerce_json(raw, maximum):
    if type(raw) is not bytes or len(raw) > maximum:
        raise HostError('paths-artifact-bound')
    def pairs(entries):
        result = {}
        for key,value in entries:
            if key in result: raise HostError('paths-json-refused')
            result[key] = value
        return result
    def integer(token):
        if len(token.lstrip('-')) > 20: raise HostError('paths-json-refused')
        return int(token)
    def numeric(token): raise HostError('paths-json-refused')
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_int=integer,
                          parse_float=numeric, parse_constant=numeric)
    except (ValueError, UnicodeError, RecursionError):
        raise HostError('paths-json-refused') from None


def _commerce_public_source_port(*, model: bytes, graph: bytes, bindings: dict, dataset_receipt: bytes, maximum_bytes: int):
    """Bind actual public whole-source validity to each held native projection.

    The whole dataset was independently reexecuted through the pinned public UMF
    producer. Every requested Field/Record/property and projected row is checked
    against original carriers before returning an owned receipt; metadata alone
    never supplies source validity or native custody.
    """
    maximum = maximum_bytes
    if type(maximum) is not int or not 1 <= maximum <= 16777216:
        raise HostError('paths-retained-bound')
    if type(model) is not bytes or type(graph) is not bytes or type(dataset_receipt) is not bytes:
        raise HostError('paths-public-source-refused')
    model_bytes, graph_bytes, dataset_bytes = model, graph, dataset_receipt
    model = parse_commerce_json(model_bytes, maximum)
    graph = parse_commerce_json(graph_bytes, maximum)
    dataset = parse_commerce_json(dataset_bytes, maximum)
    if type(model) is not dict or type(graph) is not dict or type(dataset) is not dict:
        raise HostError('paths-public-source-refused')
    receipt = dataset.get('receipt')
    validation = receipt.get('datasetValidation') if type(receipt) is dict else None
    if (dataset.get('umfRevision') != UMF_REVISION
            or dataset.get('sourceSha256') != hashlib.sha256(model_bytes).hexdigest()
            or dataset.get('graphSha256') != hashlib.sha256(graph_bytes).hexdigest()
            or type(validation) is not dict or validation.get('valid') is not True
            or validation.get('complete') is not True):
        raise HostError('paths-public-source-refused')
    bindings = parse_commerce_json(encode_bounded_commerce_json(bindings, maximum).encode(), maximum)
    fields = {(m['id'],e['id']):e for m in model['modules'] for e in m['elements']}
    properties = {tuple(p['identity']):p['property_id'] for p in bindings['properties']}
    entities = {(e['kind'],e['originalKey']):e for e in bindings['entities']}

    def correspondence(request, artifact, projections):
        request, artifact, projections = (parse_commerce_json(
            encode_bounded_commerce_json(v, maximum).encode(), maximum)
            for v in (request, artifact, projections))
        if request['modules'][0]['documentJson'].encode('utf8') != model_bytes:
            raise HostError('paths-public-source-refused')
        for projection in projections:
            check=projection['check'];record=check['record'];field=check['field']
            if (record['documentId'] != model['id'] or field['documentId'] != model['id']
                    or record['revision'] != SOURCE_SHA or field['revision'] != SOURCE_SHA
                    or check.get('publicSourceOnly') is not True):
                raise HostError('paths-public-source-refused')
            definition=fields.get((record['module'],record['element']))
            scalar=fields.get((field['module'],field['element']))
            if (not definition or not scalar or definition['kind'] != 'record'
                    or scalar['kind'] != 'field'
                    or {'module':field['module'],'element':field['element']} not in definition['members']
                    or properties.get((model['id'],field['module'],field['element'])) != check['propertyId']):
                raise HostError('paths-public-source-refused')
            logical={'family':scalar['scalarType'],'facets':scalar.get('facets',{}),
                     'nullable':scalar['nullability'] != 'required'}
            if check['logicalType'] != logical:raise HostError('paths-public-source-refused')
            expected=[]
            for obj in graph['objects']:
                if obj['type'] != {'document':model['id'],'module':record['module'],'element':record['element']}:continue
                carrier=entities[('object',obj['key'])]
                props=[]
                for member in definition['members']:
                    authored=fields[(member['module'],member['element'])]
                    key=properties[(model['id'],member['module'],member['element'])]
                    token=obj['values'][member['element']]
                    props.append(json.dumps(key)+':'+(json.dumps(token,ensure_ascii=False,separators=(',',':')) if authored['scalarType']=='string' else token))
                expected.append({'source_system':'private-original-commerce-fixture',
                                 'type_id':carrier['type_id'],'id':carrier['id'],
                                 'schema_revision':SOURCE_SHA,'props_json':'{'+','.join(props)+'}',
                                 'extracted_token':obj['values'][field['element']]})
            if Counter(encode_commerce_json(r) for r in projection['rows']) != Counter(encode_commerce_json(r) for r in expected):
                raise HostError('paths-public-source-projection-refused')
        original={'sourceText':request['modules'][0]['documentJson'],
                  'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],
                  'checks':projections}
        request_text=encode_bounded_commerce_json(original,maximum)
        receipt_text=encode_bounded_commerce_json({'originalRequestText':request_text,'umfRevision':UMF_REVISION,
                              'admitted':True,'originalDatasetReceiptText':dataset_bytes.decode('utf8'),
                              'scope':'Actual pinned public whole dataset plus exact original Field and native projection correspondence; no authority grant'},maximum)
        if len(receipt_text.encode('utf8'))>maximum:raise HostError('paths-public-source-bound')
        return {'originalRequestText':request_text,'originalReceiptText':receipt_text,
                'receiptSha256':hashlib.sha256(receipt_text.encode('utf8')).hexdigest()}
    def public_source(request, artifact, projections):
        try:
            return correspondence(request, artifact, projections)
        except (KeyError, IndexError, TypeError, AttributeError, UnicodeError):
            raise HostError('paths-public-source-refused') from None
    return public_source


def commerce_public_source_port(*, model: bytes, graph: bytes, bindings: dict,
                               dataset_receipt: bytes, maximum_bytes: int):
    """Capture immutable originals; malformed correspondence safely refuses.

    Public producer validity, current source authorization and native custody are
    independently required by the calling composition, never granted here.
    """
    try:
        return _commerce_public_source_port(model=model, graph=graph,
            bindings=bindings, dataset_receipt=dataset_receipt, maximum_bytes=maximum_bytes)
    except (KeyError, IndexError, TypeError, AttributeError, UnicodeError):
        raise HostError('paths-public-source-refused') from None


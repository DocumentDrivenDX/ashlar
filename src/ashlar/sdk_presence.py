"""Exact source-owned engine0.2 scalar envelopes; no raw/legacy widening."""
from dataclasses import dataclass

from .publication import ResolutionError
from .schema import _json
from .weft_decode import decode_exact_scalar

SDK_BACKENDS=(
 {'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-sdk-candidate','interfaceVersion':'weft-backend/0.2.8','targetProfile':'spark4.0.1-delta4.0.0-sdk-candidate'},
 {'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-sdk-pages-candidate','interfaceVersion':'weft-backend/0.2.8','targetProfile':'spark4.0.1-delta4.0.0-sdk-candidate'},
)

@dataclass(frozen=True)
class DecodedPresence:
    state:str
    value:object
    original:str


def descriptor(column,artifact):
    if type(column)is not dict or type(artifact)is not dict or artifact.get('backend')not in SDK_BACKENDS:
        raise ResolutionError('Original explicit scalar-presence profile required')
    columns=artifact.get('columns')
    if (type(columns)is not list or type(column.get('position'))is not int
            or not 1<=column['position']<=len(columns) or columns[column['position']-1]!=column
            or column.get('nullable')is not False):
        raise ResolutionError('Complete non-null original envelope column required')
    representation=column.get('representation')
    if (type(representation)is not dict or set(representation)!={'kind','descriptor','nativeNull'}
            or representation['kind']!='value' or representation['nativeNull']is not True):
        raise ResolutionError('Original native-null scalar envelope representation required')
    identity=representation['descriptor']
    if (type(identity)is not dict or set(identity)!={'documentId','revision','module','element'}
            or any(type(v)is not str or not v for v in identity.values())
            or column.get('sourceIdentities')!=[identity]):
        raise ResolutionError('Exact original projected Field identity required')
    pins=artifact.get('modelPins')
    if type(pins)is not list or not any(type(p)is dict and p.get('documentId')==identity['documentId']and p.get('revision')==identity['revision']for p in pins):
        raise ResolutionError('Projected Field outside original model pins')
    plan=artifact.get('logicalPlan')
    if type(plan)is not dict:raise ResolutionError('Original logical presence plan required')
    if plan.get('version')=='weft-ir/engine-ordered-page/0.1':plan=plan.get('base')
    if type(plan)is not dict or type(plan.get('typeGraph'))is not list:
        raise ResolutionError('Original Field type graph required')
    matches=[d for d in plan['typeGraph']if type(d)is dict and d.get('identity')==identity]
    if len(matches)!=1:raise ResolutionError('Unique original scalar Field descriptor required')
    selected=matches[0]
    logical=selected.get('type')
    if (selected.get('kind')!='scalar' or selected.get('availability')not in ('required','absent-allowed')
            or type(logical)is not dict or set(logical)!={'family','facets','nullable'}
            or logical['family']not in ('string','boolean','integer','decimal')
            or type(logical['nullable'])is not bool):
        raise ResolutionError('Original non-temporal scalar domain required')
    return selected


def decode_presence(column,value,artifact):
    selected=descriptor(column,artifact)
    if type(value)is not str:
        raise ResolutionError('Physical SQL NULL is not an encoded null or missing value')
    try:
        raw=value.encode('utf-8')
        if len(raw)>8*1024*1024:raise ResolutionError('Presence envelope exceeds held decode capacity')
        envelope=_json(raw)
    except (ValueError,UnicodeError)as error:
        raise ResolutionError('Exact bounded scalar envelope required')from error
    if type(envelope)is not dict:raise ResolutionError('Exact scalar state envelope required')
    state=envelope.get('state')
    if state=='absent' and set(envelope)=={'state'}:
        if selected['availability']!='absent-allowed':raise ResolutionError('Required Field cannot be missing')
        return DecodedPresence('missing',None,value)
    if state=='null' and set(envelope)=={'state'}:
        if selected['type']['nullable']is not True:raise ResolutionError('Original Field cannot be null')
        return DecodedPresence('null',None,value)
    if state!='value' or set(envelope)!={'state','value'}:
        raise ResolutionError('Unknown or incomplete scalar state envelope')
    logical=dict(selected['type']);logical['nullable']=False
    family=logical['family']
    representation={'kind':'scalar','logicalType':logical,'carrier':'boolean'if family=='boolean'else'text',
                    'decoder':{'string':'text','boolean':'boolean','integer':'exact-integer','decimal':'exact-decimal'}[family]}
    scalar=decode_exact_scalar(representation,envelope['value'])
    return DecodedPresence('present',scalar.value,value)

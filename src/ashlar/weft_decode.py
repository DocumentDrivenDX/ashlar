"""Exact selected string/presence results; no generic JSON or numeric coercion."""
from .publication import ResolutionError
from .schema import _json
from types import MappingProxyType


def decode_string_column(column,value,policy,intake):
    if policy.source_sha256!=intake.source_sha256:
        raise ResolutionError('Original result model differs from admitted binding')
    representation=column['representation']
    def text(value):
        if type(value) is not str or '\x00' in value:raise ResolutionError('Exact Unicode string required')
        try:value.encode('utf-8')
        except UnicodeError as error:raise ResolutionError('Unicode scalar string required') from error
        return value
    if representation.get('kind')=='scalar':
        if representation!={'carrier':'text','decoder':'text','kind':'scalar','logicalType':{'facets':{},'family':'string','nullable':False}}:
            raise ResolutionError('Unadmitted scalar decoder')
        return text(value)
    if representation.get('kind')!='value' or set(representation)!= {'descriptor','kind','nativeNull'} or representation['nativeNull'] is not False:
        raise ResolutionError('Unadmitted value decoder')
    identity=representation['descriptor']
    if set(identity)!= {'documentId','revision','module','element'} or identity['documentId']!=intake.document_id or identity['revision']!=intake.document_revision:
        raise ResolutionError('Original selected Field identity required')
    availability=[]
    for type_id,fields in policy.property_fields.items():
        for pid,ref in fields.items():
            if ref==(identity['module'],identity['element']):availability.append(policy.types[type_id][pid])
    if not availability or len(set(availability))!=1:raise ResolutionError('Unadmitted result Field')
    item=_json(text(value).encode('utf-8'))
    if type(item) is not dict:raise ResolutionError('Exact value-state envelope required')
    if item=={'state':'absent'} and availability[0]=='absent-allowed':return MappingProxyType(item)
    if set(item)=={'state','value'} and item['state']=='value':
        text(item['value']);return MappingProxyType(item)
    raise ResolutionError('Invalid absence/null/value result')

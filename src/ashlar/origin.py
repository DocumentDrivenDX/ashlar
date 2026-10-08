"""Candidate lossless canonical-tree/ExactValue origin mapping.

Does not authenticate a role or register the selected origin mapping profile.
"""
from dataclasses import dataclass
import hashlib
import json
from .schema import _json
from .truss_input import _canonical, artifact

MAX_BYTES=1048576
MAX_DEPTH=64
MAX_NODES=1000

class OriginMappingError(ValueError):
    pass

PROFILE_BYTES=b'{"status":"unregistered candidate","mapping":"null/boolean/string map to matching ExactValue kinds; arrays to sequence; objects to UTF8-key-ordered map; all keys and tag-looking objects remain literal; numbers and PostgreSQL-bound NUL refuse","bounds":"original and encoded output <=1048576 bytes; <=1000 tree nodes including keys; depth <=64","authority":"asserted origin only; actual native role capture is separate"}'
PROFILE_PIN={'identity':'ashlar-truss-canonical-tree-origin','version':'0.1','sha256':hashlib.sha256(PROFILE_BYTES).hexdigest()}


def _scalar(text):
    if type(text) is not str or '\x00' in text:raise OriginMappingError('PostgreSQL-compatible scalar string required')
    try:text.encode('utf-8')
    except UnicodeError as exc:raise OriginMappingError('Unpaired surrogate') from exc
    return text


def _walk(value,budget,depth=0):
    budget[0]+=1
    if budget[0]>MAX_NODES or depth>MAX_DEPTH:raise OriginMappingError('Origin tree bound exceeded')
    if value is None:return {'kind':'null'}
    if type(value) is bool:return {'kind':'boolean','value':value}
    if type(value) is str:return {'kind':'string','text':_scalar(value)}
    if type(value) is list:return {'kind':'sequence','items':[_walk(v,budget,depth+1) for v in value]}
    if type(value) is dict:
        for key in value:_scalar(key)
        budget[0]+=len(value)
        if budget[0]>MAX_NODES:raise OriginMappingError('Origin key bound exceeded')
        return {'kind':'map','entries':[{'key':key,'value':_walk(value[key],budget,depth+1)} for key in sorted(value,key=lambda k:k.encode('utf-8'))]}
    raise OriginMappingError('Host numbers or unsupported canonical values')


def _unwalk(value,budget,depth=0):
    budget[0]+=1
    if budget[0]>MAX_NODES or depth>MAX_DEPTH or type(value) is not dict:raise OriginMappingError('Invalid or oversized mapped origin')
    kind=value.get('kind')
    if kind=='null' and set(value)=={'kind'}:return None
    if kind=='boolean' and set(value)=={'kind','value'} and type(value['value']) is bool:return value['value']
    if kind=='string' and set(value)=={'kind','text'}:return _scalar(value['text'])
    if kind=='sequence' and set(value)=={'kind','items'} and type(value['items']) is list:return [_unwalk(v,budget,depth+1) for v in value['items']]
    if kind=='map' and set(value)=={'kind','entries'} and type(value['entries']) is list:
        result={};prior=None
        for entry in value['entries']:
            if type(entry) is not dict or set(entry)!={'key','value'}:raise OriginMappingError('Unknown mapped entry')
            key=_scalar(entry['key']);encoded=key.encode('utf-8');budget[0]+=1
            if key in result or (prior is not None and encoded<=prior):raise OriginMappingError('Duplicate or unordered map key')
            if budget[0]>MAX_NODES:raise OriginMappingError('Origin key bound exceeded')
            prior=encoded;result[key]=_unwalk(entry['value'],budget,depth+1)
        return result
    raise OriginMappingError('Unknown or coerced origin variant')


@dataclass(frozen=True)
class OriginMapping:
    original: bytes
    canonical_tree: bytes
    mapped_artifact: bytes


def map_acceptance_origin(raw):
    if type(raw) is not bytes or len(raw)>MAX_BYTES:raise OriginMappingError('Bounded original origin bytes required')
    value=_json(raw);mapped=_walk(value,[0])
    canonical=_canonical(value).encode('utf-8')
    encoded=json.dumps(mapped,ensure_ascii=False,separators=(',',':')).encode('utf-8')
    if len(canonical)>MAX_BYTES or len(encoded)>MAX_BYTES:raise OriginMappingError('Origin byte bound exceeded')
    # Preserve exact semantic tree; no numeric/tag/name interpretation occurs.
    if _canonical(_unwalk(mapped,[0])).encode('utf-8')!=canonical:raise OriginMappingError('Origin mapping correspondence failed')
    return OriginMapping(raw,canonical,encoded)


def restore_asserted_origin(raw):
    if type(raw) is not bytes or len(raw)>MAX_BYTES:raise OriginMappingError('Bounded mapped origin bytes required')
    restored=_unwalk(_json(raw),[0]);canonical=_canonical(restored).encode('utf-8')
    if len(canonical)>MAX_BYTES:raise OriginMappingError('Restored origin byte bound exceeded')
    return canonical

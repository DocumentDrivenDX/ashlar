# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded exact decoder evidence serialization; no coercion or authority grant."""
import json

def report_value(value,maximum):
    """Exact selected decoder carriers; no generic dataclass/string coercion."""
    from decimal import Decimal
    from ashlar.weft_decode import ExactScalar
    from ashlar.weft_path_decode import DecodedPaths,PathItem
    from ashlar.weft_related_keys_decode import DecodedRelatedKeys
    if type(maximum)is not int or not 0<maximum<=64*1024*1024:raise ValueError('Explicit bounded report capacity required')
    remaining=maximum
    def owned(item,depth=0,decoded=False):
        nonlocal remaining
        remaining-=1
        if remaining<0 or depth>64:raise ValueError('Complete report nesting/capacity bound')
        if item is None or type(item)is bool:return item
        if type(item)is str:
            remaining-=len(item.encode('utf-8'))
            if remaining<0:raise ValueError('Complete report payload bound')
            return item
        if type(item)is int:
            if item.bit_length()>512:raise ValueError('Exact report integer bound')
            return {'$type':'integer','text':owned(str(item),depth+1)}if decoded else item
        if type(item)is bytes:
            if len(item)>remaining//2:raise ValueError('Exact original byte report bound')
            return {'$type':'bytes','hex':owned(item.hex(),depth+1)}
        if type(item)is Decimal:
            parts=item.as_tuple()
            if not item.is_finite()or len(parts.digits)>128 or abs(parts.exponent)>128:raise ValueError('Exact report decimal bound')
            return {'$type':'Decimal','text':owned(str(item),depth+1)}
        if type(item)is ExactScalar:
            decoded_value=owned(item.value,depth+1,True)
            return {'$type':'ExactScalar','value':decoded_value,'original':owned(item.original,depth+1)}
        if type(item)is PathItem:return {'$type':'PathItem','intermediate':owned(item.intermediate,depth+1,True),'terminal':owned(item.terminal,depth+1,True),'edges':owned(item.edges,depth+1,True)}
        if type(item)is DecodedPaths:return {'$type':'DecodedPaths','original':owned(item.original,depth+1),'state':owned(item.state,depth+1),'items':owned(item.items,depth+1,True),'truncated':owned(item.truncated,depth+1)}
        if type(item)is DecodedRelatedKeys:return {'$type':'DecodedRelatedKeys','original':owned(item.original,depth+1),'items':owned(item.items,depth+1,True),'truncated':owned(item.truncated,depth+1)}
        if type(item)in (list,tuple):return [owned(v,depth+1,decoded)for v in item]
        if type(item)is dict:
            result={}
            for key,v in item.items():
                if type(key)is not str:raise ValueError('Exact report named member required')
                result[owned(key,depth+1)]=owned(v,depth+1,decoded)
            return result
        raise ValueError('Unknown report carrier refused')
    return owned(value)

def report_body(value,maximum):
    body=bytearray()
    value=report_value(value,maximum)
    for chunk in json.JSONEncoder(ensure_ascii=True,separators=(',',':'),allow_nan=False).iterencode(value):
        raw=chunk.encode()
        if len(body)+len(raw)+1>maximum:raise ValueError('Complete finite output bound')
        body.extend(raw)
    body.extend(b'\n')
    return bytes(body)

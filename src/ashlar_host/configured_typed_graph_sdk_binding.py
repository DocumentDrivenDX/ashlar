# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit required-STRING graph mapping for the public authored-Key page SDK.

The original complete model/source stay unchanged. Optional or non-STRING homes
are unselected metadata, never reinterpreted as required or claimed page support.
Native alias, publication, source and caller authority belong to separate owners.
"""
import hashlib,json,re
from ashlar.schema import decode_retained_json
from .configured_typed_graph_weft_binding import configured_typed_graph_count_star_request


def _require(value):
    if not value:raise ValueError('Configured graph SDK page binding refused')


def configured_typed_graph_sdk_request(configuration,manifest,vector,manifest_uuid,aliases,sql,parameters,*,layout_sha256):
    """Return an original Core 0.10, compile-request 0.2.7 page request, not a compiled page."""
    request=configured_typed_graph_count_star_request(configuration,manifest,vector,manifest_uuid,aliases,sql,layout_sha256=layout_sha256)
    _require(type(parameters)is dict and len(parameters)<=128)
    for name,value in parameters.items():
        _require(type(name)is str and re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,127}',name)is not None
            and type(value)is dict and set(value)=={'family','value'}and type(value['family'])is str and value['family']=='string'and type(value['value'])is str and len(value['value'].encode())<=65536)
    parameter_bytes=json.dumps(parameters,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode();_require(len(parameter_bytes)<=1048576)
    original_metadata=json.dumps([manifest,aliases],ensure_ascii=False,separators=(',',':'),sort_keys=True)
    binding=decode_retained_json(request['target']['bindingJson'].encode())
    model=decode_retained_json(configuration.model_source)
    elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    for record in binding['records']:
        selected=[]
        for prop in record['properties']:
            identity=prop['logical'];field=elements[identity['module'],identity['element']]
            if field.get('scalarType')=='string'and field.get('nullability')=='required'and field.get('cardinality')=='one'and field.get('facets',{})=={}:
                _require(set(prop['home'])=={'kind','propertyId'}and prop['home']['kind']=='props');selected.append(prop)
        record['properties']=selected
        homes={(p['logical']['module'],p['logical']['element'])for p in selected}
        owner=elements[record['logical']['module'],record['logical']['element']]
        _require(all((f['module'],f['element'])in homes for key in owner.get('keys',[])for f in key['fields']))
    raw=json.dumps(binding,ensure_ascii=False,separators=(',',':'),sort_keys=True)
    request.update(interfaceVersion='weft-compile/0.2.7',dialect='weft-sql/0.2.0',timestampComparisonProfile='umf-rfc3339-exact-instant/1.0.0',parameters=json.loads(parameter_bytes))
    request['target']={'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-pages-candidate',
        'targetProfile':'spark4.0.1-delta4.0.0-candidate','bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()}
    configuration.verify()
    _require(json.dumps(parameters,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()==parameter_bytes
        and json.dumps([manifest,aliases],ensure_ascii=False,separators=(',',':'),sort_keys=True)==original_metadata)
    return request

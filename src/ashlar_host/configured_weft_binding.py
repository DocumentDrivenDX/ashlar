"""Original configured source -> explicit Spark041 compiler binding.

Aliases describe only physical name projection. Their actual fresh namespace,
UUID/path identity and cleanup remain the held native alias owner's obligation.
No compiler SQL is produced or rewritten here, and the manifest is never relabeled.
"""
import hashlib,json,re
from ashlar.weft_binding import string_compile_request
from .configured_local_read import ConfiguredLocalRead
from .count_star_admission import BACKEND


def _require(value):
    if not value:raise ValueError('Configured Weft binding refused')


def configured_count_star_request(scope,aliases,sql,*,layout_sha256):
    """Construct a detached original-model request from an actual live read scope."""
    _require(type(scope)is ConfiguredLocalRead)
    _require(not scope.source.graph)
    scope.renew();scope.parity()
    if scope.source.typed:
        from .configured_typed_weft_binding import configured_typed_count_star_request
        original=dict(scope.descriptor.raw)
        result=configured_typed_count_star_request(scope.source.configuration,original,scope.vector,
            scope.manifest_uuid,aliases,sql,layout_sha256=layout_sha256)
        scope.renew();scope.parity();_require(dict(scope.descriptor.raw)==original)
        return result
    _require(type(sql)is str and 0<len(sql.encode())<=65536 and '\0'not in sql)
    _require(type(aliases)is dict and all(type(k)is str and type(v)is str for k,v in aliases.items())and set(aliases)==set(scope.descriptor.versions)and len(aliases)==4 and len(set(aliases.values()))==4)
    namespace=None;selected={}
    for original,alias in aliases.items():
        _require(type(alias)is str and re.fullmatch(r'spark_catalog\.ashlar_configured_query_[A-Za-z0-9_]{1,96}\.[A-Za-z_][A-Za-z0-9_]*',alias)is not None)
        parts=alias.split('.');_require(parts[-1]==original.split('.')[-1]and alias!=original)
        if namespace is None:namespace=parts[1]
        _require(parts[1]==namespace);selected[original]=alias
    source=scope.source;configured=source.configuration
    original_manifest=dict(scope.descriptor.raw)
    request=string_compile_request(sql,configured.intake,configured.policy,scope.descriptor,schema_alias=configured.schema_alias,
        table_uuids={table:pair[0]for table,pair in scope.vector.targets.items()},manifest_uuid=scope.manifest_uuid,layout_sha256=layout_sha256)
    binding=json.loads(request['target']['bindingJson'])
    for table in binding['publication']['tables']:
        original='.'.join(table['name']);_require(original in selected)
        _require((table['uuid'],table['version'])==scope.vector.targets[original])
        table['name']=selected[original].split('.')
    raw=json.dumps(binding,ensure_ascii=False,separators=(',',':'),sort_keys=True)
    request.update(interfaceVersion='weft-compile/0.4.1',dialect='weft-sql/0.4.1',options={'allowCandidate':True})
    request['target']={name:BACKEND[name]for name in ('backendId','backendVersion','targetProfile')}
    request['target'].update(bindingJson=raw,bindingSha256=hashlib.sha256(raw.encode()).hexdigest())
    scope.renew();scope.parity();_require(dict(scope.descriptor.raw)==original_manifest)
    return request

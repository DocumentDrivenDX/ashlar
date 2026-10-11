"""Original commerce SQL and metadata with the explicit 0.4.2 profile.

Construction grants no publication, source, compiler or execution authority.
"""
from .commerce_original_scenarios import commerce_original_cases
from .commerce_path_request import commerce_path_request
from .implicit_arithmetic_profile import PROFILE
from .supply_chain_request import source_snapshot


def commerce_implicit_arithmetic_request(sql, model, bindings, manifest, registry,
                                        aliases, *, pack_bytes):
    if type(sql) is not str or sql not in dict(commerce_original_cases(pack_bytes)).values():
        raise ValueError('Exact original commerce scenario SQL required')
    bindings, manifest, registry, aliases = (
        source_snapshot(value) for value in (bindings, manifest, registry, aliases))
    request = commerce_path_request(sql, model, bindings, manifest, registry,
                                    aliases, profile='paths-keys')
    request.update(interfaceVersion=PROFILE.compile_version, dialect=PROFILE.dialect)
    backend = dict(PROFILE.backend)
    request['target'].update({key: backend[key] for key in
                             ('backendId', 'backendVersion', 'targetProfile')})
    return request

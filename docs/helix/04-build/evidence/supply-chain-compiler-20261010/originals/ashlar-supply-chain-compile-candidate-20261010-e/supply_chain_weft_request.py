"""Original supply-chain requests for the explicit PathsKeys candidate profile.

This preserves authored SQL and the supplied resolved publication vector. It
constructs no authority and performs no compiler or native execution.
"""
import hashlib
import json
from ashlar_host.path_admission import PATHS_KEYS_BACKEND
from prepare_supply_chain_query_cases import prepare_supply_chain_cases
from run_pack_publication_weft import compiler_request


def supply_chain_weft_request(case_id: str, pack_bytes: bytes, model_bytes: bytes,
                              graph_bytes: bytes, bindings: dict, manifest: dict,
                              registry: list, aliases: dict) -> dict:
    """Build one unchanged original query; optional Props require native-null admission."""
    prepared = prepare_supply_chain_cases(pack_bytes, model_bytes, graph_bytes)
    cases = {case['id']: case for case in prepared['cases']}
    if type(case_id) is not str or case_id not in cases:
        raise ValueError('Original supply-chain case required')
    request = compiler_request('supply-chain', cases[case_id]['original_scenario']['sql'],
                               model_bytes, graph_bytes, bindings, manifest, registry, aliases)
    binding = json.loads(request['target']['bindingJson'])
    optional = {tuple(entry['identity']): entry for entry in prepared['optional_props_requirements']}
    seen = set()
    for record in binding['records']:
        for prop in record['properties']:
            logical = prop['logical']
            identity = (logical['documentId'], logical['module'], logical['element'])
            if identity in optional:
                requirement = optional[identity]
                if identity in seen or prop['home'] != {'kind': 'props', 'propertyId': requirement['development_property_id']}:
                    raise ValueError('Complete original optional property mapping required')
                prop['home']['encoding'] = requirement['required_home_encoding']
                seen.add(identity)
    if seen != set(optional):
        raise ValueError('Complete original optional property inventory required')
    raw = json.dumps(binding, ensure_ascii=False, separators=(',', ':'))
    request['interfaceVersion'] = 'weft-compile/0.4.0'
    request['dialect'] = 'weft-sql/0.4.0'
    request['target'] = {**PATHS_KEYS_BACKEND, 'bindingJson': raw,
                         'bindingSha256': hashlib.sha256(raw.encode()).hexdigest()}
    return request

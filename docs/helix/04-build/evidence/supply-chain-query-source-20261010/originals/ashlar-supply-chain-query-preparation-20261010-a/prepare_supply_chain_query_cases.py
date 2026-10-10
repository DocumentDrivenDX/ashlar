"""Finite original supply-chain query preparation; no compiler/native admission."""
import hashlib
import json
from supply_chain_graph_oracle import original_graph_oracle, MODEL_SHA, GRAPH_SHA
from supply_chain_source_transaction import build_transaction

PACK_SHA = 'baf888ab976749e589df385c76900ebf146959b61eaa4b55ab840e142b54ae22'
CASE_IDS = ('split-excursion', 'excursion', 'replay', 'lineage', 'sensor')
OUTPUTS = {
    'split-excursion': ('lot_code', 'shipment_count'),
    'excursion': ('id', 'value', 'unit'),
    'replay': ('upstream_event_id', 'count'),
    'lineage': ('serial', 'lot_code', 'sku', 'parent_id'),
    'sensor': ('unit', 'method'),
}


def prepare_supply_chain_cases(pack_bytes: bytes, model_bytes: bytes,
                               graph_bytes: bytes) -> dict:
    """Retain original SQL and independent graph bags with exact lexical values.

    Bindings are development mappings, not source/writer/ACK authority. Optional
    Props require owning native-null admission before any execution. Output bags
    have fixture order only: original queries contain no ORDER BY.
    """
    inputs = (('pack', pack_bytes, PACK_SHA), ('model', model_bytes, MODEL_SHA),
              ('graph', graph_bytes, GRAPH_SHA))
    for _, raw, digest in inputs:
        if type(raw) is not bytes or hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError('Exact original supply-chain inputs required')
    pack = json.loads(pack_bytes)
    model = json.loads(model_bytes)
    oracle = original_graph_oracle(model_bytes, graph_bytes)
    batch, bindings = build_transaction(model_bytes, graph_bytes,
                                       source_system='private-original-supply-chain-fixture')
    scenarios = pack['scenario_checks']
    if tuple(case['id'] for case in scenarios) != CASE_IDS or set(oracle['cases']) != set(CASE_IDS):
        raise ValueError('Complete original scenario inventory required')
    properties = {tuple(item['identity']): item['property_id'] for item in bindings['properties']}
    elements = {(module['id'], element['id']): element
                for module in model['modules'] for element in module['elements']}
    tables = []
    optional = []
    for entry in bindings['types']:
        kind, module, element = entry['identity']
        if kind != 'object':
            continue
        fields = []
        for ref in elements[(module, element)]['members']:
            identity = (model['id'], ref['module'], ref['element'])
            field = elements[(ref['module'], ref['element'])]
            fields.append({'identity': list(identity), 'development_property_id': properties[identity],
                           'original_field': field})
            if field['nullability'] == 'absent-allowed':
                optional.append({'identity': list(identity), 'development_property_id': properties[identity],
                                 'required_home_encoding': 'ashlar-weft-json-native-null/0.1-candidate',
                                 'missing_property': 'refuse', 'present_null': 'state:null'})
        tables.append({'identity': [model['id'], module, element],
                       'development_type_id': entry['type_id'], 'fields': fields})
    cases = []
    for scenario in scenarios:
        names = OUTPUTS[scenario['id']]
        rows = [[row[name] for name in names] for row in oracle['cases'][scenario['id']]]
        cases.append({'id': scenario['id'], 'original_scenario': scenario,
                      'output_names': list(names), 'original_graph_rows': rows,
                      'comparison': 'multiset; original query has no ORDER BY'})
    return {'format': 'ashlar-original-supply-chain-query-candidate/0.1',
            'original_inputs': [{'kind': name, 'sha256': digest, 'bytes': len(raw)}
                                for name, raw, digest in inputs],
            'original_umf_version': model['umf'], 'development_binding_profile': bindings['profile'],
            'source_system': batch.feed, 'tables': tables,
            'optional_props_requirements': optional, 'cases': cases,
            'qualification': 'Pure original query/graph preparation only; no compiler, publication, native or authority support claim.'}

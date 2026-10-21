# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original Weft requests over a held current-core managed publication.

This module only prepares compiler requests.  It derives semantic identities
from the retained accepted Truss/UMF source bundle and physical names/heads
from the publication owner's independently reopened four-role vector.
"""
import base64
import hashlib
import json
import re
import uuid

from ashlar_host.managed_current_carriers import GENERATED_SHA256
from .managed_current_core import BINDING_PROFILE, PROFILE

BACKEND_ID = 'ashlar.databricks.managed-uc-spark'
BACKEND03_VERSION = '0.1.0-managed-current-core-candidate'
BACKEND02_VERSION = '0.1.0-managed-current-core-pages-candidate'
BACKEND03_INTERFACE = 'weft-compile/0.4.1'
BACKEND03_DIALECT = 'weft-sql/0.4.1'
BACKEND02_INTERFACE = 'weft-compile/0.2.7'
BACKEND02_DIALECT = 'weft-sql/0.2.0'
TIMESTAMP_PROFILE = 'umf-rfc3339-exact-instant/1.0.0'
DATA_ROLES = ('object_current', 'edge_current', 'tombstone', 'whole_source_history')
_IDENT = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,127}\Z')
_SHA = re.compile(r'[0-9a-f]{64}\Z')


def _require(value, reason='managed current Weft binding refused'):
    if not value:
        raise ValueError(reason)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True,
                      allow_nan=False)


def _digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def _identity(document, revision, module, element):
    return {'documentId': document, 'revision': revision, 'module': module, 'element': element}


def _validated_inputs(source_bundle, weft_inputs):
    _require(type(source_bundle) is dict and source_bundle.get('profile') == BINDING_PROFILE
             and source_bundle.get('status') == 'preinstallation-source-bundle',
             'Original preinstallation Truss source bundle required')
    retained = dict(source_bundle)
    supplied_digest = retained.pop('bindingSha256', None)
    _require(type(supplied_digest) is str and _SHA.fullmatch(supplied_digest)
             and _digest(_canonical(retained)) == supplied_digest,
             'Digest-bound original accepted Truss source bundle required')
    _require(type(weft_inputs) is dict and set(weft_inputs) == {'manifest', 'manifestUuid', 'dataRoles'},
             'Reopened manifest, manifest UUID and four-role vector required')
    manifest = weft_inputs['manifest']
    _require(type(manifest) is dict and manifest.get('profile_version') == 'ashlar-delta/0.3'
             and type(manifest.get('publication_id')) is str and manifest['publication_id'],
             'Original current Ashlar publication manifest required')
    try:
        manifest_uuid = str(uuid.UUID(weft_inputs['manifestUuid']))
    except (ValueError, TypeError, AttributeError):
        _require(False, 'Observed publication manifest UUID required')
    _require(manifest_uuid == weft_inputs['manifestUuid'], 'Canonical publication manifest UUID required')
    input_roles = weft_inputs['dataRoles']
    _require(type(input_roles) is dict and set(input_roles) == set(DATA_ROLES),
             'Exactly four observed graph data roles required')
    data_roles = {}
    names = {}
    ids = {manifest_uuid}
    versions = {}
    for role in DATA_ROLES:
        item = input_roles[role]
        _require(type(item) is dict and set(item) == {'name', 'uuid', 'version'},
                 'Exact observed data role name, UUID and head required')
        name = item['name']
        _require(type(name) in (tuple, list) and len(name) == 3
                 and all(type(part) is str and _IDENT.fullmatch(part) for part in name)
                 and name[2] == role,
                 'Three-part Unity Catalog role name required')
        name = tuple(name)
        try:
            table_uuid = str(uuid.UUID(item['uuid']))
        except (ValueError, TypeError, AttributeError):
            _require(False, 'Observed native Delta table UUID required')
        _require(table_uuid == item['uuid'] and table_uuid not in ids
                 and type(item['version']) is int and item['version'] >= 0,
                 'Distinct native table identity and nonnegative head required')
        ids.add(table_uuid)
        names[role] = list(name)
        normalized = dict(item)
        normalized['name'] = name
        data_roles[role] = normalized
        versions['.'.join(name)] = item['version']
    namespaces = {(name[0], name[1]) for name in names.values()}
    _require(len(namespaces) == 1, 'Four graph roles must share one Unity Catalog namespace')
    destination = source_bundle.get('destination')
    _require(type(destination) is dict and namespaces == {
        (destination.get('catalog'), destination.get('schema'))},
        'Observed graph role names differ from the explicitly selected managed namespace')
    try:
        manifest_versions = json.loads(manifest['table_versions_json'])
        schema_revisions = json.loads(manifest['schema_revisions_json'])
    except (KeyError, TypeError, json.JSONDecodeError):
        _require(False, 'Canonical publication table and schema vectors required')
    _require(type(manifest_versions) is dict and manifest_versions == versions,
             'Observed four-role heads differ from the durable publication manifest')
    _require(type(schema_revisions) is dict and bool(schema_revisions)
             and _canonical(schema_revisions) == manifest['schema_revisions_json'],
             'Canonical accepted schema revision vector required')
    source = source_bundle.get('source')
    _require(type(source) is dict and source.get('trussVersion') == '0.0.44'
             and source.get('umfVersion') == '0.10.0'
             and source.get('authority') == 'development-only-until-production-owner-observed',
             'Accepted Truss 0.0.44 / UMF 0.10.0 source mapping required')
    targets = source_bundle.get('weftTargets')
    _require(type(targets) is dict and set(targets) == {'Backend03', 'Backend02'},
             'Both explicitly selected current Weft target identities required')
    for surface, version, interface, dialect in (
            ('Backend03', BACKEND03_VERSION, BACKEND03_INTERFACE, BACKEND03_DIALECT),
            ('Backend02', BACKEND02_VERSION, BACKEND02_INTERFACE, BACKEND02_DIALECT)):
        target = targets[surface]
        _require(type(target) is dict and target.get('surface') == surface
                 and target.get('backendId') == BACKEND_ID
                 and target.get('backendVersion') == version
                 and target.get('targetProfile') == PROFILE
                 and target.get('weftInterface') == interface
                 and target.get('weftDialect') == dialect,
                 'Historical, local or cross-surface Weft target identity refused')
    graph = source.get('assignedGraphByRevision')
    accepted = source.get('acceptedDocuments')
    _require(type(graph) is dict and type(accepted) is dict
             and set(graph) == set(schema_revisions),
             'Published revisions must match exact retained Truss graph assignments')
    # The manifest stores each accepted report digest by catalog revision.
    _require(all(schema_revisions.get(revision) == source.get('reportSha256', {}).get(revision)
                 for revision in graph),
             'Publication schema pins differ from retained accepted Truss reports')
    cursor = json.loads(base64.b64decode(source['cursorBeforeBase64'], validate=True))
    installation = cursor.get('installationId')
    _require(type(installation) is str and installation and '\0' not in installation,
             'Original Truss installation identity required')
    return manifest, manifest_uuid, names, data_roles, graph, accepted, installation


def _assemble_binding(source_bundle, weft_inputs):
    manifest, manifest_uuid, names, data_roles, graph, accepted, installation = _validated_inputs(
        source_bundle, weft_inputs)
    revision_keys = sorted(graph, key=lambda value: (int(value) if value.isdecimal() else value))
    _require(len(revision_keys) == 1,
             'Current managed candidate mapping requires one selected accepted catalog revision')
    catalog_revision = revision_keys[0]
    assigned = graph[catalog_revision]
    accepted_for_revision = {key.split(':', 1)[1]: item for key, item in accepted.items()
                             if key.startswith(catalog_revision + ':')}
    _require(bool(accepted_for_revision), 'Accepted UMF documents for the published revision required')
    decoded = {}
    model_pins = []
    for document_id, item in sorted(accepted_for_revision.items()):
        raw = base64.b64decode(item['sourceBytesBase64'], validate=True)
        _require(hashlib.sha256(raw).hexdigest() == item['modelSha256']
                 and item['documentId'] == document_id and item['umfVersion'] == '0.10.0')
        model = json.loads(raw)
        _require(type(model) is dict and model.get('id') == document_id and model.get('umf') == '0.10.0')
        decoded[document_id] = (model, raw, item['documentRevision'], item['modelSha256'])
        model_pins.append({'documentId': document_id, 'revision': item['documentRevision'],
                           'umfVersion': '0.10.0', 'sha256': item['modelSha256']})
    records = []
    record_logicals = {}
    object_index = DATA_ROLES.index('object_current')
    for type_id, assigned_type in sorted(assigned['types'].items(), key=lambda pair: int(pair[0])):
        logical = assigned_type['logical']
        doc = logical['documentId']
        _require(doc in decoded and type_id.isdecimal(), 'Assigned current record must have its exact accepted UMF source')
        model, _raw, document_revision, _sha = decoded[doc]
        key = (doc, logical['module'], logical['element'])
        elements = {(m['id'], e['id']): e for m in model['modules'] for e in m['elements']}
        record = elements.get((logical['module'], logical['element']))
        _require(type(record) is dict and record.get('kind') == 'record')
        properties = []
        by_id = {prop['propertyId']: prop for prop in assigned_type['properties']}
        _require(len(by_id) == len(assigned_type['properties'])
                 and len(by_id) == len(record['members']),
                 'Every exact accepted UMF member must have one Truss property assignment')
        for member in record['members']:
            prop = by_id.get(next((p['propertyId'] for p in assigned_type['properties']
                                   if p['logical'] == {'documentId': doc, 'module': member['module'],
                                                      'element': member['element']}), None))
            _require(type(prop) is dict and prop['logical'] == {
                'documentId': doc, 'module': member['module'], 'element': member['element']})
            field_identity = _identity(doc, document_revision, member['module'], member['element'])
            home = {'kind': 'props', 'propertyId': prop['propertyId']}
            field = elements[(member['module'], member['element'])]
            if field.get('nullability') == 'absent-allowed':
                home['encoding'] = 'ashlar-weft-json-native-null/0.1-candidate'
            properties.append({'logical': field_identity, 'home': home})
        logical_identity = _identity(doc, document_revision, logical['module'], logical['element'])
        records.append({'logical': logical_identity, 'table': object_index, 'kind': 'object',
            'sourceSystem': installation, 'typeId': str(type_id), 'schemaRevision': catalog_revision,
            'properties': properties})
        record_logicals[type_id] = logical_identity
    relationships = []
    edge_index = DATA_ROLES.index('edge_current')
    for edge_id, assigned_edge in sorted(assigned['relationships'].items(), key=lambda pair: int(pair[0])):
        logical = assigned_edge['logical']
        doc = logical['documentId']
        _require(doc in decoded and edge_id.isdecimal(), 'Assigned current relationship must have its exact accepted UMF source')
        _model, _raw, document_revision, _sha = decoded[doc]
        endpoints = assigned_edge['endpointTypeIds']
        _require(type(endpoints) is list and len(endpoints) == 2 and all(str(v) in record_logicals for v in endpoints),
                 'Truss-assigned relationship endpoint identities required')
        relationships.append({'logical': {'documentId': doc, 'revision': document_revision,
                'module': logical['module'], 'relationship': logical['relationship']},
            'acceptedDefinition': assigned_edge['declaration'], 'table': edge_index, 'kind': 'edge',
            'sourceSystem': installation, 'typeId': str(edge_id), 'schemaRevision': catalog_revision,
            'source': record_logicals[str(endpoints[0])], 'target': record_logicals[str(endpoints[1])]})
    binding = {'profile': PROFILE, 'layoutRevision': 'ashlar-delta/0.3',
        'layoutSha256': GENERATED_SHA256, 'modelPins': model_pins,
        'publication': {'id': manifest['publication_id'], 'manifestUuid': manifest_uuid,
            'tables': [{'name': names[role], 'uuid': data_roles[role]['uuid'],
                        'version': data_roles[role]['version']} for role in DATA_ROLES]},
        'records': records, 'relationships': relationships}
    modules = []
    for document_id, (model, raw, revision, digest) in sorted(decoded.items()):
        selected = {record['logical']['module'] for record in records
                    if record['logical']['documentId'] == document_id}
        selected.update(item['logical']['module'] for item in relationships
                        if item['logical']['documentId'] == document_id)
        selected = sorted(selected)
        modules.append({'documentJson': raw.decode('utf-8'), 'pin': {
            'documentId': document_id, 'revision': revision, 'umfVersion': '0.10.0', 'sha256': digest},
            'selectedModuleIds': selected})
    return binding, modules


def managed_current_count_star_request(source_bundle, weft_inputs, sql):
    """Prepare an unchanged grouped/HAVING SQL request for managed Backend03."""
    _require(type(sql) is str and 0 < len(sql.encode('utf-8')) <= 65536 and '\0' not in sql)
    binding, modules = _assemble_binding(source_bundle, weft_inputs)
    raw = _canonical(binding)
    request = {'interfaceVersion': BACKEND03_INTERFACE, 'dialect': BACKEND03_DIALECT,
        'sql': sql, 'modules': modules,
        'target': {'backendId': BACKEND_ID, 'backendVersion': BACKEND03_VERSION,
                   'targetProfile': PROFILE, 'bindingJson': raw, 'bindingSha256': _digest(raw)},
        'options': {'allowCandidate': True}}
    return request


def managed_current_sdk_pages_request(source_bundle, weft_inputs, sql, parameters):
    """Prepare the original typed EngineRead page request for managed Backend02."""
    _require(type(sql) is str and 0 < len(sql.encode('utf-8')) <= 65536 and '\0' not in sql)
    _require(type(parameters) is dict and len(parameters) <= 128)
    for name, value in parameters.items():
        _require(type(name) is str and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,127}', name)
                 and type(value) is dict and set(value) == {'family', 'value'}
                 and value['family'] == 'string' and type(value['value']) is str
                 and len(value['value'].encode('utf-8')) <= 65536,
                 'Exact bounded STRING page parameters required')
    encoded_parameters = _canonical(parameters).encode('utf-8')
    _require(len(encoded_parameters) <= 1048576)
    binding, modules = _assemble_binding(source_bundle, weft_inputs)
    models = {item['pin']['documentId']: json.loads(item['documentJson']) for item in modules}
    elements = {(document_id, m['id'], e['id']): e for document_id, model in models.items()
                for m in model['modules'] for e in m['elements']}
    for record in binding['records']:
        selected = []
        for prop in record['properties']:
            identity = prop['logical']
            field = elements[identity['documentId'], identity['module'], identity['element']]
            if (field.get('scalarType') == 'string' and field.get('nullability') == 'required'
                    and field.get('cardinality') == 'one' and field.get('facets', {}) == {}):
                _require(set(prop['home']) == {'kind', 'propertyId'})
                selected.append(prop)
        record['properties'] = selected
        homes = {(p['logical']['module'], p['logical']['element']) for p in selected}
        logical = record['logical']
        owner = elements[logical['documentId'], logical['module'], logical['element']]
        _require(all((field['module'], field['element']) in homes
                     for key in owner.get('keys', []) for field in key['fields']),
                 'Every current authored page key component must remain bound as required STRING')
    raw = _canonical(binding)
    return {'interfaceVersion': BACKEND02_INTERFACE, 'dialect': BACKEND02_DIALECT,
        'timestampComparisonProfile': TIMESTAMP_PROFILE, 'sql': sql, 'modules': modules,
        'parameters': json.loads(encoded_parameters), 'options': {'allowCandidate': True},
        'target': {'backendId': BACKEND_ID, 'backendVersion': BACKEND02_VERSION,
                   'targetProfile': PROFILE, 'bindingJson': raw, 'bindingSha256': _digest(raw)}}

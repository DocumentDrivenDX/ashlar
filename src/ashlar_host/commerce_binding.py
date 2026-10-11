"""Pure original commerce model/publication binding construction; no authority grant."""
import hashlib,json
from .source_identity import SOURCE_SHA
from .delta_custody import encoded
LAYOUT_SHA = 'ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e'

def compiler_request(sql, model, bindings, manifest, registry, aliases, *, fields=False):
    """Bind original qualified model identities to explicitly admitted carriers."""
    if hashlib.sha256(model).hexdigest() != SOURCE_SHA or json.loads(model)['umf'] != '0.8.0':
        raise ValueError('Exact original model bytes required')
    versions = json.loads(manifest['table_versions_json'])
    native = {r['table']: r for r in registry}
    if len(native) != len(registry) or {t.split('.')[-1] for t in versions} != {'object_current', 'edge_current', 'tombstone', 'whole_source_history'} or set(json.loads(manifest['source_progress_json'])) != {'private-original-commerce-fixture'}:
        raise ValueError('Complete original commerce publication/source inventory required')
    if set(aliases) != set(versions) or len(set(aliases.values())) != len(aliases):
        raise ValueError('Complete injective original catalog alias inventory required')
    if any((name not in native for name in versions)):
        raise ValueError('Original publication registry incomplete')
    tables = [{'name': aliases[t].split('.'), 'uuid': native[t]['uuid'], 'version': v} for (t, v) in sorted(versions.items())]
    object_table = next((t for t in versions if t.endswith('.object_current')))
    index = sorted(versions).index(object_table)
    pin = {'documentId': 'urn:umf:domain:commerce', 'revision': SOURCE_SHA, 'sha256': SOURCE_SHA, 'umfVersion': '0.8.0'}
    records = []
    for element in ('products', 'suppliers'):
        entry = next((b for b in bindings['types'] if b['identity'] == ['object', 'domain', element]))
        logical = {'documentId': pin['documentId'], 'module': 'domain', 'element': element, 'revision': SOURCE_SHA}
        properties = []
        if fields:
            if bindings['profile'] != 'ashlar-commerce-development-bindings/0.2':
                raise ValueError('Field homes require explicit canonical development property-ID bindings')
            document = json.loads(model)
            record = next((e for m in document['modules'] if m['id'] == 'domain' for e in m['elements'] if e['id'] == element))
            property_inventory = {tuple(p['identity']): p['property_id'] for p in bindings['properties']}
            if len(property_inventory) != len(bindings['properties']) or len(set(property_inventory.values())) != len(property_inventory):
                raise ValueError('Injective explicit qualified Field carrier map required')
            for member in record['members']:
                identity = (pin['documentId'], member['module'], member['element'])
                if identity not in property_inventory:
                    raise ValueError('Missing original qualified Field carrier binding')
                properties.append({'logical': {'documentId': identity[0], 'module': identity[1], 'element': identity[2], 'revision': SOURCE_SHA}, 'home': {'kind': 'props', 'propertyId': property_inventory[identity]}})
        records.append({'kind': 'object', 'logical': logical, 'sourceSystem': 'private-original-commerce-fixture', 'typeId': entry['type_id'], 'table': index, 'schemaRevision': SOURCE_SHA, 'properties': properties})
    binding = {'profile': 'ashlar-databricks-candidate/0.1.0', 'layoutRevision': 'ashlar-delta/0.3', 'layoutSha256': LAYOUT_SHA, 'modelPins': [pin], 'publication': {'id': manifest['publication_id'], 'manifestUuid': native[next((t for t in native if t.endswith('.manifest')))]['uuid'], 'tables': tables}, 'records': records}
    raw = encoded(binding)
    return {'interfaceVersion': 'weft-compile/0.2.0', 'dialect': 'weft-sql/0.2.0', 'sql': sql, 'modules': [{'documentJson': model.decode(), 'pin': pin, 'selectedModuleIds': ['domain']}], 'target': {'backendId': 'ashlar.databricks', 'backendVersion': '0.1.0-candidate', 'targetProfile': 'dbsql-candidate', 'bindingJson': raw, 'bindingSha256': hashlib.sha256(raw.encode()).hexdigest()}, 'options': {'allowCandidate': True}}


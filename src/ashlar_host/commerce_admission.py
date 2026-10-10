"""Selected local host implementation; native qualification remains version-scoped."""
import base64, datetime, hashlib, json
from pathlib import Path
from ashlar.whole_entity import changes_from_batch
from .source_identity import SOURCE_SHA, GRAPH_SHA, build_transaction
from .delta_custody import encoded
from .driver import CLOCK
UMF_PIN = 'c7c95e1c4ea5b72541f47fa0350ca467ff02f395'

class CommerceAdmission:
    """Explicit original source binding plus an actually recomputed public receipt.

    This host port admits only its exact original finite transaction. It does not
    emulate UMF validation, infer canonical Values, or mint accepted storage IDs.
    """

    def __init__(self, batch, bindings, model_path, graph_path, bindings_path, receipt_path):
        self.batch = batch
        self.changes = changes_from_batch(batch)
        paths = [Path(p) for p in (model_path, graph_path, bindings_path, receipt_path)]
        self.originals = tuple(((p, p.read_bytes()) for p in paths))
        (model_bytes, graph_bytes, binding_bytes, receipt_bytes) = [raw for (_, raw) in self.originals]
        if hashlib.sha256(model_bytes).hexdigest() != SOURCE_SHA or hashlib.sha256(graph_bytes).hexdigest() != GRAPH_SHA:
            raise PermissionError('Exact original commerce model/graph custody required')
        if json.loads(binding_bytes) != bindings:
            raise PermissionError('Original development binding bytes differ')
        document = json.loads(model_bytes)
        graph = json.loads(graph_bytes)
        public = json.loads(receipt_bytes)
        receipt = public['receipt']
        if document['umf'] != '0.8.0' or public['umfRevision'] != UMF_PIN or public['sourceSha256'] != SOURCE_SHA or (public['graphSha256'] != GRAPH_SHA):
            raise PermissionError('Exact original public0.8 receipt custody required')
        if (receipt['scope'] != 'supplied-dataset-only' or receipt['input']['scope'] != {'id': 'ashlar-original-commerce-fixture', 'closure': 'supplied-dataset-only'}) or receipt['datasetValidation']['valid'] is not True or receipt['datasetValidation']['complete'] is not True:
            raise PermissionError('Actual complete supplied finite dataset required')
        expected_records = {r['key'] for r in graph['objects']}
        actual_records = [r['instanceId'] for r in receipt['records']]
        if len(actual_records) != len(expected_records) or set(actual_records) != expected_records or len(receipt['keys']) != len(expected_records):
            raise PermissionError('Original complete finite Record/key inventory differs')
        if any((r['result']['validation']['valid'] is not True for r in receipt['records'])):
            raise PermissionError('Original finite individual Record invalid')
        expected_edges = {r['key']: (r['source'], r['target']) for r in graph['edges']}
        actual_edges = {r['instanceId']: (r['sourceInstanceId'], r['targetInstanceId']) for r in receipt['relationships']}
        if len(receipt['relationships']) != len(expected_edges) or actual_edges != expected_edges:
            raise PermissionError('Original public relationship occurrence endpoints differ')
        (rebuilt, expected_bindings) = build_transaction(model_bytes, graph_bytes, source_system=batch.feed, binding_profile=bindings['profile'])
        if rebuilt != batch or expected_bindings != bindings or any((c.operation != 'create' or c.state.schema_revision != SOURCE_SHA for c in self.changes)):
            raise PermissionError('Original fresh development source projection differs')
        self.facts = {'profile': 'ashlar-original-commerce-public-dataset-admission/0.1', 'qualification': __doc__, 'umf_revision': UMF_PIN, 'declared_umf': '0.8.0', 'source_sha256': SOURCE_SHA, 'graph_sha256': GRAPH_SHA, 'public_receipt_sha256': hashlib.sha256(receipt_bytes).hexdigest(), 'public_receipt_path': str(paths[3]), 'public_provenance': receipt['provenance'], 'dataset_validation': receipt['datasetValidation'], 'finite_scope': receipt['input']['scope'], 'receipt_scope': receipt['scope'], 'individual_record_validations': [{'instance_id': r['instanceId'], 'validation': r['result']['validation']} for r in receipt['records']], 'original_model_base64': base64.b64encode(model_bytes).decode(), 'original_graph_base64': base64.b64encode(graph_bytes).decode(), 'original_bindings_base64': base64.b64encode(binding_bytes).decode(), 'development_bindings_sha256': hashlib.sha256(binding_bytes).hexdigest(), 'source_transaction_sha256': hashlib.sha256(batch.begin + b''.join((r.raw for r in batch.records)) + batch.commit).hexdigest(), 'scope_note': 'Supplied dataset closure only; original warnings retained, public provenance remains unverified as emitted. Host private source/writer/retention policy is separately required.'}

    def metadata(self):
        if any((p.read_bytes() != raw for (p, raw) in self.originals)):
            raise PermissionError('Original commerce admission bytes changed')
        return json.loads(encoded(self.facts))

    def admit(self, change):
        self.metadata()
        if change not in self.changes:
            raise PermissionError('No admitted original finite commerce source change')

def original_commerce_oracle(model_bytes, graph_bytes, bindings, batch, columns):
    """Independent original graph/model → full native row oracle.

    Reads original lexical values and declared members directly. Does not consume
    build_transaction event projections, graph_sql_plan results or native rows.
    Bindings are separately admitted development IDs, not semantic validation.
    """
    if hashlib.sha256(model_bytes).hexdigest() != SOURCE_SHA or hashlib.sha256(graph_bytes).hexdigest() != GRAPH_SHA:
        raise ValueError('Exact original oracle bytes required')
    model = json.loads(model_bytes)
    graph = json.loads(graph_bytes)
    elements = {(m['id'], e['id']): e for m in model['modules'] for e in m['elements']}
    if bindings['profile'] == 'ashlar-commerce-development-bindings/0.2':
        properties = bindings['properties']
        expected_fields = {(model['id'], m, e) for ((m, e), field) in elements.items() if field['kind'] == 'field'}
        if len(properties) != len(expected_fields) or {tuple(p['identity']) for p in properties} != expected_fields or len({p['property_id'] for p in properties}) != len(properties) or any((str(int(p['property_id'])) != p['property_id'] or not 0 < int(p['property_id']) < 2 ** 63 for p in properties)):
            raise ValueError('Injective complete canonical development Field binding required')
    elif bindings['profile'] != 'ashlar-commerce-development-bindings/0.1':
        raise ValueError('Unknown development binding profile')
    assigned = {(b['kind'], b['originalKey']): b for b in bindings['entities']}
    if len(assigned) != 21 or {(k, key) for (k, key) in assigned} != {('object', o['key']) for o in graph['objects']} | {('edge', e['key']) for e in graph['edges']}:
        raise ValueError('Injective complete original binding inventory required')
    if len({(b['kind'], b['type_id'], b['id']) for b in bindings['entities']}) != 21:
        raise ValueError('Distinct development carrier identities required')
    instant = datetime.datetime.fromisoformat(CLOCK)
    stamp = str(int((instant - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)).total_seconds()) * 1000000)
    result = {role: [] for role in columns}
    cursor = json.dumps({'profile': batch.profile, 'offset': batch.cursor_after}, separators=(',', ':'))
    original_records = {r.delivery_id: r for r in batch.records}

    def text(value):
        return json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    for (kind, rows) in [('object', graph['objects']), ('edge', graph['edges'])]:
        role = 'object_current' if kind == 'object' else 'edge_current'
        typed = 'type_id' if kind == 'object' else 'rel_type_id'
        for (ordinal, original) in enumerate(rows, 1):
            binding = assigned[kind, original['key']]
            identity = {'source_system': batch.feed, typed: int(binding['type_id']), 'id': int(binding['id'])}
            props = []
            if kind == 'object':
                record = elements[original['type']['module'], original['type']['element']]
                for ref in record['members']:
                    field = elements[ref['module'], ref['element']]
                    lexical = original['values'][ref['element']]
                    property_key = ref['element']
                    if bindings['profile'] == 'ashlar-commerce-development-bindings/0.2':
                        candidates = [p for p in bindings['properties'] if p['identity'] == [model['id'], ref['module'], ref['element']]]
                        if len(candidates) != 1:
                            raise ValueError('Exact original qualified Field binding required')
                        property_key = candidates[0]['property_id']
                    props.append(text(property_key) + ':' + (text(lexical) if field['scalarType'] == 'string' else lexical))
            props_json = '{' + ','.join(props) + '}'
            retained = text({'profile': bindings['profile'], 'sourceSha256': SOURCE_SHA, 'graphSha256': GRAPH_SHA, 'original': original})
            delivery = kind + ':' + str(ordinal)
            raw = original_records[delivery]
            row = {name: None for (name, _) in columns[role]}
            row.update({k: str(v) for (k, v) in identity.items()})
            row.update(schema_revision=SOURCE_SHA, entity_version='1', props_json=props_json, retained_json=retained, source_feed=batch.feed, source_epoch=batch.epoch, source_cursor_json=cursor, source_delivery_id=delivery, published_at=stamp, lookup_hash=hashlib.sha256(text(identity).encode()).hexdigest(), apply_batch_id=batch.batch_id)
            endpoints = None
            if kind == 'object':
                row['logical_key_json'] = '[]'
            else:
                a = assigned['object', original['source']]
                b = assigned['object', original['target']]
                row.update(source_type=a['type_id'], source_id=a['id'], target_type=b['type_id'], target_id=b['id'])
                endpoints = [{'source': batch.feed, 'kind': 'object', 'type_id': int(x['type_id']), 'id': int(x['id'])} for x in (a, b)]
            result[role].append(row)
            state = {'key': {'source': batch.feed, 'kind': kind, 'type_id': int(binding['type_id']), 'id': int(binding['id'])}, 'version': 1, 'schema_revision': SOURCE_SHA, 'props_json': props_json, 'retained_json': retained, 'endpoints': endpoints}
            change = {'feed': batch.feed, 'epoch': batch.epoch, 'delivery_id': delivery, 'raw_digest': raw.sha256, 'operation': 'create', 'state': state}
            result['whole_source_history'].append({'feed': batch.feed, 'epoch': batch.epoch, 'delivery_id': delivery, 'digest': raw.sha256, 'change_json': json.dumps(change, separators=(',', ':')), 'raw_base64': base64.b64encode(raw.raw).decode()})
    return result

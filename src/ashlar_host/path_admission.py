"""Closed 0.4 path admission metadata; no SQL interpretation or native callbacks.

The application owns the trusted schema-validation port and freshly recompiled
artifact. Their injection is not proof of installation authenticity. Admission
retains guards to execute later under one authorized held publication; it does
not discharge them, authenticate source authority, or validate model semantics.
"""
from dataclasses import dataclass
import hashlib
import json
from types import MappingProxyType
from typing import Callable, Mapping


class PathPlanError(ValueError):
    """Safe payload-free admission refusal."""


SCHEMA_SHA256 = MappingProxyType({
    'compile-request-v0.4.schema.json': '51f9793fb7ca32fdde778e2ff8cacd1018e4142fb04dea610005895aacec3932',
    'compile-response-v0.4.schema.json': 'c177b9bbb9fff3d69a833aa5bf05dbe752a1f76e65833cae8ea35fd4b1373140',
    'logical-plan-v0.4.schema.json': 'a6f28858646e4795019037233ef0e8d6b0aeb45e7440cbcf4e7a477891a1b3d5',
})
BACKEND = MappingProxyType(dict(backendId='ashlar.databricks.paths', backendVersion='0.4.0-paths-candidate',
                               interfaceVersion='weft-backend/0.3.0', targetProfile='spark4-delta4-paths-candidate'))

PATHS_KEYS_BACKEND = MappingProxyType(dict(backendId='ashlar.databricks.paths-keys', backendVersion='0.4.0-paths-keys-candidate',
    interfaceVersion='weft-backend/0.3.0', targetProfile='spark4-delta4-paths-keys-candidate'))


def _require(condition, message):
    if not condition:
        raise PathPlanError(message)


def _closed(value, keys):
    _require(type(value) is dict and set(value) == set(keys), 'Closed original metadata required')


def _pairs(entries):
    answer = {}
    for key, value in entries:
        _require(key not in answer, 'Duplicate JSON member')
        answer[key] = value
    return answer


def _integer(token):
    _require(len(token.lstrip('-')) <= 20, 'JSON integer resource bound exceeded')
    return int(token)


def _numeric_predicate(predicate):
    """Identify coefficient guards selected by the owning predicate lowering."""
    numeric = lambda field: field.get('type', {}).get('family') in ('integer', 'decimal')
    op = predicate['op']
    if op in ('arithmeticCompare', 'arithmeticCompareExtended'):
        return True
    if op == 'scalarCompare':
        return numeric(predicate['left'])
    if op == 'legacy':
        original = predicate['predicate']
        if original['op'] == 'equal':
            return numeric(original['left'])
        if original['op'] == 'lexicographicGreater':
            return any(numeric(field) for field in original['columns'])
    return False


def _fraction(_):
    raise PathPlanError('Unadmitted JSON numeric atom')


def _parse(raw, maximum):
    _require(type(raw) is str, 'Exact original JSON text required')
    # Count before allocating UTF8 or invoking the parser.
    size = 0
    for char in raw:
        n = ord(char)
        _require(not 0xD800 <= n <= 0xDFFF, 'Invalid Unicode JSON text')
        size += 1 if n < 128 else 2 if n < 2048 else 3 if n < 65536 else 4
        _require(size <= maximum, 'JSON resource bound exceeded')
    try:
        return json.loads(raw, object_pairs_hook=_pairs, parse_int=_integer,
                          parse_float=_fraction, parse_constant=_fraction)
    except (json.JSONDecodeError, RecursionError):
        raise PathPlanError('Invalid bounded JSON metadata') from None


def _preflight(value, maximum):
    total, active = 0, set()
    def reserve(amount):
        nonlocal total
        total += amount
        _require(total <= maximum, 'Artifact resource bound exceeded')
    def visit(item, depth=0):
        _require(depth <= 64, 'Artifact nesting bound exceeded')
        if type(item) is str:
            reserve(2)
            for char in item:
                n = ord(char)
                _require(not 0xD800 <= n <= 0xDFFF, 'Invalid Unicode artifact')
                reserve(2 if char in ('"', '\\') or n in (8, 9, 10, 12, 13) else 6 if n < 32 or 127 <= n <= 65535 else 12 if n > 65535 else 1)
        elif type(item) is bool or item is None:
            reserve(5 if item is False else 4)
        elif type(item) is int:
            _require(-(10**20) < item < 10**20, 'Artifact integer resource bound exceeded')
            reserve(len(str(item)))
        elif type(item) in (dict, list):
            _require(id(item) not in active, 'Cyclic artifact refused')
            active.add(id(item))
            reserve(2)
            if type(item) is dict:
                for i, (key, child) in enumerate(item.items()):
                    _require(type(key) is str, 'JSON object names required')
                    reserve(1 + bool(i))
                    visit(key, depth + 1)
                    visit(child, depth + 1)
            else:
                for i, child in enumerate(item):
                    reserve(bool(i))
                    visit(child, depth + 1)
            active.remove(id(item))
        else:
            raise PathPlanError('Unadmitted artifact primitive')
    visit(value)


def _snapshot(value, maximum):
    _preflight(value, maximum)
    try:
        parts, size = [], 0
        for part in json.JSONEncoder(ensure_ascii=True, allow_nan=False, sort_keys=True,
                                     separators=(',', ':')).iterencode(value):
            size += len(part)
            _require(size <= maximum, 'Artifact resource bound exceeded')
            parts.append(part)
        return _parse(''.join(parts), maximum)
    except (TypeError, OverflowError, RecursionError):
        raise PathPlanError('Invalid original artifact metadata') from None


def _freeze(value):
    if type(value) is dict:
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if type(value) is list:
        return tuple(_freeze(v) for v in value)
    return value


@dataclass(frozen=True)
class PathSchemaValidation:
    """Trusted application port must actually validate offline Draft2020-12.

    Each invocation receives the entire exact immutable three-schema bundle,
    root filename and owned candidate. Return None on success; raise on failure.
    No filesystem/network retrieval is permitted. Passing a claim-only callback
    violates this port contract; this module cannot certify injected code.
    """
    schemas: Mapping[str, bytes]
    validate: Callable[[Mapping[str, bytes], str, dict], None]

    def __post_init__(self):
        _require(callable(self.validate) and set(self.schemas) == set(SCHEMA_SHA256), 'Complete trusted schema port required')
        owned = {}
        for name, expected in SCHEMA_SHA256.items():
            raw = self.schemas[name]
            _require(type(raw) is bytes and hashlib.sha256(raw).hexdigest() == expected, 'Pinned public schema bytes differ')
            owned[name] = raw
        object.__setattr__(self, 'schemas', MappingProxyType(owned))


@dataclass(frozen=True)
class PathAdmissionConfig:
    maximum_artifact_bytes: int
    schema_validation: PathSchemaValidation
    profile: str = 'paths'

    def __post_init__(self):
        _require(type(self.profile) is str and self.profile in ('paths', 'paths-keys'), 'Explicit supported path profile required')
        _require(type(self.maximum_artifact_bytes) is int and 1 <= self.maximum_artifact_bytes <= 16 * 1024 * 1024,
                 'Explicit finite artifact limit required')
        _require(type(self.schema_validation) is PathSchemaValidation, 'Trusted pinned schema port required')


@dataclass(frozen=True)
class PathAdmission:
    request: Mapping
    artifact: Mapping
    binding: Mapping
    obligations: tuple
    edge_schemas: tuple
    count_checks: tuple
    columns: tuple
    parameters: tuple


def _table(binding, index):
    tables = binding['publication']['tables']
    _require(type(index) is int and 0 <= index < len(tables), 'Original pinned table index required')
    return tables[index]


def _mapped_relationship(binding, hop):
    candidates = [r for r in binding['relationships'] if r['logical'] == hop['identity']]
    _require(len(candidates) == 1, 'Exactly one original relationship binding required')
    relation = candidates[0]
    _require(relation['kind'] == 'edge', 'Original native edge population required')
    endpoints = (relation['target'], relation['source']) if hop['inverse'] else (relation['source'], relation['target'])
    _require(endpoints == (hop['from'], hop['to']), 'Original relationship endpoint binding differs')
    return _table(binding, relation['table'])


def _checks(parameters, failure):
    _require(parameters.get('phase') == 'before-user-query' and parameters.get('samePublicationRequired') is True,
             'Same-held-publication guard phase required')
    checks = parameters.get('checks')
    _require(type(checks) is list and bool(checks), 'Complete emitted check inventory required')
    for check in checks:
        _require(type(check) is dict and (check.get('failureCode') == failure or failure == 'WFT-NUMERIC-DOMAIN' and check.get('representabilityOnly') is True and check.get('failureCode') == 'WFT-CAPABILITY' or failure == 'WFT-NUMERIC-DOMAIN' and check.get('failureCode') == 'WFT-BINDING' and set(check) == {'failureCode', 'record', 'sql'}) and type(check.get('sql')) is str and bool(check['sql']),
                 'Original exact SQL guard metadata required')


def _exact_metadata(actual, expected):
    # JSON equality distinguishes Boolean from Integer in generic obligation parameters.
    return json.dumps(actual, sort_keys=True, separators=(',', ':')) == json.dumps(expected, sort_keys=True, separators=(',', ':'))


def _authored_string_key(request, record_identity, key, maximum):
    """Local source correspondence, not a substitute for public UMF admission."""
    candidates = [m for m in request['modules'] if m['pin']['documentId'] == record_identity['documentId']
                  and m['pin']['revision'] == record_identity['revision']]
    _require(len(candidates) == 1, 'Original Record source required')
    document = _parse(candidates[0]['documentJson'], maximum)
    _require(document['id'] == record_identity['documentId'], 'Original document identity differs')
    modules = [m for m in document['modules'] if m['id'] == record_identity['module']]
    _require(len(modules) == 1, 'Original selected module required')
    records = [e for e in modules[0]['elements'] if e['id'] == record_identity['element'] and e['kind'] == 'record']
    _require(len(records) == 1, 'Original selected Record required')
    record = records[0]
    keys = [k for k in record.get('keys', []) if k['id'] == key['id']]
    _require(len(keys) == 1 and bool(keys[0]['fields']), 'Original authored key required')
    original_fields = keys[0]['fields']
    _require(len({(f['module'], f['element']) for f in original_fields}) == len(original_fields)
             and all(f in record['members'] for f in original_fields), 'Distinct original member key Fields required')
    fields, types = [], []
    for reference in original_fields:
        selected = [e for m in document['modules'] if m['id'] == reference['module']
                    for e in m['elements'] if e['id'] == reference['element'] and e['kind'] == 'field']
        _require(len(selected) == 1, 'Original key Field required')
        field = selected[0]
        _require(field.get('scalarType') == 'string' and field.get('cardinality') == 'one'
                 and field.get('nullability') == 'required' and not field.get('facets', {}), 'Required exact String authored key required')
        fields.append(dict(documentId=record_identity['documentId'], revision=record_identity['revision'], **reference))
        types.append(dict(family='string', facets={}, nullable=False))
    _require(key == dict(id=keys[0]['id'], fields=fields, types=types), 'Complete ordered original String key differs')
    return document


def _admit_keys_relationship(request, binding, plan, expression, maximum):
    _closed(expression, ('op', 'scan', 'relationship', 'bound'))
    _require(type(expression['bound']) is int and 1 <= expression['bound'] <= 1000, 'Exact bounded one-hop collection required')
    hop = expression['relationship']
    _closed(hop, ('identity', 'inverse', 'from', 'to', 'sourceKey', 'targetKey', 'sourceMultiplicity', 'targetMultiplicity', 'targetLifecycle'))
    _require(type(hop['inverse']) is bool, 'Exact original direction required')
    scans = [plan['source']] + [j['right'] for j in plan['joins']]
    visible = [scan for scan in scans if scan['occurrence'] == expression['scan']]
    _require(len(visible) == 1 and visible[0]['record'] == hop['from']
             and visible[0]['pin']['documentId'] == hop['from']['documentId']
             and visible[0]['pin']['revision'] == hop['from']['revision']
             and visible[0]['pin'] in plan['modulePins'], 'Original required root scan differs')
    _require(not any(j.get('kind', 'inner') == 'left' and j['right']['occurrence'] == expression['scan'] for j in plan['joins']), 'Potentially absent collection root refused')
    _require(plan['aggregate'] is False and plan['groups'] == [] and plan.get('having') is None and plan.get('pathExpansion') is None, 'Nonaggregate nonexpanded one-hop profile required')
    pair = (hop['identity']['documentId'], hop['identity']['revision'])
    _require(all((record['documentId'], record['revision']) == pair for record in (hop['from'], hop['to'])), 'Original relationship revision differs')
    document = _authored_string_key(request, hop['from'], hop['sourceKey'], maximum)
    _authored_string_key(request, hop['to'], hop['targetKey'], maximum)
    original = [r for m in document['modules'] if m['id'] == hop['identity']['module']
                for r in m.get('relationships', []) if r['id'] == hop['identity']['relationship']]
    _require(len(original) == 1, 'Original authored relationship required')
    definition = original[0]
    _require(definition.get('directed') is True and len(definition['source']) == len(definition['target']) == 1, 'Monomorphic original relationship required')
    source, target = (definition['target'][0], definition['source'][0]) if hop['inverse'] else (definition['source'][0], definition['target'][0])
    for endpoint, identity, key in ((source, hop['from'], hop['sourceKey']), (target, hop['to'], hop['targetKey'])):
        _require(endpoint['module'] == identity['module'] and endpoint['element'] == identity['element'], 'Original authored endpoint differs')
        if 'key' in endpoint:
            _require(endpoint['key'] == key['id'], 'Original selected endpoint key differs')
    # RelationshipRead preserves authored multiplicity/lifecycle labels even for inverse traversal.
    source_m, target_m = definition['sourceMultiplicity'], definition['targetMultiplicity']
    _require(not hop['inverse'] or type(definition.get('inverse')) is str and bool(definition['inverse']), 'Authored inverse traversal required')
    forward_source = hop['to'] if hop['inverse'] else hop['from']
    forward_key = hop['targetKey'] if hop['inverse'] else hop['sourceKey']
    source_record = next(e for m in document['modules'] if m['id'] == forward_source['module']
                         for e in m['elements'] if e['id'] == forward_source['element'])
    source_keys = source_record['keys']
    primary = [key for key in source_keys if key.get('primary') is True]
    chosen = primary[0] if len(primary) == 1 else source_keys[0] if not primary and len(source_keys) == 1 else None
    _require(chosen is not None and chosen['id'] == forward_key['id'], 'Original source endpoint key selection differs')
    _require(hop['sourceMultiplicity'] == source_m and hop['targetMultiplicity'] == target_m
             and hop['targetLifecycle'] == definition.get('targetLifecycle', 'independent'), 'Original relationship semantics differ')
    mapped = [r for r in binding['relationships'] if r['logical'] == hop['identity']]
    _require(len(mapped) == 1 and mapped[0]['acceptedDefinition'] == definition, 'Original accepted relationship definition differs')
    return _mapped_relationship(binding, hop)


def _admit_path_artifact(request: dict, artifact: dict, trusted_recompiled: dict, *, config: PathAdmissionConfig) -> PathAdmission:
    """Admit complete selected paths profile metadata before execution callbacks.

    Supports collections and expansion/grouped counts with their ordinary scalar,
    arithmetic, LEFT and positioned owning obligations. No query is rewritten or
    reduced to an illustrative subset. Actual callbacks must enforce every retained
    obligation and closing fence; this function performs none of those operations.
    """
    _require(type(config) is PathAdmissionConfig, 'Explicit path admission configuration required')
    backend = BACKEND if config.profile == 'paths' else PATHS_KEYS_BACKEND
    request, artifact, recompiled = (_snapshot(v, config.maximum_artifact_bytes) for v in (request, artifact, trusted_recompiled))
    _require(artifact == recompiled, 'Exact trusted public recompilation required')
    port = config.schema_validation
    for name, candidate in [('compile-request-v0.4.schema.json', request), ('compile-response-v0.4.schema.json', artifact)]:
        try:
            outcome = port.validate(port.schemas, name, _snapshot(candidate, config.maximum_artifact_bytes))
        except Exception:
            raise PathPlanError('Pinned public schema validation refused') from None
        _require(outcome is None, 'Schema port must validate rather than return a claim')
    _require(request['interfaceVersion'] == artifact['interfaceVersion'] == 'weft-compile/0.4.0'
             and request['dialect'] == artifact['dialect'] == 'weft-sql/0.4.0'
             and artifact['status'] == 'compiled' and artifact['backend'] == dict(backend), 'Explicit compiled paths profile required')
    _require(artifact['targetContext']['id'] == backend['targetProfile'] and artifact['targetContext']['engine'] == 'spark-sql' and artifact['targetContext']['engineVersion'] == '4.0.1' and artifact['targetContext']['storageLayoutRevision'] == 'ashlar-delta/0.3' and artifact['targetContext']['publicationRevision'] == 'ashlar-resolver/0.1-candidate', 'Exact declared candidate runtime profile required')
    target = request['target']
    _require(all(target[k] == backend[k] for k in ('backendId', 'backendVersion', 'targetProfile')), 'Original selected target differs')
    binding = _parse(target['bindingJson'], config.maximum_artifact_bytes)
    _require(hashlib.sha256(target['bindingJson'].encode('utf8')).hexdigest() == target['bindingSha256'] == artifact['bindingSha256'], 'Original binding hash differs')
    _require(binding['profile'] == 'ashlar-databricks-candidate/0.1.0' and artifact['modelPins'] == binding['modelPins'], 'Original full model pins differ')
    _require(artifact['targetContext']['storageLayoutRevision'] == binding['layoutRevision'], 'Original layout target correspondence differs')
    plan = artifact['logicalPlan']
    _require(plan['irVersion'] == 'weft-ir/0.4.0' and plan['modulePins'] == artifact['modelPins'] and plan['readProfile'] is None and plan['pageKey'] is None, 'Exact path plan/pins required')
    _require(len(request['modules']) == len(artifact['modelPins']), 'Complete original module inventory required')
    for pin in artifact['modelPins']:
        _require(pin['umfVersion'] in ('0.7.0', '0.8.0'), 'Supported original UMF pin required')
        modules = [m for m in request['modules'] if m['pin'] == pin]
        _require(len(modules) == 1, 'Complete original source module inventory required')
        module = modules[0]
        _require(hashlib.sha256(module['documentJson'].encode('utf8')).hexdigest() == pin['sha256'], 'Original source bytes differ')
    obligations = artifact['obligations']
    _require(type(obligations) is list and len({o['id'] for o in obligations}) == len(obligations), 'Unique complete obligation inventory required')
    owning = {o['id']: o for o in obligations}
    paths = []
    for output in plan['outputs']:
        expr = output['expression']
        if expr['op'] == 'relatedPaths':
            paths.append(dict(path=expr['path'], bound=expr['bound'], edgeEncoding='signed64-decimal/0.1'))
    collections = []
    keys_collections = []
    keys_tables = []
    for position, output in enumerate(plan['outputs'], 1):
        expr = output['expression']
        if expr['op'] in ('relatedKeys', 'relatedPaths'):
            collections.append(dict(outputPosition=position, kind=expr['op']))
        if expr['op'] == 'relatedKeys':
            _require(config.profile == 'paths-keys', 'One-hop profile not selected')
            table = _admit_keys_relationship(request, binding, plan, expr, config.maximum_artifact_bytes)
            keys_collections.append(dict(outputPosition=position, startScan=expr['scan'], relationship=expr['relationship'], bound=expr['bound']))
            keys_tables.append(dict(outputPosition=position, relationship=expr['relationship']['identity'], table=table, identityColumn='id', nativeType='BIGINT'))
    expansion = plan.get('pathExpansion')
    if expansion:
        paths.append(dict(path=expansion['path'], edgeEncoding='signed64-decimal/0.1'))
    required = {'ashlar.candidate.publication', 'ashlar.candidate.scalarIntegrity'}
    if paths:
        required |= {'ashlar.candidate.relationshipIntegrity', 'ashlar.path.occurrenceIntegrity'}
    if keys_collections:
        required |= {'ashlar.candidate.relationshipIntegrity', 'ashlar.relatedKeys.collectionIntegrity'}
    if config.profile == 'paths-keys' and collections:
        required.add('ashlar.relatedKeys.ordinalCapacity')
    count_kinds = set()
    if expansion:
        count_kinds = {('pathRows' if out['expression']['op'] == 'count' else 'targetDistinct')
                       for out in plan['outputs'] if out['expression']['op'] in ('count', 'countDistinctPathTargets')}
    if count_kinds:
        required.add('ashlar.path.countCapacity')
    left = [j['right'] for j in plan['joins'] if j.get('kind', 'inner') == 'left']
    if left:
        required.add('outerJoin.matchIntegrity')
    positioned = 'project.positionedOutputs' in plan['requiredCapabilities']
    if positioned:
        required.add('weft.output.positioned')
    predicates = plan['filters'] + [p for join in plan['joins'] for p in join['on']]
    if (any(c.startswith('arithmetic.') for c in plan['requiredCapabilities'])
            or 'aggregate.countDistinct' in plan['requiredCapabilities']
            or ('aggregate.count' in plan['requiredCapabilities'] and not plan.get('pathExpansion'))
            or any(_numeric_predicate(p) for p in predicates)):
        required.add('ashlar.arithmetic.exact')
    _require(set(owning) == required, 'Unknown, missing or unselected obligation')
    failures = {'ashlar.candidate.publication': 'WFT-OBLIGATION', 'ashlar.candidate.scalarIntegrity': 'WFT-NUMERIC-DOMAIN',
                'ashlar.candidate.relationshipIntegrity': 'WFT-BINDING', 'ashlar.path.occurrenceIntegrity': 'WFT-BINDING',
                'ashlar.path.countCapacity': 'WFT-CAPABILITY', 'outerJoin.matchIntegrity': 'WFT-OBLIGATION',
                'weft.output.positioned': 'WFT-OBLIGATION', 'ashlar.arithmetic.exact': 'WFT-CAPABILITY'}
    failures.update({'ashlar.relatedKeys.collectionIntegrity': 'WFT-BINDING', 'ashlar.relatedKeys.ordinalCapacity': 'WFT-CAPABILITY'})
    parameter_keys = {
        'ashlar.candidate.publication': ('layoutRevision', 'layoutSha256', 'modelPins', 'nativeProfile', 'payloadValidation', 'publication', 'publicationPhase', 'requirements', 'visibility'),
        'ashlar.candidate.scalarIntegrity': ('checks', 'noPartialPublication', 'parameters', 'phase', 'samePublicationRequired', 'success'),
        'ashlar.candidate.relationshipIntegrity': ('checks', 'lifecycle', 'multiplicity', 'phase', 'policy', 'samePublicationRequired', 'success'),
        'ashlar.path.occurrenceIntegrity': ('checks', 'edgeSchemas', 'noPartialPublication', 'paths', 'phase', 'samePublicationRequired', 'success'),
        'ashlar.path.countCapacity': ('checks', 'nativeRepresentation', 'noPartialPublication', 'phase', 'samePublicationRequired', 'success'),
        'outerJoin.matchIntegrity': ('noPartialPublication', 'phase', 'samePublicationRequired', 'scans'),
        'weft.output.positioned': ('columns', 'profile'),
        'ashlar.arithmetic.exact': ('checks', 'guardOrder', 'maxScale', 'nativeRepresentation', 'noPartialPublication', 'phase', 'result', 'samePublicationRequired', 'success'),
    }
    parameter_keys.update({
        'ashlar.relatedKeys.collectionIntegrity': ('checks', 'collections', 'edgeSchemas', 'noPartialPublication', 'phase', 'samePublicationRequired', 'success'),
        'ashlar.relatedKeys.ordinalCapacity': ('checks', 'collections', 'maximum', 'nativeRepresentation', 'noPartialPublication', 'phase', 'samePublicationRequired', 'success')})
    for name, obligation in owning.items():
        _closed(obligation, ('id', 'owner', 'failureCode', 'parameters'))
        _closed(obligation['parameters'], parameter_keys[name])
        _require(obligation['owner'] == 'host' and obligation['failureCode'] == failures[name], 'Exact owning obligation envelope required')
        if name == 'ashlar.arithmetic.exact':
            p = obligation['parameters']
            _require(p.get('phase') == 'before-user-query' and p.get('samePublicationRequired') is True and p.get('noPartialPublication') is True and type(p.get('checks')) is list and bool(p['checks']), 'Complete owning arithmetic guards required')
            for check in p['checks']:
                _closed(check, ('phase', 'sql'))
                _require(check['phase'] in ('join-candidates', 'where-candidates', 'projection-survivors', 'aggregate-candidates') and type(check['sql']) is str and bool(check['sql']), 'Original arithmetic phase/check required')
        elif name not in ('ashlar.candidate.publication', 'outerJoin.matchIntegrity', 'weft.output.positioned'):
            _checks(obligation['parameters'], failures[name])
    publication = owning['ashlar.candidate.publication']['parameters']
    _require(all(publication[k] == binding[k] for k in ('publication', 'modelPins', 'layoutRevision', 'layoutSha256')), 'Complete publication obligation differs')
    edge_schemas = []
    for name in ('ashlar.path.occurrenceIntegrity', 'ashlar.path.countCapacity'):
        if name in owning:
            params = owning[name]['parameters']
            _require(params['noPartialPublication'] is True and params['success'] == 'one exact STRING count equal to 0 per check', 'Exact path success/visibility requirement required')
    if 'ashlar.path.countCapacity' in owning:
        _require(owning['ashlar.path.countCapacity']['parameters']['nativeRepresentation'] == 'signed64', 'Exact count capacity representation required')
    if paths:
        occurrence = owning['ashlar.path.occurrenceIntegrity']['parameters']
        _require(occurrence['paths'] == paths, 'Exact output-order path inventory required')
        for check in occurrence['checks']:
            _closed(check, ('pathIndex', 'kind', 'sql', 'failureCode'))
        expected_checks = [(i, kind) for i, entry in enumerate(paths) for kind in
                           (['edgeEncoding', 'intermediateIdentity', 'collectionEncoding'] if 'bound' in entry else ['edgeEncoding', 'intermediateIdentity'])]
        _require([(c['pathIndex'], c['kind']) for c in occurrence['checks']] == expected_checks, 'Complete per-path check order required')
        for i, entry in enumerate(paths):
            for h, hop in enumerate(entry['path']['hops']):
                edge_schemas.append(dict(pathIndex=i, hop=h, relationship=hop['identity'], table=_mapped_relationship(binding, hop), identityColumn='id', nativeType='BIGINT'))
        _require(occurrence['edgeSchemas'] == edge_schemas, 'Exact per-hop pinned schema inventory required')
    if keys_collections:
        params = owning['ashlar.relatedKeys.collectionIntegrity']['parameters']
        _require(_exact_metadata(params['collections'], keys_collections) and _exact_metadata(params['edgeSchemas'], keys_tables), 'Complete original one-hop inventory differs')
        edge_schemas.extend(keys_tables)
    for name, inventory, kind in (('ashlar.relatedKeys.collectionIntegrity', keys_collections, 'collectionEncoding'),
                                  ('ashlar.relatedKeys.ordinalCapacity', collections, 'fullOccurrencePrefix')):
        if name not in owning:
            continue
        params = owning[name]['parameters']
        _require(params['noPartialPublication'] is True and params['success'] == 'one exact STRING count equal to 0 per check', 'Exact collection visibility/success required')
        if name.endswith('ordinalCapacity'):
            _require(_exact_metadata(params['collections'], inventory) and params['nativeRepresentation'] == 'decimal38'
                     and params['maximum'] == '99999999999999999999999999999999999999', 'Exact full-bag ordinal capacity required')
        for check in params['checks']:
            _closed(check, ('outputPosition', 'kind', 'sql', 'failureCode'))
        _require(all(type(c['outputPosition']) is int for c in params['checks']), 'Exact original output positions required')
        _require([(c['outputPosition'], c['kind']) for c in params['checks']] == [(c['outputPosition'], kind) for c in inventory], 'Complete ordered per-output collection checks required')
    count_checks = []
    if count_kinds:
        count_checks = owning['ashlar.path.countCapacity']['parameters']['checks']
        for check in count_checks:
            _closed(check, ('pathOccurrence', 'kind', 'sql', 'failureCode'))
        _require(len(count_checks) == len(count_kinds) and {(c['pathOccurrence'], c['kind']) for c in count_checks} == {(expansion['occurrence'], k) for k in count_kinds}, 'Complete independent count capacity inventory required')
    columns = artifact['columns']
    _require(len(columns) == len(plan['outputs']) and all(type(c['position']) is int and c['position'] == i for i, c in enumerate(columns, 1)), 'Complete ordered output columns required')
    for column, output in zip(columns, plan['outputs']):
        _require(column['outputName'] == output['name'], 'Original authored output label differs')
        expr, representation = output['expression'], column['representation']
        if expr['op'] == 'relatedPaths':
            expected = dict(kind='relatedPaths', path=expr['path'], startRecord=expr['path']['hops'][0]['from'], bound=expr['bound'], edgeEncoding='signed64-decimal/0.1')
            right = [s for s in left if s['occurrence'] == expr['path']['startScan']]
            if right:
                expected['outerJoin'] = dict(scan=right[0]['occurrence'], record=right[0]['record'])
            _require(representation == expected and column['nullable'] is False, 'Exact path output role required')
        if expr['op'] == 'relatedKeys':
            expected = dict(kind='relatedKeys', relationship=expr['relationship']['identity'], key=expr['relationship']['targetKey'], bound=expr['bound'])
            _require(representation == expected and column['nullable'] is False, 'Exact required one-hop output role required')
        if expr['op'] == 'countDistinctPathTargets':
            hop = expansion['path']['hops'][1]
            expected = dict(kind='scalar', carrier='text', decoder='exact-integer', logicalType=dict(family='integer', facets={}, nullable=False),
                            pathTarget=dict(pathOccurrence=expansion['occurrence'], record=hop['to'], key=hop['targetKey']))
            _require(representation == expected and column['nullable'] is False, 'Complete terminal count identity required')
    if positioned:
        expected_map = [{k: c[k] for k in ('position', 'outputName', 'carrierName', 'sourceIdentities')} for c in columns]
        _require(owning['weft.output.positioned']['parameters'] == dict(profile='weft-positioned-output/0.3.0', columns=expected_map)
                 and len({c['carrierName'] for c in columns}) == len(columns), 'Exact positioned output map required')
    else:
        _require(all('carrierName' not in c for c in columns) and len({c['outputName'] for c in columns}) == len(columns), 'Unadmitted output positioning')
    if left:
        match = owning['outerJoin.matchIntegrity']['parameters']
        _require(match['phase'] == 'before-user-query' and match['samePublicationRequired'] is True and match['noPartialPublication'] is True, 'Exact LEFT held phase required')
        scans = match['scans']
        _require(len(scans) == len(left), 'Complete LEFT scan inventory required')
        for scan, original in zip(scans, left):
            _closed(scan, ('scan', 'record', 'table', 'identityColumn', 'nativeType', 'sql'))
            _require(type(scan['sql']) is str and bool(scan['sql']) and scan['identityColumn'] == 'id' and scan['nativeType'] == 'BIGINT', 'Complete native LEFT schema/check declaration required')
            records = [r for r in binding['records'] if r['logical'] == original['record']]
            _require(len(records) == 1 and records[0]['kind'] == 'object' and scan['scan'] == original['occurrence'] and scan['record'] == original['record'] and scan['table'] == _table(binding, records[0]['table']), 'Exact LEFT schema source required')
    consumed_relationships = [h['identity'] for entry in paths for h in entry['path']['hops']]
    consumed_relationships += [entry['relationship']['identity'] for entry in keys_collections]
    if consumed_relationships:
        rel_checks = owning['ashlar.candidate.relationshipIntegrity']['parameters']['checks']
        for check in rel_checks:
            _closed(check, ('relationship', 'sql', 'failureCode'))
        canonical = lambda identity: json.dumps(identity, sort_keys=True)
        _require({canonical(c['relationship']) for c in rel_checks} == {canonical(r) for r in consumed_relationships}, 'Every consumed relationship must retain owning checks')
    parameters = artifact['parameters']
    _require(all(type(p['position']) is int and p['position'] == i for i, p in enumerate(parameters, 1)), 'Exact ordered parameter inventory required')
    return PathAdmission(_freeze(request), _freeze(artifact), _freeze(binding), _freeze(obligations),
                         _freeze(edge_schemas), _freeze(count_checks), _freeze(columns), _freeze(parameters))


def admit_path_artifact(request: dict, artifact: dict, trusted_recompiled: dict, *, config: PathAdmissionConfig) -> PathAdmission:
    """Public safe-refusal entry point; see PathAdmission and trusted-port contract."""
    try:
        return _admit_path_artifact(request, artifact, trusted_recompiled, config=config)
    except (KeyError, IndexError, TypeError, AttributeError, UnicodeError):
        raise PathPlanError('Malformed original path admission metadata') from None

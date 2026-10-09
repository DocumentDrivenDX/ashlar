"""Closed proof that a compiled 0.3 row plan needs no arithmetic checks.

This inspects compiler IR, never SQL or UMF source meaning. Scalar source checks
and the complete publication interval remain mandatory. Unknown operations fail
closed; this proof does not admit aggregates or numerical predicates.
"""
import hashlib,json


def admit_field_plan(artifact, binding, modules):
    def closed(value, keys):
        if type(value) is not dict or set(value) != set(keys):
            raise ValueError('Unknown field-plan structure')

    def identity(value):
        closed(value, ('documentId', 'module', 'element', 'revision'))
        if any(type(v) is not str or not v for v in value.values()):
            raise ValueError('Original complete identity required')
        return json.dumps(value, sort_keys=True, separators=(',', ':'))

    def logical_type(value, string_only=False):
        closed(value, ('family', 'facets', 'nullable'))
        if value['nullable'] is not False or type(value['facets']) is not dict:
            raise ValueError('Required original scalar type required')
        families = {'string'} if string_only else {'string', 'integer', 'decimal', 'boolean'}
        if value['family'] not in families:
            raise ValueError('Unproved scalar operation')
        int64_slot = {'family': 'integer', 'facets': {'integerWidth': {'bits': 64, 'signed': True}}, 'nullable': False}
        if value['facets'] != {}:
            width=value['facets'].get('integerWidth')
            if (value != int64_slot or type(width) is not dict or type(width.get('bits')) is not int
                    or width.get('signed') is not True):
                raise ValueError('Unknown scalar facets in field-only proof')
        if string_only and value != {'family': 'string', 'facets': {}, 'nullable': False}:
            raise ValueError('Only the original closed String comparison type admitted')

    def span(value):
        closed(value, ('start', 'end'))
        if any(type(v) is not int or v < 0 for v in value.values()) or value['end'] < value['start']:
            raise ValueError('Original ordered source span required')

    plan = artifact.get('logicalPlan')
    closed(plan, ('aggregate', 'filters', 'groups', 'irVersion', 'joins', 'limit',
                  'modulePins', 'order', 'outputs', 'pageKey', 'readProfile',
                  'requiredCapabilities', 'source', 'typeGraph'))
    if (plan['irVersion'] != 'weft-ir/0.3.0' or plan['aggregate'] is not False
            or plan['groups'] != [] or any(plan[k] is not None for k in ('limit', 'pageKey', 'readProfile'))
            or plan['modulePins'] != binding['modelPins']):
        raise ValueError('Empty arithmetic checks require a complete field-only row plan')
    allowed = {'project', 'scan', 'filter', 'equal', 'innerJoin', 'order.asc', 'and',
               'parameter.named', 'type.integer', 'type.integer.unbounded',
               'type.string', 'type.boolean', 'type.decimal'}
    if type(plan['requiredCapabilities']) is not list or any(
            type(c) is not str or c not in allowed for c in plan['requiredCapabilities']):
        raise ValueError('Unproved field-plan capability')
    for key in ('joins', 'filters', 'order', 'outputs', 'typeGraph'):
        if type(plan[key]) is not list:
            raise ValueError('Complete field-plan arrays required')
    # This is exact authored metadata correspondence, not source validation.
    # The original public UMF admission and freshly pinned compiler still own
    # interpretation. Never guess a family from native JSON or SQL text.
    original_fields = {}
    for entry in modules:
        if entry['pin'] not in binding['modelPins'] or hashlib.sha256(entry['documentJson'].encode()).hexdigest() != entry['pin']['sha256']:
            raise ValueError('Exact original model bytes and pin required')
        model = json.loads(entry['documentJson'])
        if model['id'] != entry['pin']['documentId'] or model['umf'] != entry['pin']['umfVersion']:
            raise ValueError('Original declared model version differs')
        for module in model['modules']:
            for element in module['elements']:
                if element['kind'] == 'field':
                    key = identity({'documentId': model['id'], 'module': module['id'],
                                    'element': element['id'], 'revision': entry['pin']['revision']})
                    if key in original_fields:
                        raise ValueError('Duplicate original Field metadata')
                    original_fields[key] = element
    if [entry['pin'] for entry in modules] != binding['modelPins']:
        raise ValueError('Complete original model pin vector required')
    scans = {}
    required_checks = set()
    integer_checks = set()

    def original_type(key):
        # Admit the compiler's closed default scalar realization only. Faceted
        # or unavailable Fields need their own explicit descriptor admission;
        # never manufacture facets or requiredness from observed source values.
        original = original_fields[key]
        if (original.get('cardinality') != 'one' or original.get('nullability') != 'required'
                or original.get('facets', {}) != {} or original.get('scalarType') not in {'string', 'integer', 'decimal', 'boolean'}):
            raise ValueError('Original closed required scalar metadata required')
        return {'family': original['scalarType'], 'facets': {}, 'nullable': False}

    def scan(value):
        closed(value, ('occurrence', 'pin', 'record'))
        record_key = identity(value['record'])
        name = value['occurrence']
        if type(name) is not str or not name or name in scans or value['pin'] not in binding['modelPins']:
            raise ValueError('Unique original source occurrence required')
        if any(value['record'][k] != value['pin'][k] for k in ('documentId', 'revision')):
            raise ValueError('Original record/source pin differs')
        records = [r for r in binding['records'] if r['logical'] == value['record']]
        if len(records) != 1:
            raise ValueError('Original bound source record required')
        properties = {identity(p['logical']) for p in records[0]['properties']}
        scans[name] = (record_key, properties)
        return name

    def field(value, visible, *, typed=True, string_only=False):
        closed(value, ('identity', 'scan', 'span', 'type') if typed else ('identity', 'scan', 'op'))
        if not typed and value['op'] != 'field':
            raise ValueError('Only original direct Field outputs admitted')
        key = identity(value['identity'])
        occurrence = value['scan']
        if occurrence not in visible or key not in scans[occurrence][1]:
            raise ValueError('Field is outside original source/prefix scope')
        if key not in original_fields:
            raise ValueError('Original authored Field metadata required')
        if typed:
            span(value['span'])
            logical_type(value['type'], string_only)
            if value['type'] != original_type(key):
                raise ValueError('Field type differs from original authored metadata')
        required_checks.add((scans[occurrence][0], key))
        if original_type(key)['family'] == 'integer':
            integer_checks.add((scans[occurrence][0], key))
        return key

    parameters = artifact.get('parameters')
    if type(parameters) is not list:
        raise ValueError('Original ordered parameter slots required')
    for index, p in enumerate(parameters, 1):
        closed(p, ('position', 'logicalType', 'value', 'origin'))
        if type(p['position']) is not int or p['position'] != index or type(p['value']) is not str or type(p['origin']) is not dict:
            raise ValueError('Original exact ordered slot required')
        logical_type(p['logicalType'])

    def operand(value, visible):
        if type(value) is not dict:
            raise ValueError('Original comparison operand required')
        if value.get('kind') == 'field':
            closed(value, ('kind', 'field'))
            field(value['field'], visible, string_only=True)
            return
        keys = ('kind', 'value', 'type', 'span')
        if value.get('kind') == 'parameter':
            keys += ('name',)
        elif value.get('kind') != 'literal':
            raise ValueError('Unknown comparison operand')
        closed(value, keys)
        logical_type(value['type'], True)
        span(value['span'])
        if type(value['value']) is not str:
            raise ValueError('Original string comparison value required')
        if value['kind'] == 'parameter' and (type(value['name']) is not str or not value['name']):
            raise ValueError('Original nonempty named parameter required')
        origin = {'kind': 'namedParameter', 'name': value['name'], 'sourceSpan': value['span']} if value['kind'] == 'parameter' else {'kind': 'literal', 'sourceSpan': value['span']}
        if not any(p['origin'] == origin and p['value'] == value['value'] and p['logicalType'] == value['type'] for p in parameters):
            raise ValueError('Original exact comparison slot required')

    def predicate(value, visible):
        closed(value, ('op', 'predicate'))
        if value['op'] != 'legacy':
            raise ValueError('Arithmetic/unknown predicates require independent checks')
        p = value['predicate']
        closed(p, ('op', 'left', 'right'))
        if p['op'] != 'equal':
            raise ValueError('Only string equality needs no arithmetic checks')
        field(p['left'], visible, string_only=True)
        operand(p['right'], visible)

    visible = {scan(plan['source'])}
    for join in plan['joins']:
        closed(join, ('right', 'on'))
        visible.add(scan(join['right']))
        if type(join['on']) is not list or not join['on']:
            raise ValueError('Original nonempty INNER JOIN predicate required')
        for p in join['on']:
            predicate(p, visible)
    for p in plan['filters']:
        predicate(p, visible)
    for f in plan['order']:
        field(f, visible)
    columns = artifact.get('columns')
    if type(columns) is not list or not plan['outputs'] or len(plan['outputs']) != len(columns):
        raise ValueError('Complete original output descriptors required')
    if len({o.get('name') for o in plan['outputs']}) != len(plan['outputs']):
        raise ValueError('Unique original output names required')
    graph_ids = set()
    for d in plan['typeGraph']:
        closed(d, ('identity', 'availability', 'kind', 'type'))
        key = identity(d['identity'])
        if key in graph_ids or key not in original_fields or d['availability'] != 'required' or d['kind'] != 'scalar' or d['type'] != original_type(key):
            raise ValueError('Unproved or duplicate original type descriptor')
        graph_ids.add(key)
    if graph_ids != {identity(o['expression']['identity']) for o in plan['outputs']}:
        raise ValueError('Exact original output type graph required')
    for index, (output, column) in enumerate(zip(plan['outputs'], columns), 1):
        closed(output, ('name', 'expression'))
        closed(column, ('outputName', 'position', 'sourceIdentities', 'nullable', 'representation'))
        key = field(output['expression'], visible, typed=False)
        if column.get('outputName') != output['name'] or type(column.get('position')) is not int or column['position'] != index:
            raise ValueError('Original output order differs')
        if column.get('sourceIdentities') != [output['expression']['identity']] or column.get('nullable') is not False:
            raise ValueError('Original output lineage differs')
        descriptors = [d for d in plan['typeGraph'] if d.get('identity') == output['expression']['identity']]
        if len(descriptors) != 1:
            raise ValueError('Original output type descriptor required')
        d = descriptors[0]
        closed(d, ('identity', 'availability', 'kind', 'type'))
        if d['availability'] != 'required' or d['kind'] != 'scalar':
            raise ValueError('Only required original scalar output admitted')
        logical_type(d['type'])
        if d['type'] != original_type(key):
            raise ValueError('Output type differs from original authored metadata')
        representation = column.get('representation')
        closed(representation, ('kind', 'carrier', 'logicalType', 'decoder'))
        decoders = {'string': 'text', 'integer': 'exact-integer', 'decimal': 'exact-decimal', 'boolean': 'boolean'}
        if representation['kind'] != 'scalar' or representation['carrier'] != 'text' or representation['logicalType'] != d['type'] or representation['decoder'] != decoders[d['type']['family']]:
            raise ValueError('Original exact output carrier differs')
    obligations = [o for o in artifact.get('obligations', []) if o.get('id') == 'ashlar.candidate.scalarIntegrity']
    if len(obligations) != 1:
        raise ValueError('Original source-integrity obligation required')
    checks = obligations[0]['parameters']['checks']
    covered = {(identity(c['record']), identity(c['field'])) for c in checks
               if c.get('publicSourceOnly') is not True and c.get('representabilityOnly') is not True}
    if not required_checks <= covered:
        raise ValueError('Every original consumed Field requires a native source-integrity check')
    for flag in ('publicSourceOnly', 'representabilityOnly'):
        guarded = {(identity(c['record']), identity(c['field'])) for c in checks if c.get(flag) is True}
        if not integer_checks <= guarded:
            raise ValueError('Original integer Fields require separate public source and native capacity checks')

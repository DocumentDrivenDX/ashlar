"""Closed original Field/operation proof for explicit compiled 0.3 host subsets.

This inspects compiler IR, never SQL or UMF source meaning. Scalar source checks
and the complete publication interval remain mandatory. Unknown operations fail
closed. Default routes refuse aggregates; explicit required-String COUNT DISTINCT
and literal IN require separate capacity guards and target admission.
"""
import hashlib,json


def admit_field_plan(artifact, binding, modules, *, positioned_output_only=False, native_null=False, distinct=False, count_distinct=False):
    if type(native_null) is not bool:
        raise ValueError('Explicit native-null opt-in must be Boolean')
    if type(distinct) is not bool or (distinct and native_null):
        raise ValueError("Explicit separate DISTINCT host opt-in required")
    if type(count_distinct) is not bool or (count_distinct and (native_null or distinct or positioned_output_only)):
        raise ValueError('Explicit separate distinct-count host opt-in required')
    native_null_ids = set()
    native_null_graph_ids = set()

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
        if native_null and value['family'] == 'decimal' and set(value['facets']) == {'precision', 'scale'}:
            precision, scale = value['facets']['precision'], value['facets']['scale']
            if type(precision) is not int or type(scale) is not int or not 0 <= scale <= precision <= 38:
                raise ValueError('Exact original Decimal facets required')
            return
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
                  'requiredCapabilities', 'source', 'typeGraph') + (('distinct',) if distinct else ()))
    if (plan['irVersion'] != 'weft-ir/0.3.0' or (type(plan['aggregate']) is not bool or (not count_distinct and plan['aggregate'] is not False))
            or (not count_distinct and plan['groups'] != []) or any(plan[k] is not None for k in ('pageKey', 'readProfile'))
            or (plan['limit'] is not None and (not distinct or type(plan['limit']) is not int or not 1 <= plan['limit'] <= 1000))
            or plan['modulePins'] != binding['modelPins']):
        raise ValueError('Empty arithmetic checks require a complete field-only row plan')
    allowed = {'project', 'scan', 'filter', 'equal', 'innerJoin', 'order.asc', 'and',
               'parameter.named', 'type.integer', 'type.integer.unbounded',
               'type.string', 'type.boolean', 'type.decimal', 'compare.notEqual',
               'project.positionedOutputs', 'compare.less', 'compare.lessEqual',
               'compare.greaterEqual', 'compare.scalarJoin'}
    if count_distinct:
        allowed={'project','scan','filter','equal','innerJoin','order.asc','and','type.string','aggregate','group','aggregate.countDistinct','predicate.stringIn'}
        if (type(plan['requiredCapabilities']) is not list or any(type(c) is not str for c in plan['requiredCapabilities'])
                or len(set(plan['requiredCapabilities'])) != len(plan['requiredCapabilities'])
                or not {'scan','project','type.string'} <= set(plan['requiredCapabilities'])
                or not {'aggregate.countDistinct','predicate.stringIn'} & set(plan['requiredCapabilities'])):
            raise ValueError('Closed explicit String set/count capability inventory required')
    if distinct:
        if (plan['distinct'] is not True or 'project.distinct' not in plan['requiredCapabilities']
                or 'type.string' not in plan['requiredCapabilities']
                or len(set(plan['requiredCapabilities'])) != len(plan['requiredCapabilities'])
                or ('limit' in plan['requiredCapabilities']) != (plan['limit'] is not None)
                or ('order.asc' in plan['requiredCapabilities']) != bool(plan['order'])):
            raise ValueError('Exact DISTINCT member and capability required')
        allowed |= {'project.distinct', 'limit'}
    if native_null:
        allowed |= {'predicate.nativeNull', 'compare.nullAwareStringEqual', 'value.nativeNull', 'value.presence'}
    if type(plan['requiredCapabilities']) is not list or any(
            type(c) is not str or c not in allowed for c in plan['requiredCapabilities']):
        raise ValueError('Unproved field-plan capability')
    for key in ('joins', 'filters', 'groups', 'order', 'outputs', 'typeGraph'):
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
        if not native_null and original.get('facets', {}) != {}:
            raise ValueError('Original default Field facets required')
        availability = original.get('nullability')
        facets = original.get('facets', {})
        if (original.get('cardinality') != 'one' or availability not in ({'required', 'absent-allowed'} if native_null else {'required'})
                or original.get('scalarType') not in {'string', 'integer', 'decimal', 'boolean'}):
            raise ValueError('Original closed scalar metadata required')
        result = {'family': original['scalarType'], 'facets': facets, 'nullable': False}
        logical_type(result)
        if availability == 'absent-allowed':
            if result['family'] == 'integer':
                raise ValueError('Optional mathematical Integer not qualified')
            native_null_ids.add(key)
        return result

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
        if count_distinct and original_type(key) != {'family':'string','facets':{},'nullable':False}:
            raise ValueError('Distinct-count inputs require original required String Fields')
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
        if count_distinct and p['origin'].get('kind')=='namedParameter':raise ValueError('Named slots are outside the explicit String set/count host subset')

    def operand(value, visible):
        if type(value) is not dict:
            raise ValueError('Original comparison operand required')
        if value.get('kind') == 'field':
            closed(value, ('kind', 'field'))
            field(value['field'], visible, string_only=True)
            return
        keys = ('kind', 'value', 'type', 'span')
        if value.get('kind') == 'parameter':
            if count_distinct:raise ValueError('Named parameters are outside the explicit String set/count host subset')
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

    comparison_caps=set()
    in_used=False
    equality_used=False
    def predicate(value, visible, *, join=False):
        nonlocal in_used,equality_used
        if count_distinct and type(value) is dict and value.get('op') == 'stringIn':
            closed(value, ('op','field','values'))
            field(value['field'],visible,string_only=True)
            if type(value['values']) is not list or not 1 <= len(value['values']) <= 256:
                raise ValueError('Original bounded nonempty String IN slots required')
            for v in value['values']:
                if type(v) is not dict or v.get('kind') != 'literal':
                    raise ValueError('Only original literal String IN slots admitted')
                operand(v,visible)
            in_used=True
            return
        if native_null and type(value) is dict and value.get('op') == 'nullTest':
            closed(value, ('op', 'field', 'negated'))
            if type(value['negated']) is not bool or 'predicate.nativeNull' not in plan['requiredCapabilities']:
                raise ValueError('Original explicit null-test capability required')
            native_null_graph_ids.add(field(value['field'], visible))
            return
        if native_null and type(value) is dict and value.get('op') == 'nullableStringEqual':
            closed(value, ('op', 'left', 'right'))
            if not join or 'compare.nullAwareStringEqual' not in plan['requiredCapabilities']:
                raise ValueError('Only explicit optional String ON equality admitted')
            comparison_caps.add('compare.nullAwareStringEqual')
            left = field(value['left'], visible, string_only=True)
            right = field(value['right'], visible, string_only=True)
            native_null_graph_ids.update((left, right))
            if not {left, right} & native_null_ids:
                raise ValueError('Original optional String operand required')
            return
        if type(value) is dict and value.get('op') == 'scalarCompare':
            closed(value, ('op', 'operator', 'left', 'right'))
            operators={'notEqual':'compare.notEqual','less':'compare.less','lessEqual':'compare.lessEqual','greaterEqual':'compare.greaterEqual'}
            if value['operator'] not in operators or operators[value['operator']] not in plan['requiredCapabilities'] or (join and 'compare.scalarJoin' not in plan['requiredCapabilities']):
                raise ValueError('Only explicitly admitted closed String comparison needs no arithmetic checks')
            comparison_caps.add(operators[value['operator']])
            if join:comparison_caps.add('compare.scalarJoin')
            field(value['left'], visible, string_only=True)
            operand(value['right'], visible)
            return
        closed(value, ('op', 'predicate'))
        if value['op'] != 'legacy':
            raise ValueError('Arithmetic/unknown predicates require independent checks')
        p = value['predicate']
        closed(p, ('op', 'left', 'right'))
        if p['op'] != 'equal':
            raise ValueError('Only string equality needs no arithmetic checks')
        equality_used=True
        if native_null and p['left'].get('type') == {'family': 'integer', 'facets': {}, 'nullable': False}:
            field(p['left'], visible)
            right = p['right']
            closed(right, ('kind', 'value', 'type', 'span'))
            if right['kind'] != 'literal' or right['value'] != '0' or right['type'] != p['left']['type']:
                raise ValueError('Only exact original mathematical Integer zero equality qualified')
            span(right['span'])
            if not any(q['value'] == '0' and q['logicalType'] == right['type'] and q['origin'] == {'kind': 'literal', 'sourceSpan': right['span']} for q in parameters):
                raise ValueError('Original exact zero parameter custody required')
            return
        field(p['left'], visible, string_only=True)
        operand(p['right'], visible)

    visible = {scan(plan['source'])}
    for join in plan['joins']:
        closed(join, ('right', 'on'))
        visible.add(scan(join['right']))
        if type(join['on']) is not list or not join['on']:
            raise ValueError('Original nonempty INNER JOIN predicate required')
        for p in join['on']:
            if not positioned_output_only:predicate(p, visible, join=True)
    for p in plan['filters']:
        if not positioned_output_only:predicate(p, visible)
    if not positioned_output_only and ({c for c in plan['requiredCapabilities'] if c.startswith('compare.')} != comparison_caps):
        raise ValueError('Exact String predicate/capability correspondence required')
    groups=[]
    for f in plan['groups']:
        key=field(f,visible,string_only=True);pair=(f['scan'],key)
        if pair in groups:raise ValueError('Original unique grouping Fields required')
        groups.append(pair)
    for f in plan['order']:
        key=field(f, visible,string_only=count_distinct)
        if count_distinct and plan['aggregate'] and (f['scan'],key) not in groups:
            raise ValueError('Distinct-count ordering requires original grouping identity')
    columns = artifact.get('columns')
    if type(columns) is not list or not plan['outputs'] or len(plan['outputs']) != len(columns):
        raise ValueError('Complete original output descriptors required')
    positioned = 'project.positionedOutputs' in plan['requiredCapabilities']
    if positioned_output_only and not positioned:
        raise ValueError('Separate output-only proof is restricted to positioned descriptors')
    if (len({o.get('name') for o in plan['outputs']}) != len(plan['outputs'])) != positioned:
        raise ValueError('Exact original repeated-output capability required')
    if count_distinct and positioned:raise ValueError('Distinct-count host requires unique original output names')
    if positioned:
        admit_positioned_outputs(artifact)
    graph_ids = set()
    for d in plan['typeGraph']:
        closed(d, ('identity', 'availability', 'kind', 'type'))
        key = identity(d['identity'])
        if key in graph_ids or key not in original_fields or d['availability'] != original_fields[key].get('nullability') or d['kind'] != 'scalar' or d['type'] != original_type(key):
            raise ValueError('Unproved or duplicate original type descriptor')
        graph_ids.add(key)
    expected_graph = {identity(o['expression']['identity']) for o in plan['outputs'] if not (count_distinct and o['expression'].get('op')=='countDistinct')}
    if native_null:
        expected_graph |= native_null_graph_ids
    if graph_ids != expected_graph:
        raise ValueError('Exact original output type graph required')
    count_used=False
    for index, (output, column) in enumerate(zip(plan['outputs'], columns), 1):
        closed(output, ('name', 'expression'))
        closed(column, ('outputName', 'position', 'sourceIdentities', 'nullable', 'representation') + (('carrierName',) if positioned else ()))
        expression=output['expression']
        if count_distinct and type(expression) is dict and expression.get('op') == 'countDistinct':
            closed(expression,('op','argument','type'))
            count_type={'family':'integer','facets':{},'nullable':False}
            logical_type(expression['type'])
            if expression['type'] != count_type:raise ValueError('Exact mathematical Integer count result required')
            field(expression['argument'],visible,string_only=True)
            closed(column['representation'],('kind','carrier','logicalType','decoder'))
            logical_type(column['representation']['logicalType'])
            if (column['outputName'] != output['name'] or type(column['position']) is not int or column['position'] != index
                    or column['nullable'] is not False or column['sourceIdentities'] != [expression['argument']['identity']]
                    or column['representation'] != {'kind':'scalar','carrier':'text','logicalType':count_type,'decoder':'exact-integer'}):
                raise ValueError('Exact original distinct-count output descriptor required')
            count_used=True
            continue
        key = field(expression, visible, typed=False)
        if count_distinct and plan['aggregate'] and (expression['scan'],key) not in groups:
            raise ValueError('Distinct-count direct outputs must be original grouping Fields')
        if column.get('outputName') != output['name'] or type(column.get('position')) is not int or column['position'] != index:
            raise ValueError('Original output order differs')
        if column.get('sourceIdentities') != [output['expression']['identity']] or column.get('nullable') is not False:
            raise ValueError('Original output lineage differs')
        descriptors = [d for d in plan['typeGraph'] if d.get('identity') == output['expression']['identity']]
        if len(descriptors) != 1:
            raise ValueError('Original output type descriptor required')
        d = descriptors[0]
        closed(d, ('identity', 'availability', 'kind', 'type'))
        if d['availability'] != original_fields[key].get('nullability') or d['kind'] != 'scalar':
            raise ValueError('Only required original scalar output admitted')
        logical_type(d['type'])
        if d['type'] != original_type(key):
            raise ValueError('Output type differs from original authored metadata')
        representation = column.get('representation')
        if native_null and key in native_null_ids:
            if ('value.nativeNull' not in plan['requiredCapabilities'] or representation.get('nativeNull') is not True or representation != {'kind': 'value', 'descriptor': d['identity'], 'nativeNull': True}):
                raise ValueError('Exact original tagged optional descriptor required')
            continue
        closed(representation, ('kind', 'carrier', 'logicalType', 'decoder'))
        decoders = {'string': 'text', 'integer': 'exact-integer', 'decimal': 'exact-decimal', 'boolean': 'boolean'}
        if representation['kind'] != 'scalar' or representation['carrier'] != 'text' or representation['logicalType'] != d['type'] or representation['decoder'] != decoders[d['type']['family']]:
            raise ValueError('Original exact output carrier differs')
    if count_distinct:
        caps=set(plan['requiredCapabilities'])
        if (plan['aggregate'] is not (count_used or bool(groups)) or bool(groups) != ('group' in caps)
                or count_used != ('aggregate.countDistinct' in caps) or plan['aggregate'] != ('aggregate' in caps)
                or in_used != ('predicate.stringIn' in caps) or bool(plan['order']) != ('order.asc' in caps)
                or bool(plan['filters']) != ('filter' in caps) or bool(plan['joins']) != ('innerJoin' in caps)
                or equality_used != ('equal' in caps)
                or (len(plan['filters'])>1 or any(len(j['on'])>1 for j in plan['joins'])) != ('and' in caps)):
            raise ValueError('Exact String set/count operation and capability correspondence required')
        if plan['aggregate'] and not count_used:raise ValueError('Only explicitly projected distinct-count grouping admitted')
        projected_groups={(o['expression']['scan'],identity(o['expression']['identity'])) for o in plan['outputs'] if o['expression'].get('op')!='countDistinct'}
        if plan['aggregate'] and set(groups)!=projected_groups:raise ValueError('Every exact group identity must be projected for this closed count host')
    if distinct:
        projected = {(o['expression']['scan'], identity(o['expression']['identity'])) for o in plan['outputs']}
        if any((f['scan'], identity(f['identity'])) not in projected for f in plan['order']):
            raise ValueError('DISTINCT order requires exact projected scan and Field identity')
        for column in columns:
            if column['representation'] != {'kind':'scalar','carrier':'text','logicalType':{'family':'string','facets':{},'nullable':False},'decoder':'text'}:
                raise ValueError('DISTINCT requires required exact String tuple cells')
    obligations = [o for o in artifact.get('obligations', []) if o.get('id') == 'ashlar.candidate.scalarIntegrity']
    if len(obligations) != 1:
        raise ValueError('Original source-integrity obligation required')
    checks = obligations[0]['parameters']['checks']
    covered = {(identity(c['record']), identity(c['field'])) for c in checks
               if c.get('publicSourceOnly') is not True and c.get('representabilityOnly') is not True}
    if not required_checks <= covered:
        raise ValueError('Every original consumed Field requires a native source-integrity check')
    if native_null:
        if native_null_ids and 'value.nativeNull' not in plan['requiredCapabilities']:
            raise ValueError('Explicit native-null capability required')
        for key in native_null_ids:
            properties = [p for r in binding['records'] for p in r['properties'] if identity(p['logical']) == key]
            if len(properties) != 1 or properties[0]['home'].get('encoding') != 'ashlar-weft-json-native-null/0.1-candidate':
                raise ValueError('Explicit original per-Field native-null encoding required')
            home = properties[0]['home']
            matched = [c for c in checks if identity(c['field']) == key]
            if not matched or any(c.get('encoding') != home['encoding'] or c.get('propertyId') != home['propertyId'] for c in matched):
                raise ValueError('Exact native-null home guard correspondence required')
            if not any(c.get('representabilityOnly') is True and c.get('failureCode') == 'WFT-CAPABILITY' for c in matched):
                raise ValueError('Missing optional representation capability guard required')
    for flag in ('publicSourceOnly', 'representabilityOnly'):
        guarded = {(identity(c['record']), identity(c['field'])) for c in checks if c.get(flag) is True}
        if not integer_checks <= guarded:
            raise ValueError('Original integer Fields require separate public source and native capacity checks')


_POSITIONED_TEXT = {
    'profile': 'weft-positioned-output/0.3.0',
    'decoding': 'exact ordered row arrays; complete output count/order and unique physical names; no logical-name dictionary',
    'lineage': 'each ordinal binds the original logicalPlan output, including scan occurrence',
    'host': 'explicit opt-in before SQL; reject unknown carrierName/obligations; buffer and preserve all cells',
}


def admit_positioned_outputs(artifact):
    """Closed descriptor correspondence, not an independent SQL semantics proof.

    Caller must first admit exact original compiler/model/source bytes and hold
    the complete publication. Native rows must subsequently use ordered cells.
    """
    plan = artifact.get('logicalPlan', {})
    columns, outputs = artifact.get('columns'), plan.get('outputs')
    if (plan.get('irVersion') != 'weft-ir/0.3.0'
            or type(columns) is not list or type(outputs) is not list or not outputs
            or len(columns) != len(outputs)
            or 'project.positionedOutputs' not in plan.get('requiredCapabilities', [])
            or len({o.get('name') for o in outputs}) == len(outputs)):
        raise ValueError('Complete explicitly positioned original outputs required')
    all_obligations=artifact.get('obligations', [])
    expected_obligations={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.arithmetic.exact','weft.output.positioned'}
    if type(all_obligations) is not list or len(all_obligations)!=4 or any(type(o) is not dict for o in all_obligations) or {o.get('id')for o in all_obligations}!=expected_obligations:
        raise ValueError('Unknown or duplicate positioned query obligation')
    obligations = [o for o in all_obligations if o.get('id') == 'weft.output.positioned']
    if len(obligations) != 1:
        raise ValueError('One original positioned obligation required')
    obligation = obligations[0]
    if (set(obligation) != {'id', 'owner', 'failureCode', 'parameters'}
            or obligation['owner'] != 'host' or obligation['failureCode'] != 'WFT-OBLIGATION'):
        raise ValueError('Original positioned host obligation required')
    names, mapping, lineage = set(), [], []
    scans = {plan['source']['occurrence']: plan['source']}
    for join in plan['joins']:
        right = join['right']
        if right['occurrence'] in scans:
            raise ValueError('Original unique scan occurrences required')
        scans[right['occurrence']] = right
    for position, (column, output) in enumerate(zip(columns, outputs), 1):
        if set(column) != {'position', 'outputName', 'carrierName', 'sourceIdentities', 'nullable', 'representation'} or set(output) != {'name', 'expression'}:
            raise ValueError('Closed original positioned descriptors required')
        expression = output['expression']
        if (set(expression) != {'op', 'scan', 'identity'} or expression['op'] != 'field'
                or expression['scan'] not in scans or type(column['position']) is not int
                or column['position'] != position or column['outputName'] != output['name']
                or column['sourceIdentities'] != [expression['identity']]):
            raise ValueError('Original ordered Field lineage and scan required')
        name = column['carrierName']
        if type(name) is not str or not name or len(name.encode('utf-8')) > 128 or '\0' in name or name in names:
            raise ValueError('Unique bounded physical carrier names required')
        names.add(name)
        mapping.append({k: column[k] for k in ('position', 'outputName', 'carrierName', 'sourceIdentities')})
        lineage.append({'position': position, 'scan': expression['scan'], 'identity': expression['identity']})
    if obligation['parameters'] != {**_POSITIONED_TEXT, 'columns': mapping}:
        raise ValueError('Exact original positioned obligation map required')
    return {'columns': mapping, 'lineage': lineage}


def admit_positioned_cells(artifact, result):
    """Verify actual native schema and complete ordered cells, never a dict."""
    disposition = admit_positioned_outputs(artifact)
    if type(result) is not dict or set(result) != {'schema', 'rows'}:
        raise ValueError('Explicit original native ordered result required')
    schema, rows = result['schema'], result['rows']
    expected = [[c['carrierName'], 'STRING'] for c in artifact['columns']]
    if type(schema) is not list or schema != expected:
        raise ValueError('Native positioned column count/order/names/types differ')
    if type(rows) is not list or any(type(row) is not list or len(row) != len(expected)
            or any(type(cell) is not str for cell in row) for row in rows):
        raise ValueError('Complete required exact String carrier cell arrays required')
    return {'schema': schema, 'rows': rows, **disposition}

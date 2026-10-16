"""Pure original17 GQL plans; compilation establishes no engine/source authority."""
import json
import re
from .pack_scenario_queries import plans
from .graphframes_pack_scenarios import WITNESSES


def gql_plan(profile, name):
    """Return bounded original relational GQL and exact ordered output names."""
    if type(profile) is not dict or profile.get('pack') not in ('archaeology', 'ecology'):
        raise ValueError('Closed original scenario pack required')
    pack = profile['pack']
    cases = plans(pack)
    if type(name) is not str or name not in cases:
        raise ValueError('Known original authored scenario required')
    scans, joins, predicates, outputs, group = cases[name]
    records = dict(s.split(':') for s in scans)
    fields = {}
    for f in profile['fields']:
        identity = f['record_identity']
        if (type(identity) is not list or len(identity) != 3
                or any(type(v) is not str for v in identity)):
            raise ValueError('Exact original record identity required')
        key = (identity[2], f['original_field']['name'])
        if key in fields:
            raise ValueError('Unambiguous original field required')
        for kind, prefix in [('lexical_column', 'field_'), ('presence_column', 'presence_'), ('signed64_column', 'integer_')]:
            value = f[kind]
            if value is None and kind == 'signed64_column':
                continue
            if type(value) is not str or not re.fullmatch(prefix + r'[0-9]+', value):
                raise ValueError('Named finite scenario column required')
        fields[key] = f
    label = 'Scenario' + pack.title() + 'Node'

    def literal(value):
        # GQL double-quoted string literal; JSON escapes retain original text.
        return json.dumps(value, ensure_ascii=False)

    def field(token):
        alias, member = token.split('.')
        f = fields[(records[alias], member)]
        column = f['signed64_column'] if member in ('earliest', 'latest', 'count') else f['lexical_column']
        if column is None:
            raise ValueError('Named finite scalar capacity required')
        return alias + '.' + column

    def identity(alias):
        candidates = {tuple(f['record_identity']) for (record, _), f in fields.items() if record == records[alias]}
        if len(candidates) != 1:
            raise ValueError('Unambiguous original record identity required')
        document, module, element = candidates.pop()
        return alias + '.original_type = ' + literal(json.dumps(
            {'document': document, 'module': module, 'element': element},
            sort_keys=True, separators=(',', ':')))

    def condition(text):
        if text.startswith('NOT_NULL:'):
            return field(text[9:]) + ' IS NOT NULL'
        if text.startswith('NULL:'):
            return field(text[5:]) + ' IS NULL'
        if text.startswith('IN:'):
            _, token, *values = text.split(':')
            return field(token) + ' IN [' + ','.join(literal(v) for v in values) + ']'
        left, operator, right = re.fullmatch(r'(.+?)(<=|<>|=|<)(.+)', text).groups()
        rhs = literal(right[7:]) if right.startswith('STRING:') else right[8:] if right.startswith('INTEGER:') else field(right)
        return field(left) + ' ' + operator + ' ' + rhs

    optional = {j[9:].split('.')[0] for j in joins if j.startswith('OPTIONAL:')}
    required = [s.split(':')[0] for s in scans if s.split(':')[0] not in optional]
    query = 'MATCH ' + ','.join('(' + a + ':' + label + ')' for a in required)
    where = [identity(a) for a in required]
    where += [condition(j) for j in joins if not j.startswith('OPTIONAL:')]
    where += [condition(p) for p in predicates]
    query += ' WHERE ' + ' AND '.join(where)
    for scan in scans:
        alias = scan.split(':')[0]
        if alias in optional:
            checks = [identity(alias)] + [condition(j[9:]) for j in joins if j.startswith('OPTIONAL:' + alias + '.')]
            query += ' OPTIONAL MATCH (' + alias + ':' + label + ') WHERE ' + ' AND '.join(checks)
    names = ['result_' + str(i) for i in range(len(outputs))]
    if group:
        query += ' LET ashlarScenarioGroup = ' + field(group[0])
        selected = ['ashlarScenarioGroup AS result_0', 'count(DISTINCT ' + field(outputs[1][15:]) + ') AS result_1']
    else:
        selected = [field(token) + ' AS ' + n for token, n in zip(outputs, names)]
    if name == 'evidence-links':
        witnesses = ['i.original_key', 'e.original_key', field('i.id'), 'p.original_key', 'f.original_key',
                     'p.' + fields[('pottery_results', 'form')]['presence_column'],
                     'f.' + fields[('fauna_results', 'taxon')]['presence_column']]
        selected += [value + ' AS ' + n for value, n in zip(witnesses, WITNESSES)]
        names += WITNESSES
    query += ' RETURN ' + ('DISTINCT ' if name == 'comparability' else '') + ','.join(selected)
    if group:
        query += ' GROUP BY ashlarScenarioGroup'
        if group[1] is not None:
            query += ' NEXT FILTER result_1 > ' + str(group[1]) + ' RETURN ' + ','.join(names)
    if name == 'connected-measurements':
        query += ' ORDER BY result_0'
    if name == 'evidence-links':
        query += ' ORDER BY interpretation_order_token'
    return query, names


def validate_gql_case(profile,name,result):
    """Compare complete decoded native cells without host joins or row repair."""
    import hashlib
    from .fabric_gql import FabricGqlResult
    from .graphframes_pack_scenarios import validate_case
    query,names=gql_plan(profile,name)
    expected_digest=hashlib.sha256(json.dumps({'query':query},ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
    if type(result)is not FabricGqlResult or result.request_sha256!=expected_digest:
        raise ValueError('Original scenario query result required')
    if [n for n,_ in result.columns]!=names:raise ValueError('Exact ordered GQL columns required')
    schema=[]
    for column,kind in result.columns:
        if kind not in ('STRING','INT64','UINT64'):raise ValueError('Original scenario scalar subset required')
        schema.append({'name':column,'type':'string'if kind=='STRING'else'bigint'})
    return validate_case(profile,name,schema,[list(row)for row in result.rows])


def execute_gql_case(profile,name,binding,*,transport,policy,context,limits):
    """Use the actual GQL authority boundary, then the independent pack oracle."""
    from .fabric_gql import execute_gql
    query,_=gql_plan(profile,name)
    result=execute_gql(binding,query,transport=transport,policy=policy,context=context,**limits)
    validate_gql_case(profile,name,result)
    return result

"""Provisional, bounded path execution in one caller-owned publication hold.

This module creates no engine, installs no compiler, persists no report and grants
no source authority. The outer composition must close its reader and stop Spark
before publishing the provisional evidence returned here.
"""
from dataclasses import dataclass
from contextlib import contextmanager
from copy import deepcopy
from collections import Counter
from decimal import Decimal
import hashlib
import json
import re
from typing import Callable, Optional

from ashlar.weft_decode import decode_exact_scalar, ExactScalar
from ashlar.weft_path_decode import PathDecodeConfig, decode_related_paths
from ashlar.weft_related_keys_decode import RelatedKeysDecodeConfig, decode_related_keys
from .count_star_admission import CountStarAdmissionConfig, admit_count_star_artifact
from .path_capture import PathCaptureConfig, capture_string_frame
from .lifecycle import owned_context


class CountStarExecutionError(ValueError):
    """Payload-free refusal; no partial result is returned."""


@dataclass(frozen=True)
class CountStarExecutionConfig:
    admission: CountStarAdmissionConfig
    capture: PathCaptureConfig
    decoder: PathDecodeConfig
    public_source: Optional[Callable]
    public_source_revision: Optional[str]
    native_observer: Optional[Callable] = None

    def __post_init__(self):
        if (type(self.admission) is not CountStarAdmissionConfig
                or type(self.capture) is not PathCaptureConfig
                or type(self.decoder) is not PathDecodeConfig):
            raise CountStarExecutionError('Explicit typed execution settings required')
        if self.public_source is not None and (not callable(self.public_source)
                or type(self.public_source_revision) is not str
                or not self.public_source_revision):
            raise CountStarExecutionError('Explicit source producer and revision required')
        if self.native_observer is not None and not callable(self.native_observer):
            raise CountStarExecutionError('Callable native observation port required')


def _plain(value):
    if hasattr(value, 'items'):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(v) for v in value]
    return value


def _oracle_snapshot(value, maximum):
    remaining = maximum
    def copy(item, depth=0):
        nonlocal remaining
        _require(depth <= 64, 'Independent oracle nesting capacity exceeded')
        remaining -= 1
        _require(remaining >= 0, 'Independent oracle retained capacity exceeded')
        if type(item) is str:
            for char in item:
                n = ord(char)
                _require(not 0xD800 <= n <= 0xDFFF, 'Independent oracle UTF8 refused')
                remaining -= 1 if n < 128 else 2 if n < 2048 else 3 if n < 65536 else 4
                _require(remaining >= 0, 'Independent oracle retained capacity exceeded')
            return item
        if item is None or type(item) is bool: return item
        if type(item) is int:
            _require(abs(item) < 10**20, 'Independent oracle integer capacity exceeded')
            return item
        if type(item) is list: return [copy(v, depth+1) for v in item]
        if type(item) is dict:
            result = {}
            for key, child in item.items():
                _require(type(key) is str, 'Independent oracle named members required')
                result[copy(key, depth+1)] = copy(child, depth+1)
            return result
        raise CountStarExecutionError('Independent oracle closed values required')
    return copy(value)


def _exact(value):
    return json.dumps(_plain(value), sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def _require(condition, message):
    if not condition:
        raise CountStarExecutionError(message)


class HeldFrameAdapter:
    """Only the original active provider grants this frame execution permission."""
    def __init__(self, provider, context, config: PathCaptureConfig):
        self.provider, self.context, self.config = provider, context, config
        self.retained_bytes = 0

    def capture(self, sql: str, slots: dict) -> dict:
        _require(self.provider.active is True and self.provider.context is self.context,
                 'Original active publication context required')
        remaining = self.config.maximum_total_cell_bytes - self.retained_bytes
        _require(remaining > 0, 'Complete execution payload capacity exceeded')
        bounded = PathCaptureConfig(self.config.maximum_rows,
            min(self.config.maximum_cell_bytes, remaining), remaining)
        result = capture_string_frame(self.provider.driver.transport.spark.sql(sql, args=slots),
                                      config=bounded)
        self.retained_bytes += sum(len(value.encode('utf8')) for row in result['rows']
                                   for value in row if value is not None)
        return result


def _zero(result):
    _require(result['rows'] == [['0']] and len(result['native_schema']) == 1,
             'Original owning native guard refused')


def _schema(provider, context, resolved, table, required):
    observed = provider.native_table_schema(table, resolved, context)
    _require(type(observed) is dict and set(observed) == {'table', 'schema', 'nativeTypes'}
             and observed['table'] == table, 'Native schema pin correspondence differs')
    schema = observed['schema']
    _require(type(schema) is dict and set(schema) == {'type', 'fields'}
             and schema['type'] == 'struct' and type(schema['fields']) is list,
             'Complete native Struct metadata required')
    fields = schema['fields']
    _require(all(type(f) is dict and set(f) == {'name', 'type', 'nullable', 'metadata'}
                 and type(f['name']) is str and bool(f['name'])
                 and type(f['nullable']) is bool and type(f['metadata']) is dict for f in fields)
             and len({f['name'] for f in fields}) == len(fields),
             'Complete native field metadata required')
    types = observed['nativeTypes']
    _require(type(types) is list and len(types) == len(fields)
             and all(type(pair) is list and len(pair) == 2
                     and all(type(v) is str for v in pair) for pair in types)
             and [pair[0] for pair in types] == [f['name'] for f in fields],
             'Complete ordered native field types required')
    field_map = {f['name']: f for f in fields}
    type_map = dict(types)
    for name, native in required.items():
        _require(name in field_map and type_map.get(name) == native
                 and field_map[name]['type'] == {'STRING': 'string', 'BIGINT': 'long'}[native],
                 'Owning native column types differ')
    return deepcopy(observed)


def _scalar(column, expression, value, arithmetic, type_graph=()):
    rep = dict(column['representation'])
    rep.pop('pathTarget', None)
    if rep.get('kind') == 'value':
        _require(value is not None, 'Tagged scalar presence required')
        def pairs(items):
            result = {}
            for key, item in items:
                _require(key not in result, 'Duplicate presence member')
                result[key] = item
            return result
        def numeric(_):
            raise CountStarExecutionError('Native numeric JSON atom refused')
        try:
            item = json.loads(value, object_pairs_hook=pairs, parse_int=numeric,
                              parse_float=numeric, parse_constant=numeric)
        except (ValueError, TypeError):
            raise CountStarExecutionError('Closed scalar presence required') from None
        descriptors = [d for d in type_graph if d['identity'] == rep['descriptor']]
        _require(len(descriptors) == 1 and descriptors[0]['kind'] == 'scalar'
                 and rep.get('nativeNull') is False, 'Original scalar value descriptor required')
        descriptor = descriptors[0]
        if item == {'state': 'absent'}:
            _require('outerJoin' in rep or descriptor['availability'] == 'absent-allowed',
                     'Unadmitted scalar absence')
            return item
        if item == {'state': 'null'}:
            _require(descriptor['type']['nullable'] is True, 'Unadmitted source null')
            return item
        _require(type(item) is dict and set(item) == {'state', 'value'}
                 and item['state'] == 'value', 'Closed scalar presence required')
        # The emitted presence descriptor owns the scalar representation.
        logical = dict(descriptor['type']); logical['nullable'] = False
        decoder = {'string': 'text', 'boolean': 'boolean', 'integer': 'exact-integer', 'decimal': 'exact-decimal'}.get(logical['family'])
        _require(decoder is not None, 'Selected scalar value family required')
        scalar = {'kind': 'scalar', 'carrier': 'text', 'decoder': decoder, 'logicalType': logical}
        return {'state': 'value', 'value': _scalar({'representation': scalar}, expression, item['value'], arithmetic, type_graph)}
    _require(rep.get('kind') == 'scalar', 'Selected scalar output role required')
    count = expression['op'] in ('count', 'countDistinct', 'countDistinctPathTargets')
    logical = rep.get('logicalType', {})
    facets = logical.get('facets')
    if not count and arithmetic and logical.get('nullable') is False:
        if logical.get('family') == 'integer' and facets == {}:
            _require(rep == {'kind': 'scalar', 'carrier': 'text', 'decoder': 'exact-integer',
                             'logicalType': logical} and type(value) is str
                     and re.fullmatch('-?(0|[1-9][0-9]*)', value)
                     and len(value.lstrip('-')) <= 38, 'Finite exact arithmetic Integer required')
            return ExactScalar(int(value), value)
        if logical.get('family') == 'decimal' and type(facets) is dict and set(facets) == {'scale'}:
            scale = facets['scale']
            _require(rep['carrier'] == 'text' and rep['decoder'] == 'exact-decimal'
                     and type(scale) is int and 0 <= scale <= 38 and type(value) is str
                     and re.fullmatch('-?[0-9]+(?:\.[0-9]+)?', value)
                     and len(value.replace('-', '').replace('.', '')) <= 38
                     and len(value.split('.')[1] if '.' in value else '') <= scale,
                     'Finite exact arithmetic Decimal required')
            return ExactScalar(Decimal(value), value)
    return decode_exact_scalar(rep, value, count=count)


def _source_receipt(port, revision, request, artifact, checks, maximum):
    receipt = port(deepcopy(request), deepcopy(artifact), deepcopy(checks))
    _require(type(receipt) is dict and set(receipt) == {
        'originalRequestText', 'originalReceiptText', 'receiptSha256'},
        'Complete original public source receipt required')
    raw = receipt['originalReceiptText']
    request_raw = receipt['originalRequestText']
    for text in (raw, request_raw):
        _require(type(text) is str and len(text) <= maximum, 'Bounded original source text required')
        size = 0
        for char in text:
            n = ord(char)
            _require(not 0xD800 <= n <= 0xDFFF, 'Original source UTF8 scalar required')
            size += 1 if n < 128 else 2 if n < 2048 else 3 if n < 65536 else 4
            _require(size <= maximum, 'Bounded original source UTF8 bytes required')
    _require(type(raw) is str and type(request_raw) is str
             and len(raw.encode('utf8')) <= maximum and len(request_raw.encode('utf8')) <= maximum
             and hashlib.sha256(raw.encode('utf8')).hexdigest() == receipt['receiptSha256'],
             'Bounded original source receipt bytes required')
    try:
        def pairs(items):
            result = {}
            for key, value in items:
                _require(key not in result, 'Duplicate public source receipt member')
                result[key] = value
            return result
        def integer(token):
            _require(len(token.lstrip('-')) <= 20, 'Bounded public receipt integer required')
            return int(token)
        def floating(token):
            raise CountStarExecutionError('Unadmitted public receipt numeric atom')
        original = json.loads(request_raw, object_pairs_hook=pairs, parse_int=integer, parse_float=floating, parse_constant=floating)
        result = json.loads(raw, object_pairs_hook=pairs, parse_int=integer, parse_float=floating, parse_constant=floating)
    except (ValueError, UnicodeError):
        raise CountStarExecutionError('Public source receipt encoding refused') from None
    expected = {'sourceText': request['modules'][0]['documentJson'],
                'modelPins': artifact['modelPins'], 'bindingSha256': artifact['bindingSha256'],
                'checks': checks}
    _require(original == expected and result.get('originalRequestText') == request_raw
             and result.get('umfRevision') == revision and result.get('admitted') is True,
             'Public source receipt correspondence refused')
    return {key: receipt[key] for key in (
        'originalRequestText', 'originalReceiptText', 'receiptSha256')}


@contextmanager
def _preserving_interval(provider, context):
    """Apply the shared owned lifecycle cancellation precedence to held reads."""
    with owned_context(provider.interval(context)):
        yield


def execute_commerce_count_star(opened, request: dict, artifact: dict, trusted_recompiled: dict,
                          *, config: CountStarExecutionConfig, original_oracle: Callable) -> dict:
    """Return provisional evidence only after all held checks and interval closure."""
    _require(type(config) is CountStarExecutionConfig and callable(original_oracle),
             'Explicit execution configuration and independent oracle required')
    admitted = admit_count_star_artifact(request, artifact, trusted_recompiled, config=config.admission)
    request, artifact, binding = map(_plain, (admitted.request, admitted.artifact, admitted.binding))
    obligations = _plain(admitted.obligations)
    source_checks = [c for o in obligations for c in o['parameters'].get('checks', [])
                     if c.get('publicSourceOnly') is True]
    _require(not source_checks or config.public_source is not None,
             'Actual owning public source port required before native callbacks')
    model, graph = bytes(opened.model), bytes(opened.graph)
    _require(any(module['documentJson'].encode('utf8') == model for module in request['modules']),
             'Opened original source model differs from request')
    expected = _oracle_snapshot(original_oracle(model, graph, deepcopy(request)),
                                config.admission.maximum_artifact_bytes)
    _require(type(expected) is dict and set(expected) == {'rows', 'witnesses', 'scope'}
             and type(expected['rows']) is list, 'Named independent original oracle required')
    opening_files = dict(opened.native_files())
    _require(opening_files == opened.original_native_files, 'Original native source files differ')
    provider, context = opened.provider, opened.context
    slots = {'p' + str(p['position']): p['value'] for p in artifact['parameters']}
    evidence = {'native_schemas': [], 'guards': [], 'public_source': None}
    adapter = HeldFrameAdapter(provider, context, config.capture)
    completed = False
    with _preserving_interval(provider, context):
        provider.runtime(context)
        resolved = provider.resolve(context)
        provider.admit_binding(binding, resolved, context)
        # Every publication table is observed, including empty base tables.
        for index, table in enumerate(binding['publication']['tables']):
            kinds = {record['kind'] for record in binding['records'] if record['table'] == index}
            required = {}
            if kinds:
                required = {'id': 'BIGINT', 'source_system': 'STRING',
                            'schema_revision': 'STRING', 'props_json': 'STRING'}
                if 'object' in kinds: required['type_id'] = 'BIGINT'
                if 'edge' in kinds: required.update(rel_type_id='BIGINT', source_id='BIGINT', target_id='BIGINT')
            evidence['native_schemas'].append(_schema(provider, context, resolved, table, required))
        for declaration in _plain(admitted.edge_schemas):
            evidence['native_schemas'].append(_schema(provider, context, resolved,
                declaration['table'], {declaration['identityColumn']: declaration['nativeType'],
                    'source_system': 'STRING', 'rel_type_id': 'BIGINT',
                    'source_id': 'BIGINT', 'target_id': 'BIGINT'}))
        source_rows = []
        source_ids = {'ashlar.candidate.scalarIntegrity',
                      'ashlar.candidate.relationshipIntegrity', 'outerJoin.matchIntegrity'}
        # Source integrity precedes dependent arithmetic/capacity phases. Preserve
        # each owning check array's original order; do not derive or rewrite SQL.
        for source_phase in (True, False):
            for obligation in obligations:
                if (obligation['id'] in source_ids) is not source_phase:
                    continue
                params = obligation['parameters']
                if obligation['id'] == 'outerJoin.matchIntegrity':
                    for declaration in params['scans']:
                        evidence['native_schemas'].append(_schema(provider, context, resolved,
                            declaration['table'], {declaration['identityColumn']: declaration['nativeType']}))
                checks = params.get('checks', params.get('scans', []))
                for check in checks:
                    captured = adapter.capture(check['sql'], slots)
                    if check.get('publicSourceOnly') is True:
                        names = [s[0] for s in captured['schema']]
                        _require(len(names) == len(set(names)), 'Distinct source projection names required')
                        source_rows.append({'check': check, 'rows': [dict(zip(names, row)) for row in captured['rows']]})
                    else:
                        _zero(captured)
                    evidence['guards'].append({'obligation': obligation['id'], 'check': check, 'native': captured})
            if source_phase and source_rows:
                evidence['public_source'] = _source_receipt(config.public_source,
                    config.public_source_revision, request, artifact, source_rows,
                    config.admission.maximum_artifact_bytes)
        captured = adapter.capture(artifact['sql'], slots)
        if config.native_observer is not None:
            config.native_observer(_oracle_snapshot(captured,
                config.capture.maximum_total_cell_bytes + config.admission.maximum_artifact_bytes))
        names = [c.get('carrierName', c['outputName']) for c in artifact['columns']]
        _require([s[0] for s in captured['schema']] == names,
                 'Actual ordered output names differ')
        arithmetic = any(o['id'] == 'ashlar.arithmetic.exact' for o in obligations)
        scalar_checks = [c for o in obligations if o['id'] == 'ashlar.candidate.scalarIntegrity'
                         for c in o['parameters']['checks']]
        decoded = []
        for row in captured['rows']:
            values = []
            for column, output, value in zip(artifact['columns'], artifact['logicalPlan']['outputs'], row):
                rep = column['representation']
                if rep['kind'] == 'relatedPaths':
                    _require(value is not None, 'Tagged path output cannot be native NULL')
                    values.append(decode_related_paths(rep, value, model_pins=artifact['modelPins'], config=config.decoder))
                elif rep['kind'] == 'relatedKeys':
                    _require(value is not None, 'Key collection cannot be native NULL')
                    values.append(decode_related_keys(rep, value,
                        model_pins=artifact['modelPins'],
                        config=RelatedKeysDecodeConfig(config.decoder.maximum_cell_bytes)))
                else:
                    expression = output['expression']
                    numeric_admitted = arithmetic and expression['op'] in ('arithmetic', 'sum')
                    if expression['op'] == 'field':
                        identity = expression['identity']
                        scans = [artifact['logicalPlan']['source']] + [j['right'] for j in artifact['logicalPlan']['joins']]
                        records = [scan['record'] for scan in scans if scan['occurrence'] == expression['scan']]
                        field_checks = [c for c in scalar_checks if c.get('field') == identity
                                        and len(records) == 1 and c.get('record') == records[0]]
                        logical = rep.get('logicalType')
                        if rep['kind'] == 'value':
                            descriptors = [d for d in artifact['logicalPlan']['typeGraph'] if d['identity'] == rep['descriptor']]
                            logical = descriptors[0].get('type') if len(descriptors) == 1 else None
                        numeric_admitted = (evidence['public_source'] is not None
                            and any(c.get('representabilityOnly') is True for c in field_checks)
                            and any(c.get('publicSourceOnly') is True and c.get('logicalType') == logical for c in field_checks))
                    values.append(_scalar(column, expression, value, numeric_admitted, artifact['logicalPlan']['typeGraph']))
            decoded.append(values)
        # Compare lexical native cells; decoding never repairs bags or ordering.
        if artifact['logicalPlan']['order']:
            _require(captured['rows'] == expected['rows'], 'Original ordered native result differs')
        else:
            _require(Counter(_exact(r) for r in captured['rows']) == Counter(_exact(r) for r in expected['rows']),
                     'Original native result bag differs')
        provider.runtime(context)
        closing = provider.resolve(context)
        provider.admit_binding(binding, closing, context)
        closing_files = dict(opened.native_files())
        _require(resolved == closing and closing_files == opening_files
                 and bytes(opened.model) == model and bytes(opened.graph) == graph,
                 'Original closing publication or source differs')
        evidence.update(native_result=captured, decoded=decoded, oracle=expected,
                        opening_native_files=opening_files, closing_native_files=closing_files)
        completed = True
    _require(completed, 'Complete held execution body required')
    evidence['interval'] = provider.closed_interval_custody(context)
    evidence['qualification'] = 'Provisional held local evidence; outer reader and Spark cleanup required before persistence.'
    return evidence

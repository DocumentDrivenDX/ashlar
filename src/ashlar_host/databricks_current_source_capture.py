# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded local Spark/Delta current-value observations, never native authority.

The caller owns genuine Delta UUID/schema/caller/policy correspondence, held
publication/current-cut custody and independent closing. Count and capture are
separate observations of the same explicitly pinned Delta version.
"""
from __future__ import annotations
from dataclasses import dataclass
import json
from .path_capture import PathCaptureConfig, capture_string_frame


@dataclass(frozen=True)
class CurrentSourceCapture:
    original_binding: str
    scans_json: str


def _identifier(value):
    if type(value) is not str or not value or len(value.encode()) > 255 or '\0' in value:
        raise ValueError('Bounded exact physical identifier required')
    return '`' + value.replace('`', '``') + '`'


def capture_current_query_sources(spark, original_binding, *, config: PathCaptureConfig, engine='databricks'):
    """Capture every eligible source row, not merely returned contributors."""
    if type(engine) is not str or engine not in ('databricks', 'spark'):
        raise ValueError('Explicit source capture engine required')
    if (type(config) is not PathCaptureConfig or config.maximum_rows > 10_000
            or config.maximum_cell_bytes > 65_536 or config.maximum_total_cell_bytes > 16_777_216):
        raise ValueError('Explicit bounded source capture configuration required')
    if type(original_binding) is not str or len(original_binding.encode()) > 4_000_000:
        raise ValueError('Bounded original binding text required')
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value: raise ValueError('Duplicate original binding member')
            value[key] = item
        return value
    def reject(_): raise ValueError('Exact binding JSON required')
    binding = json.loads(original_binding, object_pairs_hook=pairs,
        parse_constant=reject, parse_float=reject)
    if (type(binding) is not dict or binding.get('version') not in
            (('weft.databricks.current-values/0.1-candidate', 'weft.databricks.current-values/0.2-candidate')
             if engine == 'databricks' else ('weft.spark.current-values/0.1-candidate',))
            or type(binding.get('scans')) is not list or not 1 <= len(binding['scans']) <= 16
            or type(binding.get('values')) is not list or len(binding['values']) > 4096):
        raise ValueError('Original current-value binding required')
    names = [scan['scan'] for scan in binding['scans']]
    if len(set(names)) != len(names) or any(type(x) is not str or not x for x in names):
        raise ValueError('Distinct original scan occurrences required')
    if any(value.get('scan') not in names for value in binding['values']):
        raise ValueError('Unknown value scan occurrence')
    output = []; total_rows = total_bytes = total_cells = 0
    for scan in binding['scans']:
        relation = scan['relation']; version = relation['version']
        if (type(version) is not str or not version.isascii() or not version.isdecimal()
                or len(version) > 19 or str(int(version)) != version or int(version) > 2**63-1):
            raise ValueError('Exact pinned Delta version required')
        table = '.'.join(_identifier(relation[key]) for key in ('catalog', 'schema', 'table'))
        key = relation['rowKeyColumn']; eligible = relation['eligibleColumn']
        columns = list(dict.fromkeys(value['column'] for value in binding['values'] if value['scan'] == scan['scan']))
        for name in [key, eligible, *columns]: _identifier(name)
        source = spark.read.format('delta').option('versionAsOf', version).table(table)
        fields = source.schema.fields
        fieldmap = {field.name: field.dataType.simpleString().upper() for field in fields}
        if len({field.name.casefold() for field in fields}) != len(fields):
            raise ValueError('Ambiguous native source column names')
        if fieldmap.get(key) != 'STRING' or fieldmap.get(eligible) != 'BOOLEAN':
            raise ValueError('Native String key and Boolean eligibility required')
        if any(fieldmap.get(column) not in ('STRING', 'BIGINT') for column in columns):
            raise ValueError('Exact native String/signed64 values required')
        selected = source.filter(_identifier(eligible) + ' = TRUE')
        count = selected.count()
        if type(count) is not int or count < 0 or total_rows + count > config.maximum_rows:
            raise ValueError('Complete eligible source population capacity exceeded')
        total_cells += count * (1 + len(columns))
        if total_cells > 100_000:
            raise ValueError('Complete source structural cell capacity exceeded')
        expressions = [_identifier(key) + ' AS `__weft_key`'] + [
            'CAST(' + _identifier(column) + ' AS STRING) AS `__weft_value_' + str(i) + '`'
            for i, column in enumerate(columns)]
        remaining_rows = max(1, config.maximum_rows - total_rows)
        remaining_bytes = max(1, config.maximum_total_cell_bytes - total_bytes)
        remaining = PathCaptureConfig(remaining_rows,
            min(config.maximum_cell_bytes, remaining_bytes), remaining_bytes)
        captured = capture_string_frame(selected.selectExpr(*expressions).limit(remaining_rows + 1), config=remaining)
        if len(captured['rows']) != count:
            raise ValueError('Complete eligible count and captured population differ')
        rows = []; keys = set()
        for row in captured['rows']:
            token = row[0]
            if type(token) is not str or not token or len(token.encode()) > 256 or token in keys:
                raise ValueError('Distinct exact bounded source keys required')
            keys.add(token); cells = {}
            for column, value in zip(columns, row[1:]):
                if type(value) is not str: raise ValueError('Selected native source NULL refused')
                if fieldmap[column] == 'BIGINT':
                    if len(value) > 20 or str(int(value)) != value or not -(2**63) <= int(value) < 2**63:
                        raise ValueError('Exact native signed64 token required')
                cells[column] = {'family': 'integer' if fieldmap[column] == 'BIGINT' else 'string', 'value': value}
            total_bytes += sum(len(value.encode()) for value in row)
            if total_bytes > config.maximum_total_cell_bytes:
                raise ValueError('Complete source payload capacity exceeded')
            rows.append({'key': token, 'cells': cells})
        total_rows += count
        output.append({'scan': scan['scan'], 'expectedRowCount': str(count), 'rows': rows})
    scans_json = json.dumps(output, ensure_ascii=False, separators=(',', ':'))
    if len(scans_json.encode()) > 16_777_216:
        raise ValueError('Complete source frame capacity exceeded')
    return CurrentSourceCapture(original_binding, scans_json)

"""Bounded Fabric beta GQL reads on explicitly supplied publication/native ports.

HTTP authentication and graph/release authority belong to the supplied owners.
Supported scalar projection: STRING, BOOL, INT64, UINT64 and null. Raw responses
retain extension content; other value types refuse rather than lose meaning.
Wire contract: https://learn.microsoft.com/en-us/fabric/graph/gql-query-api
"""
from dataclasses import dataclass
import hashlib
import json
import re
import time
from urllib.parse import quote
from .lifecycle import owned_context


class FabricGqlError(ValueError):
    pass


@dataclass(frozen=True)
class FabricGraphBinding:
    workspace_id: str
    graph_model_id: str
    release_sha256: str

    def __post_init__(self):
        for value in (self.workspace_id, self.graph_model_id):
            if type(value) is not str or not re.fullmatch(
                    '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', value):
                raise FabricGqlError('Explicit canonical Fabric graph identifiers required')
        if type(self.release_sha256) is not str or not re.fullmatch('[0-9a-f]{64}', self.release_sha256):
            raise FabricGqlError('Exact independently admitted original release required')


@dataclass(frozen=True)
class FabricGqlResult:
    request_sha256: str
    responses: tuple
    columns: tuple
    rows: tuple


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise FabricGqlError('Duplicate native JSON member')
        value[key] = item
    return value


def _integer(value):
    if (len(value) > (21 if value.startswith('-') else 20)
            or not re.fullmatch('0|-?[1-9][0-9]*', value)):
        raise FabricGqlError('Bounded canonical native integer required')
    return int(value)


def _scalar(kind, value):
    if value is None:
        return None
    if kind == 'STRING' and type(value) is str:
        return value
    if kind == 'BOOL' and type(value) is bool:
        return value
    if kind in ('INT64', 'UINT64'):
        if type(value) is int:
            number = value
        elif type(value) is str:
            number = _integer(value)
        else:
            raise FabricGqlError('Exact native integer encoding required')
        lower, upper = (-(2 ** 63), 2 ** 63 - 1) if kind == 'INT64' else (0, 2 ** 64 - 1)
        if lower <= number <= upper:
            return number
    raise FabricGqlError('Unsupported or invalid native scalar; raw observation retained by transport')


def execute_gql(binding, query, *, transport, policy, context,
                maximum_response_bytes=1048576, maximum_total_bytes=8388608,
                maximum_rows=1000, maximum_requests=32, deadline_seconds=60):
    """Hold independent graph/publication authority through complete native read.

    transport.post(url, original_body, context, timeout_seconds) returns
    (HTTP status, raw bytes) and must retain failed/partial observations itself.
    policy.hold(binding, context) and policy.admit(binding, query, context)
    own actual immutable graph/release correspondence, ordinary authentication,
    current native access and cleanup. A binding or hash grants no authority.
    """
    if type(binding) is not FabricGraphBinding:
        raise FabricGqlError('Exact typed Fabric binding required')
    binding.__post_init__()
    if type(query) is not str or not 0 < len(query.encode('utf-8')) <= 65536:
        raise FabricGqlError('Bounded original GQL query required')
    for value, limit in ((maximum_response_bytes, 16777216), (maximum_total_bytes, 67108864),
                         (maximum_rows, 10000), (maximum_requests, 128), (deadline_seconds, 60)):
        if type(value) is not int or not 0 < value <= limit:
            raise FabricGqlError('Explicit finite query capacity required')
    if (not callable(getattr(transport, 'post', None))
            or any(not callable(getattr(policy, name, None)) for name in ('hold', 'admit'))):
        raise FabricGqlError('Mandatory ordinary transport and independent native authority required')
    body = json.dumps({'query': query}, ensure_ascii=False, separators=(',', ':')).encode()
    endpoint = ('https://api.fabric.microsoft.com/v1/workspaces/' + binding.workspace_id
                + '/graphModels/' + binding.graph_model_id + '/executeQuery?beta=true')
    responses = []; total = 0; result = None
    expires = time.monotonic() + deadline_seconds
    with owned_context(policy.hold(binding, context)) as permit:
        if permit is not None:
            raise FabricGqlError('Native hold admission incomplete')
        url = endpoint
        for _ in range(maximum_requests):
            if policy.admit(binding, query, context) is not None:
                raise FabricGqlError('Current original graph/publication admission incomplete')
            remaining = expires - time.monotonic()
            if remaining <= 0:
                raise FabricGqlError('Native query deadline exceeded')
            reply = transport.post(url, body, context, min(15, remaining))
            if type(reply) is not tuple or len(reply) != 2:
                raise FabricGqlError('Exact ordinary HTTP observation required')
            status, raw = reply
            if (type(status) is not int or status != 200 or type(raw) is not bytes
                    or len(raw) > maximum_response_bytes):
                raise FabricGqlError('Native HTTP refusal or response capacity exceeded')
            total += len(raw)
            if total > maximum_total_bytes:
                raise FabricGqlError('Complete native response capacity exceeded')
            responses.append(raw)
            value = json.loads(raw.decode('utf-8'), object_pairs_hook=_object,
                parse_int=_integer,
                parse_constant=lambda item: (_ for _ in ()).throw(FabricGqlError('Nonfinite native JSON')))
            if type(value) is not dict or type(value.get('status')) is not dict:
                raise FabricGqlError('Exact native execution status required')
            native_status = value['status']
            if (native_status.get('code') not in ('00000', '02000')
                    or native_status.get('cause') is not None or value.get('additionalStatuses', []) != []):
                raise FabricGqlError('Native warning, truncation or incomplete execution refuses results')
            table = value.get('result')
            if type(table) is not dict or table.get('kind') != 'TABLE':
                raise FabricGqlError('Complete native table required')
            columns, rows = table.get('columns'), table.get('data')
            if type(columns) is not list or type(rows) is not list or len(rows) > maximum_rows:
                raise FabricGqlError('Bounded complete native table required')
            token = table.get('nextPage')
            if token is not None:
                if (type(token) is not str or not 0 < len(token.encode()) <= 8192
                        or rows or columns or native_status.get('code') != '02000'):
                    raise FabricGqlError('Unsupported continuation shape; no partial rows released')
                url = endpoint + '&continuationToken=' + quote(token, safe='')
                continue
            if native_status.get('code') == '02000' and rows:
                raise FabricGqlError('Native empty status disagrees with rows')
            definitions = []
            for column in columns:
                if (type(column) is not dict or type(column.get('name')) is not str
                        or not column['name'] or column.get('gqlType') not in ('STRING', 'BOOL', 'INT64', 'UINT64')):
                    raise FabricGqlError('Unsupported native column projection')
                definitions.append((column['name'], column['gqlType']))
            names = [name for name, _ in definitions]
            if len(set(names)) != len(names):
                raise FabricGqlError('Ambiguous native column names')
            decoded = []
            for row in rows:
                if type(row) is not dict or set(row) != set(names):
                    raise FabricGqlError('Exact complete native row projection required')
                decoded.append(tuple(_scalar(kind, row[name]) for name, kind in definitions))
            if time.monotonic() >= expires or policy.admit(binding, query, context) is not None:
                raise FabricGqlError('Closing native authority or deadline incomplete')
            result = FabricGqlResult(hashlib.sha256(body).hexdigest(), tuple(responses),
                                     tuple(definitions), tuple(decoded))
            break
        if result is None:
            raise FabricGqlError('Native continuation capacity exhausted')
    if time.monotonic() >= expires:
        raise FabricGqlError('Native hold cleanup exceeded query deadline')
    return result

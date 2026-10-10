"""Indexed compiler adapter for an already admitted original commerce reader.

The caller owns fresh publication setup, native aliases and reader lifetime.
This adapter does not provision engines, discover credentials, rewrite SQL or
turn private source authority into Truss/production authority. Installed compiler
admission is separate from the original provider's publication/ACK guards.
"""
import hashlib
import json
from collections import Counter

from ashlar.weft_distribution import DistributionPaths, compile_distribution
from run_commerce_publication_weft import compiler_request, execute_guarded

CASES = (
    ('string-projection', 'SELECT p.id FROM products p', True),
    ('global-count', 'SELECT COUNT(*) AS n FROM products p', False),
)
LIMIT = 16 * 1024 * 1024


def _encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _duplicate_free(pairs):
    result = {}
    for key, value in pairs:
        if key in result: raise ValueError('Duplicate compiler JSON member')
        result[key] = value
    return result


def decode_compile_artifact(raw: bytes) -> dict:
    if type(raw) is not bytes or len(raw) > LIMIT or not raw.endswith(b'\n') or raw.count(b'\n') != 1:
        raise ValueError('Closed compiler response framing required')
    return json.loads(raw, object_pairs_hook=_duplicate_free,
                      parse_constant=lambda token: (_ for _ in ()).throw(ValueError('Nonfinite compiler JSON')))


def run_indexed_queries(opened, paths: DistributionPaths):
    """Buffer original rows until every native hold and source oracle closes.

    Requires open_commerce_reader()'s genuine public source admission and ordinary
    protected ACK/native pin policy. No query result can supply that authority.
    """
    graph = json.loads(opened.graph, object_pairs_hook=_duplicate_free)
    products = [o['values'] for o in graph['objects'] if o['type']['element'] == 'products']
    expected = {
        'string-projection': [{'id': p['products.id']} for p in products],
        'global-count': [{'n': str(len(products))}],
    }
    original_files = dict(opened.original_native_files)
    if opened.native_files() != original_files: raise ValueError('Opening native file vector differs')
    queries = []
    for name, sql, fields in CASES:
        request = compiler_request(sql, opened.model, opened.bindings, opened.manifest,
                                   opened.original_report['table_registry'], opened.aliases, fields=fields)
        request_bytes = _encoded(request).encode('utf-8') + b'\n'
        response = compile_distribution(paths, request_bytes)
        artifact = decode_compile_artifact(response)
        expected_backend = {'backendId': 'ashlar.databricks', 'backendVersion': '0.1.0-candidate', 'interfaceVersion': 'weft-backend/0.2.0', 'targetProfile': 'dbsql-candidate'}
        if artifact.get('backend') != expected_backend:
            raise ValueError('Installed candidate backend profile differs')
        # The unchanged historical host gate owns complete obligation admission,
        # original binding/model custody and every emitted scalar SQL check.
        opened.provider.expected_binding = _encoded(json.loads(request['target']['bindingJson']))
        result = execute_guarded(opened.provider, request, artifact, context=opened.context)
        if Counter(_encoded(row) for row in result['rows']) != Counter(_encoded(row) for row in expected[name]):
            raise ValueError('Independent original source result bag differs')
        custody = opened.provider.closed_interval_custody(opened.context)
        queries.append({'name': name, 'request': request, 'request_sha256': hashlib.sha256(request_bytes).hexdigest(),
                        'artifact': artifact, 'response_sha256': hashlib.sha256(response).hexdigest(),
                        'result': result, 'original_source_expected': expected[name], 'closed_interval': custody})
    if opened.native_files() != original_files: raise ValueError('Closing native file vector differs')
    return {'format': 'ashlar-indexed-original-commerce-query/0.1', 'queries': queries,
            'qualification': 'Indexed compiler and private original-source publication consumer; not Truss/source production authority or native acceptance evidence before actual execution.'}

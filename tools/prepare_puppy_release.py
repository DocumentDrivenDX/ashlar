"""Prepare one immutable hash-qualified local PuppyGraph carrier, no activation.

Uses the existing owning graph-release reader. The selected local database and
model are inputs to a separately reviewed public native activation adapter.
"""
from pathlib import Path
import hashlib
import json
import os
from ashlar_host.puppy_release import PuppyReleaseHandle
from run_graph_release_graphframes import load_release, columns
from ashlar.graph_release import GraphRelease


def prepare(payload, trusted_sha256, output):
    import duckdb
    handle = PuppyReleaseHandle(GraphRelease(payload, trusted_sha256))
    value = load_release(payload, trusted_sha256)
    output = Path(output)
    if (not output.is_absolute() or output.parent.resolve() != output.parent
            or output.parent.stat().st_uid != os.getuid() or output.parent.stat().st_mode & 0o022):
        raise ValueError('Canonical explicit private carrier destination required')
    output.mkdir(mode=0o700, exist_ok=False)
    # Native catalog identifiers have a narrower observed surface than release
    # hashes. This display/storage name grants no identity or repair authority;
    # exact original model collisions refuse, and full SHA custody stays below.
    name = 'ashlar_' + trusted_sha256[:24]
    database = output / ('ashlar_' + trusted_sha256 + '.duckdb')
    model = {'catalog': [{'name': name, 'type': 'duckdb',
        'jdbc': {'jdbcUri': 'jdbc:duckdb:/tmp/' + database.name}}], 'node': [], 'edge': []}
    connection = duckdb.connect(str(database))
    try:
        connection.execute("SET threads=1")
        connection.execute("SET memory_limit='64MB'")
        connection.execute('CREATE SCHEMA carrier')
        for kind, role, label in [('node', 'nodes', handle.node_label),
                ('edge', 'edges', handle.edge_label)]:
            names = list(columns(kind)); rows = value[role]
            if any(set(row) != set(names) for row in rows):
                raise ValueError('Closed original carrier required')
            connection.execute('CREATE TABLE carrier.' + role + '(' +
                ','.join('"' + n + '" VARCHAR' for n in names) + ',carrier_key VARCHAR,carrier_id VARCHAR)')
            entries = [[row[n] for n in names] + [row['graph_id'], row['id']] for row in rows]
            if entries:
                connection.executemany('INSERT INTO carrier.' + role + ' VALUES (' +
                    ','.join('?' for _ in entries[0]) + ')', entries)
            actual = connection.execute('SELECT ' + ','.join('"' + n + '"' for n in names) +
                ' FROM carrier.' + role).fetchall()
            encode = lambda row: json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
            if sorted(encode(dict(zip(names, row))) for row in actual) != sorted(encode(row) for row in rows):
                raise ValueError('Independent full carrier value parity differs')
            key = 'node_key' if kind == 'node' else 'edge_key'
            fields = [n for n in names if n not in ('id', 'graph_id')] + ['carrier_key', 'carrier_id']
            entry = {'label': label, 'dataSourceGroup': {'externalDataSource': {
                'enabled': True, 'catalog': name, 'schema': 'carrier', 'table': role,
                'mappedField': [{'sourceFieldName': n, 'targetFieldName': n} for n in fields] +
                    [{'sourceFieldName': 'graph_id', 'targetFieldName': key}]}},
                'id': [{'name': key, 'type': 'STRING'}],
                'attribute': [{'name': n, 'type': 'STRING'} for n in fields]}
            if kind == 'edge':
                entry.update(fromNodeLabel=handle.node_label, toNodeLabel=handle.node_label,
                    fromKey=[{'name': 'src', 'type': 'STRING'}], toKey=[{'name': 'dst', 'type': 'STRING'}])
            model[kind].append(entry)
    finally:
        connection.close()
    raw = database.read_bytes()
    if len(raw) > 32 * 1024 * 1024:
        raise ValueError('Bounded local immutable carrier required')
    database.chmod(0o444)
    model_bytes = (json.dumps(model, sort_keys=True, ensure_ascii=False, separators=(',', ':')) + '\n').encode()
    with (output / 'model.json').open('xb') as target: target.write(model_bytes)
    (output / 'model.json').chmod(0o444)
    receipt = {'profile': 'ashlar-puppy-release-carrier/0.1', 'release_sha256': trusted_sha256,
        'database_sha256': hashlib.sha256(raw).hexdigest(), 'database_bytes': len(raw),
        'model_sha256': hashlib.sha256(model_bytes).hexdigest(), 'database_name': database.name,
        'duckdb_version': duckdb.__version__, 'qualification': 'Single exact local export only; no native activation/query or catalog authority.'}
    with (output / 'receipt.json').open('x') as target: target.write(json.dumps(receipt, indent=2) + '\n')
    return receipt

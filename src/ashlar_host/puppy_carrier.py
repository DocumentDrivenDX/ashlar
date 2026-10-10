"""Prepare one immutable hash-qualified local PuppyGraph carrier, no activation.

Uses the existing owning graph-release reader. The selected local database and
model are inputs to a separately reviewed public native activation adapter.
"""
from pathlib import Path
import hashlib
import json
import os
import stat
from ashlar_host.puppy_release import PuppyReleaseHandle
from .puppy_release_document import load_graph_release as load_release
from ashlar.graph_release import release_columns as columns
from .lifecycle import finish
from .puppy_release_cli import directory_identity, retain
from ashlar.graph_release import GraphRelease


def prepare_carrier(payload, trusted_sha256, output, *, policy, context):
    if not callable(getattr(policy, "admit_original_release", None)):
        raise ValueError("Independent original release policy required")
    handle = PuppyReleaseHandle(GraphRelease(payload, trusted_sha256))
    value = load_release(payload, trusted_sha256)
    def admit():
        if policy.admit_original_release(handle.release, context) is not None:
            raise ValueError("Current original release authority refused")
    admit()
    import duckdb
    output = Path(output)
    if (not output.is_absolute() or output.parent.resolve() != output.parent
            or output.parent.stat().st_uid != os.getuid() or output.parent.stat().st_mode & 0o022):
        raise ValueError('Canonical explicit private carrier destination required')
    output.mkdir(mode=0o700, exist_ok=False)
    identity = directory_identity(output)
    retain(output, "original-intent.json", (json.dumps({"release_sha256":trusted_sha256,"operation":"prepare-carrier"},sort_keys=True)+"\n").encode(), identity)
    # Native catalog identifiers have a narrower observed surface than release
    # hashes. This display/storage name grants no identity or repair authority;
    # exact original model collisions refuse, and full SHA custody stays below.
    name = 'ashlar_' + trusted_sha256[:24]
    database = output / ('ashlar_' + trusted_sha256 + '.duckdb')
    model = {'catalog': [{'name': name, 'type': 'duckdb',
        'jdbc': {'jdbcUri': 'jdbc:duckdb:/tmp/' + database.name}}], 'node': [], 'edge': []}
    connection = duckdb.connect(str(database))
    primary = None; committed = False
    try:
        connection.execute("SET threads=1")
        connection.execute("SET memory_limit='64MB'")
        connection.execute('BEGIN TRANSACTION')
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
        admit()
        if directory_identity(output) != identity: raise ValueError("Original carrier directory replaced")
        connection.execute("COMMIT"); committed = True
    except BaseException as error: primary = error
    finish(primary, [lambda: connection.execute("ROLLBACK") if not committed else None, connection.close])
    if directory_identity(output) != identity: raise ValueError('Original carrier directory replaced')
    fd = os.open(database, os.O_RDONLY | os.O_NOFOLLOW)
    primary = None
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid() or before.st_size > 32*1024*1024:
            raise ValueError('Bounded owned local carrier required')
        raw = bytearray()
        while len(raw) <= before.st_size:
            chunk = os.read(fd, min(65536, before.st_size + 1 - len(raw)))
            if not chunk: break
            raw.extend(chunk)
        after = os.fstat(fd)
        if len(raw) != before.st_size or (before.st_dev,before.st_ino,before.st_mtime_ns,before.st_size) != (after.st_dev,after.st_ino,after.st_mtime_ns,after.st_size):
            raise ValueError('Original carrier changed during capture')
        os.fchmod(fd,0o444); os.fsync(fd)
        current = database.lstat()
        if (current.st_dev,current.st_ino) != (before.st_dev,before.st_ino):
            raise ValueError('Original carrier path replaced')
    except BaseException as error: primary = error
    finish(primary,[lambda:os.close(fd)])
    model_bytes = (json.dumps(model, sort_keys=True, ensure_ascii=False, separators=(',', ':')) + '\n').encode()
    admit()
    retain(output, 'model.json', model_bytes, identity)
    receipt = {'profile': 'ashlar-puppy-release-carrier/0.1', 'release_sha256': trusted_sha256,
        'database_sha256': hashlib.sha256(raw).hexdigest(), 'database_bytes': len(raw),
        'model_sha256': hashlib.sha256(model_bytes).hexdigest(), 'database_name': database.name,
        'duckdb_version': duckdb.__version__, 'qualification': 'Single exact local export only; no native activation/query or catalog authority.'}
    admit()
    return receipt

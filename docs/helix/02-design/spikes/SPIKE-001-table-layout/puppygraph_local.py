"""Prepare the pinned r66 typed carrier in DuckDB; not a UC Delta qualification."""
import hashlib, json, sys
from pathlib import Path
import duckdb
BASE = Path(__file__).resolve().parent
release = BASE / 'adapters/release-r66'
plan = json.loads((release / 'mapping-plan.json').read_text())
connection = duckdb.connect(sys.argv[1])
connection.execute('CREATE SCHEMA spike')
model = {'catalog': [{'name': 'ashlar_release', 'type': 'duckdb',
                      'jdbc': {'jdbcUri': 'jdbc:duckdb:/tmp/' + Path(sys.argv[1]).name}}],
         'node': [], 'edge': []}
for name, item in plan['tables'].items():
    payload = (release / item['file']).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == item['sha256']
    rows = json.loads(payload)
    assert len(rows) == item['rows']
    columns = list(rows[0])
    assert all(set(r) == set(columns) for r in rows)
    assert all(isinstance(v, str) for r in rows for v in r.values())
    ddl = ','.join('"' + c + '" VARCHAR' for c in columns)
    connection.execute('CREATE TABLE spike.' + name + '(' + ddl + ')')
    connection.executemany('INSERT INTO spike.' + name + ' VALUES (' +
                           ','.join('?' for _ in columns) + ')',
                           [[r[c] for c in columns] for r in rows])
    for row in rows:
        actual = connection.execute('SELECT * FROM spike.' + name +
                                    ' WHERE id = ?', [row['id']]).fetchone()
        assert list(actual) == [row[c] for c in columns]
    key = 'node_key' if item['kind'] == 'node' else 'edge_key'
    connection.execute('ALTER TABLE spike.' + name + ' ADD COLUMN carrier_key VARCHAR')
    connection.execute('ALTER TABLE spike.' + name + ' ADD COLUMN carrier_id VARCHAR')
    connection.execute('UPDATE spike.' + name + ' SET carrier_key = ' + key + ', carrier_id = id')
    columns = [c for c in columns if c != 'id'] + ['carrier_key', 'carrier_id']
    entry = {'label': item['label'], 'dataSourceGroup': {'externalDataSource': {
        'enabled': True, 'catalog': 'ashlar_release', 'schema': 'spike',
        'table': name, 'mappedField': [{'sourceFieldName': c,
                                      'targetFieldName': c} for c in columns]}},
        'id': [{'name': key, 'type': 'STRING'}],
        'attribute': [{'name': c, 'type': 'STRING'} for c in columns if c != key]}
    if item['kind'] == 'edge':
        entry.update(fromNodeLabel=item['sourceLabel'], toNodeLabel=item['targetLabel'],
                     fromKey=[{'name': 'src', 'type': 'STRING'}],
                     toKey=[{'name': 'dst', 'type': 'STRING'}])
    model[item['kind']].append(entry)
connection.close()
Path(sys.argv[2]).write_text(json.dumps(model, indent=2) + '\n')
print('Prepared 3 vertices / 3 edges; every scalar carrier byte preserved')

"""Actual Bolt conformance for the immutable r66 carrier; no UC/scale claim."""
import json
from pathlib import Path
from neo4j import GraphDatabase
BASE = Path(__file__).resolve().parent
release = BASE / 'adapters/release-r66'
plan = json.loads((release / 'mapping-plan.json').read_text())
records = []
with GraphDatabase.driver('bolt://127.0.0.1:17687', auth=('ashlar', 'local-spike-only'), connection_timeout=10) as driver:
    with driver.session() as session:
        for name, item in plan['tables'].items():
            expected = json.loads((release / item['file']).read_text())
            columns = list(expected[0])
            pattern = '(x:' + item['label'] + ')' if item['kind'] == 'node' else '()-[x:' + item['label'] + ']->()'
            query = 'MATCH ' + pattern + ' RETURN ' + ','.join('x.' + ('carrier_id' if c == 'id' else 'carrier_key' if c in ('node_key', 'edge_key') else c) + ' AS ' + c for c in columns)
            actual = session.run(query).data()
            normalize = lambda rows: sorted(json.dumps(r, sort_keys=True) for r in rows)
            assert normalize(actual) == normalize(expected), name
            records.append({'table': name, 'query': query, 'rows': actual, 'exact_parity': True})
        controls = [
            ('vertices', 'MATCH (n) RETURN count(n) AS value', 3),
            ('edges', 'MATCH ()-[e]->() RETURN count(e) AS value', 3),
            ('isolates', 'MATCH (n) WHERE NOT (n)--() RETURN count(n) AS value', 1),
            ('self_loops', 'MATCH (n)-[e]->(n) RETURN count(e) AS value', 1),
            ('parallel_edges', 'MATCH (:Type1)-[e:Rel7_1_2]->(:Type2) RETURN count(e) AS value', 2),
        ]
        for label, query, expected in controls:
            actual = session.run(query).data()
            assert actual == [{'value': expected}], (label, actual)
            records.append({'check': label, 'query': query, 'rows': actual, 'passed': True})
result = {'runtime': 'PuppyGraph 1.13.0 ARM64', 'image_digest': 'sha256:6d016d5a7e3eab0a57bd4d23c4e218467d00c7661fcda215ea2a429941a68cee',
          'source': 'DuckDB 1.4.1 file copied from hash-pinned r66 typed carrier',
          'driver': 'neo4j 5.28.2 Bolt', 'resource_bounds': {'cpus': 4, 'memory_gib': 8},
          'records': records, 'all_passed': True,
          'limits': ['Not direct Unity Catalog or Delta protocol evidence', 'Not scale or latency evidence',
                     'Parallel carrier fixture is not a legal Truss duplicate endpoint pair',
                     'No cross-engine atomic release activation or rollback qualification']}
(BASE / 'out/puppygraph-local-r105.json').write_text(json.dumps(result, indent=2)+'\n')
print('Passed all four exact-carrier comparisons and five graph structural controls')

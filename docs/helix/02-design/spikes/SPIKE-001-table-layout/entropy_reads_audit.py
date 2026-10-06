"""Audit final history for the r88 full-carrier singleton experiment."""
import argparse
import json
import math
import statistics
from pathlib import Path
from persistent_sql import Client

parser = argparse.ArgumentParser()
parser.add_argument('--version', type=int, choices=(0, 1, 2), default=0)
version = parser.parse_args().version
O = Path(__file__).resolve().parent / {0: 'out/native/ashlar_entropy_reads_20261006_r88', 1: 'out/native/ashlar_entropy_post_reads_20261006_r90', 2: 'out/native/ashlar_entropy_maintained_reads_20261006_r92'}[version]
pairs = []
for i in range(4):
    folder = O / f'client-{i}'
    records = [json.loads(line) for line in (folder / 'statements.jsonl').read_text().splitlines()]
    client = Client(folder)
    client.records = records
    client.history()
    history = {row['query_id']: row for row in json.loads((folder / 'query-history.json').read_text())}
    pairs.extend((record, history.get(record['statement_id'])) for record in records)

def percentile(values):
    return sorted(values)[math.ceil(.95 * len(values)) - 1]

def summarize(prefix):
    group = [(r, q) for r, q in pairs if r['label'].startswith(prefix)]
    assert len(group) == 50
    assert all(q and q.get('is_final') for r, q in group), 'Refresh final history; do not repeat reads'
    assert all(q['metrics'].get('result_from_cache') is False for r, q in group)
    result = {'count': 50, 'caller_p95_ms': percentile([r['wall_ms'] for r, q in group]), 'result_cache_hits': 0}
    for key in ('execution_time_ms', 'compilation_time_ms', 'read_files_count', 'read_bytes', 'read_remote_bytes'):
        values = [q['metrics'][key] for r, q in group]
        result[key] = {'p95': percentile(values), 'median': statistics.median(values), 'min': min(values), 'max': max(values)}
    result['remote_read_count'] = sum(q['metrics']['read_remote_bytes'] > 0 for r, q in group)
    return result

result = {'state': 'completed; exact query IDs and final uncached metrics audited',
          'table': 'client_dev.ashlar_entropy_20261006_r86.edge_current', 'version': version,
          'first_touch': summarize('first-touch-'), 'repeat': summarize('repeat-'),
          'four_clients': summarize('four-client-'),
          'scope': '20M synthetic edges / 33.95 GB; full carriers returned; no controlled cold or billion-scale admission'}
if version > 0:
    result['changed_rows'] = summarize('changed-row-')
    result['scope'] = '20M-edge synthetic table after 200k property updates; mixed fixed-key and explicit changed-row cohorts; full carriers returned; no controlled cold or billion-scale admission'
if version == 2:
    result['scope'] += '; routine OPTIMIZE version2 is not installed in a new publication descriptor'
(O / 'audited-summary.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))

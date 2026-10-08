"""Derive phase costs from terminal R274 evidence; no native operations."""
import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent

def main():
    package = json.loads((BASE / 'layout-package-candidate.json').read_text())
    for name, expected in package['files'].items():
        assert hashlib.sha256((BASE / name).read_bytes()).hexdigest() == expected, name
    paths = ['out/native/ashlar_scale_edges_r274/audited-summary.json',
             'out/native/ashlar_scale_edges_r274/statements.jsonl',
             'out/native/ashlar_scale_edges_r274/shared-history.json']
    summary = json.loads((BASE / paths[0]).read_text())
    records = [json.loads(line) for line in (BASE / paths[1]).read_text().splitlines()]
    history = json.loads((BASE / paths[2]).read_text())
    queries = {q['query_id']: q for q in history['queries']}
    phases = {}
    seen = set()
    for record in records:
        sid = record['statement_id']
        assert sid not in seen
        seen.add(sid)
        q = queries[sid]
        assert q['is_final'] and q['status'] == 'FINISHED'
        assert record['response']['status']['state'] == 'SUCCEEDED'
        assert q['query_text'] == record['sql']
        sql = record['sql'].lstrip().upper()
        phase = 'write' if sql.startswith(('INSERT ', 'CREATE ', 'MERGE ', 'UPDATE ', 'DELETE ')) else 'validation' if sql.startswith(('SELECT ', 'WITH ')) else 'control_metadata'
        p = phases.setdefault(phase, {'statements': [], 'caller_ms': 0, 'engine_ms': 0, 'read_bytes': 0, 'write_remote_bytes': 0, 'spill_to_disk_bytes': 0})
        p['statements'].append({'label': record['label'], 'statement_id': sid,
                              'caller_ms': record['wall_ms'], 'engine_ms': q['metrics']['execution_time_ms'],
                              'read_bytes': q['metrics']['read_bytes'], 'write_remote_bytes': q['metrics']['write_remote_bytes']})
        p['caller_ms'] += record['wall_ms']
        p['engine_ms'] += q['metrics']['execution_time_ms']
        for key in ['read_bytes', 'write_remote_bytes', 'spill_to_disk_bytes']:
            p[key] += q['metrics'][key]
    for key, expected in summary['costs'].items():
        assert sum(p[key] for p in phases.values()) == expected, key
    result = {'format': 'ashlar-growth-cost-basis/1',
              'sources': {p: hashlib.sha256((BASE / p).read_bytes()).hexdigest() for p in paths},
              'graph': summary['planned_graph'], 'new_edges': 8000000,
              'wall_s': summary['wall_s'], 'local_oracle_s': summary['oracle_s'],
              'phases': phases,
              'next_action': 'Calibrate a separate append allocation profile locally before admitting native node-plus-edge growth. Preserve all existing published identities/endpoints and reconcile physical edge head12 separately.',
              'qualification': 'Historical bootstrap append costs, not current seven-batch ingest, next doubling bounds, billing estimates, or billion-scale admission. Phase labels classify SQL by leading verb. Caller sums do not establish parallel critical path or causal engine throughput. No native SQL executed.'}
    (BASE / 'out/growth-cost-basis-r635.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: {x: v for x, v in p.items() if x != 'statements'} for k,p in phases.items()}, indent=2))

if __name__ == '__main__':
    main()

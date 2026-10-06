"""Audit stored r93 query IDs and explain its narrow-key physical plan."""
import json
from pathlib import Path
from persistent_sql import Client

O = Path(__file__).resolve().parent / 'out/native/ashlar_entropy_changed_validation_20261006_r93'
records = [json.loads(line) for line in (O / 'statements.jsonl').read_text().splitlines()]
c = Client(O)
c.records = records
history = {q['query_id']: q for q in c.history()}
assert all(history.get(r['statement_id'], {}).get('is_final') for r in records)
assert all(not r['cancel_requested'] for r in records)
assert all(history[r['statement_id']]['metrics'].get('result_from_cache') is False for r in records)
summary = json.loads((O / 'summary.json').read_text())
summary['state'] = 'completed exact affected-row validation; final uncached metrics audited'
summary['phases'] = [{'label': r['label'], 'caller_ms': r['wall_ms'], 'metrics': history[r['statement_id']]['metrics']} for r in records]
plan_client = Client(O / 'plan')
query = next(r['sql'] for r in records if r['label'] == 'narrow-key-prior-property-preservation')
rows = plan_client.sql('explain-narrow-key-validation', 'EXPLAIN FORMATTED ' + query)
plan = '\n'.join(row[0] for row in rows)
assert 'BroadcastHashJoin' in plan
(O / 'plan/physical-plan.txt').write_text(plan + '\n')
summary['broadcast_join_in_plan'] = True
(O / 'audited-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
plan_client.history()

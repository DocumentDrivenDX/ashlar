"""Audit the finite r96 admission schedule without repeating SQL mutations."""
import json
from pathlib import Path
from persistent_sql import Client

B = Path(__file__).resolve().parent
O = B/'out/native/ashlar_scheduled_publication_20261006_r96/version-aware-preflight'
summary = json.loads((O/'summary.json').read_text())
assert summary['state'] == 'completed finite controlled schedule; correctness checks passed'
records = [json.loads(line) for line in (O/'statements.jsonl').read_text().splitlines()]
c = Client(O)
c.records = records
history = {q['query_id']:q for q in c.history()}
assert all(history.get(r['statement_id'], {}).get('is_final') for r in records), 'Refresh history only; never repeat writes'
assert all(r['response']['status']['state'] == 'SUCCEEDED' and not r['cancel_requested'] for r in records)
assert not any(history[r['statement_id']]['metrics'].get('result_from_cache') for r in records)
freshness = []
for batch in summary['batches']:
    first, ready = batch['first_arrival_offset_s'], batch['complete_input_ready_offset_s']
    finish = batch['verified_manifest_offset_s']
    freshness.extend(finish-(first+(ready-first)*(i+.5)/batch['rows']) for i in range(batch['rows']))
    commits = batch['commits']
    assert {int(v['version']) for v in commits} == set(range(batch['old_current_version']+1, batch['versions']['client_dev.ashlar_entropy_20261006_r86.edge_current']+1))
    batch['merge_metrics'] = json.loads(next(v['operationMetrics'] for v in commits if v['operation']=='MERGE'))
freshness.sort()
result = dict(summary)
result.update(
    state='audited finite controlled schedule; exact queries final and uncached',
    modeled_record_freshness_s={'p50':freshness[len(freshness)//2], 'p95':freshness[int(.95*len(freshness))-1], 'maximum':freshness[-1]},
    distribution_scope='300k uniformly modeled arrivals; actual controller releases three complete pre-staged batches. No measured producer arrival distribution, extraction/network rate, sustained admission or reader contention.',
    phases=[{'label':r['label'], 'caller_ms':r['wall_ms'], 'metrics':history[r['statement_id']]['metrics']} for r in records])
(O/'audited-summary.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'batches':[{k:b[k] for k in ('batch','queue_wait_s','processing_s','complete_input_to_manifest_s')} for b in result['batches']], 'modeled_freshness':result['modeled_record_freshness_s']},indent=2))

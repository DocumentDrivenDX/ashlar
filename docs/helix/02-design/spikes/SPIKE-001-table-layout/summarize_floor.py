import json,math
from importlib.metadata import version
from pathlib import Path
BASE=Path(__file__).resolve().parent
schema='ashlar_floor_20261005_b1';F='client_dev.'+schema;out=BASE/'out/native'/schema
measurements=[json.loads(x) for x in (out/'statements.jsonl').read_text().splitlines() if any(json.loads(x)['label'].startswith(n+'-') for n in ['no_table','tiny','typed_repetitive','typed_varied_pinned']) and not json.loads(x)['label'].endswith('-0')]
history=json.loads((out/'query-history.json').read_text());byid={q['query_id']:q for q in history}
def pct(xs,p):return sorted(xs)[math.ceil(len(xs)*p)-1]
summary={'schema':F,'sdk':version('databricks-sdk'),'requests':version('requests'),'samples':25,'state':'completed','groups':{}}
for name in ['no_table','tiny','typed_repetitive','typed_varied_pinned']:
 rs=[r for r in measurements if r['label'].startswith(name+'-')];qs=[byid[r['statement_id']] for r in rs];m=[q['metrics'] for q in qs]
 summary['groups'][name]={'n':len(rs),'wall_p50_ms':pct([r['wall_ms'] for r in rs],.5),'wall_p95_ms':pct([r['wall_ms'] for r in rs],.95),'execution_p95_ms':pct([q['execution_time_ms'] for q in m],.95),'compile_p95_ms':pct([q['compilation_time_ms'] for q in m],.95),'server_total_p95_ms':pct([q['total_time_ms'] for q in m],.95),'result_cache_hits':sum(q['result_from_cache'] for q in m),'read_files_median':pct([q['read_files_count'] for q in m],.5),'pruned_files_median':pct([q['pruned_files_count'] for q in m],.5)}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)

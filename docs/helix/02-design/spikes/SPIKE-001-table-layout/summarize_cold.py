import argparse,json,math
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--run',default='ashlar_cold_20261005_d1');a=p.parse_args()
BASE=Path(__file__).resolve().parent;out=BASE/'out/native'/a.run
rs=[json.loads(x) for x in (out/'statements.jsonl').read_text().splitlines()];qs={q['query_id']:q for q in json.loads((out/'query-history.json').read_text())}
def pct(xs,p):return sorted(xs)[math.ceil(len(xs)*p)-1] if xs else None
summary={'state':'completed','groups':{}}
for table in ['liquid16','liquid128']:
 for phase in ['first','warm']:
  rows=[r for r in rs if r['label'].startswith(phase+'-'+table+'-')];ms=[qs[r['statement_id']]['metrics'] for r in rows]
  assert all('execution_time_ms' in m and 'read_remote_bytes' in m for m in ms),'Refresh asynchronous metrics; do not substitute missing values'
  cold=[r for r in rows if qs[r['statement_id']]['metrics']['read_remote_bytes']>0]
  summary['groups'][phase+'-'+table]={'n':len(rows),'qualified_remote_io_n':len(cold),'remote_io_caller_p95_ms':pct([r['wall_ms'] for r in cold],.95),'all_caller_p95_ms':pct([r['wall_ms'] for r in rows],.95),'engine_p95_ms':pct([m['execution_time_ms'] for m in ms],.95),'remote_bytes_sum':sum(m['read_remote_bytes'] for m in ms),'result_cache_hits':sum(m['result_from_cache'] for m in ms),'read_files_median':pct([m['read_files_count'] for m in ms],.5)}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary))

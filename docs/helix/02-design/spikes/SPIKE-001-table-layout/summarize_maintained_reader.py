"""Summarize exact reader calls overlapping the actual writer work interval."""
import argparse,json,math
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('layout',choices=['lc','partition']);a=p.parse_args()
B=Path(__file__).resolve().parent;out=B/('out/native/ashlar_maintained_schedule_20261005_r40_'+a.layout)
run=json.loads((out/'summary.json').read_text());assert run['state']=='completed'
writer=[json.loads(l) for l in (out/'statements.jsonl').read_text().splitlines()]
start=min(r['start_epoch'] for r in writer if r['label'].startswith('atomic-data-'))
end=max(r['start_epoch']+r['wall_ms']/1000 for r in writer if r['label'].startswith('publish-'))
reader=[json.loads(l) for l in (out/'reader/statements.jsonl').read_text().splitlines()]
rows=[r for r in reader if r['label'].startswith('read-') and r['start_epoch']<end and r['start_epoch']+r['wall_ms']/1000>start]
queries={q['query_id']:q for q in json.loads((out/'reader/query-history.json').read_text())};metrics=[queries[r['statement_id']]['metrics'] for r in rows]
assert rows and all(all(k in m for k in ['execution_time_ms','compilation_time_ms','result_from_cache','read_files_count','read_remote_bytes']) for m in metrics),'Refresh same reader IDs'
def pct(xs):return sorted(xs)[math.ceil(len(xs)*.95)-1]
result={'state':'completed','n':len(rows),'writer_start_epoch':start,'writer_end_epoch':end,'caller_p95_ms':pct([r['wall_ms'] for r in rows]),'engine_p95_ms':pct([m['execution_time_ms'] for m in metrics]),'compile_p95_ms':pct([m['compilation_time_ms'] for m in metrics]),'result_cache_hits':sum(m['result_from_cache'] for m in metrics),'remote_query_count':sum(m['read_remote_bytes']>0 for m in metrics),'scope':'read calls whose actual time interval overlaps first atomic-data start through final publication end; excludes priming and initial arrival wait; fixed old vector, exact 17-field equality'}
(out/'reader-overlap-summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))

"""Summarize completed client controls; missing telemetry is not zero latency."""
import argparse,json,math
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('run');a=p.parse_args();out=Path(__file__).resolve().parent/'out/native'/a.run
rs=json.loads((out/'all-statements.json').read_text()) if (out/'all-statements.json').exists() else [json.loads(x) for x in (out/'statements.jsonl').read_text().splitlines()]
qs={q['query_id']:q for q in json.loads((out/'query-history.json').read_text())}
def pct(xs,p):return sorted(xs)[math.ceil(len(xs)*p)-1]
groups={}
for r in rs:
 name,_,rep=r['label'].rpartition('-')
 if not rep.isdigit() or int(rep)==0:continue
 groups.setdefault(name,[]).append(r)
summary={}
for name,rows in groups.items():
 ms=[qs[r['statement_id']]['metrics'] for r in rows]
 assert all(all(k in m for k in ['execution_time_ms','compilation_time_ms','total_time_ms','result_from_cache','read_files_count']) for m in ms),'Refresh same completed statement metrics'
 summary[name]={'n':len(rows),'caller_p50_ms':pct([r['wall_ms'] for r in rows],.5),'caller_p95_ms':pct([r['wall_ms'] for r in rows],.95),'engine_p50_ms':pct([m['execution_time_ms'] for m in ms],.5),'engine_p95_ms':pct([m['execution_time_ms'] for m in ms],.95),'compile_p50_ms':pct([m['compilation_time_ms'] for m in ms],.5),'compile_p95_ms':pct([m['compilation_time_ms'] for m in ms],.95),'server_p95_ms':pct([m['total_time_ms'] for m in ms],.95),'result_cache_hits':sum(m['result_from_cache'] for m in ms),'read_files_median':pct([m['read_files_count'] for m in ms],.5)}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary))

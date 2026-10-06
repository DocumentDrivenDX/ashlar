import json,math
from pathlib import Path
BASE=Path(__file__).resolve().parent;schema='ashlar_pruning_20261005_c1';F='client_dev.'+schema;out=BASE/'out/native'/schema
variants=[('liquid_16m',None,'16777216'),('liquid_128m',None,'134217728'),('bucket64_z',None,'134217728')]
measures=[json.loads(x) for x in (out/'statements.jsonl').read_text().splitlines() if json.loads(x)['label'].startswith('measure-') and not json.loads(x)['label'].endswith('-0')]
history=json.loads((out/'query-history.json').read_text());byid={q['query_id']:q for q in history}
def pct(xs,p):return sorted(xs)[math.ceil(len(xs)*p)-1]
summary={'state':'completed','schema':F,'rows':1000000,'groups':{}}
for name,_,size in variants:
 rs=[r for r in measures if r['label'].startswith('measure-'+name+'-')];ms=[byid[r['statement_id']]['metrics'] for r in rs]
 summary['groups'][name]={'samples':len(rs),'target_file_bytes':int(size),'wall_p95_ms':pct([r['wall_ms'] for r in rs],.95),'engine_p95_ms':pct([m['execution_time_ms'] for m in ms],.95),'server_total_p95_ms':pct([m['total_time_ms'] for m in ms],.95),'read_files_median':pct([m['read_files_count'] for m in ms],.5),'pruned_files_median':pct([m['pruned_files_count'] for m in ms],.5),'read_bytes_median':pct([m['read_bytes'] for m in ms],.5),'io_cache_percent_median':pct([m['bytes_read_from_cache_percentage'] for m in ms],.5),'result_cache_hits':sum(m['result_from_cache'] for m in ms)}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)

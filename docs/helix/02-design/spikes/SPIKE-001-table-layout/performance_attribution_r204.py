"""Deterministic per-query attribution and publisher timeline coverage from saved evidence."""
import json,math,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent;paths=[]
def read(path):
 p=B/path;paths.append(path);return json.loads(p.read_text())
p95=lambda a:sorted(a)[math.ceil(.95*len(a))-1]
rpath='out/native/ashlar_publication_reads_r202/';records=[json.loads(x) for x in (B/rpath/'statements.jsonl').read_text().splitlines()];paths.append(rpath+'statements.jsonl');h={x['query_id']:x for x in read(rpath+'query-history.json')};read_s={}
for phase in [0,1]:
 rs=[r for r in records if r['label'].startswith(f'p{phase}-serial-')];assert len(rs)==20
 rows=[]
 for r in rs:
  m=h[r['statement_id']]['metrics'];total=m['total_time_ms'];compile_ms=m['compilation_time_ms'];execution=m['execution_time_ms'];other=total-compile_ms-execution
  assert other>=0 and h[r['statement_id']]['is_final']
  rows.append({'query_id':r['statement_id'],'caller_ms':r['wall_ms'],'compile_ms':compile_ms,'engine_ms':execution,'native_other_ms':other,'caller_minus_native_ms':r['wall_ms']-total,'compile_elimination_counterfactual_ms':r['wall_ms']-compile_ms,'compile_reduction_needed_for_250_ms':max(0,r['wall_ms']-250)})
 read_s[str(phase)]={'per_query':rows,'p95_of_per_query_components':{k:p95([x[k] for x in rows]) for k in rows[0] if k!='query_id'},'qualification':'Component percentiles are separate distributions and must not be added. Eliminating compilation assumes all other per-query timings remain unchanged; it is a mathematical sensitivity, not an executable optimization or achieved latency.'}
publishers={}
for name,run in [('overlap','ashlar_queue_resume_r192'),('serial','ashlar_queue_serial_r197')]:
 root='out/native/'+run+'/';s=read(root+'audited-summary.json');batch=s['batches'][0];start=s['clock_epoch']+batch['publisher_start_offset_s'];end=s['clock_epoch']+batch['verified_manifest_offset_s'];spans=[];labels=[]
 for lane in ['', 'journal-lane/','apply-lane/']:
  path=root+lane+'statements.jsonl';paths.append(path)
  for r in map(json.loads,(B/path).read_text().splitlines()):
   a=max(start,r['start_epoch']);z=min(end,r['start_epoch']+r['wall_ms']/1000)
   if z>a:spans.append((a,z));labels.append({'label':r['label'],'caller_ms':r['wall_ms'],'clipped_inside_publication_ms':(z-a)*1000})
 merged=[]
 for a,z in sorted(spans):
  if merged and a<=merged[-1][1]:merged[-1][1]=max(merged[-1][1],z)
  else:merged.append([a,z])
 covered=sum(z-a for a,z in merged);processing=batch['processing_s'];assert 0<=covered<=processing+.01
 publishers[name]={'processing_s':processing,'union_of_timed_sql_call_intervals_s':covered,'not_covered_by_sql_calls_s':processing-covered,'covered_intervals_relative_s':[[a-start,z-start] for a,z in merged],'timed_calls':labels,'qualification':'Coverage uses local submission epoch plus monotonic call duration against the controller epoch anchor; assumes no local wall-clock jump. Uncovered time includes history/telemetry, Python work and controller gaps, not a measured removable overhead. Never subtract it to advertise publication freshness. Overlapping SQL durations are unioned rather than summed.'}
out={'kind':'Saved-evidence decomposition and counterfactual sensitivity, not native workload','reads':read_s,'publishers':publishers,'source_sha256':{p:hashlib.sha256((B/p).read_bytes()).hexdigest() for p in sorted(set(paths))},'next_action':'Measure a concrete compile/caller improvement and separately instrument publisher telemetry before any new full publisher; preserve exact checks and all original gates.'}
(B/'out/performance-attribution-r204.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'reads':{p:x['p95_of_per_query_components'] for p,x in read_s.items()},'publishers':{p:{k:x[k] for k in ['processing_s','union_of_timed_sql_call_intervals_s','not_covered_by_sql_calls_s']} for p,x in publishers.items()}},indent=2))

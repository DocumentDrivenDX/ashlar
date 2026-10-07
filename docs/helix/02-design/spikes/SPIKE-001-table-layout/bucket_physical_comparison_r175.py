"""One consistent cache/I/O qualification across all four matched read phases."""
import json,math
from pathlib import Path
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_full_reads_r175'
def p95(a):return sorted(a)[math.ceil(.95*len(a))-1]
phases={};total=0
for run,phase in [('r170','before-initial'),('r171','before-repeat'),('r173','after-initial'),('r175','after-repeat')]:
 d=B/('out/native/ashlar_bucket_full_reads_'+run);s=json.loads((d/'summary.json').read_text());h={q['query_id']:q for q in json.loads((d/'query-history.json').read_text())};records=[json.loads(x) for x in (d/'statements.jsonl').read_text().splitlines()]
 assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in records)
 subsets={}
 for layout in ('lc','part'):
  rs=[r for r in records if r['label'].startswith(layout+'-read-')];assert len(rs)==30 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
  cohorts={'all':rs,'no-remote':[r for r in rs if h[r['statement_id']]['metrics']['read_remote_bytes']==0],'remote':[r for r in rs if h[r['statement_id']]['metrics']['read_remote_bytes']>0]}
  subsets[layout]={name:{'n':len(a),'caller_p95_ms':p95([r['wall_ms'] for r in a]) if len(a)>=20 else None,'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in a]) if len(a)>=20 else None,'qualification':'Observed subset; p95 withheld below20samples. This cache-condition subset is descriptive, not an independent or graph-wide SLA.'} for name,a in cohorts.items()}
 phases[phase]={'owned_version':s['version'],'full_metrics':s['reads'],'cache_condition_subsets':subsets,'costs':s['costs']};total+=s['costs']['read_bytes']
z=json.loads((B/'out/native/ashlar_bucket_zorder_r172/audited-summary.json').read_text())
summary={'state':'Actual full20M ZORDER and four matched60-read phases qualified; caller target still missed','table':z['table'],'id':z['id'],'versions':{'before':4,'after':8},'phases':phases,'rewrite_footprint':z['rewrite_footprint'],'reader_phase_total_read_bytes':total,'maintenance_reported_costs':z['costs'],'maintenance_read_qualification':z['actual_read_cost_qualification'],'preservation':'Input4 full20M20-field equality passed r169. Output8 globalIDs/bucket invariant and120 after-maintenance full-carrier reads exact; full-wide output8 obligation remains open. Derived hash/bucket never replaces full native tuple. Canonical r139 pins unchanged.','qualification':'Canonical E23 is existing LC with DVs and fragmented hot files; bucket4 was a consolidated copy. Comparisons are operational, not isolated causal partitioning effects. ZORDER comparison is same owned table/keys but sequential cache/load varies. No controlled cold, sustained/burst freshness, generic source authority or1B/5B admission. Remote subsets are not cold-only because cached bytes also occur. No-remote28 after-repeat engine99ms/caller416.783ms supports a scoped warm-engine observation, not goal completion.','next':'Matched100k high-entropy updates and full-wide final-state preservation under bounded costs; retain owned8 for these phases.'}
(O/'comparison-summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'phases':{p:v['cache_condition_subsets']['part'] for p,v in phases.items()},'read_bytes':total},indent=2))

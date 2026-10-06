"""Fused relational admission, followed only by admitted pinned traversal."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
BASE=Path(__file__).resolve().parent
out=BASE/'out/native/ashlar_fused_guard_20261005_m3'
c=DriverClient(out)
A='client_dev.ashlar_budget_20261005_m1.adjacency VERSION AS OF 0'
D='client_dev.ashlar_budget_20261005_m1.degree VERSION AS OF 0'
N='client_dev.ashlar_scale_20261005_i1.object_current VERSION AS OF 2'
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
results=[]
for rep in range(21):
 for shape,root in [('regular',1+(rep*7919)%180000),('hub',200001),('isolate',9999999),('missing',20000001)]:
  started=time.perf_counter()
  rows=c.sql(f'{shape}-admission-{rep}',f"""WITH node AS (SELECT count(*) n FROM {N} WHERE source_system='pilot' AND type_id=1 AND id={root}), first AS (SELECT coalesce(max(out_degree),0) degree FROM {D} WHERE source_system='pilot' AND rel_type_id=7 AND source_type=1 AND source_id={root}), expansion AS (SELECT coalesce(sum(cast(d.out_degree AS DECIMAL(38,0))),0) degree FROM {A} a LEFT JOIN {D} d ON a.source_system=d.source_system AND a.target_type=d.source_type AND a.target_id=d.source_id AND d.rel_type_id=7 WHERE a.source_system='pilot' AND a.rel_type_id=7 AND a.source_type=1 AND a.source_id={root}) SELECT node.n,first.degree,cast(first.degree AS DECIMAL(38,0))+expansion.degree FROM node CROSS JOIN first CROSS JOIN expansion""")
  n,first,work=map(int,rows[0]);paths=[]
  status='NOT_FOUND' if n==0 else 'INTEGRITY_FAILURE' if n!=1 else 'QUERY_BUDGET_EXCEEDED' if work>100000 else 'OK'
  if status=='OK' and first:
   paths=c.sql(f'{shape}-paths-{rep}',f"SELECT a.id,b.id,a.target_id,b.target_id FROM {A} a JOIN {A} b ON a.source_system=b.source_system AND a.target_type=b.source_type AND a.target_id=b.source_id WHERE a.source_system='pilot' AND a.rel_type_id=7 AND b.rel_type_id=7 AND a.source_type=1 AND a.source_id={root} ORDER BY a.id,b.id")
  if shape=='regular':assert status=='OK' and work==30 and len(paths)==25 and len({tuple(p[:2]) for p in paths})==25
  elif shape=='hub':assert status=='QUERY_BUDGET_EXCEEDED' and work==120006 and not paths
  elif shape=='isolate':assert status=='OK' and work==0 and not paths
  else:assert status=='NOT_FOUND' and not paths
  results.append(dict(shape=shape,rep=rep,root=root,status=status,work=work,paths=len(paths),wall_ms=(time.perf_counter()-started)*1000))
 if rep%5==0:print('round',rep,flush=True)
c.history();c.close()
(out/'results.json').write_text(json.dumps(results,indent=2)+'\n')
(out/'scope.json').write_text(json.dumps(dict(state='completed',node_version=2,adjacency_version=0,degree_version=0,budget='logical first plus second edge expansions, not physical scans',limitations='Static degree snapshot; one source/type/relation; admission may scan edges even for refused roots; no maintained publication or billion-scale claim'),indent=2)+'\n')
for shape in ['regular','hub','isolate','missing']:
 vals=sorted(r['wall_ms'] for r in results if r['shape']==shape and r['rep']>0)
 print(shape,'caller_p95_ms',round(vals[18],2),flush=True)

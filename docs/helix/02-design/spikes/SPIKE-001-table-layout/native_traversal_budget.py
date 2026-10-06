"""Snapshot-pinned, client-coordinated traversal budget; no physical scan-cap claim."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
BASE=Path(__file__).resolve().parent;F='client_dev.ashlar_budget_20261005_m1';out=BASE/'out/native/ashlar_budget_20261005_m1';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar synthetic traversal budget and degree skew'")
c.sql('adjacency',f"CREATE TABLE {F}.adjacency USING DELTA CLUSTER BY (source_system,rel_type_id,source_type,source_id) AS SELECT * FROM client_dev.ashlar_edge_ingest_20261005_l1.adjacency VERSION AS OF 0 UNION ALL SELECT 'pilot',7,id,1,200001,1,id-1000000 FROM range(1000001,1020002)")
c.sql('optimize',f'OPTIMIZE {F}.adjacency FULL')
v=int(c.sql('version',f'DESCRIBE HISTORY {F}.adjacency LIMIT 1')[0][0])
# Degree is derived from this exact adjacency snapshot, not a mutable cache.
c.sql('degree',f"CREATE TABLE {F}.degree USING DELTA CLUSTER BY (source_system,rel_type_id,source_type,source_id) AS SELECT source_system,rel_type_id,source_type,source_id,count(*) out_degree FROM {F}.adjacency VERSION AS OF {v} GROUP BY ALL")
dv=int(c.sql('degree-version',f'DESCRIBE HISTORY {F}.degree LIMIT 1')[0][0])
A=f'{F}.adjacency VERSION AS OF {v}';D=f'{F}.degree VERSION AS OF {dv}'
assert c.sql('hub-degree',f"SELECT out_degree FROM {D} WHERE source_id=200001")==[['20001']]
results=[]
for root in [1,200001,9999999]:
 start=time.perf_counter()
 # This fixture's vertex existence is checked against its pinned node snapshot.
 assert c.sql(f'node-{root}',f"SELECT id FROM client_dev.ashlar_scale_20261005_i1.object_current VERSION AS OF 2 WHERE source_system='pilot' AND type_id=1 AND id={root}")==[[str(root)]]
 degree=c.sql(f'degree-{root}',f"SELECT out_degree FROM {D} WHERE source_system='pilot' AND rel_type_id=7 AND source_type=1 AND source_id={root}")
 first_degree=int(degree[0][0]) if degree else 0
 if first_degree>100000:status='QUERY_BUDGET_EXCEEDED';estimated=first_degree;paths=[]
 else:
  # Count every first edge, including parallel edges, times its target degree.
  estimate=c.sql(f'estimate-{root}',f"SELECT cast({first_degree} AS DECIMAL(38,0))+coalesce(sum(cast(d.out_degree AS DECIMAL(38,0))),0) FROM {A} a LEFT JOIN {D} d ON a.source_system=d.source_system AND a.target_type=d.source_type AND a.target_id=d.source_id AND d.rel_type_id=7 WHERE a.source_system='pilot' AND a.rel_type_id=7 AND a.source_type=1 AND a.source_id={root}")
  estimated=int(estimate[0][0])
  if estimated>100000:status='QUERY_BUDGET_EXCEEDED';paths=[]
  else:
   first=c.sql(f'frontier-{root}',f"SELECT id,target_type,target_id FROM {A} WHERE source_system='pilot' AND rel_type_id=7 AND source_type=1 AND source_id={root} ORDER BY id LIMIT 100001")
   assert len(first)==first_degree and len(first)<=100000
   targets=sorted({int(r[2]) for r in first});second=[]
   if targets:second=c.sql(f'second-{root}',f"SELECT id,source_id,target_id FROM {A} WHERE source_system='pilot' AND rel_type_id=7 AND source_type=1 AND source_id IN ("+','.join(map(str,targets))+') ORDER BY id LIMIT 100001')
   assert len(second)<=100000
   by_source={}
   for b in second:by_source.setdefault(b[1],[]).append(b)
   paths=[(a[0],b[0],a[2],b[2]) for a in first for b in by_source.get(a[2],[])]
   assert len(first)+len(paths)==estimated
   status='OK'
 result={'root':root,'status':status,'estimated_candidate_expansions':estimated,'path_count':len(paths),'wall_ms':(time.perf_counter()-start)*1000};results.append(result);print(json.dumps(result),flush=True)
assert results[0]['status']=='OK' and results[0]['path_count']==25 and results[0]['estimated_candidate_expansions']==30
assert results[1]['status']=='QUERY_BUDGET_EXCEEDED' and results[1]['estimated_candidate_expansions']==120006
assert results[2]['status']=='OK' and results[2]['path_count']==0
assert not any(r['label'] in ['frontier-200001','second-200001'] for r in c.records)
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'completed','adjacency_version':v,'degree_version':dv,'budget':100000,'budget_metric':'first-edge count plus multiplicity-preserving second-edge expansions, not physical rows scanned','results':results,'scope':'single source/type/relationship; static pinned degree; client orchestration; no physical scan cap, maintained degree publication, engine integration, p95 or billion-scale admission'},indent=2)+'\n')

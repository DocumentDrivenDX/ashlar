"""Server-side snapshot-pinned logical expansion guard; no physical scan cap."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
BASE=Path(__file__).resolve().parent;out=BASE/'out/native/ashlar_guard_script_20261005_m2';c=DriverClient(out)
A='client_dev.ashlar_budget_20261005_m1.adjacency VERSION AS OF 0';D='client_dev.ashlar_budget_20261005_m1.degree VERSION AS OF 0';N='client_dev.ashlar_scale_20261005_i1.object_current VERSION AS OF 2'
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
def marker(status):return f"SELECT '{status}' status, v_work candidate_expansions,cast(NULL AS BIGINT) first_edge,cast(NULL AS BIGINT) second_edge,cast(NULL AS BIGINT) mid_node,cast(NULL AS BIGINT) end_node;"
results=[]
for rep in range(21):
 for shape,root in [('regular',1+(rep*7919)%180000),('hub',200001),('isolate',9999999),('missing',20000001)]:
  statement=f"""BEGIN
  DECLARE v_node_count BIGINT DEFAULT 0;
  DECLARE v_first BIGINT DEFAULT 0;
  DECLARE v_work DECIMAL(38,0) DEFAULT 0;
  SET v_node_count=(SELECT count(*) FROM {N} WHERE source_system='pilot' AND type_id=1 AND id={root});
  IF v_node_count=0 THEN {marker('NOT_FOUND')}
  ELSEIF v_node_count<>1 THEN {marker('INTEGRITY_FAILURE')}
  ELSE
   SET v_first=coalesce((SELECT out_degree FROM {D} WHERE source_system='pilot' AND rel_type_id=7 AND source_type=1 AND source_id={root}),0);
   SET v_work=cast(v_first AS DECIMAL(38,0));
   IF v_work>100000 THEN {marker('QUERY_BUDGET_EXCEEDED')}
   ELSE
    SET v_work=v_work+(SELECT coalesce(sum(cast(d.out_degree AS DECIMAL(38,0))),0) FROM {A} a LEFT JOIN {D} d ON a.source_system=d.source_system AND a.target_type=d.source_type AND a.target_id=d.source_id AND d.rel_type_id=7 WHERE a.source_system='pilot' AND a.rel_type_id=7 AND a.source_type=1 AND a.source_id={root});
    IF v_work>100000 THEN {marker('QUERY_BUDGET_EXCEEDED')}
    ELSEIF v_first=0 THEN {marker('OK')}
    ELSE
     SELECT 'OK' status,v_work candidate_expansions,a.id first_edge,b.id second_edge,a.target_id mid_node,b.target_id end_node FROM {A} a JOIN {A} b ON a.source_system=b.source_system AND a.target_type=b.source_type AND a.target_id=b.source_id WHERE a.source_system='pilot' AND a.rel_type_id=7 AND b.rel_type_id=7 AND a.source_type=1 AND a.source_id={root} ORDER BY a.id,b.id;
    END IF;
   END IF;
  END IF;
  END"""
  rows=c.sql(f'{shape}-{rep}',statement)
  if shape=='regular':assert len(rows)==25 and all(r[0]=='OK' and r[1]=='30' for r in rows) and len({(r[2],r[3]) for r in rows})==25
  elif shape=='hub':assert rows==[['QUERY_BUDGET_EXCEEDED','120006',None,None,None,None]]
  elif shape=='isolate':assert rows==[['OK','0',None,None,None,None]]
  else:assert rows==[['NOT_FOUND','0',None,None,None,None]]
  results.append({'shape':shape,'rep':rep,'root':root,'status':rows[0][0],'path_rows':sum(r[2] is not None for r in rows),'wall_ms':c.records[-1]['wall_ms']})
 if rep%5==0:print('round',rep,flush=True)
c.history();c.close();(out/'results.json').write_text(json.dumps(results,indent=2)+'\n');(out/'scope.json').write_text(json.dumps({'state':'completed','adjacency_version':0,'degree_version':0,'node_version':2,'budget_metric':'first-edge count plus multiplicity-preserving second-edge expansions; not physical rows scanned','scope':'single source/type/relationship; static degree snapshot; native SQL scripting guard, no maintained degree publication/physical scan cap/external/billion-scale admission'},indent=2)+'\n');print('Server-side guard controls completed',flush=True)

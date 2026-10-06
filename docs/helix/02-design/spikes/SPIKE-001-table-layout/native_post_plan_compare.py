"""Matched fused post checks, changing only broadcast journal choice."""
import json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import post
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_post_plan_compare_20261005_r24';c=DriverClient(out)
q=f"SELECT HINT count(*),count(DISTINCT struct(s.source_system,s.rel_type_id,s.id)),count_if({post(3)}) FROM {F}.stage_r17_3 s JOIN {F}.edge_current VERSION AS OF 16 e ON s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id LEFT JOIN {F}.property_journal VERSION AS OF 17 j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-fused300k' AND j.source_position=3"
variants={'both':'/*+ BROADCAST(s,j) */','stage_only':'/*+ BROADCAST(s), MERGE(j) */'}
for name,hint in variants.items():
 rows=c.sql('explain-'+name,'EXPLAIN FORMATTED '+q.replace('HINT',hint));(out/('plan-'+name+'.txt')).write_text('\n'.join(str(x[0]) for x in rows))
results=[]
for round in range(2):
 for name in (['both','stage_only'] if round==0 else ['stage_only','both']):
  assert c.sql(f'{name}-{round}',q.replace('HINT',variants[name]))==[['300000','300000','0']]
  results.append({'variant':name,'round':round,'wall_ms':c.records[-1]['wall_ms'],'statement_id':c.records[-1]['statement_id']})
(out/'summary.json').write_text(json.dumps({'state':'passed','checks':results,'scope':'four matched cached pinned post checks; initial plans only; no uncommitted transaction or integrated throughput admission'},indent=2));c.history();c.close();print('Matched post plans passed')

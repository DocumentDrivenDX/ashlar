"""Count targeted optimizer statistics maintenance and matched pinned validation calls."""
import json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import post
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_optimizer_stats_20261005_r25';c=DriverClient(out)
q=f"SELECT /*+ BROADCAST(s,j) */ count(*),count(DISTINCT struct(s.source_system,s.rel_type_id,s.id)),count_if({post(3)}) FROM {F}.stage_r17_3 s JOIN {F}.edge_current VERSION AS OF 16 e ON s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id LEFT JOIN {F}.property_journal VERSION AS OF 17 j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-fused300k' AND j.source_position=3"
results=[]
for phase in ['before','after']:
 if phase=='after':
  for table,cols in [('edge_current','source_system,rel_type_id,id,lookup_hash'),('property_journal','source_system,type_id,id,source_feed,source_epoch,source_position')]:c.sql('analyze-'+table,f'ANALYZE TABLE {F}.{table} COMPUTE STATISTICS FOR COLUMNS {cols}')
 rows=c.sql('plan-'+phase,'EXPLAIN FORMATTED '+q);(out/('plan-'+phase+'.txt')).write_text('\n'.join(str(x[0]) for x in rows))
 for rep in range(2):
  assert c.sql(f'check-{phase}-{rep}',q)==[['300000','300000','0']];results.append({'phase':phase,'rep':rep,'wall_ms':c.records[-1]['wall_ms']})
(out/'summary.json').write_text(json.dumps({'state':'passed','checks':results,'maintenance':[{'label':x['label'],'wall_ms':x['wall_ms']} for x in c.records if x['label'].startswith('analyze-')],'scope':'two checks per phase, pinned same snapshots; targeted catalog optimizer statistics, not Delta skipping stats; no integrated rate admission'},indent=2));c.history();c.close();print('Optimizer statistics comparison passed')

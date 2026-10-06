"""Read-only current statistics versus historical-snapshot plan inspection."""
import json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import post
B=Path(__file__).resolve().parent
F='client_dev.ashlar_composite_edges_20261005_q1'
out=B/'out/native/ashlar_stats_inspect_20261005_r26'
c=DriverClient(out)
for table in ['edge_current','property_journal']:
 rows=c.sql('stats-'+table,f'SHOW STATISTICS {F}.{table} FOR ALL COLUMNS AS JSON')
 (out/(table+'-statistics.json')).write_text(json.dumps(rows,indent=2))
 rows=c.sql('history-'+table,f'DESCRIBE HISTORY {F}.{table} LIMIT 1')
 (out/(table+'-latest.json')).write_text(json.dumps(rows,indent=2))
for mode in ['current','pinned']:
 ev=' VERSION AS OF 16' if mode=='pinned' else ''
 jv=' VERSION AS OF 17' if mode=='pinned' else ''
 q=f"SELECT /*+ BROADCAST(s,j) */ count(*),count(DISTINCT struct(s.source_system,s.rel_type_id,s.id)),count_if({post(3)}) FROM {F}.stage_r17_3 s JOIN {F}.edge_current{ev} e ON s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id LEFT JOIN {F}.property_journal{jv} j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-fused300k' AND j.source_position=3"
 rows=c.sql('plan-'+mode,'EXPLAIN FORMATTED '+q)
 (out/('plan-'+mode+'.txt')).write_text('\n'.join(str(x[0]) for x in rows))
c.history();c.close()
(out/'summary.json').write_text(json.dumps({'state':'passed','scope':'read-only catalog statistics and initial plans, no execution timing or changed graph rows'},indent=2))
print('Statistics inspection completed')

"""Exact inherited journal multiset and publication metadata verification."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_lc64_20261005_r42'
run=json.loads((B/'out/native/ashlar_lc64_guarded_apply_20261005_r45/summary.json').read_text());assert run['state']=='completed'
out=B/'out/native/ashlar_lc64_history_verify_20261006_r47';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
progress={k:{'epoch':'e','position':1} for k in ['fixture-multi','fixture-multi-edge','fixture-edge-structure','fixture-recovery','fixture-fenced','fixture-file-target','fixture-layout-compare']}
progress.update({'fixture-scheduled-fenced':{'epoch':'e','position':4},'fixture-broadcast300k':{'epoch':'e','position':3},'fixture-fused300k':{'epoch':'e','position':3},'fixture-maintained':{'epoch':'e','position':2}})
revisions={'pilot:'+str(i):'r1' for i in range(5)}
for result in run['batches']:
 layout=result['layout'];ns=F if layout=='lc16' else N;v=result['versions'][ns+'.property_journal']
 prior=f'SELECT * FROM {F}.property_journal VERSION AS OF 20'
 after=f"SELECT * FROM {ns}.property_journal VERSION AS OF {v} WHERE source_feed IS DISTINCT FROM 'fixture-file-target'"
 assert c.sql('inherited-count-'+layout,'SELECT count(*) FROM ('+after+')')==[['3720023']]
 assert c.sql('inherited-exact-'+layout,f'SELECT count(*) FROM (({prior} EXCEPT ALL {after}) UNION ALL ({after} EXCEPT ALL {prior}))')==[['0']]
 rows=c.sql('descriptor-'+layout,f"SELECT profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json,recorded_at FROM {N}.manifest_r45 WHERE publication_id='r45-{layout}'")
 assert len(rows)==1 and rows[0][0]=='ashlar-delta/0.1-spike' and json.loads(rows[0][1])==result['versions'] and json.loads(rows[0][2])==progress and json.loads(rows[0][3])==revisions and json.loads(rows[0][4])=={'pending_and_exact_carriers':'passed'} and rows[0][5] is not None
 rows=c.sql('receipt-'+layout,f"SELECT stage_name,expected_count,progress_json,revisions_json FROM {N}.receipt_r45 WHERE layout='{layout}'")
 assert len(rows)==1 and rows[0][:2]==[N+'.stage_r44','300000'] and json.loads(rows[0][2])==progress and json.loads(rows[0][3])==revisions
 print(layout,'history and metadata passed',flush=True)
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'passed','inherited_rows_per_layout':3720023,'scope':'two-direction EXCEPT ALL exact inherited journal multiset; complete vector/progress/revisions/report/profile/timestamp descriptors and durable receipts; not performance, permissions or external-source semantic admission'},indent=2));print('History and publication metadata verification passed',flush=True)

"""Independent final carrier/history/descriptor verification of maintained schedules."""
import argparse,json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS,post
p=argparse.ArgumentParser();p.add_argument('layout',choices=['lc','partition']);layout=p.parse_args().layout
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_partition_zorder_20261005_r29';ns=F if layout=='lc' else N
run=json.loads((B/('out/native/ashlar_maintained_schedule_20261005_r40_'+layout)/'summary.json').read_text());assert run['state']=='completed'
initial=json.loads((B/'out/native/ashlar_partition_maintenance_publish_20261005_r38/summary.json').read_text());base=next(r['versions'] for r in initial['publications'] if r['layout']==layout)
out=B/('out/native/ashlar_maintained_final_verify_20261005_r41_'+layout);c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
final=run['batches'][-1]['versions'];ev=final[ns+'.edge_current'];jv=final[ns+'.property_journal'];checks=' OR '.join(f'a.{x} IS DISTINCT FROM b.{x}' for x in COLS)
assert c.sql('untouched',f"WITH a AS (SELECT * FROM {ns}.edge_current VERSION AS OF {base[ns+'.edge_current']} WHERE id NOT BETWEEN 240001 AND 300000),b AS (SELECT * FROM {ns}.edge_current VERSION AS OF {ev} WHERE id NOT BETWEEN 240001 AND 300000) SELECT count(*),count_if(a.id IS NULL),count_if(b.id IS NULL),count_if({checks}) FROM a FULL OUTER JOIN b ON a.source_system=b.source_system AND a.rel_type_id=b.rel_type_id AND a.id=b.id")==[['9419981','0','0','0']]
assert c.sql('unique',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {ns}.edge_current VERSION AS OF {ev}')==[['10019981','10019981']]
for result in run['batches']:
 batch=result['batch'];stage=N+'.stage_r39_'+str(batch);v=result['versions'];guard=post(batch).replace("'fixture-fused300k'","'fixture-maintained'").replace('IS DISTINCT FROM 7','IS DISTINCT FROM 9').replace(f"'synthetic-fused:{batch}'",f"'synthetic-maintained:{batch}'")
 for snapshot,edge,journal in [('published',v[ns+'.edge_current'],v[ns+'.property_journal']),('final',ev,jv)]:
  assert c.sql('changed-'+str(batch)+'-'+snapshot,f"SELECT /*+ BROADCAST(s,j) */ count(*),count(DISTINCT struct(s.source_system,s.rel_type_id,s.id)),count_if({guard}) FROM {stage} s JOIN {ns}.edge_current VERSION AS OF {edge} e ON s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id LEFT JOIN {ns}.property_journal VERSION AS OF {journal} j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-maintained' AND j.source_position={batch}")==[['300000','300000','0']]
 progress={k:{'epoch':'e','position':1} for k in ['fixture-multi','fixture-multi-edge','fixture-edge-structure','fixture-recovery','fixture-fenced','fixture-layout-compare']};progress.update({'fixture-scheduled-fenced':{'epoch':'e','position':4},'fixture-broadcast300k':{'epoch':'e','position':3},'fixture-fused300k':{'epoch':'e','position':3},'fixture-maintained':{'epoch':'e','position':batch}});revs={'pilot:'+str(i):'r1' for i in range(5)}
 rows=c.sql('descriptor-'+str(batch),f"SELECT profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json,recorded_at FROM {N}.manifest_r32 WHERE publication_id='r40-{layout}-{batch}'")
 assert len(rows)==1 and rows[0][0]=='ashlar-delta/0.1-spike' and json.loads(rows[0][1])==v and json.loads(rows[0][2])==progress and json.loads(rows[0][3])==revs and json.loads(rows[0][4])=={'maintenance_and_exact_changed_carriers':'passed'} and rows[0][5] is not None
 rows=c.sql('receipt-'+str(batch),f'SELECT stage_name,expected_count,progress_json,revisions_json FROM {N}.receipt_r40_{layout} WHERE batch={batch}')
 assert len(rows)==1 and rows[0][:2]==[stage,'300000'] and json.loads(rows[0][2])==progress and json.loads(rows[0][3])==revs
prior=f"SELECT * FROM {ns}.property_journal VERSION AS OF {base[ns+'.property_journal']}";after=f"SELECT * FROM {ns}.property_journal VERSION AS OF {jv} WHERE source_feed IS DISTINCT FROM 'fixture-maintained'"
assert c.sql('inherited-count','SELECT count(*) FROM ('+after+')')==[['3120023']]
assert c.sql('inherited-exact',f'SELECT count(*) FROM (({prior} EXCEPT ALL {after}) UNION ALL ({after} EXCEPT ALL {prior}))')==[['0']]
assert c.sql('new-origins',f"SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {ns}.property_journal VERSION AS OF {jv} WHERE source_feed='fixture-maintained'")==[['600000','600000']]
if layout=='partition':assert c.sql('buckets',f'SELECT count_if(lookup_bucket IS DISTINCT FROM (CAST(conv(substr(lookup_hash,1,1),16,10) AS INT) DIV 4)) FROM {ns}.edge_current VERSION AS OF {ev}')==[['0']]
assert c.sql('barrier',f"SELECT pending,sequence FROM {N}.barrier_r32 WHERE stream='{layout}' AND epoch=1 AND owner='publisher'")==[[None,'8']]
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'passed','layout':layout,'versions':final,'scope':'9,419,981 untouched canonical rows, 600k changed/journal records at own and final vectors, typed identities, 3,120,023 inherited journal multiset, full descriptors/receipts and cleared barrier; no performance admission'},indent=2));print(layout,'final verification passed',flush=True)

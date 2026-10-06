"""Terminal full preservation and accepted descriptor checks for the two-batch r23 clock."""
import json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS,post
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';s=json.loads((B/'out/native/ashlar_barrier_graph_20261005_r23/summary.json').read_text());assert s['state']=='completed';out=B/'out/native/ashlar_barrier_graph_verify_20261005_r23';c=DriverClient(out);final=s['batches'][-1]['versions'];ev=final[F+'.edge_current'];jv=final[F+'.property_journal'];key=lambda a,b:' AND '.join(f'{a}.{x}={b}.{x}' for x in ['source_system','rel_type_id','id'])
for b in s['batches']:
 i=b['batch'];stage=F+'.stage_r17_'+str(i)
 assert c.sql('exact-post-'+str(i),f"SELECT /*+ BROADCAST(s,j) */ count(*),count(DISTINCT struct(s.source_system,s.rel_type_id,s.id)),count_if({post(i)}) FROM {stage} s JOIN {F}.edge_current VERSION AS OF {ev} e ON {key('s','e')} LEFT JOIN {F}.property_journal VERSION AS OF {jv} j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-fused300k' AND j.source_position={i}")==[['300000','300000','0']]
 r=c.sql('descriptor-'+str(i),f"SELECT table_versions_json,source_progress_json,schema_revisions_json FROM {F}.manifest_r11 WHERE publication_id='r23-{i}'");assert len(r)==1 and json.loads(r[0][0])==b['versions'];progress=json.loads(r[0][1]);assert progress['fixture-fused300k']=={'epoch':'e','position':i} and progress['fixture-scheduled-fenced']=={'epoch':'e','position':4} and progress['fixture-broadcast300k']=={'epoch':'e','position':3};assert json.loads(r[0][2])=={'pilot:'+str(k):'r1' for k in range(5)}
assert c.sql('all-identities',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {F}.edge_current VERSION AS OF {ev}')==[['10019981','10019981']]
stages=' UNION ALL '.join(f'SELECT source_system,rel_type_id,id FROM {F}.stage_r17_{i}' for i in [2,3]);diff=' OR '.join(f'o.{x} IS DISTINCT FROM e.{x}' for x in COLS)
assert c.sql('untouched-full17',f'WITH stages AS ({stages}) SELECT count(*),count_if({diff}) FROM {F}.edge_current VERSION AS OF 14 o JOIN {F}.edge_current VERSION AS OF {ev} e ON {key("o","e")} LEFT ANTI JOIN stages t ON {key("o","t")}')==[['9419981','0']]
assert c.sql('origins',f"SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {F}.property_journal VERSION AS OF {jv} WHERE source_feed='fixture-fused300k' AND source_position IN (2,3)")==[['600000','600000']]
assert c.sql('barrier-cleared',f"SELECT pending,sequence FROM {F}.barrier_r23 WHERE stream='graph'")==[[None,'4']]
assert c.sql('receipts',f'SELECT batch,expected_count FROM {F}.receipt_r23 ORDER BY batch')==[['2','300000'],['3','300000']]
c.sql('final-detail',f'DESCRIBE DETAIL {F}.edge_current');(out/'summary.json').write_text(json.dumps({'state':'passed','changed_journal_full':600000,'untouched_full17':9419981,'accepted_descriptors':2,'pending_cleared':True,'scope':'final full preservation/readback; no original-r17-clock repair or concurrent-reader/scale admission'},indent=2));c.history();c.close();print('Integrated barrier graph preservation passed')

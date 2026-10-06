"""Independent paired 64/16MiB canonical and vector verification."""
import json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS,post
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_lc64_20261005_r42';stage=N+'.stage_r44'
run=json.loads((B/'out/native/ashlar_lc64_guarded_apply_20261005_r45/summary.json').read_text());assert run['state']=='completed'
out=B/'out/native/ashlar_lc64_apply_verify_20261005_r46';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
guard=post(1).replace("'fixture-fused300k'","'fixture-file-target'").replace('IS DISTINCT FROM 7','IS DISTINCT FROM 10').replace("'synthetic-fused:1'","'synthetic-file-target:1'")
key='s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id'
checks=' OR '.join(f'a.{x} IS DISTINCT FROM b.{x}' for x in COLS)
for result in run['batches']:
 layout=result['layout'];ns=F if layout=='lc16' else N;oldv=26 if layout=='lc16' else 0;ev=result['versions'][ns+'.edge_current'];jv=result['versions'][ns+'.property_journal']
 assert c.sql('changed-'+layout,f"SELECT /*+ BROADCAST(s,j) */ count(*),count(DISTINCT struct(s.source_system,s.rel_type_id,s.id)),count_if({guard}) FROM {stage} s JOIN {ns}.edge_current VERSION AS OF {ev} e ON {key} LEFT JOIN {ns}.property_journal VERSION AS OF {jv} j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-file-target' AND j.source_position=1")==[['300000','300000','0']]
 assert c.sql('untouched-'+layout,f"WITH a AS (SELECT * FROM {ns}.edge_current VERSION AS OF {oldv} WHERE id NOT BETWEEN 300001 AND 330000),b AS (SELECT * FROM {ns}.edge_current VERSION AS OF {ev} WHERE id NOT BETWEEN 300001 AND 330000) SELECT count(*),count_if(a.id IS NULL),count_if(b.id IS NULL),count_if({checks}) FROM a FULL OUTER JOIN b ON a.source_system=b.source_system AND a.rel_type_id=b.rel_type_id AND a.id=b.id")==[['9719981','0','0','0']]
 assert c.sql('unique-'+layout,f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {ns}.edge_current VERSION AS OF {ev}')==[['10019981','10019981']]
 assert c.sql('journal-'+layout,f"SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {ns}.property_journal VERSION AS OF {jv} WHERE source_feed='fixture-file-target'")==[['300000','300000']]
 rows=c.sql('manifest-'+layout,f"SELECT * FROM {N}.manifest_r45 WHERE publication_id='r45-{layout}'")
 assert len(rows)==1
 # Column names are read from this exact statement's result schema.
 d=dict(zip([x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']],rows[0]))
 assert any(isinstance(value,str) and value.startswith('{') and json.loads(value)==result['versions'] for value in d.values())
 print(layout,'verification passed',flush=True)
assert c.sql('barriers',f'SELECT stream,pending,sequence FROM {N}.barrier_r45 ORDER BY stream')==[['lc16',None,'2'],['lc64',None,'2']]
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'passed','scope':'300k changed/journal per layout, 9,719,981 untouched full carriers per layout, total unique typed keys, actual descriptor vectors and cleared barriers; inherited-journal parity after apply and sustained/maintenance performance separate'},indent=2));print('Paired LC64/16 verification passed',flush=True)

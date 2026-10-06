"""Exhaustive maintenance preservation and fenced equivalent-vector publication."""
import json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_partition_zorder_20261005_r29'
run=json.loads((B/'out/native/ashlar_partition_guarded_apply_20261005_r32/summary.json').read_text());m=json.loads((B/'out/native/ashlar_partition_maintenance_reads_20261005_r37/scope.json').read_text());assert run['state']=='completed' and m['state']=='completed'
out=B/'out/native/ashlar_partition_maintenance_publish_20261005_r38';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');results=[]
for result in run['batches']:
 layout=result['layout'];ns=F if layout=='lc' else N;before=result['versions'][ns+'.edge_current'];after=m['versions'][layout]
 assert int(c.sql('current-'+layout,f'DESCRIBE HISTORY {ns}.edge_current LIMIT 1')[0][0])==after
 c.sql('hold-'+layout,f"""BEGIN ATOMIC
 IF (SELECT count(*) FROM {N}.barrier_r32 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending IS NULL AND sequence=2)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='maintenance publication barrier mismatch'; END IF;
 UPDATE {N}.barrier_r32 SET pending=-38,sequence=sequence+1 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending IS NULL AND sequence=2; END""")
 cols=COLS+(['lookup_bucket'] if layout=='partition' else [])
 checks=' OR '.join(f'a.{x} IS DISTINCT FROM b.{x}' for x in cols)+ ' OR a.rid IS DISTINCT FROM b.rid OR a.rcv IS DISTINCT FROM b.rcv'
 q=f"WITH a AS (SELECT e.*,e._metadata.row_id rid,e._metadata.row_commit_version rcv FROM {ns}.edge_current VERSION AS OF {before} e),b AS (SELECT e.*,e._metadata.row_id rid,e._metadata.row_commit_version rcv FROM {ns}.edge_current VERSION AS OF {after} e) SELECT count(*),count_if(a.id IS NULL),count_if(b.id IS NULL),count_if({checks}) FROM a FULL OUTER JOIN b ON a.source_system=b.source_system AND a.rel_type_id=b.rel_type_id AND a.id=b.id"
 assert c.sql('full-parity-'+layout,q)==[['10019981','0','0','0']]
 for v in [before,after]:assert c.sql('unique-'+layout+'-'+str(v),f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {ns}.edge_current VERSION AS OF {v}')==[['10019981','10019981']]
 vector=dict(result['versions']);vector[ns+'.edge_current']=after
 c.sql('publish-'+layout,f"""BEGIN ATOMIC
 IF (SELECT count(*) FROM {N}.barrier_r32 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending=-38 AND sequence=3)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='maintenance hold mismatch'; END IF;
 UPDATE {N}.barrier_r32 SET sequence=sequence+1 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending=-38 AND sequence=3;
 IF (SELECT count(*) FROM {N}.manifest_r32 WHERE publication_id='r32-{layout}')<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='prior descriptor mismatch'; END IF;
 INSERT INTO {N}.manifest_r32 SELECT 'r38-{layout}',profile_version,'{json.dumps(vector,separators=(',',':'))}',source_progress_json,schema_revisions_json,'{{"exhaustive_maintenance_carriers_and_row_metadata":"passed"}}',current_timestamp() FROM {N}.manifest_r32 WHERE publication_id='r32-{layout}';
 UPDATE {N}.barrier_r32 SET pending=NULL WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending=-38; END""")
 rows=c.sql('descriptor-'+layout,f"SELECT table_versions_json,source_progress_json,schema_revisions_json FROM {N}.manifest_r32 WHERE publication_id IN ('r32-{layout}','r38-{layout}') ORDER BY publication_id")
 assert len(rows)==2 and json.loads(rows[1][0])==vector and rows[0][1:]==rows[1][1:]
 results.append({'layout':layout,'before':before,'after':after,'versions':vector});(out/'progress.json').write_text(json.dumps(results,indent=2));print(layout,'full parity and publication passed',flush=True)
assert c.sql('barriers',f'SELECT stream,pending,sequence FROM {N}.barrier_r32 ORDER BY stream')==[['lc',None,'4'],['partition',None,'4']]
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'passed','publications':results,'scope':'10,019,981 exact canonical+hidden metadata rows per layout, native unique keys and partition buckets; equivalent vectors published behind cooperative barrier with inherited progress/revisions; no sustained/caller/scale admission'},indent=2));print('Maintenance preservation and publication passed',flush=True)

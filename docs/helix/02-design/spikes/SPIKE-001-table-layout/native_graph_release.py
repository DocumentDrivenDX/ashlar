"""Validate and publish an explicit synthetic node/edge version vector; no graph-engine claim."""
import json
from pathlib import Path
from driver_sql import DriverClient
BASE=Path(__file__).resolve().parent;out=BASE/'out/native/ashlar_graph_release_20261005_l2';c=DriverClient(out)
N='client_dev.ashlar_scale_20261005_i1.object_current';F='client_dev.ashlar_edge_ingest_20261005_l1'
versions={N:2,F+'.edge_current':2,F+'.adjacency':0,F+'.property_journal':1}
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
c.sql('environment','SELECT current_version()')
assert c.sql('node-identity',f"SELECT count(*),count(DISTINCT concat('N',length(source_system),':',source_system,':',type_id,':',id)) FROM {N} VERSION AS OF 2")==[['10000000','10000000']]
assert c.sql('edge-identity',f"SELECT count(*),count(DISTINCT concat('E',length(source_system),':',source_system,':',rel_type_id,':',id)) FROM {F}.edge_current VERSION AS OF 2")==[['1000000','1000000']]
assert c.sql('typed-endpoints',f"SELECT count(*) FROM {F}.edge_current VERSION AS OF 2 e LEFT JOIN {N} VERSION AS OF 2 s ON e.source_system=s.source_system AND e.source_type=s.type_id AND e.source_id=s.id LEFT JOIN {N} VERSION AS OF 2 t ON e.source_system=t.source_system AND e.target_type=t.type_id AND e.target_id=t.id WHERE s.id IS NULL OR t.id IS NULL")==[['0']]
assert c.sql('adjacency-parity',f"SELECT count(*) FROM {F}.adjacency VERSION AS OF 0 a FULL OUTER JOIN {F}.edge_current VERSION AS OF 2 e ON a.source_system=e.source_system AND a.rel_type_id=e.rel_type_id AND a.id=e.id WHERE a.id IS NULL OR e.id IS NULL OR a.source_type<>e.source_type OR a.source_id<>e.source_id OR a.target_type<>e.target_type OR a.target_id<>e.target_id")==[['0']]
assert c.sql('parallel-pairs',f'SELECT count(*) FROM (SELECT source_system,rel_type_id,source_type,source_id,target_type,target_id,count(*) n FROM {F}.edge_current VERSION AS OF 2 GROUP BY ALL HAVING n=2)')==[['200000']]
assert c.sql('isolates',f"SELECT count(*) FROM {N} VERSION AS OF 2 n LEFT ANTI JOIN (SELECT source_system,source_type type_id,source_id id FROM {F}.edge_current VERSION AS OF 2 UNION SELECT source_system,target_type type_id,target_id id FROM {F}.edge_current VERSION AS OF 2) e ON n.source_system=e.source_system AND n.type_id=e.type_id AND n.id=e.id")==[['9800000']]
v=json.dumps(versions,separators=(',',':'));report=json.dumps({'nodes':10000000,'edges':1000000,'parallel_pairs':200000,'isolates':9800000,'typed_endpoint_mismatches':0,'adjacency_mismatches':0,'external_engines':'unexecuted'},separators=(',',':'))
c.sql('publish',f"INSERT INTO {F}.publication_manifest VALUES ('synthetic-graph-release-1','ashlar-delta/0.1-spike','{v}','{{\"S\":{{\"epoch\":\"e\",\"position\":1}}}}','{{\"pilot\":\"r1\"}}','{report}',current_timestamp())")
rows=c.sql('descriptor',f"SELECT table_versions_json,validation_report_json FROM {F}.publication_manifest WHERE publication_id='synthetic-graph-release-1'")
assert len(rows)==1 and json.loads(rows[0][0])==versions and json.loads(rows[0][1])==json.loads(report)
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'completed','versions':versions,'validation':json.loads(report),'scope':'synthetic snapshot descriptor across fixed existing tables; no source transaction/recovery/policy/external-engine/scale/freshness admission'},indent=2)+'\n');print('Explicit graph vector, typed endpoints, parallel edges and isolates verified',flush=True)

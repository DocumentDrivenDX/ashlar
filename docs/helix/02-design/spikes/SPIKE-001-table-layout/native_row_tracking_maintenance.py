"""Exact row metadata/content preservation across forced small-table reclustering."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';T=F+'.tracked_maintenance_r21';out=B/'out/native/ashlar_tracked_maintenance_20261005_r21';c=DriverClient(out)
payload='concat('+','.join(f"sha2(concat(cast(id AS STRING),':r21:{i}'),256)" for i in range(8))+')'
c.sql('ddl',f"CREATE TABLE {T} USING DELTA CLUSTER BY (id) TBLPROPERTIES ('delta.enableRowTracking'='true','delta.feature.catalogManaged'='supported','delta.targetFileSize'='16777216') AS SELECT id,'seed' batch,{payload} payload FROM range(1,10001)")
c.sql('update-one',f"UPDATE {T} SET batch='r21-1',payload='updated' WHERE id=1")
before=int(c.sql('before-version',f'DESCRIBE HISTORY {T} LIMIT 1')[0][0]);marker=c.sql('marker-before',f"SELECT _metadata.row_commit_version FROM {T} VERSION AS OF {before} WHERE id=1");assert marker==[[str(before)]]
c.sql('change-cluster',f'ALTER TABLE {T} CLUSTER BY (payload)');c.sql('forced-optimize',f'OPTIMIZE {T} FULL')
r=c.sql('history',f'DESCRIBE HISTORY {T} LIMIT 5');columns=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];latest=int(r[0][0]);opts=[x for x in r if x[columns.index('operation')]=='OPTIMIZE'];assert len(opts)>=1
metrics=json.loads(opts[0][columns.index('operationMetrics')]);removed=int(metrics.get('numRemovedFiles',0));added=int(metrics.get('numAddedFiles',0));assert removed>0 and added>0,metrics
q=f"SELECT count(*),count_if(o.id IS DISTINCT FROM n.id OR o.batch IS DISTINCT FROM n.batch OR o.payload IS DISTINCT FROM n.payload OR o.rid IS DISTINCT FROM n.rid OR o.rcv IS DISTINCT FROM n.rcv) FROM (SELECT id,batch,payload,_metadata.row_id rid,_metadata.row_commit_version rcv FROM {T} VERSION AS OF {before}) o FULL OUTER JOIN (SELECT id,batch,payload,_metadata.row_id rid,_metadata.row_commit_version rcv FROM {T} VERSION AS OF {latest}) n ON o.id=n.id"
assert c.sql('all-rows-metadata-exact',q)==[['10000','0']]
assert c.sql('marker-after',f'SELECT _metadata.row_commit_version FROM {T} VERSION AS OF {latest} WHERE id=1')==marker
(out/'summary.json').write_text(json.dumps({'state':'passed','before':before,'after':latest,'removed_files':removed,'added_files':added,'exact_rows':10000,'scope':'forced reclustering of private tracked table; selected UPDATE lineage; no large-scale, deletes/restore/clones or integrated throughput proof'},indent=2));c.history();c.close();print('Physical rewrite preserves row metadata')

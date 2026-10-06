"""Read-only current protocol/features of unchanged tiny release tables."""
import json,time
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_release_protocol_20261006_r83';c=Client(O);start=time.time()
release=json.loads((B/'out/native/ashlar_layout_v02_release_20261006_r68/summary.json').read_text());assert release['state']=='passed';result={}
for table,expected in release['projection_versions'].items():
 before=int(c.sql('before-'+table.split('.')[-1],f'DESCRIBE HISTORY {table} LIMIT 1')[0][0]);assert before==expected,'Release changed; current feature inspection cannot qualify historical version'
 rows=c.sql('detail-'+table.split('.')[-1],f'DESCRIBE DETAIL {table}');columns=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];detail=dict(zip(columns,rows[0]))
 after=int(c.sql('after-'+table.split('.')[-1],f'DESCRIBE HISTORY {table} LIMIT 1')[0][0]);assert after==before
 result[table]={'version':before,'min_reader_version':int(detail['minReaderVersion']),'min_writer_version':int(detail['minWriterVersion']),'features':json.loads(detail['tableFeatures']),'properties':json.loads(detail['properties']),'partition_columns':json.loads(detail['partitionColumns']),'clustering_columns':json.loads(detail.get('clusteringColumns') or '[]'),'files':int(detail['numFiles']),'stored_bytes':int(detail['sizeInBytes'])}
summary={'state':'observed','publication':release['publication'],'tables':result,'elapsed_seconds':time.time()-start,'scope':'Read-only unchanged latest-version protocol metadata for four existing tiny release tables. Metadata is not a connector negotiation or successful graph import. No file rewrite, feature disabling, export provisioning, ACL enforcement, performance or scale claim.','cost':'Twelve metadata reads on existing authorized warehouse; no writes/new compute; billing dollars unavailable.'}
(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({t:{'reader':d['min_reader_version'],'writer':d['min_writer_version'],'features':d['features']} for t,d in result.items()}),flush=True)

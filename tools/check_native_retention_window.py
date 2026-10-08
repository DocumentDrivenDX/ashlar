"""Observe a finite retention window for the previously verified CSV snapshots."""
import argparse,hashlib,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from databricks_transport import sql_result
from ashlar.native import _quoted
from ashlar.retention import observe_retention_configuration,publication_retention_report,validate_publication_retention
from ashlar.publication import Descriptor
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--replay-original',action='store_true',help='Recover interpretation from exact retained read-only receipts; no network calls')
p.add_argument('--output',type=Path,default=B/'out/native/csv_retention_window_20261008')
args=p.parse_args();OUT=args.output
if args.replay_original:
    receipt_bytes=(OUT/'statements.jsonl').read_bytes()
    original_records=[json.loads(line) for line in receipt_bytes.splitlines()]
    if len(original_records)!=17:raise ValueError('Complete original observation inventory required')
    remaining=iter(original_records)
else:
    if OUT.exists():raise ValueError('Preserve original observation; use a new iteration output')
    from persistent_sql import Client
    c=Client(OUT)
class Transport:
    def query(self,sql,parameters):
        native_parameters=[{'name':k,'type':'STRING','value':v} for k,v in parameters.items()] or None
        if args.replay_original:
            record=next(remaining)
            if record['sql']!=sql or record['parameters']!=native_parameters:raise ValueError('Original SQL/parameter custody differs')
        else:
            c.sql('finite-retention-observation',sql,parameters=native_parameters)
            record=c.records[-1]
        return sql_result(record['response'])
e=Transport()
original=json.loads((B/'out/native/csv_delta_full_parity_20261008/summary.json').read_text())
versions=original['version_vector'];uuids={row['table']:row['uuid'] for row in original['reports']}
observations={};stamps={};snapshots={}
for table,version in versions.items():
    observations[table]=observe_retention_configuration(e,table,uuids[table],defaults={'data_retention':'interval 7 days','log_retention':'interval 30 days'},default_profile='azure-databricks-delta-documented-defaults/2026-09-11')
    history=e.query('DESCRIBE HISTORY '+_quoted(table),{}).rows
    matching=[row for row in history if row['version']==str(version)]
    if len(matching)!=1:raise ValueError('Original snapshot commit unavailable')
    stamps['t'+str(len(stamps))]=matching[0]['timestamp']
params=dict(stamps)
query='SELECT current_timezone() AS timezone,cast(unix_micros(current_timestamp()) AS STRING) AS now_us,'+','.join('cast(unix_micros(cast(:'+name+' AS TIMESTAMP)) AS STRING) AS '+name for name in stamps)
clock=e.query(query,params).rows
if len(clock)!=1 or clock[0]['timezone'] not in ('UTC','Etc/UTC'):raise ValueError('Qualified fixed UTC SQL timestamp interpretation required')
for index,(table,version) in enumerate(versions.items()):snapshots[table]={'uuid':uuids[table],'version':version,'committed_at':clock[0]['t'+str(index)]}
configs={table:observation['configuration'] for table,observation in observations.items()}
report=publication_retention_report(snapshots,configs,margin_us=60000000)
d=Descriptor('unpublished-retention-control','development',versions,{'fixture':'3'},{},{'retention':report},{})
deadline=validate_publication_retention(d,configs,now_us=clock[0]['now_us'],table_uuids=uuids)
if args.replay_original:
    if next(remaining,None) is not None:raise ValueError('Unconsumed original observations')
else:receipt_bytes=(OUT/'statements.jsonl').read_bytes()
summary={'state':'eligible-window-observed','observation_mode':'original-receipt-recovery' if args.replay_original else 'fresh-live','original_statements_sha256':hashlib.sha256(receipt_bytes).hexdigest(),'observations':observations,'snapshot_retention':report,'observed_now_us':clock[0]['now_us'],'observed_timezone':clock[0]['timezone'],'effective_readable_until':deadline,'default_sources':['https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-optimization','https://learn.microsoft.com/en-us/azure/databricks/tables/history'],'qualification':'UUID/properties/native snapshot timestamp and fixed UTC clock observations as of the retained native observed_now_us, under explicitly selected documented defaults and 60-second margin. Recovery makes no new observations or network calls. Not a current admission, committed publication, source ACK, current file proof or indefinite retention promise. No settings changed.'}
destination=OUT/('recovery-summary.json' if args.replay_original else 'summary.json')
text=json.dumps(summary,indent=2)+'\n'
if destination.exists():
    if destination.read_text()!=text:raise ValueError('Preserve original summary; changed interpretation requires a separate artifact')
else:destination.write_text(text)
print('Observed finite window for four original snapshots as of retained native clock; no publication or settings changed')

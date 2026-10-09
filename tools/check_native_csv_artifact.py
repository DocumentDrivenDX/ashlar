"""Validate the existing immutable CSV descriptor read-only; no stored phase claim."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
ROOT=Path(__file__).resolve().parents[1]
B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.authority import validate_writer_inventory
from ashlar.manifest import manifest_pin_vector,bind_manifest_pins
from ashlar.native import _quoted
from ashlar.pins import PostgresPins
from ashlar.protocol import ReaderProtocolProfile
from ashlar.publication import Descriptor,_decode,_freeze
from ashlar.retention_policy import SQLRetentionProvider,RetentionGate
from native_artifact_validation import NativeArtifactValidator
from databricks_transport import DatabricksTransport
from fixture_oracle import fixture_batches,fixture_columns,fixture_inventory
from run_local_example import fixture_inputs
from sandbox_pins import PrivatePinTransactions
from persistent_sql import Client


def _endpoint(value):
    if not value.strip():raise argparse.ArgumentTypeError('Explicit nonempty dedicated endpoint required')
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal',type=Path,required=True,help='Original CSV publication journal, opened read-only')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--profile', type=_endpoint, required=True, help='Explicit dedicated Databricks profile')
    parser.add_argument('--warehouse', type=_endpoint, required=True, help='Explicit dedicated SQL warehouse ID')
    args=parser.parse_args()
    if args.output.exists():parser.error('Fresh output directory required')
    receipt=json.loads((B/'out/native/csv_publication_20261008_recovery/summary.json').read_text())
    installation=json.loads((B/'out/native/csv_setup_20261008/summary.json').read_text())
    intake_proof=json.loads((B/'out/native/private_schema_v3_20261008/summary.json').read_text())
    row=receipt['manifest'];namespace=installation['namespace'];uuids=receipt['table_uuids']
    if namespace!='ashlar_e2e_private_20261008.runtime_csv':raise ValueError('Wrong private namespace')
    with sqlite3.connect(args.journal.resolve().as_uri()+'?mode=ro',uri=True) as original:
        retained=original.execute('SELECT row_json FROM csv_publication_proposal WHERE publication_id=?',(row['publication_id'],)).fetchone()
    if retained is None or json.loads(retained[0])!=row:raise ValueError('Original retained proposal differs')
    descriptor=Descriptor(row['publication_id'],row['profile_version'],_freeze(_decode(row['table_versions_json'])),
        _freeze(_decode(row['schema_revisions_json'])),_freeze(_decode(row['source_progress_json'])),_freeze(_decode(row['validation_report_json'])),_freeze(row))
    source,batches=fixture_batches(ROOT,'csv');columns=fixture_columns(ROOT)
    expected,_=fixture_inventory(batches,columns,materialized_at='2026-10-08T17:00:00+00:00')
    source_sha=hashlib.sha256(source.read_bytes()).hexdigest()
    if source_sha!=descriptor.validation_report['source_sha256']:raise ValueError('Original source differs')
    intake,_,_=fixture_inputs()
    client=Client(args.output,warehouse_id=args.warehouse,profile=args.profile)
    user=client.w.current_user.me()
    if user.user_name!=installation['authenticated_owner']:raise PermissionError('Original private owner differs')
    transport=DatabricksTransport(client,SimpleNamespace(warehouse=client.warehouse_id,api=client.w.api_client))
    context=object();lane=False;pin_held=False
    vector=manifest_pin_vector(row,uuids,authority=user.user_name)
    def custody():
        if not lane:raise PermissionError('Same-host original publication lane required')
        status=source.stat()
        if status.st_uid!=os.getuid() or status.st_mode&0o022 or hashlib.sha256(source.read_bytes()).hexdigest()!=source_sha:
            raise PermissionError('Private original source custody differs')
        fresh=client.w.current_user.me()
        if (fresh.id,fresh.user_name)!=(user.id,user.user_name):raise PermissionError('Current actor differs')
    class SourcePolicy:
        def admit(self,value,targets,supplied):
            if supplied is not context or value!=descriptor:raise PermissionError('Original descriptor context required')
            custody()
            resources=[('CATALOG',namespace.split('.')[0],client.w.catalogs.get(name=namespace.split('.')[0]).owner),
                ('SCHEMA',namespace,client.w.schemas.get(full_name=namespace).owner)]
            for table in [*uuids,namespace+'.publication_manifest',intake_proof['table']]:
                resources.append(('TABLE',table,client.w.tables.get(full_name=table).owner))
            for kind,name,owner in resources:
                validate_writer_inventory(owner,transport.query('SHOW GRANTS ON '+kind+' '+(_quoted(name) if kind=='TABLE' else name),{}).rows,trusted_writers=[user.user_name])
            table=intake_proof['table']
            if transport.query('DESCRIBE DETAIL '+_quoted(table),{}).rows[0]['id']!=intake_proof['table_uuid']:
                raise ValueError('Original native schema intake UUID differs')
            original=intake.row()
            projection=','.join('cast(complete_interpretation AS STRING) AS complete_interpretation' if key=='complete_interpretation' else key for key in original)
            actual=transport.query('SELECT '+projection+' FROM '+_quoted(table)+' WHERE document_id=:id AND document_revision=:revision',{'id':intake.document_id,'revision':'3'}).rows
            if actual!=[dict(original,complete_interpretation=str(original['complete_interpretation']).lower())]:raise ValueError('Original native UMF intake differs')
    class PinPolicy:
        def authorize_read(self,value,supplied):
            if supplied is not context or value!=vector:raise PermissionError('Complete original pin vector required')
            custody()
    def pins(value,supplied):
        if supplied is not context or not pin_held:raise PermissionError('Actual native pin interval required')
        bind_manifest_pins(value,vector,uuids,authority=user.user_name)
    targets={table:{'uuid':uuids[table],'version':version,'columns':columns[table.rsplit('.',1)[1]],'rows':expected[table.rsplit('.',1)[1]]} for table,version in descriptor.versions.items()}
    gate=RetentionGate(SQLRetentionProvider(transport,uuids,defaults={'data_retention':'interval 7 days','log_retention':'interval 30 days'},
        default_profile='azure-databricks-delta-documented-defaults/2026-09-11',max_observation_span_us=180_000_000),minimum_margin_us=600_000_000)
    manifest=namespace+'.publication_manifest'
    validator=NativeArtifactValidator(transport,targets,SourcePolicy(),gate,pins,
        reader_profile=ReaderProtocolProfile('private-databricks-sql-fixture/2026-10-08',(3,),(7,),{'appendOnly','clustering','deletionVectors','domainMetadata','invariants','rowTracking','v2Checkpoint'}),
        manifest_table=manifest,manifest_uuid=receipt['manifest_uuid'])
    with open(str(args.journal)+'.private-csv-publication-lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX);lane=True
        try:
            custody()
            native=transport.query('SELECT '+','.join('cast(unix_micros(recorded_at) AS STRING) AS recorded_at' if key=='recorded_at' else key for key in row)+' FROM '+_quoted(manifest)+' WHERE publication_id=:id',{'id':row['publication_id']}).rows
            if native!=[row]:raise ValueError('Actual immutable native manifest differs')
            with PostgresPins(PrivatePinTransactions(context,'ashlar_pin_reader'),PinPolicy()).hold(vector,context=context):
                pin_held=True
                try:validator.validate_descriptor(descriptor,context)
                finally:pin_held=False
        finally:lane=False
    (args.output/'summary.json').write_text(json.dumps({'state':'validated','publication_id':row['publication_id'],'version_vector':dict(descriptor.versions),
        'complete_tables':len(targets),'native_read_statements':len(client.records),'source_sha256':source_sha,'published':False,'acknowledged':False,
        'qualification':'Actual existing CSV descriptor checked under complete ordinary PG pin guards, current private owner/grants, exact native raw UMF intake, full-row/version parity, recognized protocol and finite retention. Original journal read-only; no new data/manifest writes, source ACK or stored-publisher phase claim. Trusted admins and same-host cooperating lane; not real Truss or remote writer fencing.'},indent=2)+'\n')
    print('Native original CSV artifact validation passed; '+str(len(client.records))+' read statements; no publication or ACK.')

if __name__=='__main__':main()

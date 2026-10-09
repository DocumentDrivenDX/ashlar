"""Publish/read the already-applied private CSV fixture; no real Truss or source ACK.

Original journal/proposal custody is mandatory. Private administrative trust and
same-host cooperating writer exclusion are explicit; this is not remote fencing.
No cluster, grants, retention settings or predictive optimization is changed.
"""
import argparse
import collections
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
B = ROOT / 'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(B))
from ashlar.authority import validate_writer_inventory
from ashlar.manifest import manifest_pin_vector, bind_manifest_pins
from ashlar.native import NativeBackend, _quoted
from ashlar.pins import PostgresPins
from ashlar.protocol import ReaderProtocolProfile, inspect_protocol
from ashlar.publication import Descriptor, _decode, _freeze
from ashlar.retention import publication_retention_report
from ashlar.retention_policy import SQLRetentionProvider, RetentionGate, RetentionManifestPolicy
from ashlar.singleton import read_singleton
from databricks_transport import DatabricksTransport
from durable_sql import DurableSQL, SQLPending
from fixture_oracle import fixture_columns, fixture_batches, fixture_inventory
from journaled_manifest import JournaledManifestStore
from run_local_example import fixture_inputs

PUBLICATION = 'csv-fixture-publication-20261008'
NAMESPACE = 'ashlar_e2e_private_20261008.runtime_csv'
DEFAULTS = {'data_retention': 'interval 7 days', 'log_retention': 'interval 30 days'}
DEFAULT_PROFILE = 'azure-databricks-delta-documented-defaults/2026-09-11'
MARGIN = 600_000_000


def _endpoint(value):
    if not value.strip():raise argparse.ArgumentTypeError('Explicit nonempty dedicated endpoint required')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--profile', type=_endpoint, required=True, help='Explicit dedicated Databricks profile')
    parser.add_argument('--warehouse', type=_endpoint, required=True, help='Explicit dedicated SQL warehouse ID')
    args = parser.parse_args()
    if args.output.exists():parser.error('Fresh output directory required; preserve original receipts')
    from databricks.sdk import WorkspaceClient
    from persistent_sql import Client
    from sandbox_pins import PrivatePinTransactions
    installation = json.loads((B / 'out/native/csv_setup_20261008/summary.json').read_text())
    parity = json.loads((B / 'out/native/csv_delta_full_parity_20261008/summary.json').read_text())
    anchors = json.loads((B / 'out/native/csv_retention_window_20261008/recovery-summary.json').read_text())['snapshot_retention']['targets']
    intake_proof = json.loads((B / 'out/native/private_schema_v3_20261008/summary.json').read_text())
    source, batches = fixture_batches(ROOT, 'csv')
    columns = fixture_columns(ROOT)
    expected, _ = fixture_inventory(batches, columns, materialized_at='2026-10-08T17:00:00+00:00')
    source_digest = hashlib.sha256(source.read_bytes()).hexdigest()
    if installation['namespace'] != NAMESPACE or source_digest != parity['original_source_sha256'] or len(batches) != 4:
        raise ValueError('Original private fixture differs')
    versions = parity['version_vector']
    uuids = {table: installation['tables'][table]['uuid'] for table in versions}
    if set(versions) != {NAMESPACE + '.' + role for role in expected} or set(anchors) != set(versions):
        raise ValueError('Complete original snapshot vector required')
    snapshots = {table: {'uuid': uuids[table], 'version': version, 'committed_at': anchors[table]['committed_at']} for table, version in versions.items()}
    for table, version in versions.items():
        if (anchors[table]['uuid'], anchors[table]['version']) != (uuids[table], version):raise ValueError('Original commit anchor differs')
    intake, semantic_policy, _ = fixture_inputs()
    if (intake.document_revision, intake.source_sha256, intake.artifact_sha256, intake.validator_revision) != (
        '3', intake_proof['source_sha256'], intake_proof['artifact_sha256'], intake_proof['validator_revision']):
        raise ValueError('Original UMF intake differs')
    # Validate every complete event under the declared fixture-only StringRecord
    # semantic policy. These IDs are not accepted native Truss catalog identities.
    from ashlar.apply import empty_state, plan_apply
    from ashlar.whole_entity import changes_from_batch
    state = empty_state()
    for batch in batches:state = plan_apply(state, changes_from_batch(batch), schema_policy=semantic_policy)
    if (len(state.current), len(state.tombstones), len(state.history)) != (1, 1, 4):raise ValueError('Fixture semantics differ')
    w = WorkspaceClient(profile=args.profile)
    user = w.current_user.me()
    if user.user_name != installation['authenticated_owner']:raise PermissionError('Original private owner differs')
    client = Client(args.output, warehouse_id=args.warehouse, profile=args.profile)
    client.w = w
    journal = DurableSQL(args.journal, w.api_client, client.warehouse_id, user.id)
    transport = DatabricksTransport(client, journal)
    provider = SQLRetentionProvider(transport, uuids, defaults=DEFAULTS, default_profile=DEFAULT_PROFILE,
        max_observation_span_us=180_000_000)
    gate = RetentionGate(provider, minimum_margin_us=MARGIN)
    reader_profile = ReaderProtocolProfile('private-databricks-sql-fixture/2026-10-08', (3,), (7,),
        {'appendOnly', 'clustering', 'deletionVectors', 'domainMetadata', 'invariants', 'rowTracking', 'v2Checkpoint'})
    manifest_table = NAMESPACE + '.publication_manifest'
    manifest_uuid = installation['tables'][manifest_table]['uuid']
    context = object()
    held = False
    proposal = None
    vector = None
    authority_observations = []

    def custody():
        if not held:raise PermissionError('Original private writer lane is not held')
        status = source.stat()
        if status.st_uid != os.getuid() or status.st_mode & 0o022 or hashlib.sha256(source.read_bytes()).hexdigest() != source_digest:
            raise PermissionError('Original private source permission/content differs')
        fresh = w.current_user.me()
        if fresh.id != user.id or fresh.user_name != user.user_name:raise PermissionError('Current authenticated actor differs')
        resources = [('catalog', NAMESPACE.split('.')[0], w.catalogs.get(name=NAMESPACE.split('.')[0]).owner),
            ('schema', NAMESPACE, w.schemas.get(full_name=NAMESPACE).owner)]
        for table in [*versions, manifest_table, intake_proof['table']]:
            resources.append(('table', table, w.tables.get(full_name=table).owner))
        observed = []
        for kind, name, owner in resources:
            token = None;seen = set();grants = [];principals = set()
            for _ in range(64):
                page = w.grants.get_effective(kind, name, max_results=0, page_token=token)
                for assignment in page.privilege_assignments or []:
                    if not assignment.principal or assignment.principal in principals:raise ValueError('Incomplete/duplicate effective principal inventory')
                    principals.add(assignment.principal)
                    for privilege in assignment.privileges or []:
                        if privilege.privilege is None:raise ValueError('Missing effective privilege')
                        grants.append({'Principal': assignment.principal, 'ActionType': privilege.privilege.value.replace('_', ' '),
                            'ObjectType': privilege.inherited_from_type.value if privilege.inherited_from_type else kind,
                            'ObjectKey': privilege.inherited_from_name or name})
                token = page.next_page_token
                if token is None:break
                if not token or token in seen:raise ValueError('Incomplete grant pagination')
                seen.add(token)
            else:raise ValueError('Grant inventory page budget exceeded')
            validate_writer_inventory(owner, grants, trusted_writers=[user.user_name])
            observed.append({'kind': kind, 'name': name, 'owner': owner, 'grants': grants})
        authority_observations.append(observed)
        table = intake_proof['table']
        if transport.query('DESCRIBE DETAIL ' + _quoted(table), {}).rows[0].get('id') != intake_proof['table_uuid']:
            raise ValueError('Original schema intake carrier replaced')
        row = intake.row()
        projection = ','.join('cast(complete_interpretation AS STRING) AS complete_interpretation' if key == 'complete_interpretation' else key for key in row)
        actual = transport.query('SELECT ' + projection + ' FROM ' + _quoted(table) + ' WHERE document_id=:id AND document_revision=:revision',
            {'id': intake.document_id, 'revision': '3'}).rows
        if actual != [dict(row, complete_interpretation=str(row['complete_interpretation']).lower())]:raise ValueError('Actual native raw intake differs')

    def files(table, uuid, version):
        role = table.rsplit('.', 1)[1]
        if table not in versions or (uuid, version) != (uuids[table], versions[table]):raise ValueError('Snapshot outside original fixture')
        inspect_protocol(transport, table, uuid, profile=reader_profile)
        pinned = _quoted(table) + ' VERSION AS OF ' + str(version)
        shape = transport.query('SELECT * FROM ' + pinned + ' LIMIT 0', {})
        if shape.rows or tuple(shape.columns) != columns[role]:raise ValueError('Complete pinned schema differs')
        names = [name for name, kind in columns[role]]
        projection = ','.join(('cast(unix_micros(`' + name + '`) AS STRING)' if kind == 'TIMESTAMP' else 'cast(`' + name + '` AS STRING)') + ' AS `' + name + '`' for name, kind in columns[role])
        rows = transport.query('SELECT ' + projection + ' FROM ' + _quoted(table) + ' VERSION AS OF ' + str(version) + ' LIMIT 1001', {}).rows
        def multiset(values):return collections.Counter(tuple(row[name] for name in names) for row in values)
        if len(rows) > 1000 or any(set(row) != set(names) for row in rows) or multiset(rows) != multiset(expected[role]):
            raise ValueError('Actual complete pinned inventory differs')

    def descriptor(row):
        return Descriptor(row['publication_id'], row['profile_version'], _freeze(_decode(row['table_versions_json'])),
            _freeze(_decode(row['schema_revisions_json'])), _freeze(_decode(row['source_progress_json'])),
            _freeze(_decode(row['validation_report_json'])), _freeze(row))

    def exact(row):
        if proposal is None or dict(row) != proposal:raise PermissionError('Descriptor differs from original retained proposal')
        from ashlar.source_checkpoint import csv_checkpoint
        if (row['publication_id'], row['profile_version']) != (PUBLICATION, 'ashlar-delta/0.3') or _decode(row['table_versions_json']) != versions or _decode(row['schema_revisions_json']) != {'fixture': '3'}:
            raise ValueError('Proposal differs from independently admitted fixture vector/schema')
        if _decode(row['source_progress_json']) != {'csv-example': _decode(csv_checkpoint(batches[-1]))}:
            raise ValueError('Proposal differs from original complete source progress')
        report = _decode(row['validation_report_json'])
        if report.get('source_sha256') != source_digest or report.get('source_groups') != 4 or report.get('intake_source_sha256') != intake.source_sha256 or report.get('intake_artifact_sha256') != intake.artifact_sha256:
            raise ValueError('Proposal source/schema correspondence differs')
        targets = report.get('retention', {}).get('targets', {})
        if set(targets) != set(snapshots) or any(any(targets[table].get(key) != value for key, value in snapshot.items()) for table, snapshot in snapshots.items()):
            raise ValueError('Proposal changes original native snapshot commit anchors')

    class ManifestPolicy:
        @contextmanager
        def writer(self, table, uuid, supplied):
            if supplied is not context or (table, uuid) != (manifest_table, manifest_uuid):raise PermissionError('Wrong original manifest context')
            custody()
            yield
            custody()
        def admit(self, row, supplied):
            if supplied is not context:raise PermissionError('Wrong publication context')
            exact(row);custody()
            for table, version in versions.items():files(table, uuids[table], version)
            inspect_protocol(transport, manifest_table, manifest_uuid, profile=reader_profile)

    class PinPolicy:
        def check(self, supplied_vector, supplied):
            if supplied is not context or supplied_vector != vector:raise PermissionError('Wrong original complete pin scope')
            exact(proposal);custody()
        def admit_registration(self, supplied_vector, supplied):
            self.check(supplied_vector, supplied)
            for table, version in versions.items():files(table, uuids[table], version)
            gate.check(descriptor(proposal), supplied)
        def authorize_read(self, supplied_vector, supplied):self.check(supplied_vector, supplied)

    pin_policy = PinPolicy()
    pin_writer = PostgresPins(PrivatePinTransactions(context, 'ashlar_pin_writer'), pin_policy)
    pin_reader = PostgresPins(PrivatePinTransactions(context, 'ashlar_pin_reader'), pin_policy)

    class ReadPolicy:
        def authorize(self, supplied, publication_id, tables):
            if supplied is not context or publication_id != PUBLICATION or set(tables) != set(versions):raise PermissionError('Wrong original read scope')
            custody()
        def validate_descriptor(self, value, supplied):
            if supplied is not context:raise PermissionError('Wrong original descriptor context')
            exact(value.raw);custody();gate.check(value, supplied)
            for table, version in versions.items():files(table, uuids[table], version)
        def validate_snapshot(self, table, uuid, version, actual_columns):
            role = table.rsplit('.', 1)[1]
            if tuple(actual_columns) != columns[role]:raise ValueError('Exact native fixture schema differs')
            files(table, uuid, version)
        def bind_descriptor(self, value, supplied_vector, supplied):
            if supplied is not context:raise PermissionError('Wrong original singleton context')
            exact(value.raw);bind_manifest_pins(value, supplied_vector, uuids, authority=user.user_name)
        def authorize_row(self, value, table, row, supplied):
            if supplied is not context or table != NAMESPACE + '.object_current' or row is None:raise PermissionError('Wrong singleton scope or missing fixture object')
            expected_row = expected['object_current'][0]
            for key in ['source_system', 'type_id', 'id', 'entity_version', 'schema_revision', 'props_json', 'retained_json', 'lookup_hash']:
                if row.get(key) != expected_row[key]:raise ValueError('Returned singleton differs from original source')
            custody()

    try:
        with open(args.journal + '.private-csv-publication-lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX);held = True
            custody()
            with journal.db:
                journal.db.execute('CREATE TABLE IF NOT EXISTS csv_publication_proposal (publication_id TEXT PRIMARY KEY, row_json TEXT NOT NULL)')
            retained = journal.db.execute('SELECT row_json FROM csv_publication_proposal WHERE publication_id=?', (PUBLICATION,)).fetchone()
            if retained is None:
                for table, version in versions.items():files(table, uuids[table], version)
                observed = provider.observe(Descriptor(PUBLICATION, 'ashlar-delta/0.3', versions, {'fixture': '3'}, {}, {}, {}), context)
                report = publication_retention_report(snapshots, observed['configurations'], margin_us=MARGIN)
                from ashlar.source_checkpoint import csv_checkpoint
                progress = json.loads(csv_checkpoint(batches[-1]))
                proposal = {'publication_id': PUBLICATION, 'profile_version': 'ashlar-delta/0.3',
                    'table_versions_json': json.dumps(versions, sort_keys=True, separators=(',', ':')),
                    'source_progress_json': json.dumps({'csv-example': progress}, sort_keys=True, separators=(',', ':')),
                    'schema_revisions_json': '{"fixture":"3"}',
                    'validation_report_json': json.dumps({'complete': True, 'source_sha256': source_digest, 'source_groups': 4,
                        'intake_source_sha256': intake.source_sha256, 'intake_artifact_sha256': intake.artifact_sha256,
                        'retention': report, 'qualification': 'Private already-applied four-event CSV fixture; declared StringRecord fixture IDs, original commit anchors, complete native parity and current admission; not accepted Truss identities or remote writer fencing.'}, sort_keys=True, separators=(',', ':')),
                    'recorded_at': observed['now_us']}
                text = json.dumps(proposal, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
                with journal.db:journal.db.execute('INSERT INTO csv_publication_proposal VALUES (?,?)', (PUBLICATION, text))
            else:proposal = json.loads(retained[0])
            proposal = json.loads(journal.db.execute('SELECT row_json FROM csv_publication_proposal WHERE publication_id=?', (PUBLICATION,)).fetchone()[0])
            vector = manifest_pin_vector(proposal, uuids, authority=user.user_name)
            pin_writer.register(vector, context=context)
            print('Complete four-table pin vector active; no source ACK.', flush=True)
            manifest_policy = RetentionManifestPolicy(ManifestPolicy(), gate)
            store = JournaledManifestStore(transport, manifest_policy, manifest_table, manifest_uuid,
                operation='csv-fixture-manifest:' + PUBLICATION)
            # Original proposal/request/handle are unchanged on a new-process repeat.
            try:
                actual = store.commit(proposal, context=context)
            except SQLPending:
                # Poll only the original retained statement; no policy-loop SQL
                # resubmission. Renew full commit admission after terminal recovery.
                operation, text = journal.db.execute('SELECT operation,request FROM submission WHERE operation=?',
                    ('csv-fixture-manifest:' + PUBLICATION,)).fetchone()
                body = json.loads(text)['body']
                deadline = time.monotonic() + 60
                while True:
                    try:
                        journal.query(operation, body['statement'], {value['name']: value['value'] for value in body['parameters']})
                        break
                    except SQLPending:
                        if time.monotonic() >= deadline:raise
                        time.sleep(2)
                actual = store.recover(proposal, context=context)
            exact(actual)
            print('Original immutable manifest admitted; resolving held singleton.', flush=True)
            policy = ReadPolicy()
            backend = NativeBackend(transport, policy, manifest_table, manifest_uuid,
                {table: columns[table.rsplit('.', 1)[1]] for table in versions})
            row = read_singleton(transport, backend, pin_reader, vector, policy, publication_id=PUBLICATION,
                table=NAMESPACE + '.object_current', kind='object', source='local-example', type_id=17, entity_id=1,
                context=context, supported_profiles=['ashlar-delta/0.3'], supported_revisions={'fixture': ['3']})
            custody()
            summary = {'state': 'published-and-resolved', 'publication_id': PUBLICATION, 'manifest': proposal,
                'manifest_uuid': manifest_uuid, 'table_uuids': uuids, 'version_vector': versions,
                'singleton': dict(row), 'active_pin_count': len(vector.targets), 'source_acknowledged': False,
                'sql_read_statements': len(client.records), 'authority_observations': authority_observations,
                'qualification': 'Actual private four-event CSV snapshot manifest and resolver singleton under complete ordinary PostgreSQL pin guards, exact raw UMF intake, declared StringRecord fixture semantics, current effective grants, protocol recognition, complete pinned inventory reads and configured finite retention. Trusted platform/local administrators and same-host cooperating writer lane; no real Truss runtime, remote writer fence, native publisher phase demonstration or source ACK. Predictive optimization unchanged.'}
            (args.output / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n')
            journal.db.row_factory = __import__('sqlite3').Row
            (args.output / 'original-journal.json').write_text(json.dumps({table: [dict(value) for value in journal.db.execute('SELECT * FROM ' + table)] for table in ['submission', 'csv_publication_proposal']}, indent=2) + '\n')
            print('Published original CSV fixture and resolved one singleton; no source ACK or settings change.')
    finally:
        held = False;journal.close()


if __name__ == '__main__':main()

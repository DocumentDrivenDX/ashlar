"""Apply only the four-event local fixture to the installed private UC sandbox.

Original journals are mandatory for replay/recovery. Owner development lane only;
no accepted Truss IDs, remote writer fencing, publication or acknowledgement.
"""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
B = ROOT / 'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(B))
from databricks.sdk import WorkspaceClient
from ashlar.apply import empty_state
from ashlar.authority import validate_writer_inventory
from ashlar.staging import batch_row, batch_from_row
from durable_sql import DurableSQL, SQLPending
from durable_effects import DurableEffects
from materialization_clock import materialization_clock, CLOCK_OPERATION
from databricks_transport import DatabricksTransport
from persistent_sql import Client
from run_local_example import fixture_inputs
from whole_graph_sql import graph_sql_plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--installation', default=str(B / 'out/native/private_setup_20261008/summary.json'))
    parser.add_argument('--source', choices=['local', 'outbox', 'csv'], default='local')
    args = parser.parse_args()
    # Fixed fixture/deployment only; never generalize its initial-state authority.
    installation = json.loads(Path(args.installation).read_text())
    namespace = 'ashlar_e2e_private_20261008.' + {'local':'runtime','outbox':'runtime_outbox','csv':'runtime_csv'}[args.source]
    if installation['namespace'] != namespace:
        raise ValueError('Wrong private development installation')
    intake, policy, batches = fixture_inputs()
    transition = None
    checkpoints = {}
    operation_prefix = 'local-example:'
    if args.source == 'outbox':
        from run_schema_evolution import inputs
        from sandbox_postgres import connect
        from postgres_transactions import PostgresTransactions
        from ashlar.outbox import PostgresOutbox
        from ashlar.source_checkpoint import outbox_checkpoint
        _, policy, transition, originals = inputs()
        pg_context = object()
        def factory(context):
            if context is not pg_context:
                raise PermissionError('Wrong private source context')
            return connect('ashlar_outbox_reader')
        with PostgresTransactions(factory).transaction(pg_context) as session:
            transactions = PostgresOutbox(session, feed='native-evolution', epoch='example-1').read('2', limit=3)
        if len(transactions) != 3:
            raise ValueError('Incomplete native source interval')
        for position, transaction, original in zip(range(3, 6), transactions, originals):
            blob = lambda batch: batch.begin + b''.join(record.raw for record in batch.records) + batch.commit
            if transaction.position != str(position) or blob(transaction.batch) != blob(original):
                raise ValueError('Native source interval differs from original fixture')
            checkpoints[transaction.batch.batch_id] = outbox_checkpoint(transaction)
        batches = tuple(transaction.batch for transaction in transactions)
        operation_prefix = 'outbox-example:'
    if args.source == 'csv':
        from ashlar.csv_source import csv_batches,validate_csv_batch
        from ashlar.source_checkpoint import csv_checkpoint
        with (ROOT / 'examples/end-to-end/string-source.csv').open('rb') as source:
            batches = tuple(csv_batches(source,feed='csv-example',epoch='immutable-example-1',
                source_system='local-example',schema_revision='3',type_id='17',properties={'label':'23','caption':'24'}))
        for batch in batches:
            validate_csv_batch(batch,feed='csv-example',epoch='immutable-example-1',source_system='local-example',schema_revision='3',type_id='17',properties={'label':'23','caption':'24'})
        checkpoints = {batch.batch_id:csv_checkpoint(batch) for batch in batches}
        operation_prefix = 'csv-example:'
    batches = tuple(batch_from_row(batch_row(batch)) for batch in batches)
    tables = {key: namespace + '.' + key for key in
              ['object_current', 'edge_current', 'tombstone', 'whole_source_history']}
    w = WorkspaceClient(profile='aidev-cus')
    user = w.current_user.me()
    actor = user.user_name
    if actor != installation['authenticated_owner']:
        raise PermissionError('Private installed owner differs')
    client = Client(Path(args.output), warehouse_id='2439e1f2e37ac563', profile='aidev-cus')
    client.w = w
    journal = DurableSQL(args.journal, w.api_client, client.warehouse_id, user.id)
    transport = DatabricksTransport(client, journal)

    def admit_targets():
        owners = [('CATALOG', namespace.split('.')[0], w.catalogs.get(name=namespace.split('.')[0]).owner),
                  ('SCHEMA', namespace, w.schemas.get(full_name=namespace).owner)]
        owners += [('TABLE', table, w.tables.get(full_name=table).owner) for table in tables.values()]
        for kind, target, owner in owners:
            validate_writer_inventory(owner, transport.query('SHOW GRANTS ON ' + kind + ' ' + target, {}).rows,
                                      trusted_writers=[actor])
        for table in tables.values():
            rows = transport.query('DESCRIBE DETAIL ' + table, {}).rows
            if len(rows) != 1 or rows[0]['id'] != installation['tables'][table]['uuid']:
                raise ValueError('Private target identity changed')

    class Policy:
        @contextmanager
        def writer(self, operation, context):
            if context is not lock:
                raise PermissionError('Wrong held development lane')
            yield
        def admit(self, plan, context):
            if plan['steps'] != expected_steps:
                raise PermissionError('Original fixture plan differs')

    try:
        with open(args.journal + '.private-graph-writer-lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            admit_targets()
            runner = DurableEffects(journal, Policy())
            operations = {row[0] for row in journal.db.execute('SELECT operation FROM effect_plan')}
            if not operations <= {operation_prefix + batch.batch_id for batch in batches}:
                raise ValueError('Journal contains a different workload')
            if not operations:
                for table in tables.values():
                    if transport.query('SELECT count(*) AS n FROM ' + table, {}).rows != [{'n': '0'}]:
                        raise ValueError('Fresh fixture requires empty private graph; retain original journal')
            clock_record = journal.db.execute('SELECT operation FROM submission WHERE operation=?', (CLOCK_OPERATION,)).fetchone()
            # Existing immutable fixture plans predate clock custody. Regenerate
            # their exact original timestamp; DurableEffects checks every plan.
            legacy_clock = bool(operations) and clock_record is None
            workload = hashlib.sha256(json.dumps({'namespace': namespace,
                'batches': [batch_row(batch) for batch in batches], 'checkpoints': checkpoints},
                sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            if legacy_clock:
                materialized_at = '2026-10-08T17:00:00+00:00'
            else:
                materialized_at = materialization_clock(journal, workload)
            state = empty_state()
            plans = []
            for batch in batches:
                state, steps = graph_sql_plan(state, batch, tables,
                    materialized_at=materialized_at, schema_policy=policy,
                    schema_transition_policy=transition)
                plans.append((batch, steps))
            results = []
            for batch, expected_steps in plans:
                row = batch_row(batch)
                digest = hashlib.sha256(row['batch_json'].encode()).hexdigest()
                if args.source in ('outbox','csv'):
                    intent = json.dumps({'batch_row': row, 'source_checkpoint_json': checkpoints[batch.batch_id]},
                                        sort_keys=True, separators=(',', ':'))
                    digest = hashlib.sha256(intent.encode()).hexdigest()
                    with journal.db:
                        journal.db.execute('CREATE TABLE IF NOT EXISTS source_intent (operation TEXT PRIMARY KEY,original_json TEXT NOT NULL,digest TEXT NOT NULL)')
                        operation = operation_prefix + batch.batch_id
                        journal.db.execute('INSERT OR IGNORE INTO source_intent VALUES (?,?,?)', (operation, intent, digest))
                        if journal.db.execute('SELECT original_json,digest FROM source_intent WHERE operation=?', (operation,)).fetchone() != (intent, digest):
                            raise ValueError('Original native source intent conflict')
                deadline = time.monotonic() + 180
                while True:
                    try:
                        result = runner.run(operation_prefix + batch.batch_id, digest, expected_steps, context=lock)
                        break
                    except SQLPending:
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(.2)
                results.append({'batch_id': batch.batch_id, 'plan_digest': result['plan_digest']})
            observed = transport.query('SELECT cast(id AS STRING) AS id,cast(entity_version AS STRING) AS version,props_json,retained_json FROM ' + tables['object_current'], {}).rows
            expected_props = '{"23":"updated","24":"雪"}'
            expected_retained = '{"future":18446744073709551615}'
            if args.source == 'csv':
                expected_props = '{"23":"updated, quoted","24":"雪"}'
                expected_retained = '{"source_profile":"ashlar-single-line-csv/0.1","unmapped_columns":{"future":"18446744073709551615"}}'
            if observed != [{'id':'1','version':'2','props_json':expected_props,'retained_json':expected_retained}]:
                raise ValueError('Final selected object inventory differs')
            for key, count in [('edge_current', '0'), ('tombstone', '1'), ('whole_source_history', '4')]:
                if transport.query('SELECT count(*) AS n FROM ' + tables[key], {}).rows != [{'n': count}]:
                    raise ValueError('Final fixture inventory differs: ' + key)
            admit_targets()
            summary = {'state': 'applied', 'namespace': namespace, 'batches': results,
                       'source': args.source, 'source_checkpoints': checkpoints,
                       'objects': observed, 'published': False, 'acknowledged': False,
                       'qualification': 'Four-event UMF-backed fixture; explicit fixture IDs, original same-host SQL plan/handle recovery, owner/grant/UUID observations and selected final inventory. Full-column parity, remote fencing, retention and publication remain unqualified.'}
            if not legacy_clock:
                summary['materialization_clock'] = {'operation': CLOCK_OPERATION,
                    'workload': workload, 'materialized_at': materialized_at,
                    'qualification': 'Original retained server clock before effects; not a Delta commit timestamp or publication retention anchor.'}
            (Path(args.output) / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
            journal.db.row_factory = __import__('sqlite3').Row
            custody = {table: [dict(row) for row in journal.db.execute('SELECT * FROM ' + table)]
                       for table in ['effect_plan', 'submission']}
            if args.source in ('outbox','csv'):
                custody['source_intent'] = [dict(row) for row in journal.db.execute('SELECT * FROM source_intent')]
            (Path(args.output) / 'original-journal.json').write_text(json.dumps(custody, indent=2) + '\n')
            print('Applied '+str(len(batches))+' fixture transactions; no publication or acknowledgement')
    finally:
        journal.close()


if __name__ == '__main__':
    main()

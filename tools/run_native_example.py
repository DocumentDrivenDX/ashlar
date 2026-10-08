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
from databricks_transport import DatabricksTransport
from persistent_sql import Client
from run_local_example import fixture_inputs
from whole_graph_sql import graph_sql_plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    # Fixed fixture/deployment only; never generalize its initial-state authority.
    installation = json.loads((B / 'out/native/private_setup_20261008/summary.json').read_text())
    namespace = 'ashlar_e2e_private_20261008.runtime'
    if installation['namespace'] != namespace:
        raise ValueError('Wrong private development installation')
    intake, policy, batches = fixture_inputs()
    batches = tuple(batch_from_row(batch_row(batch)) for batch in batches)
    tables = {key: namespace + '.' + key for key in
              ['object_current', 'edge_current', 'tombstone', 'whole_source_history']}
    state = empty_state()
    plans = []
    for batch in batches:
        state, steps = graph_sql_plan(state, batch, tables,
            materialized_at='2026-10-08T17:00:00+00:00', schema_policy=policy)
        plans.append((batch, steps))
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
            if not operations <= {'local-example:' + batch.batch_id for batch in batches}:
                raise ValueError('Journal contains a different workload')
            if not operations:
                for table in tables.values():
                    if transport.query('SELECT count(*) AS n FROM ' + table, {}).rows != [{'n': '0'}]:
                        raise ValueError('Fresh fixture requires empty private graph; retain original journal')
            results = []
            for batch, expected_steps in plans:
                row = batch_row(batch)
                digest = hashlib.sha256(row['batch_json'].encode()).hexdigest()
                deadline = time.monotonic() + 180
                while True:
                    try:
                        result = runner.run('local-example:' + batch.batch_id, digest, expected_steps, context=lock)
                        break
                    except SQLPending:
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(.2)
                results.append({'batch_id': batch.batch_id, 'plan_digest': result['plan_digest']})
            observed = transport.query('SELECT cast(id AS STRING) AS id,cast(entity_version AS STRING) AS version,props_json,retained_json FROM ' + tables['object_current'], {}).rows
            if observed != [{'id': '1', 'version': '2', 'props_json': '{"23":"updated","24":"雪"}',
                             'retained_json': '{"future":18446744073709551615}'}]:
                raise ValueError('Final selected object inventory differs')
            for key, count in [('edge_current', '0'), ('tombstone', '1'), ('whole_source_history', '4')]:
                if transport.query('SELECT count(*) AS n FROM ' + tables[key], {}).rows != [{'n': count}]:
                    raise ValueError('Final fixture inventory differs: ' + key)
            admit_targets()
            summary = {'state': 'applied', 'namespace': namespace, 'batches': results,
                       'objects': observed, 'published': False, 'acknowledged': False,
                       'qualification': 'Four-event UMF-backed fixture; explicit fixture IDs, original same-host SQL plan/handle recovery, owner/grant/UUID observations and selected final inventory. Full-column parity, remote fencing, retention and publication remain unqualified.'}
            (Path(args.output) / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
            journal.db.row_factory = __import__('sqlite3').Row
            custody = {table: [dict(row) for row in journal.db.execute('SELECT * FROM ' + table)]
                       for table in ['effect_plan', 'submission']}
            (Path(args.output) / 'original-journal.json').write_text(json.dumps(custody, indent=2) + '\n')
            print('Applied 3 fixture transactions; no publication or acknowledgement')
    finally:
        journal.close()


if __name__ == '__main__':
    main()

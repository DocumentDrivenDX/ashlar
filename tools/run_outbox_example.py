"""Run the fixed UMF evolution source through the existing private PG outbox.

Appends three exact fixture groups idempotently. Reads native checkpoints 2..5;
no source ACK, Delta writes, publication or Truss-native claim.
"""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from ashlar.apply import empty_state
from ashlar.outbox import PostgresOutbox, apply_outbox_transactions
from run_schema_evolution import inputs
from postgres_transactions import PostgresTransactions
from sandbox_postgres import connect


def main():
    _, policies, transition, batches = inputs()
    context = object()
    receipts = []
    for expected, original in enumerate(batches, 3):
        raw = original.begin + b''.join(record.raw for record in original.records) + original.commit
        with connect('ashlar_outbox_writer') as writer:
            position = writer.execute('SELECT ashlar_outbox.append(%s,%s)::text',
                                      (original.batch_id, raw.decode('utf-8'))).fetchone()[0]
            if position != str(expected):
                raise ValueError('Native fixture source position differs; transaction rolls back')
            receipts.append({'batch_id': original.batch_id, 'position': position,
                             'payload_sha256': hashlib.sha256(raw).hexdigest()})
    def factory(received):
        if received is not context:
            raise PermissionError('Wrong private source context')
        return connect('ashlar_outbox_reader')
    executor = PostgresTransactions(factory)
    with executor.transaction(context) as session:
        role = session.query('SELECT current_user AS role', {}).rows
        if role != [{'role': 'ashlar_outbox_reader'}]:
            raise PermissionError('Wrong native source role')
        reader = PostgresOutbox(session, feed='native-evolution', epoch='example-1')
        transactions = reader.read('2', limit=3)
        if len(transactions) != 3:
            raise ValueError('Incomplete native fixture interval')
        for transaction, original, receipt in zip(transactions, batches, receipts):
            observed = transaction.batch.begin + b''.join(record.raw for record in transaction.batch.records) + transaction.batch.commit
            expected = original.begin + b''.join(record.raw for record in original.records) + original.commit
            if observed != expected or transaction.payload_digest != receipt['payload_sha256']:
                raise ValueError('Native original transaction differs from fixture')
        applied = apply_outbox_transactions(transactions, prior=empty_state(),
            feed='native-evolution', epoch='example-1', after='2', expected_position='5',
            schema_policy=policies, schema_transition_policy=transition)
        first = apply_outbox_transactions(transactions[:1], prior=empty_state(),
            feed='native-evolution', epoch='example-1', after='2', expected_position='3',
            schema_policy=policies, schema_transition_policy=transition)
        resumed = apply_outbox_transactions(reader.read('3', limit=2), prior=first.state,
            feed='native-evolution', epoch='example-1', after='3', expected_position='5',
            schema_policy=policies, schema_transition_policy=transition)
        if resumed.state != applied.state or reader.read('5') != ():
            raise ValueError('Native page resume/head differs')
    live = tuple(applied.state.current.values())
    if len(live) != 1 or live[0].schema_revision != '3' or live[0].props_json != '{"23":"updated","24":"雪"}':
        raise ValueError('Native source graph differs')
    if len(applied.state.history) != 4 or len(applied.state.tombstones) != 1:
        raise ValueError('Native source history/delete inventory differs')
    out = ROOT / 'docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/outbox_evolution_20261008'
    out.mkdir(parents=True, exist_ok=True)
    summary = {'state': 'applied', 'source_profile': 'ashlar-postgresql-outbox/0.1',
               'previous': '2', 'position': applied.position, 'groups': receipts,
               'native_reader': role, 'objects': 1, 'history': 4, 'tombstones': 1,
               'native_page_resume_equal': True, 'published': False, 'acknowledged': False,
               'qualification': 'Actual protected private PG outbox append/read and schema-evolved graph reconstruction; fixture IDs, isolated admin-authenticated ordinary roles. Not Truss property feed, Delta publication or source ACK.'}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    from ashlar.staging import batch_row
    # Preserve outer PG positions beside complete exact inner transaction custody.
    retained = [{key: value for key, value in asdict(transaction).items() if key != 'batch'}
                | {'batch_row': batch_row(transaction.batch)} for transaction in transactions]
    (out / 'original-transactions.json').write_text(json.dumps(retained, indent=2) + '\n')
    print('Applied 3 native outbox groups; page resume matched; no publication or ACK')


if __name__ == '__main__':
    main()

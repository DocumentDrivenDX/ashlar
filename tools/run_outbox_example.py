"""Run the fixed UMF evolution source through the existing private PG outbox.

Optionally appends three exact fixture groups idempotently; --read-only performs
no appends. Reads native checkpoints 2..5;
no source ACK, Delta writes, publication or Truss-native claim.
"""
import argparse
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
from run_local_example import check_original_records
from ashlar.schema import _json
from ashlar.source_checkpoint import outbox_checkpoint
from postgres_transactions import PostgresTransactions
from sandbox_postgres import connect


def main(umf_source=None,read_only=False,output_dir=None):
    intakes, policies, transition, batches = inputs()
    context = object()
    receipts = []
    for expected, original in enumerate(batches, 3):
        raw = original.begin + b''.join(record.raw for record in original.records) + original.commit
        if read_only:
            position=str(expected)
        else:
            with connect('ashlar_outbox_writer') as writer:
                position=writer.execute('SELECT ashlar_outbox.append(%s,%s)::text',
                    (original.batch_id,raw.decode('utf-8'))).fetchone()[0]
                if position!=str(expected):
                    raise ValueError('Native fixture source position differs; transaction rolls back')
        receipts.append({'batch_id':original.batch_id,'position':position,
                         'payload_sha256':hashlib.sha256(raw).hexdigest()})
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
        upstream=[]
        if umf_source is not None:
            for intake in intakes:
                selected=[]
                for transaction in transactions:
                    revisions={_json(record.raw)['schema_revision'] for record in transaction.batch.records}
                    if len(revisions)!=1:raise ValueError('Explicit single-revision fixture groups required')
                    if revisions=={intake.document_revision}:selected.append(transaction)
                receipt_path=None if output_dir is None else output_dir/('schema-'+intake.document_revision+'.json')
                result=check_original_records(umf_source,intake,[t.batch for t in selected],receipt_path,
                    ROOT/('examples/end-to-end/schema-v'+intake.document_revision+'.umf.json'))
                upstream.append({'revision':intake.document_revision,'records':len(result['records']),
                    'source_sha256':result['sourceSha256'],'producer_revision':result['producerRevision'],
                    'original_document_complete':result['originalValidation']['complete'],
                    'source_checkpoints':[outbox_checkpoint(t) for t in selected],'native_acceptance':False})
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
    out = output_dir or ROOT / 'docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/outbox_evolution_20261008'
    out.mkdir(parents=True, exist_ok=True)
    summary = {'state': 'applied', 'source_profile': 'ashlar-postgresql-outbox/0.1',
               'read_only':read_only,'upstream_record_checks':upstream,'previous': '2', 'position': applied.position, 'groups': receipts,
               'native_reader': role, 'objects': 1, 'history': 4, 'tombstones': 1,
               'native_page_resume_equal': True, 'published': False, 'acknowledged': False,
               'qualification': 'Actual protected private PG outbox read and schema-evolved graph reconstruction; optional append path is separate;  fixture IDs, isolated admin-authenticated ordinary roles. Not Truss property feed, Delta publication or source ACK.'}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    from ashlar.staging import batch_row
    # Preserve outer PG positions beside complete exact inner transaction custody.
    retained = [{key: value for key, value in asdict(transaction).items() if key != 'batch'}
                | {'batch_row': batch_row(transaction.batch)} for transaction in transactions]
    (out / 'original-transactions.json').write_text(json.dumps(retained, indent=2) + '\n')
    print('Applied 3 native outbox groups; page resume matched; no publication or ACK')


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--umf-source',type=Path)
    parser.add_argument('--read-only',action='store_true',help='Read the existing exact groups without appending')
    parser.add_argument('--output-dir',type=Path,help='Retain original native groups, summary and UMF receipts')
    args=parser.parse_args()
    main(args.umf_source,args.read_only,args.output_dir)

from contextlib import contextmanager
import json
from pathlib import Path
import sys
import tempfile
import unittest

from ashlar.attempt_store import DeltaAttemptStore
from ashlar.native import SQLResult
from ashlar.publisher import PublicationError, publish_batch
from ashlar.stored_publisher import StoredPublisherBackend
from ashlar.csv_source import csv_batches
from ashlar.source_checkpoint import csv_checkpoint

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from durable_sql import DurableSQL, SQLCustodyError, SQLPending
from databricks_transport import sql_result
from journaled_manifest import JournaledManifestStore


class PhaseExecutor:
    def __init__(self):self.rows = []
    def query(self, sql, parameters):
        if sql.startswith('DESCRIBE'):return SQLResult([{'id': 'attempt-uuid'}])
        if sql.startswith('MERGE'):
            self.rows.append(json.loads(parameters['payload']))
            return SQLResult([])
        rows = [row for row in self.rows if row['stream'] == parameters['stream'] and row['batch_id'] == parameters['batch']]
        return SQLResult([{key: row[key] for key in ['phase', 'request_digest', 'payload_json', 'payload_digest']} for row in rows])


class API:
    def __init__(self):self.rows = [];self.calls = [];self.pending = False;self.lost = False
    def do(self, method, path, **kwargs):
        self.calls.append(method)
        if method == 'POST':
            row = json.loads(kwargs['body']['parameters'][0]['value'])
            self.rows.append(row)
            if self.lost:raise RuntimeError('Lost original response')
        return {'statement_id': 'original-manifest', 'status': {'state': 'RUNNING' if self.pending else 'SUCCEEDED'},
                'manifest': {}, 'result': {'data_array': []}}


class Transport:
    def __init__(self, journal, api):self.journal = journal;self.api = api
    def query(self, sql, parameters):
        if sql.startswith('DESCRIBE'):return SQLResult([{'id': 'manifest-uuid'}])
        return SQLResult([dict(row) for row in self.api.rows if row['publication_id'] == parameters['id']])
    def mutation(self, operation, sql, parameters):return sql_result(self.journal.query(operation, sql, parameters))


class Policy:
    def __init__(self):self.closing_failure = False;self.admissions = 0
    @contextmanager
    def writer(self, *args):yield
    def admit(self, row, context):
        self.admissions += 1
        if self.closing_failure and self.admissions % 2 == 0:raise PermissionError('Admission expired after native commit')


class Driver:
    def __init__(self):self.calls = [];self.fail = None;self.original = None
    @contextmanager
    def writer(self, stream, context):
        if context is None:raise PermissionError('Denied writer')
        yield
    def artifact(self, request):
        checkpoint = json.loads(request['source_checkpoint_json'])
        row = {'publication_id': 'original-' + request['request_digest'], 'profile_version': 'ashlar-delta/0.3',
            'table_versions_json': '{"c.s.object_current":7}',
            'source_progress_json': json.dumps({checkpoint['feed']: checkpoint}),
            'schema_revisions_json': request['schema_revisions_json'],
            'validation_report_json': json.dumps({'complete': True, 'request_digest': request['request_digest']}),
            'recorded_at': '1791478800123456'}
        return json.dumps({'effects': {'qualification': 'test transport only'}, 'manifest': row})
    def apply(self, request, context):
        self.calls.append('apply');self.original = self.artifact(request)
        if self.fail == 'apply':raise RuntimeError('Original apply pending')
        return self.original
    def recover_apply(self, request, context):self.calls.append('recover-apply');return self.original
    def validate(self, request, result, descriptor, context):
        self.calls.append('validate')
        if self.fail == 'validate':raise PermissionError('Native admission denied')
    def acknowledge(self, request, descriptor, context):
        self.calls.append('ack')
        if self.fail == 'ack':raise RuntimeError('Original ACK lost')


class StoredPublisherTests(unittest.TestCase):
    def batch(self):
        data = b'id,entity_version,operation,label,caption,future\n1,1,create,label,,opaque\n'
        return next(csv_batches(data.splitlines(keepends=True), feed='csv', epoch='original', source_system='example', schema_revision='3', type_id='17', properties={'label': '23', 'caption': '24'}))
    def backend(self, phases, driver, transport, policy):
        return StoredPublisherBackend(DeltaAttemptStore(phases, Policy(), 'c.s.attempt', 'attempt-uuid'), driver,
            lambda request, context: JournaledManifestStore(transport, policy, 'c.s.manifest', 'manifest-uuid', operation='manifest:' + request['request_digest']))
    def publish(self, backend, context, predecessor='original-prior'):
        batch = self.batch()
        return publish_batch(backend, 'stream', batch, predecessor=predecessor,
            schema_revisions_json='{"csv":"3"}', source_checkpoint_json=csv_checkpoint(batch), context=context)
    @contextmanager
    def fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            api = API();path = str(Path(directory) / 'journal.sqlite')
            journal = DurableSQL(path, api, '2439e1f2e37ac563', 'actor')
            phases = PhaseExecutor();driver = Driver();policy = Policy();transport = Transport(journal, api)
            try:yield phases, driver, policy, transport, api
            finally:transport.journal.close()

    def test_complete_composition_and_new_backend_replay_original_descriptor(self):
        with self.fixture() as (phases, driver, policy, transport, api):
            context = object();backend = self.backend(phases, driver, transport, policy)
            original = self.publish(backend, context)
            self.assertEqual([row['phase'] for row in phases.rows], ['prepared', 'applying', 'applied', 'committing', 'committed'])
            recreated = self.backend(phases, driver, transport, policy)
            self.assertEqual(self.publish(recreated, context), original)
            self.assertEqual(api.calls, ['POST']);self.assertEqual(driver.calls.count('apply'), 1)
            self.assertEqual(driver.calls.count('ack'), 2)
            with self.assertRaises(PublicationError):self.publish(recreated, context, predecessor='changed')
            with self.assertRaises(PublicationError):backend.observe('stream', self.batch().batch_id)

    def test_interrupted_apply_and_lost_ack_recover_without_reapplying(self):
        for failure in ['apply', 'ack']:
            with self.fixture() as (phases, driver, policy, transport, api):
                context = object();driver.fail = failure;backend = self.backend(phases, driver, transport, policy)
                with self.assertRaises(RuntimeError):self.publish(backend, context)
                driver.fail = None
                self.publish(self.backend(phases, driver, transport, policy), context)
                self.assertEqual(driver.calls.count('apply'), 1)
                self.assertEqual(driver.calls.count('recover-apply'), int(failure == 'apply'))
                self.assertEqual(api.calls, ['POST'])

    def test_pending_commit_recovers_same_native_handle(self):
        with self.fixture() as (phases, driver, policy, transport, api):
            context = object();api.pending = True
            with self.assertRaises(SQLPending):self.publish(self.backend(phases, driver, transport, policy), context)
            self.assertEqual(phases.rows[-1]['phase'], 'committing');self.assertNotIn('ack', driver.calls)
            api.pending = False
            path = transport.journal.db.execute('PRAGMA database_list').fetchone()[2]
            transport.journal.close()
            transport.journal = DurableSQL(path, api, '2439e1f2e37ac563', 'actor')
            self.publish(self.backend(phases, driver, transport, policy), context)
            self.assertEqual(api.calls, ['POST', 'GET']);self.assertEqual(len(api.rows), 1)

    def test_postcommit_admission_failure_preserves_row_and_no_ack(self):
        with self.fixture() as (phases, driver, policy, transport, api):
            context = object();policy.closing_failure = True
            with self.assertRaises(PermissionError):self.publish(self.backend(phases, driver, transport, policy), context)
            self.assertEqual(len(api.rows), 1);self.assertNotIn('ack', driver.calls)
            policy.closing_failure = False
            self.publish(self.backend(phases, driver, transport, policy), context)
            self.assertEqual(api.calls, ['POST'])

    def test_lost_handle_and_missing_submission_refuse_replacement(self):
        with self.fixture() as (phases, driver, policy, transport, api):
            context = object();api.lost = True
            with self.assertRaises(RuntimeError):self.publish(self.backend(phases, driver, transport, policy), context)
            api.lost = False
            with self.assertRaises(SQLCustodyError):self.publish(self.backend(phases, driver, transport, policy), context)
            self.assertEqual(api.calls, ['POST']);self.assertNotIn('ack', driver.calls)
        with self.fixture() as (phases, driver, policy, transport, api):
            context = object();driver.fail = 'validate'
            backend = self.backend(phases, driver, transport, policy)
            with self.assertRaises(PermissionError):self.publish(backend, context)
            # Model a crash after committing-phase custody but before any POST.
            with backend.writer('stream', context):
                last = backend.observe('stream', self.batch().batch_id)
                backend.start_commit('stream', self.batch().batch_id, last.request_digest)
            driver.fail = None
            with self.assertRaises(SQLCustodyError):self.publish(self.backend(phases, driver, transport, policy), context)
            self.assertEqual(api.calls, []);self.assertNotIn('ack', driver.calls)

    def test_wrong_source_progress_and_non_none_admission_stop_before_commit(self):
        for failure in ['progress', 'incomplete']:
            with self.fixture() as (phases, driver, policy, transport, api):
                context = object()
                if failure == 'progress':
                    original = driver.artifact
                    def changed(request):
                        value = json.loads(original(request));value['manifest']['source_progress_json'] = '{"csv":{"position":"other"}}'
                        return json.dumps(value)
                    driver.artifact = changed
                else:driver.validate = lambda *args: False
                with self.assertRaises(ValueError):self.publish(self.backend(phases, driver, transport, policy), context)
                self.assertEqual(api.calls, []);self.assertNotIn('ack', driver.calls)

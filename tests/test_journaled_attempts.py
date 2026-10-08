from pathlib import Path
import json
import sys
import tempfile
import unittest
from ashlar.native import SQLResult
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from durable_sql import DurableSQL, SQLCustodyError, SQLPending
from databricks_transport import sql_result
from journaled_attempts import JournaledAttemptExecutor


class API:
    def __init__(self):self.calls = [];self.pending = True;self.lost = False
    def do(self, method, path, **kwargs):
        self.calls.append(method)
        if self.lost:raise RuntimeError('Original response lost')
        return {'statement_id': 'original-phase', 'status': {'state': 'RUNNING' if self.pending else 'SUCCEEDED'}, 'manifest': {}, 'result': {'data_array': []}}


class Transport:
    def __init__(self, journal):self.journal = journal;self.reads = 0
    def query(self, sql, parameters):self.reads += 1;return SQLResult([])
    def mutation(self, operation, sql, parameters):return sql_result(self.journal.query(operation, sql, parameters))


def payload(text='original'):
    return {'payload': json.dumps({'stream': 's', 'batch_id': 'b', 'phase': 'prepared', 'request_digest': 'a'*64, 'payload_json': text, 'payload_digest': 'b'*64})}


class JournaledAttemptTests(unittest.TestCase):
    def test_read_reconciles_original_pending_handle_before_absence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'original.sqlite');api = API()
            journal = DurableSQL(path, api, '2439e1f2e37ac563', 'actor');transport = Transport(journal)
            executor = JournaledAttemptExecutor(transport, namespace='original-table')
            with self.assertRaises(SQLPending):executor.query('MERGE original', payload())
            with self.assertRaises(SQLPending):executor.query('SELECT original phase', {})
            self.assertEqual(transport.reads, 0)
            journal.close();api.pending = False
            transport.journal = DurableSQL(path, api, '2439e1f2e37ac563', 'actor')
            executor = JournaledAttemptExecutor(transport, namespace='original-table')
            executor.query('SELECT original phase', {})
            self.assertEqual(api.calls, ['POST', 'GET', 'GET']);self.assertEqual(transport.reads, 1)
            executor.query('MERGE original', payload())
            self.assertEqual(api.calls.count('POST'), 1)
            with self.assertRaises(SQLCustodyError):executor.query('MERGE original', payload('changed'))
            transport.journal.close()

    def test_uncertain_no_handle_cannot_be_reported_absent(self):
        with tempfile.TemporaryDirectory() as directory:
            api = API();api.lost = True
            journal = DurableSQL(str(Path(directory) / 'original.sqlite'), api, '2439e1f2e37ac563', 'actor')
            transport = Transport(journal);executor = JournaledAttemptExecutor(transport, namespace='original-table')
            with self.assertRaises(RuntimeError):executor.query('MERGE original', payload())
            api.lost = False
            with self.assertRaises(SQLCustodyError):executor.query('SELECT original phase', {})
            self.assertEqual(transport.reads, 0);self.assertEqual(api.calls, ['POST'])
            journal.close()

    def test_changed_authority_cannot_recover_another_original(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'original.sqlite');api = API()
            journal = DurableSQL(path, api, '2439e1f2e37ac563', 'actor');transport = Transport(journal)
            with self.assertRaises(SQLPending):JournaledAttemptExecutor(transport, namespace='original-table').query('MERGE original', payload())
            journal.close();transport.journal = DurableSQL(path, api, '2439e1f2e37ac563', 'other')
            with self.assertRaises(SQLCustodyError):JournaledAttemptExecutor(transport, namespace='original-table').query('SELECT phase', {})
            self.assertEqual(api.calls, ['POST']);self.assertEqual(transport.reads, 0)
            transport.journal.close()

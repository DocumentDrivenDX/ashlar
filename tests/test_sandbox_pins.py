from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from ashlar.pins import INVENTORY_SQL
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from sandbox_pins import PrivatePinTransactions
from postgres_transactions import PostgresTransactionError


class Connection:
    autocommit = False
    info = SimpleNamespace(transaction_status=0)
    def __init__(self, role='ashlar_pin_writer'):
        self.role = role;self.calls = [];self.commits = 0;self.rollbacks = 0;self.closed = False;self.fail = False
    def cursor(self):return Cursor(self)
    def commit(self):self.commits += 1
    def rollback(self):self.rollbacks += 1
    def close(self):self.closed = True


class Cursor:
    def __init__(self, connection):self.connection = connection;self.description = None;self.values = []
    def __enter__(self):return self
    def __exit__(self, *args):pass
    def execute(self, sql, parameters):
        c = self.connection;c.calls.append((c.role, sql))
        if sql.startswith('SET LOCAL ROLE '):c.role = sql.split()[-1]
        elif sql == 'SELECT current_user AS role':
            self.description = [SimpleNamespace(name='role')];self.values = [(c.role,)]
        elif sql.startswith('SELECT table_name,table_uuid,'):
            if c.role != 'ashlar_pin_reader':raise PermissionError('Writer cannot SELECT inventory')
            if c.fail:raise RuntimeError('Inventory read failed')
            self.description = [SimpleNamespace(name='table_name')];self.values = [('c.s.t',)]
        elif sql == 'SELECT writer_operation':
            if c.role != 'ashlar_pin_writer':raise PermissionError('Reader cannot write')
    def fetchmany(self, size):return self.values[:size]


class PrivatePinTests(unittest.TestCase):
    def test_inventory_routes_existing_reader_and_restores_writer_in_same_transaction(self):
        context = object();connection = Connection()
        adapter = PrivatePinTransactions(context, 'ashlar_pin_writer', connection_factory=lambda supplied: connection)
        with adapter.transaction(context) as session:
            session.query('SELECT writer_operation', {})
            self.assertEqual(session.query(INVENTORY_SQL, {'authority': 'a', 'kind': 'manifest', 'scope': 's'}).rows, [{'table_name': 'c.s.t'}])
            session.query('SELECT writer_operation', {})
        self.assertEqual((connection.commits, connection.rollbacks, connection.role, connection.closed), (1, 0, 'ashlar_pin_writer', True))
        self.assertEqual([role for role, sql in connection.calls if sql == 'SELECT writer_operation'], ['ashlar_pin_writer', 'ashlar_pin_writer'])
        with self.assertRaises(PostgresTransactionError):session.query('SELECT writer_operation', {})

    def test_inventory_error_rolls_back_without_masking_original_failure(self):
        context = object();connection = Connection();connection.fail = True
        adapter = PrivatePinTransactions(context, 'ashlar_pin_writer', connection_factory=lambda supplied: connection)
        with self.assertRaisesRegex(RuntimeError, 'Inventory read failed'):
            with adapter.transaction(context) as session:session.query(INVENTORY_SQL, {'authority': 'a', 'kind': 'manifest', 'scope': 's'})
        self.assertEqual((connection.commits, connection.rollbacks, connection.closed), (0, 1, True))
        self.assertFalse(any(sql == 'SET LOCAL ROLE ashlar_pin_writer' for _, sql in connection.calls))

    def test_wrong_context_or_current_role_refuses_before_generated_operations(self):
        context = object();connection = Connection('unexpected')
        adapter = PrivatePinTransactions(context, 'ashlar_pin_writer', connection_factory=lambda supplied: connection)
        with self.assertRaises(PermissionError):
            with adapter.transaction(object()):pass
        self.assertEqual(connection.calls, [])
        with self.assertRaises(PermissionError):
            with adapter.transaction(context):pass
        self.assertEqual(connection.calls, [('unexpected', 'SELECT current_user AS role')])
        self.assertEqual(connection.rollbacks, 1)

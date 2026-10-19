# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual SQLite allocation cleanup; no native transport qualification."""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ashlar_host.delta_custody import LocalDeltaError
from ashlar_host.read_only_transport import ReadOnlyDeltaTransport


class TrackedConnection(sqlite3.Connection):
    query_failure = None
    close_failure = None
    close_count = 0

    def execute(self, *args, **kwargs):
        result = super().execute(*args, **kwargs)
        if self.query_failure is not None:
            raise self.query_failure
        return result

    def close(self):
        self.close_count += 1
        super().close()
        if self.close_failure is not None:
            raise self.close_failure


class ReadOnlyJournalCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / 'journal.sqlite'
        connection = sqlite3.connect(self.path)
        try:
            connection.execute('CREATE TABLE local_installation(id INTEGER, original TEXT)')
            connection.execute('INSERT INTO local_installation VALUES(1,?)', ('original',))
            connection.commit()
        finally:
            connection.close()
        self.original_bytes = self.path.read_bytes()
        self.connections = []
        original_connect = sqlite3.connect

        def connect(*args, **kwargs):
            connection = original_connect(*args, factory=TrackedConnection, **kwargs)
            connection.query_failure = self.query_failure
            connection.close_failure = self.close_failure
            self.connections.append(connection)
            self.addCleanup(sqlite3.Connection.close, connection)
            return connection

        self.query_failure = None
        self.close_failure = None
        patcher = patch('ashlar_host.read_only_transport.sqlite3.connect', side_effect=connect)
        self.addCleanup(patcher.stop)
        patcher.start()

    def assert_closed_without_writes(self):
        self.assertEqual(len(self.connections), 1)
        connection = self.connections[0]
        self.assertEqual(connection.close_count, 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute('SELECT 1')
        self.assertEqual(self.path.read_bytes(), self.original_bytes)
        self.assertEqual({p.name for p in self.path.parent.iterdir()}, {'journal.sqlite'})

    def test_registration_mismatch_closes_acquired_connection(self):
        owner = SimpleNamespace(journal_path=self.path)
        with self.assertRaisesRegex(LocalDeltaError, 'registration differs'):
            ReadOnlyDeltaTransport._journal(owner, 'foreign', create=False)
        self.assert_closed_without_writes()

    def test_missing_schema_closes_acquired_connection(self):
        self.path.unlink()
        self.path.write_bytes(b'')
        self.original_bytes = b''
        with self.assertRaises(sqlite3.OperationalError):
            ReadOnlyDeltaTransport._journal(SimpleNamespace(journal_path=self.path), 'original', create=False)
        self.assert_closed_without_writes()

    def test_cancellation_preserved_even_when_closing_raises(self):
        self.query_failure = KeyboardInterrupt('query cancellation')
        self.close_failure = LookupError('closing failure')
        with self.assertRaises(KeyboardInterrupt) as caught:
            ReadOnlyDeltaTransport._journal(SimpleNamespace(journal_path=self.path), 'original', create=False)
        self.assertIs(caught.exception, self.query_failure)
        self.assert_closed_without_writes()


if __name__ == '__main__':
    unittest.main()

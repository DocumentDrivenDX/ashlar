"""Small construction preflight controls; no retry or native qualification."""
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.driver import NativeDriver, PrivatePolicy
from ashlar_host.delta_custody import LocalDeltaTransport
from ashlar_host.truss_feed_sessions import TrussFeedSessions
from ashlar_host.truss_graph_admission import TrussGraphAdmission


class Tests(unittest.TestCase):
    def test_missing_journal_refuses_before_constructor_and_original_ports_forward(self):
        with TemporaryDirectory() as directory:
            context = object(); policy = PrivatePolicy(context, {})
            transport = object.__new__(LocalDeltaTransport)
            transport.policy = policy; transport.journal_path = Path(directory).resolve()/'missing.sqlite'
            transport.installation_id = 'original'; transport.targets = {}; transport.operation_capacity = None
            transport.db = sqlite3.connect(':memory:')
            admission = object.__new__(TrussGraphAdmission)
            sessions = object.__new__(TrussFeedSessions)
            object.__setattr__(sessions, 'context', context)
            args = (transport, policy, context, {'object_current': 'nodes'}, (), {})
            ports = {'source_admission': admission, 'source_sessions': sessions, 'ack_sessions': sessions}
            try:
                with patch.object(NativeDriver, '__init__', return_value=None) as constructor:
                    with self.assertRaises(FileNotFoundError): NativeDriver.from_existing_truss(*args, **ports)
                    constructor.assert_not_called()
                    self.assertEqual(transport.db.execute("SELECT name FROM sqlite_master").fetchall(), [])
                    with patch.object(LocalDeltaTransport, 'require_existing_journal') as preflight:
                        driver = NativeDriver.from_existing_truss(*args, **ports)
                        self.assertIs(type(driver), NativeDriver)
                        preflight.assert_called_once_with(transport.journal_path, 'original', (), capacity=None)
                        constructor.assert_called_once_with(*args[:4], {}, *args[4:], **ports)
            finally: transport.db.close()

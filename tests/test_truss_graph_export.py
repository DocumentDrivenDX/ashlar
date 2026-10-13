import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.graph_release import GraphRelease
from ashlar_host.driver import NativeDriver
from ashlar_host.truss_feed_sessions import TrussFeedSessions
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_graph_export import read_original_truss_graph_export


class Tests(unittest.TestCase):
    def test_original_owner_full_bag_and_writer_closing(self):
        driver = object.__new__(NativeDriver)
        driver.source_admission = object.__new__(TrussGraphAdmission)
        driver.source_sessions = object.__new__(TrussFeedSessions)
        driver.ack_sessions = driver.source_sessions
        driver.context = object()
        driver.tables = {'object_current': 'nodes', 'edge_current': 'edges'}
        manifest = {'publication_id': 'p', 'table_versions_json': '{"nodes":1,"edges":2}',
                    'schema_revisions_json': '{"schema":"r1"}'}
        row = {'id': '1', 'published_at': '123456'}
        driver.policy = SimpleNamespace(active={'request': {'stream': 'feed'}, 'manifest': manifest,
            'publication_id': 'p', 'expected': {'object_current': [row], 'edge_current': []}})
        driver.transport = SimpleNamespace(targets={name: SimpleNamespace(uuid=name+'-uuid') for name in ('nodes', 'edges')})
        life = []
        @contextmanager
        def writer(stream, context):
            self.assertEqual(stream, 'feed'); self.assertIs(context, driver.context)
            life.append('open')
            try: yield
            finally: life.append('closed')
        driver.writer = writer
        driver.authorize = lambda *args: self.assertEqual(life, ['open'])
        vector = SimpleNamespace(targets={'nodes': ('nodes-uuid', 1), 'edges': ('edges-uuid', 2)})
        release = GraphRelease(b'original\n', 'digest')
        def read(executor, backend, pins, supplied, policy, **kwargs):
            self.assertIs(backend, driver); self.assertIs(pins, driver); self.assertIs(supplied, vector)
            self.assertEqual(kwargs['max_nodes'], 1); self.assertEqual(kwargs['max_edges'], 0)
            descriptor = SimpleNamespace(raw=manifest)
            policy.authorize_graph_row(descriptor, 'nodes', row, driver.context)
            policy.authorize_graph_result(descriptor, [dict(row, graph_id='key')], [], driver.context)
            with self.assertRaises(PermissionError):
                policy.authorize_graph_result(descriptor, [], [], driver.context)
            return release
        with patch('ashlar_host.truss_graph_export.manifest_pin_vector', return_value=vector), patch('ashlar_host.truss_graph_export.read_graph_release', side_effect=read):
            self.assertIs(read_original_truss_graph_export(driver), release)
        self.assertEqual(life, ['open', 'closed'])

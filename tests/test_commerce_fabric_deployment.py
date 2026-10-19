# SPDX-License-Identifier: MIT OR Apache-2.0
"""Real HTTP/SQLite composition with nominal source owners over explicit SDK doubles.

Parquet/native SDK observations are inert: this is NOT a native Delta/Fabric proof.
"""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit, parse_qs

import test_commerce_graphframes_installed as commerce
from test_fabric_pack_delta import Spark
from ashlar.graph_release import release_columns
from ashlar_host.commerce_fabric_deployment import CommerceFabricDeploymentConfig, open_commerce_fabric_deployment
from ashlar_host.fabric_deployment_http import FabricDeploymentHttpConfig, open_fabric_deployment_loopback_for_test
from ashlar_host.fabric_release_journal import FabricReleaseJournal
from ashlar_host.onelake_release_upload import OneLakeUploadConfig, open_onelake_upload_loopback_for_test

W = '11111111-1111-1111-1111-111111111111'
L = '22222222-2222-2222-2222-222222222222'
C = '33333333-3333-3333-3333-333333333333'
O = '44444444-4444-4444-4444-444444444444'
G = '55555555-5555-5555-5555-555555555555'
J = '66666666-6666-6666-6666-666666666666'


class Tests(unittest.TestCase):
    def setUp(self):
        fixture = commerce.ReaderTests()
        self.addCleanup(fixture.doCleanups)
        session, self.events = fixture.active_session()
        self.opened = session.opened
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.spark = Spark()
        value = json.loads(commerce.RELEASE)
        for index, role in enumerate(('nodes', 'edges'), 1):
            path = self.root / role
            (path / '_delta_log').mkdir(parents=True)
            (path / 'original.parquet').write_bytes(('inert-' + role).encode())
            commit = [{'metaData': {'id': '00000000-0000-0000-0000-00000000000' + str(index)}}, {'add': {'path': 'original.parquet'}}]
            (path / '_delta_log/00000000000000000000.json').write_text('\n'.join(json.dumps(row) for row in commit))
            columns = release_columns('node' if role == 'nodes' else 'edge')
            frame = self.spark.createDataFrame([tuple(row[name] for name in columns) for row in value[role]], ','.join(name + ' STRING' for name in columns))
            frame.write.format('delta').mode('errorifexists').save(str(path))
        auth = FabricDeploymentHttpConfig(Path('/usr/bin/true').resolve(), self.root, W, W, 'user@example.com', W)
        self.config = CommerceFabricDeploymentConfig(OneLakeUploadConfig(auth, L), self.root / 'nodes', self.root / 'edges',
            'Tables/dbo/unique_release_nodes', 'Tables/dbo/unique_release_edges', 'Original candidate', 'private-cooperative-sole-creator/0.1')
        self.journal = FabricReleaseJournal.initialize(self.root / 'journal')

    @contextmanager
    def transport(self, *, lost=False, jobs=1, synchronous=False, admission=False, load_status="Completed", corrupt_bag=False, definition_drift=False):
        state = {'files': {}, 'dirs': set(), 'calls': [], 'holds': []}
        opened = self.opened
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self): self.respond()
            def do_GET(self): self.respond()
            def do_PUT(self): self.respond()
            def do_PATCH(self): self.respond()
            def respond(self):
                path = urlsplit(self.path).path
                query = parse_qs(urlsplit(self.path).query)
                body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                state['calls'].append((self.command, self.path, body))
                state['holds'].append((opened.provider.active, opened.driver.held, opened.driver.pin_held))
                code, raw, headers = 200, b'', []
                if path.startswith('/v1/'):
                    if self.command == 'POST' and path.endswith('/getDefinition') and admission:
                        import base64
                        definition = json.loads(state['creation_payload'])['definition']
                        if state.get('definition_drift', definition_drift):
                            definition['parts'][0]['payload'] = base64.b64encode(b'{"changed":true}').decode()
                        raw = json.dumps({'definition': definition}).encode()
                    elif self.command == 'POST' and path.endswith('/executeQuery') and admission:
                        from ashlar_host.fabric_graph_definition import fabric_carrier_queries
                        value = json.loads(commerce.RELEASE)
                        selected = next((case for case in fabric_carrier_queries() if case[1] == json.loads(body)['query']), None)
                        if selected is None:
                            if state.get('reader_probe') is not None:
                                state['reader_probe']()
                            columns, rows = [('row_count', 'INT64')], [{'row_count': '11'}]
                        else:
                            role, _, names = selected
                            columns = [(name, 'STRING') for name in names]
                            rows = value[role] if role != 'endpoints' else [
                                {'source': edge['src'], 'relationship_id': edge['graph_id'], 'destination': edge['dst']} for edge in value['edges']]
                            if state.get('corrupt_bag', corrupt_bag) and role == 'nodes':
                                rows = rows + rows[:1]
                        raw = json.dumps({'status': {'code': '00000'}, 'result': {'kind': 'TABLE',
                            'columns': [{'name': name, 'gqlType': kind} for name, kind in columns], 'data': rows}}).encode()
                    elif self.command == 'POST':
                        state['creation_payload'] = body
                        code = 500 if lost else 201 if synchronous else 202
                        if synchronous and not lost:
                            raw = json.dumps({'id': G, 'workspaceId': W, 'type': 'GraphModel', 'displayName': 'Original candidate'}).encode()
                        elif not lost:
                            headers = [('Location', 'https://api.fabric.microsoft.com/v1/operations/' + O), ('x-ms-operation-id', O), ('Retry-After', '0')]
                    elif path.endswith('/operations/' + O):
                        raw = b'{"status":"Succeeded"}'
                    elif path.endswith('/operations/' + O + '/result'):
                        raw = json.dumps({'id': G, 'workspaceId': W, 'type': 'GraphModel', 'displayName': 'Original candidate'}).encode()
                    elif path.endswith('/jobs/instances'):
                        raw = json.dumps({'value': [{'id': J, 'itemId': G, 'jobType': 'Refresh', 'invokeType': 'Manual', 'status': 'NotStarted'}] * jobs}).encode()
                    else:
                        raw = json.dumps({'id': J, 'itemId': G, 'jobType': 'Refresh', 'invokeType': 'Manual', 'status': load_status}).encode()
                elif self.command == 'PUT':
                    if path in state['dirs'] or path in state['files']:
                        code = 412
                    elif self.headers.get('If-None-Match') != '*':
                        code = 400
                    elif query['resource'] == ['directory']:
                        state['dirs'].add(path); code = 201
                    else:
                        state['files'][path] = b''; code = 201
                elif self.command == 'PATCH' and query['action'] == ['append']:
                    state['files'][path] += body; code = 202
                elif self.command == 'PATCH':
                    if int(query['position'][0]) != len(state['files'][path]): code = 400
                else:
                    raw = state['files'].get(path, b'')
                    if path not in state['files']: code = 404
                self.send_response(code)
                for key, value in headers: self.send_header(key, value)
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers(); self.wfile.write(raw)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.02), daemon=True)
        thread.start()
        origin = 'http://127.0.0.1:' + str(server.server_port)
        def capture(argv, **kwargs):
            value = {'tenant': W, 'subscription': W, 'tokenType': 'Bearer', 'accessToken': 'private-held-secret', 'expires_on': str(int(time.time()) + 3600)} if 'get-access-token' in argv else {
                'id': W, 'tenantId': W, 'user': {'name': 'user@example.com', 'type': 'user'}, 'state': 'Enabled', 'environmentName': 'AzureCloud'}
            return json.dumps(value).encode(), b''
        try:
            with patch('ashlar_host.fabric_deployment_http.capture', capture), patch('ashlar_host.onelake_release_upload.capture', capture), \
                 patch('ashlar_host.commerce_fabric_deployment.open_onelake_release_upload', lambda cfg, permit, candidate: open_onelake_upload_loopback_for_test(cfg, permit, candidate, origin)), \
                 patch('ashlar_host.commerce_fabric_deployment.open_fabric_deployment_http', lambda cfg: open_fabric_deployment_loopback_for_test(cfg, origin)):
                state['origin'] = origin
                state['capture'] = capture
                yield state
        finally:
            server.shutdown(); server.server_close(); thread.join(2)

    def deployment(self):
        return open_commerce_fabric_deployment(self.spark, self.opened, self.journal, self.config, C)

    def test_actual_source_owners_span_bytes_native_creation_and_original_load_observation(self):
        with self.transport() as state:
            with self.deployment() as owner:
                owner.stage()
                accepted = owner.submit_creation()
                self.assertEqual(accepted.operation_id, O)
                self.assertEqual(self.journal.root, self.root / 'journal')
                owner.observe_creation()
                result = owner.observe_initial_load()
                self.assertEqual(json.loads(result.body)['status'], 'Completed')
                self.assertFalse(hasattr(owner, 'activate'))
            self.assertTrue(all(value == (True, True, True) for value in state['holds']))
        self.assertFalse(self.opened.provider.active)
        self.assertFalse(self.opened.driver.held)
        self.assertEqual(self.journal.first_load(C), J)
        self.assertEqual(self.journal.creation(C).graph_model_id, G)
        with self.assertRaises(PermissionError): owner.renew()
        with self.assertRaisesRegex(ValueError, 'no local active'):
            with self.journal.reader(): self.fail('activated')

    def test_unknown_creation_refuses_resubmission_and_missing_handle_recovery(self):
        with self.transport(lost=True) as state:
            with self.deployment() as owner:
                owner.stage()
                with self.assertRaises(ValueError): owner.submit_creation()
                with self.assertRaisesRegex(PermissionError, 'already attempted'): owner.submit_creation()
                with self.assertRaisesRegex(PermissionError, 'No original creation handle'): owner.observe_creation()
            creates = [call for call in state['calls'] if call[0] == 'POST']
            self.assertEqual(len(creates), 1)
        self.assertEqual(self.journal.creation(C).kind, 'unknown')

    def test_reopen_recovers_original_intent_and_bytes_without_remote_rewrite(self):
        with self.transport() as state:
            with self.deployment() as owner: owner.stage()
            before = len(state['calls'])
            with self.deployment() as recovered:
                recovered.staging_readback()
                self.assertTrue(all(call[0] == 'GET' for call in state['calls'][before:]))
                recovered.submit_creation()
            self.assertEqual(self.journal.creation(C).operation_handle, O)

    def test_ambiguous_first_load_and_changed_projection_refuse_without_activation(self):
        with self.transport(jobs=2):
            with self.deployment() as owner:
                owner.stage(); owner.submit_creation(); owner.observe_creation()
                with self.assertRaisesRegex(PermissionError, 'sole initial load'): owner.observe_initial_load()
            self.assertIsNone(self.journal.first_load(C))
        self.spark.corruption = 'row'
        with self.assertRaises(ValueError):
            with self.deployment(): self.fail('changed projection admitted')

    def test_cancellation_and_changed_projection_close_actual_source_owners(self):
        cancellation = KeyboardInterrupt('cancelled deployment')
        with self.transport() as state:
            with self.assertRaises(KeyboardInterrupt) as captured:
                with self.deployment() as owner:
                    (self.root / 'nodes/original.parquet').write_bytes(b'changed')
                    raise cancellation
            self.assertIs(captured.exception, cancellation)
            self.assertTrue(cancellation.cleanup_failed)
            self.assertFalse(state['calls'])
        self.assertFalse(self.opened.provider.active)
        self.assertFalse(self.opened.driver.held)
        self.assertFalse(self.opened.driver.pin_held)
        self.assertEqual(self.events[-1], 'writer-close')

    def test_real_synchronous_201_is_retained_without_fabricated_operation(self):
        with self.transport(synchronous=True) as state:
            with self.deployment() as owner:
                owner.stage()
                created = owner.submit_creation()
                self.assertEqual(created.graph_model_id, G)
                retained = owner.observe_creation()
                self.assertEqual(retained.kind, 'synchronous')
                self.assertIsNone(retained.operation_handle)
                owner.observe_initial_load()
            self.assertFalse(any('/v1/operations/' in call[1] for call in state['calls']))
        self.assertEqual(self.journal.creation(C).synchronous_response, created.response.body)

"""Actual loopback DFS byte transport plus real journal permit; no cloud claim."""
from contextlib import contextmanager
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit, parse_qs

from ashlar_host.fabric_deployment_http import FabricDeploymentHttpConfig
from ashlar_host.fabric_release_journal import FabricReleaseJournal, JournalRefusal, ReleaseIntent
from ashlar_host.onelake_release_upload import (OneLakeUploadConfig, OneLakeReleaseFile, OneLakeReleaseTable,
    open_onelake_upload_loopback_for_test)

W = '11111111-1111-1111-1111-111111111111'
L = '22222222-2222-2222-2222-222222222222'
C = '33333333-3333-3333-3333-333333333333'
TABLE = OneLakeReleaseTable('Tables/dbo/release_unique_nodes', (
    OneLakeReleaseFile('_delta_log/00000000000000000000.json', b'{"commit":"original"}\n'),
    OneLakeReleaseFile('part-original.parquet', b'original-parquet-bytes')))


class Tests(unittest.TestCase):
    @contextmanager
    def fixture(self, *, corrupt=False, fail_append=False, existing=False, **limits):
        state = {'files': {}, 'directories': set(), 'calls': []}
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                self.respond()
            def do_PUT(self):
                self.respond()
            def do_PATCH(self):
                self.respond()
            def respond(self):
                parsed = urlsplit(self.path)
                query = parse_qs(parsed.query)
                raw = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                state['calls'].append((self.command, self.path, raw, self.headers.get('If-None-Match'), self.headers.get('Authorization')))
                path = parsed.path
                code, response = 200, b''
                if self.command == 'PUT':
                    if existing or path in state['files'] or path in state['directories']:
                        code = 412
                    elif self.headers.get('If-None-Match') != '*':
                        code = 400
                    elif query['resource'] == ['directory']:
                        state['directories'].add(path)
                        code = 201
                    else:
                        state['files'][path] = b''
                        code = 201
                elif self.command == 'PATCH' and query['action'] == ['append']:
                    if fail_append:
                        code = 500
                    else:
                        state['files'][path] += raw
                        code = 202
                elif self.command == 'PATCH':
                    if int(query['position'][0]) != len(state['files'][path]):
                        code = 400
                else:
                    if path not in state['files']:
                        code = 404
                    else:
                        response = b'changed' if corrupt else state['files'][path]
                self.send_response(code)
                self.send_header('Content-Length', str(len(response)))
                self.send_header('ETag', '"retained-etag"')
                self.end_headers()
                self.wfile.write(response)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp).resolve()
                cfg = OneLakeUploadConfig(FabricDeploymentHttpConfig(Path('/usr/bin/true').resolve(), root, W, W, 'user@example.com', W, **limits), L)
                journal = FabricReleaseJournal.initialize(root / 'journal')
                location = 'abfss://' + W + '@onelake.dfs.fabric.microsoft.com/' + L + '/' + TABLE.path
                journal.record_intent(ReleaseIntent(C, W, 'a' * 64, '{"publication":"original"}', '{"mapping":"original"}', ((location, TABLE.sha256()),)))
                account = {'id': W, 'tenantId': W, 'user': {'name': 'user@example.com', 'type': 'user'}, 'state': 'Enabled', 'environmentName': 'AzureCloud'}
                calls = []
                def capture(argv, **kwargs):
                    calls.append(argv)
                    value = {'tenant': W, 'subscription': W, 'tokenType': 'Bearer', 'accessToken': 'private-storage-secret', 'expires_on': str(int(time.time()) + 3600)} if 'get-access-token' in argv else account
                    return json.dumps(value).encode(), b''
                with patch('ashlar_host.onelake_release_upload.capture', capture):
                    yield cfg, journal, 'http://127.0.0.1:' + str(server.server_port), state, account, calls
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)

    def test_real_fresh_upload_readback_data_before_commit_and_storage_audience(self):
        with self.fixture() as (cfg, journal, origin, state, account, calls):
            with journal.mutation() as permit:
                with open_onelake_upload_loopback_for_test(cfg, permit, C, origin) as owner:
                    receipt = owner.upload(TABLE)
                    self.assertEqual(receipt.inventory_sha256, TABLE.sha256())
                    self.assertEqual(len(receipt.responses), 10)
                    self.assertEqual(owner.readback(TABLE).inventory, TABLE.inventory())
                    with self.assertRaisesRegex(PermissionError, 'already attempted'):
                        owner.upload(TABLE)
                self.assertIsNone(owner._token)
            requests = state['calls']
            self.assertEqual(len(requests), 12)
            self.assertTrue(all(x[4] == 'Bearer private-storage-secret' for x in requests))
            self.assertTrue(all(x[3] == '*' for x in requests if x[0] == 'PUT'))
            self.assertTrue(requests[2][1].endswith('/part-original.parquet?resource=file'))
            self.assertTrue(requests[6][1].endswith('/_delta_log/00000000000000000000.json?resource=file'))
            self.assertIn('https://storage.azure.com/', calls[1])
            self.assertEqual(len(calls), 3)
            self.assertEqual(journal.candidate(C)[1], 'recorded')  # no creation/activation grant

    def test_existing_remote_root_refuses_without_overwrite_or_retry(self):
        with self.fixture(existing=True) as (cfg, journal, origin, state, account, calls):
            with journal.mutation() as permit:
                with open_onelake_upload_loopback_for_test(cfg, permit, C, origin) as owner:
                    with self.assertRaisesRegex(ValueError, 'no retry or overwrite'):
                        owner.upload(TABLE)
                    with self.assertRaises(PermissionError):
                        owner.upload(TABLE)
            self.assertEqual(len(state['calls']), 1)
            self.assertFalse(state['files'])

    def test_failed_append_keeps_partial_output_and_never_uploads_commit(self):
        with self.fixture(fail_append=True) as (cfg, journal, origin, state, account, calls):
            with journal.mutation() as permit:
                with open_onelake_upload_loopback_for_test(cfg, permit, C, origin) as owner:
                    with self.assertRaises(ValueError):
                        owner.upload(TABLE)
                    self.assertEqual(owner.observations()[-1][2], 500)
                    with self.assertRaisesRegex(PermissionError, 'already attempted'):
                        owner.upload(TABLE)
            self.assertEqual(len(state['files']), 1)
            self.assertFalse(any(path.endswith('.json') for path in state['files']))
            self.assertEqual(len(state['calls']), 4)

    def test_corrupt_readback_refuses_before_commit_and_retains_partial_bytes(self):
        with self.fixture(corrupt=True) as (cfg, journal, origin, state, account, calls):
            with journal.mutation() as permit:
                with self.assertRaisesRegex(PermissionError, 'readback differs'):
                    with open_onelake_upload_loopback_for_test(cfg, permit, C, origin) as owner:
                        owner.upload(TABLE)
            self.assertFalse(any(path.endswith('.json') for path in state['files']))

    def test_changed_inventory_and_expired_or_ducktyped_permit_refuse_before_network(self):
        with self.fixture() as (cfg, journal, origin, state, account, calls):
            with self.assertRaises(ValueError):
                with open_onelake_upload_loopback_for_test(cfg, object(), C, origin):
                    self.fail('opened')
            with journal.mutation() as permit:
                with open_onelake_upload_loopback_for_test(cfg, permit, C, origin) as owner:
                    changed = replace(TABLE, files=(TABLE.files[0], replace(TABLE.files[1], payload=b'changed')))
                    with self.assertRaisesRegex(PermissionError, 'candidate intent'):
                        owner.upload(changed)
            with self.assertRaises(JournalRefusal):
                with open_onelake_upload_loopback_for_test(cfg, permit, C, origin):
                    self.fail('opened')
            self.assertFalse(state['calls'])

    def test_cancellation_and_account_closing_failure_preserve_primary_and_close_token(self):
        with self.fixture() as (cfg, journal, origin, state, account, calls):
            cancellation = KeyboardInterrupt('cancelled staging')
            with journal.mutation() as permit:
                with self.assertRaises(KeyboardInterrupt) as captured:
                    with open_onelake_upload_loopback_for_test(cfg, permit, C, origin) as owner:
                        account['tenantId'] = L
                        raise cancellation
                self.assertIs(captured.exception, cancellation)
                self.assertTrue(cancellation.cleanup_failed)
                self.assertIsNone(owner._token)
            self.assertFalse(state['calls'])

    def test_selected_request_bound_and_submitted_candidate_refuse_before_remote_write(self):
        with self.fixture(maximum_request_bytes=2) as (cfg, journal, origin, state, account, calls):
            with journal.mutation() as permit:
                with open_onelake_upload_loopback_for_test(cfg, permit, C, origin) as owner:
                    with self.assertRaisesRegex(ValueError, 'table/file capacity'):
                        owner.upload(TABLE)
            self.assertFalse(state['calls'])
        with self.fixture() as (cfg, journal, origin, state, account, calls):
            with journal.mutation() as permit:
                permit.begin_creation(C)
                with open_onelake_upload_loopback_for_test(cfg, permit, C, origin) as owner:
                    with self.assertRaisesRegex(PermissionError, 'unsubmitted candidate'):
                        owner.upload(TABLE)
            self.assertFalse(state['calls'])

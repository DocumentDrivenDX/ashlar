# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual loopback HTTP and finite inert delegated authentication; no cloud claim."""
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

from ashlar_host.fabric_deployment_http import (FabricDeploymentHttpConfig, FabricAcceptedCreation,
    FabricCreatedGraph, open_fabric_deployment_loopback_for_test)

W = '11111111-1111-1111-1111-111111111111'
G = '22222222-2222-2222-2222-222222222222'
O = '33333333-3333-3333-3333-333333333333'
J = '44444444-4444-4444-4444-444444444444'
PAYLOAD = b'{"displayName":"candidate","definition":{"format":"json","parts":[]}}'


class Tests(unittest.TestCase):
    @contextmanager
    def fixture(self, replies, **limits):
        calls = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                self.respond()
            def do_POST(self):
                self.respond()
            def respond(self):
                body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                calls.append((self.command, self.path, body, self.headers.get('Authorization')))
                code, headers, raw = replies.pop(0)
                self.send_response(code)
                for key, value in headers:
                    self.send_header(key, value)
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                config = FabricDeploymentHttpConfig(Path('/usr/bin/true').resolve(), Path(tmp).resolve(), W, W, 'user@example.com', W, **limits)
                auth = []
                account = {'id': W, 'tenantId': W, 'user': {'name': config.account_user, 'type': 'user'}, 'state': 'Enabled', 'environmentName': 'AzureCloud'}
                def capture(argv, **kwargs):
                    auth.append(argv)
                    result = {'tenant': W, 'subscription': W, 'tokenType': 'Bearer', 'accessToken': 'private-secret', 'expires_on': str(int(time.time()) + 3600)} if 'get-access-token' in argv else account
                    return json.dumps(result).encode(), b''
                with patch('ashlar_host.fabric_deployment_http.capture', capture):
                    yield config, 'http://127.0.0.1:' + str(server.server_port), calls, auth, account
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)

    def item(self):
        return json.dumps({'id': G, 'workspaceId': W, 'type': 'GraphModel', 'extension': {'kept': True}}).encode()

    def job(self, **changes):
        return {'id': J, 'itemId': G, 'jobType': 'Refresh', 'invokeType': 'Manual', 'status': 'NotStarted', **changes}

    def accepted(self, **headers):
        selected = {'Location': 'https://wabi-west-us3-a-primary-redirect.analysis.windows.net/v1/operations/' + O,
                    'x-ms-operation-id': O, 'Retry-After': '20', **headers}
        return 202, list(selected.items()), b''

    def test_async_original_response_then_only_explicit_canonical_observations(self):
        replies = [self.accepted(), (200, [], b'{"status":"Succeeded"}'),
                   (200, [], self.item()), (200, [], json.dumps({'value': [self.job()], 'continuationToken': 'opaque+/='}).encode()),
                   (200, [('Retry-After', '10')], json.dumps(self.job(status='Completed')).encode())]
        with self.fixture(replies) as (cfg, origin, calls, auth, account):
            with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                accepted = owner.create_graph_model(PAYLOAD)
                self.assertIs(type(accepted), FabricAcceptedCreation)
                self.assertEqual(accepted.operation_id, O)
                self.assertEqual(len(calls), 1)  # no hidden poll
                owner.creation_state(O)
                result = owner.creation_result(O)
                self.assertEqual(result.body, self.item())
                owner.item_jobs(G)
                owner.item_job(G, J)
                with self.assertRaisesRegex(PermissionError, 'already submitted'):
                    owner.create_graph_model(PAYLOAD)
            self.assertEqual(len(auth), 3)
            self.assertEqual([x[0] for x in calls], ['POST', 'GET', 'GET', 'GET', 'GET'])
            self.assertEqual(calls[0][2], PAYLOAD)
            self.assertTrue(all(x[3] == 'Bearer private-secret' for x in calls))
            self.assertEqual(calls[1][1], '/v1/operations/' + O)
            self.assertEqual(calls[2][1], '/v1/operations/' + O + '/result')
            self.assertIsNone(owner._token)
            with self.assertRaises(PermissionError):
                owner.creation_state(O)

    def test_synchronous_201_has_real_item_and_no_invented_operation(self):
        with self.fixture([(201, [], self.item())]) as (cfg, origin, calls, auth, account):
            with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                result = owner.create_graph_model(PAYLOAD)
                self.assertIs(type(result), FabricCreatedGraph)
                self.assertEqual(result.graph_model_id, G)
                self.assertFalse(hasattr(result, 'operation_id'))
            self.assertEqual(len(calls), 1)

    def test_redirect_refuses_and_never_retries_submission(self):
        with self.fixture([(307, [('Location', 'http://127.0.0.1:1/foreign')], b'')]) as (cfg, origin, calls, auth, account):
            with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                with self.assertRaisesRegex(PermissionError, 'redirect'):
                    owner.create_graph_model(PAYLOAD)
                with self.assertRaisesRegex(PermissionError, 'already submitted'):
                    owner.create_graph_model(PAYLOAD)
            self.assertEqual(len(calls), 1)

    def test_wrong_operation_header_or_foreign_item_refuses(self):
        with self.fixture([self.accepted(Location='https://attacker.invalid/v1/operations/' + O),
                           (200, [], json.dumps(self.job(itemId=W)).encode())]) as (cfg, origin, calls, auth, account):
            with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                with self.assertRaisesRegex(PermissionError, 'location'):
                    owner.create_graph_model(PAYLOAD)
                with self.assertRaisesRegex(PermissionError, 'item job'):
                    owner.item_job(G, J)

    def test_capacity_and_credential_echo_withhold_original_response(self):
        with self.fixture([(200, [], b'x' * 33)], maximum_response_bytes=32) as (cfg, origin, calls, auth, account):
            with self.assertRaisesRegex(ValueError, 'overflow'):
                with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                    owner.creation_state(O)
            self.assertEqual(len(auth), 3)
        with self.fixture([(200, [], b'{"status":"private-secret"}')]) as (cfg, origin, calls, auth, account):
            with self.assertRaisesRegex(PermissionError, 'Credential-bearing'):
                with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                    owner.creation_state(O)

    def test_cancellation_primary_and_account_closing_failure_still_erases_token(self):
        with self.fixture([]) as (cfg, origin, calls, auth, account):
            cancellation = KeyboardInterrupt('body cancelled')
            with self.assertRaises(KeyboardInterrupt) as captured:
                with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                    account['tenantId'] = G
                    raise cancellation
            self.assertIs(captured.exception, cancellation)
            self.assertTrue(cancellation.cleanup_failed)
            self.assertIsNone(owner._token)
            self.assertTrue(owner._closed)
            self.assertFalse(calls)

    def test_closed_selection_and_wrong_account_refuse_before_http(self):
        with self.fixture([]) as (cfg, origin, calls, auth, account):
            with self.assertRaises(ValueError):
                replace(cfg, maximum_requests=True)
            with self.assertRaises(TypeError):
                FabricDeploymentHttpConfig(**{**cfg.__dict__, 'graph_model_id': G})
            account['tenantId'] = G
            with self.assertRaises(PermissionError):
                with open_fabric_deployment_loopback_for_test(cfg, origin):
                    self.fail('opened')
            self.assertFalse(calls)

    def test_exact_job_and_continuation_routes_do_not_follow_foreign_uri(self):
        body = json.dumps({'value': [], 'continuationUri': 'https://attacker.invalid'}).encode()
        with self.fixture([(200, [], body), (200, [], json.dumps(self.job(id=O)).encode())]) as (cfg, origin, calls, auth, account):
            with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                result = owner.item_jobs(G, 'opaque+/=')
                self.assertEqual(result.body, body)
                with self.assertRaisesRegex(PermissionError, 'job ID'):
                    owner.item_job(G, J)
            self.assertTrue(calls[0][1].endswith('?continuationToken=opaque%2B%2F%3D'))

    def test_mutated_origin_and_request_capacity_refuse_before_submission(self):
        with self.fixture([(200, [], b'{"status":"Running"}')], maximum_requests=1) as (cfg, origin, calls, auth, account):
            with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                owner._origin = 'https://attacker.invalid'
                with self.assertRaises(PermissionError):
                    owner.creation_state(O)
                self.assertFalse(calls)
                owner._origin = origin
                owner.creation_state(O)
                with self.assertRaisesRegex(ValueError, 'request capacity'):
                    owner.creation_state(O)
            self.assertEqual(len(calls), 1)

    def test_duplicate_lro_headers_and_unknown_status_refuse_without_hidden_calls(self):
        headers = self.accepted()[1] + [('x-ms-operation-id', O)]
        with self.fixture([(202, headers, b''), (200, [], b'{"status":"NewUnqualifiedState"}')]) as (cfg, origin, calls, auth, account):
            with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                with self.assertRaisesRegex(ValueError, 'Duplicate operation header'):
                    owner.create_graph_model(PAYLOAD)
                with self.assertRaisesRegex(ValueError, 'Unqualified creation status'):
                    owner.creation_state(O)
            self.assertEqual(len(calls), 2)

    def test_definition_200_or_async_result_preserves_distinct_definition_envelope(self):
        raw = b'{"definition":{"format":"json","parts":[]},"extension":"retained"}'
        with self.fixture([(200, [], raw), self.accepted(), (200, [], b'{"status":"Succeeded"}'), (200, [], raw)]) as (cfg, origin, calls, auth, account):
            with open_fabric_deployment_loopback_for_test(cfg, origin) as owner:
                self.assertEqual(owner.graph_definition(G).body, raw)
                accepted = owner.graph_definition(G)
                self.assertEqual(accepted.status, 202)
                self.assertEqual(len(calls), 2)  # no implicit definition polling
                owner.creation_state(O)  # same public LRO state envelope
                self.assertEqual(owner.operation_result(O).body, raw)
            self.assertEqual(calls[0][1], '/v1/workspaces/' + W + '/graphModels/' + G + '/getDefinition')
            self.assertEqual(calls[0][2], b'')
            self.assertEqual(calls[-1][1], '/v1/operations/' + O + '/result')

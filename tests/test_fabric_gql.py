"""Injected HTTP controls; no Fabric access or native execution claim."""
from contextlib import contextmanager
import json
import unittest
from unittest.mock import patch
from ashlar_host.fabric_gql import FabricGraphBinding, FabricGqlError, execute_gql


class Tests(unittest.TestCase):
    def setUp(self):
        self.binding = FabricGraphBinding('11111111-1111-1111-1111-111111111111',
            '22222222-2222-2222-2222-222222222222', 'a' * 64)
        self.calls = []; self.admissions = []; self.closed = False
        self.packets = []
        outer = self
        class Policy:
            @contextmanager
            def hold(self, binding, context):
                try: yield None
                finally: outer.closed = True
            def admit(self, binding, query, context):
                outer.admissions.append((binding, query, context))
        class Transport:
            def post(self, *args):
                outer.calls.append(args)
                return outer.packets.pop(0)
        self.policy = Policy(); self.transport = Transport()

    def packet(self, rows, columns=None, code='00000', **extra):
        value = {'status': {'code': code}, 'result': {'kind': 'TABLE',
            'columns': columns if columns is not None else [{'name': 'count', 'gqlType': 'UINT64'}],
            'data': rows}, **extra}
        return 200, json.dumps(value, separators=(',', ':')).encode()

    def execute(self, **options):
        return execute_gql(self.binding, 'MATCH (n) RETURN COUNT(*) AS count',
            policy=self.policy, transport=self.transport, context='held', **options)

    def test_exact_integer_original_request_and_closing_hold(self):
        self.packets = [self.packet([{'count': '18446744073709551615'}], unknownExtension={'future': 'retained'})]
        raw = self.packets[0][1]
        result = self.execute()
        self.assertEqual(result.rows, ((18446744073709551615,),))
        self.assertEqual(result.responses, (raw,))
        self.assertEqual(self.calls[0][1], b'{"query":"MATCH (n) RETURN COUNT(*) AS count"}')
        self.assertTrue(self.closed); self.assertEqual(len(self.admissions), 2)

    def test_opaque_continuation_once_encoded_and_same_original_body(self):
        first = self.packet([], [], '02000')
        value = json.loads(first[1]); value['result']['nextPage'] = 'a%2F/+?='
        self.packets = [(200, json.dumps(value).encode()), self.packet([{'count': 2}])]
        self.assertEqual(self.execute().rows, ((2,),))
        self.assertTrue(self.calls[1][0].endswith('&continuationToken=a%252F%2F%2B%3F%3D'))
        self.assertEqual(self.calls[0][1], self.calls[1][1])

    def test_truncation_and_invalid_integer_refuse_all_results(self):
        for packet in (self.packet([{'count': 1}], additionalStatuses=[{'code': '01000',
                'diagnostics': {'_graphaneGqlStatus': {'gqlType': 'STRING', 'value': '01M11'}}}]),
                self.packet([{'count': True}]), self.packet([{'count': '-1'}]),
                self.packet([{'count': '18446744073709551616'}])):
            self.packets = [packet]
            with self.assertRaises(FabricGqlError): self.execute()
            self.assertTrue(self.closed)

    def test_capacity_duplicate_and_partial_continuation_refuse(self):
        malformed = b'{"status":{"code":"00000","code":"02000"},"result":{}}'
        partial = json.loads(self.packet([{'count': 1}])[1]); partial['result']['nextPage'] = 'opaque'
        for packet in ((200, malformed), (200, json.dumps(partial).encode()), self.packet([{'count': 1}, {'count': 2}])):
            self.packets = [packet]
            with self.assertRaises(FabricGqlError): self.execute(maximum_rows=1)
        value = json.loads(self.packet([], [], '02000')[1]); value['result']['nextPage'] = 'same-token'
        self.packets = [(200, json.dumps(value).encode())] * 2
        with self.assertRaises(FabricGqlError): self.execute(maximum_requests=2)

    def test_large_integer_tokens_and_decimal_strings_refuse_before_conversion(self):
        for raw in (b'{"status":{"code":"00000"},"result":{"kind":"TABLE",'
                b'"columns":[{"name":"count","gqlType":"UINT64"}],"data":[{"count":'
                + b'9' * 20000 + b'}]}}', self.packet([{'count': '9' * 20000}])[1]):
            self.packets = [(200, raw)]
            with self.assertRaises(FabricGqlError): self.execute()
        self.assertTrue(self.closed)

    def test_cleanup_exceeding_deadline_withholds_complete_rows(self):
        self.packets = [self.packet([{'count': 1}])]
        with patch('ashlar_host.fabric_gql.time.monotonic', side_effect=[0, 0, 0, 61]):
            with self.assertRaises(FabricGqlError): self.execute(deadline_seconds=60)
        self.assertTrue(self.closed)

    def test_transport_cancellation_identity_survives_suppressing_hold(self):
        cancellation = KeyboardInterrupt('original transport cancellation')
        class Transport:
            def post(self, *args): raise cancellation
        class Policy:
            @contextmanager
            def hold(self, *args):
                try: yield None
                except BaseException: pass
            def admit(self, *args): pass
        with self.assertRaises(KeyboardInterrupt) as observed:
            execute_gql(self.binding, 'RETURN 1', transport=Transport(), policy=Policy(), context=None)
        self.assertIs(observed.exception, cancellation)


if __name__ == '__main__': unittest.main()

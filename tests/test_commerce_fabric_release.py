# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual commerce owner composition over inert graph and HTTP ports; no native claim."""
from contextlib import contextmanager
import json
import unittest
from unittest.mock import patch

from ashlar_host.commerce_fabric_release import execute_commerce_fabric_gql
from ashlar_host.commerce_graph_release import hold_original_commerce_graph_release, HeldOriginalCommerceGraphRelease
from ashlar_host.fabric_gql import FabricGraphBinding
import test_commerce_graphframes_installed as fixtures


class Tests(unittest.TestCase):
    opened = fixtures.ReaderTests.opened
    active_session = fixtures.ReaderTests.active_session

    def setup_case(self, *, permit=None, admit_failure=False, close_failure=False, transport_failure=None):
        session, events = self.active_session()
        opened = session.opened
        with hold_original_commerce_graph_release(opened) as held:
            digest = held.release.sha256
        events.clear()
        binding = FabricGraphBinding('11111111-1111-1111-1111-111111111111',
                                    '22222222-2222-2222-2222-222222222222', digest)
        graph_context, http_context = object(), object()
        query = 'MATCH (n) RETURN COUNT(*) AS count'
        outer = self
        class Graph:
            active = False
            admissions = 0
            @contextmanager
            def hold(self, selected, context):
                outer.assertEqual(selected, binding)
                outer.assertIs(context, graph_context)
                outer.assertTrue(opened.driver.held)
                outer.assertTrue(opened.driver.pin_held)
                self.active = True
                events.append('graph-open')
                try:
                    yield permit
                finally:
                    self.active = False
                    events.append('graph-close')
                    if close_failure:
                        raise PermissionError('inert graph closing refusal')
            def admit(self, selected, text, context):
                outer.assertTrue(self.active)
                outer.assertEqual((selected, text), (binding, query))
                outer.assertIs(context, graph_context)
                self.admissions += 1
                events.append('graph-admit')
                if admit_failure:
                    raise PermissionError('inert graph admission refusal')
        graph = Graph()
        class HTTP:
            calls = 0
            def post(self, url, body, context, timeout):
                outer.assertIs(context, http_context)
                outer.assertTrue(graph.active)
                outer.assertTrue(opened.driver.held)
                outer.assertTrue(opened.driver.pin_held)
                outer.assertEqual(json.loads(body), {'query': query})
                self.calls += 1
                events.append('http')
                if transport_failure is not None:
                    raise transport_failure
                return 200, b'{"status":{"code":"00000"},"result":{"kind":"TABLE","columns":[{"name":"count","gqlType":"UINT64"}],"data":[{"count":"11"}]}}'
        http = HTTP()
        def execute(selected=binding):
            return execute_commerce_fabric_gql(opened, selected, query, transport=http,
                transport_context=http_context, loaded_graph_policy=graph,
                loaded_graph_context=graph_context)
        return opened, binding, graph, http, events, execute

    def assert_closed(self, opened, graph, events):
        self.assertFalse(graph.active)
        self.assertFalse(opened.driver.held)
        self.assertFalse(opened.driver.pin_held)
        self.assertFalse(opened.provider.active)
        self.assertIn('writer-close', events)
        reads = [call for call in opened.driver.transport.query.call_args_list
                 if call.args[0].startswith('SELECT ')]
        # Both neutral discovery and the tested query independently open/close raw bags.
        self.assertEqual(len(reads), 8)

    def test_exact_release_separate_contexts_and_all_closing_before_result(self):
        opened, binding, graph, http, events, execute = self.setup_case()
        result = execute()
        self.assertEqual(result.rows, ((11,),))
        self.assertEqual(http.calls, 1)
        self.assertGreaterEqual(graph.admissions, 3)
        self.assertLess(events.index('graph-open'), events.index('http'))
        self.assertLess(events.index('http'), events.index('graph-close'))
        self.assertLess(events.index('graph-close'), events.index('writer-close'))
        self.assert_closed(opened, graph, events)

    def test_release_mismatch_refuses_before_loaded_graph_or_http(self):
        opened, binding, graph, http, events, execute = self.setup_case()
        wrong = FabricGraphBinding(binding.workspace_id, binding.graph_model_id, 'f' * 64)
        with self.assertRaises(PermissionError):
            execute(wrong)
        self.assertEqual(http.calls, 0)
        self.assertNotIn('graph-open', events)
        self.assert_closed(opened, graph, events)

    def test_graph_admission_hold_and_closing_refusals_withhold_rows(self):
        for options in ({'admit_failure': True}, {'permit': object()}, {'close_failure': True}):
            with self.subTest(options=tuple(options)):
                opened, binding, graph, http, events, execute = self.setup_case(**options)
                with self.assertRaises(PermissionError):
                    execute()
                self.assertIn('graph-close', events)
                self.assertEqual(http.calls, int('close_failure' in options))
                self.assert_closed(opened, graph, events)

    def test_source_refusal_and_transport_cancellation_attempt_independent_closing(self):
        cancellation = KeyboardInterrupt('inert transport cancellation')
        for source_failure in (False, True):
            with self.subTest(source_failure=source_failure):
                opened, binding, graph, http, events, execute = self.setup_case(
                    transport_failure=None if source_failure else cancellation)
                original = HeldOriginalCommerceGraphRelease.renew
                renewals = []
                def renew(owner):
                    renewals.append('source-renew')
                    original(owner)
                    if source_failure and http.calls:
                        raise PermissionError('inert source closing refusal')
                with patch.object(HeldOriginalCommerceGraphRelease, 'renew', renew):
                    with self.assertRaises(PermissionError if source_failure else KeyboardInterrupt) as caught:
                        execute()
                if not source_failure:
                    self.assertIs(caught.exception, cancellation)
                self.assertGreaterEqual(len(renewals), 4)
                self.assertGreaterEqual(graph.admissions, 3)
                self.assertIn('graph-close', events)
                self.assert_closed(opened, graph, events)

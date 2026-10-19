# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual HTTP/SQLite and real source-owner composition over explicit SDK doubles.

No native Fabric/Delta or production mutation-isolation qualification is claimed.
"""
from contextlib import contextmanager
import json
import multiprocessing
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.request

import test_commerce_fabric_deployment as fixtures
from ashlar_host.commerce_fabric_admission import admit_commerce_fabric_deployment, CommerceFabricLoadedGraphPolicy
from ashlar_host.commerce_fabric_release import execute_commerce_fabric_gql
from ashlar_host.fabric_ordinary_http import FabricOrdinaryHttpConfig, open_fabric_ordinary_http
from ashlar_host.fabric_release_journal import JournalRefusal, FabricReleaseJournal


def mutation_probe(root, sender):
    try:
        with FabricReleaseJournal(Path(root)).mutation():
            sender.send('unexpected mutation access')
    except JournalRefusal as error:
        sender.send(str(error))
    finally:
        sender.close()


class Tests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.Tests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    @contextmanager
    def gql(self, state):
        config = FabricOrdinaryHttpConfig(**vars(self.fixture.config.upload.authentication), graph_model_id=fixtures.G)
        original = urllib.request.build_opener
        origin = state['origin']
        def opener(*handlers):
            actual = original(*handlers)
            class Routing:
                def open(self, request, timeout):
                    if request.full_url.startswith('https://api.fabric.microsoft.com/'):
                        request = urllib.request.Request(origin + request.full_url[len('https://api.fabric.microsoft.com'):],
                            data=request.data, headers=dict(request.header_items()), method=request.method)
                    return actual.open(request, timeout=timeout)
            return Routing()
        with patch('ashlar_host.fabric_ordinary_http.capture', state['capture']), patch('urllib.request.build_opener', opener):
            with open_fabric_ordinary_http(config) as transport:
                yield transport

    def created(self, owner):
        owner.stage()
        owner.submit_creation()
        owner.observe_creation()
        owner.observe_initial_load()

    def test_native_full_bags_atomic_admission_then_actual_source_bound_reader_policy(self):
        fixture = self.fixture
        with fixture.transport(admission=True) as state:
            with self.gql(state) as transport:
                with fixture.deployment() as owner:
                    self.created(owner)
                    binding, revision = admit_commerce_fabric_deployment(owner, transport=transport,
                        transport_context=transport.context, expected_revision=0)
                    self.assertEqual(revision, 1)
                predecessor, raw = fixture.journal.admission(fixtures.C)
                receipt = json.loads(raw)
                self.assertEqual(predecessor, 0)
                self.assertEqual(set(receipt['native']['bags']), {'nodes', 'edges', 'endpoints'})
                policy = CommerceFabricLoadedGraphPolicy(FabricReleaseJournal(fixture.journal.root), fixture.config, transport, transport.context)
                with self.assertRaisesRegex(PermissionError, 'source-held'):
                    with policy.hold(binding, policy.context): self.fail('source absent')
                def probe():
                    context = multiprocessing.get_context('spawn')
                    receiver, sender = context.Pipe(duplex=False)
                    child = context.Process(target=mutation_probe, args=(str(fixture.journal.root), sender))
                    child.start()
                    sender.close()
                    try:
                        state['reader_probe_result'] = receiver.recv() if receiver.poll(10) else 'timeout'
                        child.join(10)
                    finally:
                        if child.is_alive(): child.kill(); child.join()
                        receiver.close()
                state['reader_probe'] = probe
                result = execute_commerce_fabric_gql(fixture.opened, binding,
                    'MATCH (n:AshlarNode) RETURN count(n) AS row_count', transport=transport,
                    transport_context=transport.context, loaded_graph_policy=policy, loaded_graph_context=policy.context)
                self.assertEqual(result.rows, ((11,),))
                self.assertEqual(state['reader_probe_result'], 'lease busy')
                self.assertIsNone(policy._scope)
                self.assertIsNone(policy._source)
            self.assertTrue(all(hold == (True, True, True) for hold in state['holds']))
        self.assertFalse(fixture.opened.provider.active)
        with fixture.journal.reader() as active: self.assertEqual(active.graph_model_id, fixtures.G)

    def test_no_query_before_actual_original_load_completed(self):
        fixture = self.fixture
        with fixture.transport(admission=True, load_status='NotStarted') as state:
            with self.gql(state) as transport:
                with fixture.deployment() as owner:
                    self.created(owner)
                    with self.assertRaisesRegex(PermissionError, 'terminal Completed'):
                        admit_commerce_fabric_deployment(owner, transport=transport, transport_context=transport.context, expected_revision=0)
            self.assertFalse(any('/executeQuery' in call[1] for call in state['calls']))
        with self.assertRaises(JournalRefusal):
            with fixture.journal.reader(): self.fail('early activation')

    def test_definition_drift_refuses_before_bags_or_activation(self):
        fixture = self.fixture
        with fixture.transport(admission=True, definition_drift=True) as state:
            with self.gql(state) as transport:
                with fixture.deployment() as owner:
                    self.created(owner)
                    with self.assertRaisesRegex(PermissionError, 'definition/mapping'):
                        admit_commerce_fabric_deployment(owner, transport=transport, transport_context=transport.context, expected_revision=0)
            self.assertFalse(any('/executeQuery' in call[1] for call in state['calls']))
        with self.assertRaises(JournalRefusal): fixture.journal.admission(fixtures.C)

    def test_multiplicity_drift_never_promotes_partial_evidence(self):
        fixture = self.fixture
        with fixture.transport(admission=True, corrupt_bag=True) as state:
            with self.gql(state) as transport:
                with fixture.deployment() as owner:
                    self.created(owner)
                    with self.assertRaisesRegex(PermissionError, 'multiplicity bag'):
                        admit_commerce_fabric_deployment(owner, transport=transport, transport_context=transport.context, expected_revision=0)
            self.assertTrue(any('/getDefinition' in call[1] for call in state['calls']))
        with self.assertRaises(JournalRefusal): fixture.journal.admission(fixtures.C)

    def test_restarted_policy_rechecks_current_native_bags_instead_of_trusting_receipt(self):
        fixture = self.fixture
        with fixture.transport(admission=True) as state:
            with self.gql(state) as transport:
                with fixture.deployment() as owner:
                    self.created(owner)
                    binding, _ = admit_commerce_fabric_deployment(owner, transport=transport,
                        transport_context=transport.context, expected_revision=0)
                state['corrupt_bag'] = True
                before = len(state['calls'])
                policy = CommerceFabricLoadedGraphPolicy(FabricReleaseJournal(fixture.journal.root), fixture.config,
                    transport, transport.context)
                with self.assertRaisesRegex(PermissionError, 'multiplicity bag'):
                    execute_commerce_fabric_gql(fixture.opened, binding,
                        'MATCH (n:AshlarNode) RETURN count(n) AS row_count', transport=transport,
                        transport_context=transport.context, loaded_graph_policy=policy, loaded_graph_context=policy.context)
                self.assertFalse(any(b'row_count' in call[2] for call in state['calls'][before:]))
        self.assertFalse(fixture.opened.provider.active)

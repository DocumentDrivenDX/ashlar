# SPDX-License-Identifier: MIT OR Apache-2.0
"""Protocol-fake safety tests for the read-only Truss .51 source seam."""
import unittest
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import test_truss_feed_sessions as session_fixtures
from ashlar_host.truss_feed_sessions import TrussFeedSessions, TrussSessionError
from ashlar_host.truss051_source_ack_interval import acknowledge_post_verifier_source_interval
from ashlar_host.managed_current_spark_receipt_read import (
    ManagedCurrentSparkReceiptReopenObservation,
)


class Tests(unittest.TestCase):
    def setUp(self):
        old, _, context, events, connection, _, _ = session_fixtures.Tests().setup_case()
        with patch('ashlar_host.truss_feed_sessions._sdk', return_value=(
                None, None, None, None, session_fixtures.Page,
                session_fixtures.Acknowledged)):
            self.owner = TrussFeedSessions(old.page, old.feed, old.connection_factory, context)
        self.context, self.events, self.connection = context, events, connection

    def _runtime(self, *, version='0.0.51', profile='ashlar-truss-current-catalog-evidence/0.6'):
        return (patch('ashlar_host.truss_current_catalog.installed_current_truss_profile',
                      return_value=profile),
                patch('ashlar_host.truss_current_catalog.installed_truss_distribution',
                      return_value=SimpleNamespace(version=version)))

    def _assembly(self, *, foreign_current=False, foreign_retained=False):
        owner = self.owner
        events = self.events
        @contextmanager
        def assembly(connection, readonly):
            self.assertTrue(readonly)
            self.assertIs(connection, self.connection)
            tx = object()
            def read(_tx, *, consumer_id, maximum_facts, maximum_bytes):
                self.assertIs(_tx, tx)
                self.assertEqual((consumer_id, maximum_facts, maximum_bytes),
                                 (owner.page.consumer_id, 4096, 8388608))
                events.append('read-current')
                return replace(owner.page, prior=owner.page.next) if foreign_current else owner.page
            def reread(_tx, page, *, maximum_facts, maximum_bytes):
                self.assertIs(_tx, tx)
                self.assertIs(page, owner.page)
                self.assertEqual((maximum_facts, maximum_bytes), (4096, 8388608))
                events.append('reread-retained')
                return replace(owner.page, content=b'foreign') if foreign_retained else owner.page
            yield SimpleNamespace(read_feed_in_transaction=read,
                                  reread_feed_in_transaction=reread), tx
        return assembly

    def test_offline_managed_reopen_observation_cannot_authorize_ack(self):
        offline = ManagedCurrentSparkReceiptReopenObservation(
            profile='ashlar-managed-current-spark-receipt-read/0.1',
            publication_id='p', manifest={}, manifest_sha256='0' * 64,
            source_receipt_bytes=b'opaque', source_receipt_sha256='1' * 64,
            vector=object(), installation_inventory={}, attempt_custody=None)
        with patch.object(TrussFeedSessions, 'acknowledge', autospec=True) as ack, \
             patch.object(TrussFeedSessions, 'reconcile_051_source_interval', autospec=True) as reconcile, \
             patch.object(TrussFeedSessions, 'close_051_source_interval', autospec=True) as close:
            with self.assertRaisesRegex(TrussSessionError,
                    'live managed-read and native-pin owner handles are required'):
                acknowledge_post_verifier_source_interval(
                    self.owner, b'request', b'manifest', object(),
                    verified_observation=offline, context=self.context)
        ack.assert_not_called()
        reconcile.assert_not_called()
        close.assert_not_called()

    def test_renewal_reads_current_then_exact_retained_page_and_is_read_only(self):
        runtime = self._runtime()
        with runtime[0], runtime[1], \
             patch('ashlar_host.truss_feed_sessions._assembly', self._assembly()), \
             patch('ashlar_host.truss_feed_sessions._sdk', return_value=(
                 None, None, None, None, session_fixtures.Page,
                 session_fixtures.Acknowledged)), \
             patch('ashlar_host.truss_feed_sessions._value', side_effect=lambda value: value):
            self.owner.renew_051_source_custody(context=self.context)
        self.assertEqual(self.events, ['connect', 'read-current', 'reread-retained', 'rollback', 'close'])

    def test_renewal_refuses_current_or_retained_drift_before_any_ack(self):
        for assembly, expected in (
                (self._assembly(foreign_current=True), 'current source page differs'),
                (self._assembly(foreign_retained=True), 'exact retained source page differs')):
            self.events.clear()
            runtime = self._runtime()
            with runtime[0], runtime[1], \
                 patch('ashlar_host.truss_feed_sessions._assembly', assembly), \
                 patch('ashlar_host.truss_feed_sessions._sdk', return_value=(
                     None, None, None, None, session_fixtures.Page,
                     session_fixtures.Acknowledged)), \
                 patch('ashlar_host.truss_feed_sessions._value', side_effect=lambda value: value):
                with self.assertRaisesRegex(TrussSessionError, expected):
                    self.owner.renew_051_source_custody(context=self.context)
            self.assertEqual(self.events[-2:], ['rollback', 'close'])

    def test_read_only_source_seam_refuses_historical_050(self):
        runtime = self._runtime(version='0.0.50', profile='ashlar-truss-current-catalog-evidence/0.5')
        with runtime[0], runtime[1], patch('ashlar_host.truss_feed_sessions._assembly') as assembly:
            with self.assertRaisesRegex(TrussSessionError, 'Pinned Truss 0.0.51'):
                self.owner.renew_051_source_custody(context=self.context)
        assembly.assert_not_called()


if __name__ == '__main__':
    unittest.main()

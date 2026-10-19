# SPDX-License-Identifier: MIT OR Apache-2.0
import unittest
from dataclasses import dataclass
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock, patch
from ashlar_host.ordinary_query_connection import (
    OrdinaryQueryConnectionFactory, acquire_ordinary_query_connection)
from ashlar_host.publication_reader import PublicationProvider, open_commerce_reader

@dataclass
class Scope:
    scope_id: str = 'inert'


class Tests(unittest.TestCase):
    def test_exact_port_and_role_required_before_factory(self):
        callback = Mock()
        port = OrdinaryQueryConnectionFactory(callback)
        for factory, role in ((object(), 'reader'), (port, ''), (port, False)):
            with self.assertRaises(ValueError):
                acquire_ordinary_query_connection(factory, role)
        callback.assert_not_called()
        self.assertIs(acquire_ordinary_query_connection(port, 'reader'), callback.return_value)
        callback.assert_called_once_with('reader')

    def test_reader_rejects_untyped_port_before_io(self):
        with self.assertRaises(ValueError):
            with open_commerce_reader(None, None, ordinary_session=object()):
                self.fail('Invalid port reached reader')

    def provider(self, port):
        driver = SimpleNamespace(policy=SimpleNamespace(active={'manifest': {
            'table_versions_json': '{}'}}), transport=SimpleNamespace(targets={}),
            writer=lambda *args: nullcontext(), hold=lambda *args, **kwargs: nullcontext())
        return PublicationProvider(driver, {}, {'role': 'ordinary-reader', 'scope': Scope(),
            'source': 'fixture', 'signature': 'fixture'}, b'{}',
            b'{}', {}, context=object(), ordinary_session=port)

    def test_supplied_connection_renews_ack_and_closes_without_discovery(self):
        connection = Mock()
        factory = Mock(return_value=connection)
        provider = self.provider(OrdinaryQueryConnectionFactory(factory))
        with patch('ashlar.manifest.manifest_pin_vector', return_value=object()), \
             patch('ashlar_host.connection.connect', side_effect=AssertionError('Discovery')), \
             patch('ashlar_host.postgres.Session', return_value=object()), \
             patch.object(provider, '_ack', return_value={}) as ack:
            with provider.interval(provider.context):
                self.assertTrue(provider.active)
        self.assertEqual(ack.call_count, 2)
        factory.assert_called_once_with('ordinary-reader')
        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()
        self.assertFalse(provider.active)

    def test_failure_preserves_primary_and_closes_connection(self):
        connection = Mock()
        provider = self.provider(OrdinaryQueryConnectionFactory(lambda role: connection))
        primary = KeyboardInterrupt()
        with patch('ashlar.manifest.manifest_pin_vector', return_value=object()), \
             patch('ashlar_host.postgres.Session', return_value=object()), \
             patch.object(provider, '_ack', return_value={}):
            with self.assertRaises(KeyboardInterrupt) as caught:
                with provider.interval(provider.context):
                    raise primary
        self.assertIs(caught.exception, primary)
        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()
        self.assertFalse(provider.active)

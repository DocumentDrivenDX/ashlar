# SPDX-License-Identifier: MIT OR Apache-2.0
"""Ordinary connection composition controls, without a PostgreSQL service."""
from dataclasses import replace
import os
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from ashlar_host.ack import AckScope
from ashlar_host.original_medical_local_configuration import OriginalMedicalLocalSource
from ashlar_host.original_medical_local_sources import original_medical_local_source_ports


class Tests(unittest.TestCase):
    def selection(self):
        scope = AckScope('ashlar_ack_medical', '11111111-1111-4111-8111-111111111111',
            '22222222-2222-4222-8222-222222222222', 'medical-consumer',
            'private-original-medical-fixture', 'original-medical-v1')
        return OriginalMedicalLocalSource('127.0.0.1', 25435, 'medical_fixture',
            'isolated-loopback-trust', scope, 'medical_source', 'a'*64,
            'medical_writer', 'medical_reader', 'medical_ack', 3, 7000, 1200)

    def factories(self, ports):
        return (ports.writer_connection, ports.registration.source.connection_factory,
                ports.registration.metadata_factory)

    def test_inert_construction_and_exact_three_role_connections(self):
        connect = Mock(side_effect=[object(), object(), object()])
        with patch.dict(sys.modules, {'psycopg':SimpleNamespace(connect=connect)}), patch.dict(os.environ, {}, clear=True):
            ports = original_medical_local_source_ports(self.selection())
            connect.assert_not_called()
            outputs = [factory(object()) for factory in self.factories(ports)]
        self.assertEqual(len({id(value) for value in outputs}), 3)
        for call, role in zip(connect.call_args_list, ('medical_writer', 'medical_reader', 'medical_ack')):
            self.assertEqual(call.args, ())
            self.assertEqual(call.kwargs, dict(host='127.0.0.1', hostaddr='127.0.0.1',
                port=25435, dbname='medical_fixture', user=role, password='',
                passfile='/dev/null', sslmode='disable', gssencmode='disable',
                options='-c statement_timeout=7000 -c lock_timeout=1200',
                connect_timeout=3, autocommit=False))
        self.assertEqual(ports.registration.source.scope, self.selection().scope)
        self.assertEqual(ports.registration.source.source_signature_sha256, 'a'*64)

    def test_ambient_pg_settings_refuse_every_factory_before_connection(self):
        connect = Mock()
        ports = original_medical_local_source_ports(self.selection())
        with patch.dict(sys.modules, {'psycopg':SimpleNamespace(connect=connect)}):
            for name in ('PGHOST', 'PGSERVICEFILE', 'PGOPTIONS', 'PGPASSWORD'):
                with self.subTest(name=name), patch.dict(os.environ, {name:'foreign'}, clear=True):
                    for factory in self.factories(ports):
                        with self.assertRaises(PermissionError): factory(object())
        connect.assert_not_called()

    def test_valid_configuration_drift_refuses_original_registration(self):
        for field, value in (('reader_role','replacement_reader'), ('port',25436),
                             ('statement_ms',8000), ('source_signature_sha256','b'*64)):
            selection = self.selection(); ports = original_medical_local_source_ports(selection)
            object.__setattr__(selection, field, value)
            with self.subTest(field=field), patch.dict(os.environ, {}, clear=True):
                for factory in self.factories(ports):
                    with self.assertRaises(PermissionError): factory(object())

    def test_exact_selection_and_revalidation_before_connection(self):
        with self.assertRaises(ValueError): original_medical_local_source_ports(SimpleNamespace())
        for field, value in (('host','remote'), ('writer_role','medical_reader'), ('connect_seconds',True)):
            with self.subTest(field=field), self.assertRaises(ValueError): replace(self.selection(), **{field:value})
        selection = self.selection(); ports = original_medical_local_source_ports(selection)
        object.__setattr__(selection, 'host', 'remote')
        connect = Mock()
        with patch.dict(sys.modules, {'psycopg':SimpleNamespace(connect=connect)}), patch.dict(os.environ, {}, clear=True):
            for factory in self.factories(ports):
                with self.assertRaises(ValueError): factory(object())
        connect.assert_not_called()

    def test_connect_failure_preserves_original_exception_without_retry(self):
        error = OSError('connection unavailable'); connect = Mock(side_effect=error)
        with patch.dict(sys.modules, {'psycopg':SimpleNamespace(connect=connect)}), patch.dict(os.environ, {}, clear=True):
            ports = original_medical_local_source_ports(self.selection())
            with self.assertRaises(OSError) as caught: ports.writer_connection(object())
        self.assertIs(caught.exception, error); self.assertEqual(connect.call_count, 1)

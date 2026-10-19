# SPDX-License-Identifier: MIT OR Apache-2.0
"""Configured ordinary reader/ACK SELECT connection custody; no native database."""
import os,sys,unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
import test_original_medical_local_sources as fixtures
from ashlar_host.original_medical_local_read_sources import original_medical_local_read_sources
from ashlar_host.medical_read_sources import MedicalReadSourceFactory

class Tests(unittest.TestCase):
    def selection(self):return fixtures.Tests().selection()
    def test_exact_read_factory_never_exposes_or_opens_writer(self):
        connect=Mock(side_effect=[object(),object()])
        with patch.dict(sys.modules,{'psycopg':SimpleNamespace(connect=connect)}),patch.dict(os.environ,{},clear=True):
            factory=original_medical_local_read_sources(self.selection());self.assertIs(type(factory),MedicalReadSourceFactory);connect.assert_not_called()
            r=factory.registration;r.source.connection_factory(object());r.metadata_factory(object())
        self.assertEqual([c.kwargs['user']for c in connect.call_args_list],['medical_reader','medical_ack'])
        self.assertFalse(hasattr(factory,'writer_connection'));self.assertTrue(all(c.kwargs['autocommit'] is False and c.kwargs['passfile']=='/dev/null'for c in connect.call_args_list))
    def test_foreign_selection_or_ambient_routing_refuses_before_connection(self):
        with self.assertRaises(ValueError):original_medical_local_read_sources(SimpleNamespace())
        s=self.selection();factory=original_medical_local_read_sources(s);r=factory.registration;connect=Mock()
        with patch.dict(sys.modules,{'psycopg':SimpleNamespace(connect=connect)}),patch.dict(os.environ,{'PGHOST':'foreign'},clear=True):
            for port in [r.source.connection_factory,r.metadata_factory]:
                with self.assertRaises(PermissionError):port(object())
        object.__setattr__(s,'reader_role','replacement')
        with patch.dict(os.environ,{},clear=True):
            for port in [r.source.connection_factory,r.metadata_factory]:
                with self.assertRaises(PermissionError):port(object())
        connect.assert_not_called()

if __name__=='__main__':unittest.main()

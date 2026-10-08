from pathlib import Path
from types import SimpleNamespace as S
from unittest.mock import Mock
import unittest
import test_native_csv_configuration as configuration
from native_registry_authority import NativeRegistryAuthority
from ashlar.schema import SchemaIntakeError
from ashlar.authority import AuthorityError

ROOT=Path(__file__).resolve().parents[1]
class RegistryAuthorityTests(unittest.TestCase):
    def authority(self):
        w=Mock();w.current_user.me.return_value=S(user_name='owner')
        w.catalogs.get.return_value=S(owner='owner');w.schemas.get.return_value=S(owner='owner')
        w.tables.get.return_value=S(owner='owner',table_type=S(value='MANAGED'))
        w.api_client.do.return_value={}
        retained=[]
        def record(kind,name,page,ordinal):retained.append((kind,name,page,ordinal))
        return NativeRegistryAuthority(w,configuration.ConfigurationTests().proof(),ROOT,record),w,retained
    def test_fresh_actor_and_complete_inherited_permissions_required_on_every_admission(self):
        authority,w,retained=self.authority()
        w.api_client.do.side_effect=[{'next_page_token':'page2'},{},{},{}]
        authority.admit('customer.graph.schema_intake')
        self.assertEqual(len(retained),4)
        self.assertEqual(w.api_client.do.call_args_list[1].kwargs['query']['page_token'],'page2')
        w.current_user.me.return_value=S(user_name='other')
        with self.assertRaises(PermissionError):authority.admit('customer.graph.schema_intake')
        self.assertEqual(len(retained),4)
    def test_external_registry_foreign_namespace_and_untrusted_writer_refuse(self):
        for mode in ('external','foreign','writer'):
            authority,w,retained=self.authority()
            if mode=='external':w.tables.get.return_value.table_type.value='EXTERNAL'
            if mode=='writer':w.api_client.do.return_value={'privilege_assignments':[{'principal':'other','privileges':[{'privilege':'MODIFY','inherited_from_type':'catalog','inherited_from_name':'customer'}]}]}
            table='foreign.graph.schema_intake' if mode=='foreign' else 'customer.graph.schema_intake'
            with self.subTest(mode=mode),self.assertRaises((PermissionError,SchemaIntakeError,AuthorityError)):authority.admit(table)

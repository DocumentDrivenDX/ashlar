# SPDX-License-Identifier: MIT OR Apache-2.0
import json
import unittest
from ashlar_host.managed_carriers import load_managed_carriers
from ashlar_host.managed_current_carriers import ROLES,load_current_managed_carriers

class Tests(unittest.TestCase):
    def test_current_umf_migration_preserves_all_six_physical_layouts(self):
        old=load_managed_carriers();current=load_current_managed_carriers()
        for role in ROLES:
            with self.subTest(role=role):
                self.assertEqual(current.ordered_fields(role),old.ordered_fields(role))
                source=json.loads(dict(current.inputs)[role+'.umf.json'])
                self.assertEqual(source['umf'],'0.10.0')
                self.assertEqual(source['vocabularies']['umf.delta']['version'],'0.1.0')
                self.assertEqual(source['vocabularies']['umf.delta.definition']['version'],'0.1.0')
                ddl=current.create_sql(role,('client_dev','trial_schema',role))
                self.assertTrue(ddl.startswith(f'CREATE TABLE `client_dev`.`trial_schema`.`{role}` ('))
        self.assertEqual(len(ROLES),6)

    def test_identifier_and_role_are_closed(self):
        carriers=load_current_managed_carriers()
        with self.assertRaises(ValueError):carriers.create_sql('source_record',('c','s','t'))
        with self.assertRaises(ValueError):carriers.create_sql('object_current',('c;drop','s','object_current'))

if __name__=='__main__':unittest.main()

import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import patch
from prepare_pack_query_cases import prepare,ROOT

class Tests(unittest.TestCase):
    def test_complete_original_sql_and_graph_expectations_stay_distinct(self):
        for pack,count in [('archaeology',8),('ecology',9)]:
            result=prepare(pack)
            original=ROOT/'examples/domain-packs'/pack/'upstream'
            scenarios=json.loads((original/'pack.json').read_bytes())['scenario_checks']
            self.assertEqual([case['original_scenario'] for case in result['cases']],scenarios)
            self.assertEqual(len(result['cases']),count)
            for item in result['original_inputs']:
                self.assertEqual(hashlib.sha256(Path(item['path']).read_bytes()).hexdigest(),item['sha256'])
            self.assertEqual(result['source_metadata'],json.loads((original/'graph/fixture.json').read_bytes())['source_metadata'])
        cycle=prepare('archaeology')['cases'][0]
        self.assertEqual(cycle['original_scenario']['expected'],[['ST1','ST2']])
        self.assertEqual(cycle['original_graph_expected'],[['[42,0,"ST1"]','[42,0,"ST2"]']])

    def test_injective_qualified_field_bindings_preserve_declared_fields(self):
        for pack in ('archaeology','ecology'):
            result=prepare(pack)
            source=json.loads((ROOT/'examples/domain-packs'/pack/'upstream/ontology.json').read_bytes())
            original={(source['id'],module['id'],field['id']):field for module in source['modules'] for field in module['elements'] if field['kind']=='field'}
            binding={}
            for table in result['tables']:
                self.assertEqual(table['identity'][0],source['id'])
                for field in table['fields']:
                    identity=tuple(field['identity']);self.assertEqual(field['original_field'],original[identity])
                    self.assertTrue(0<int(field['development_property_id'])<2**63)
                    self.assertEqual(binding.setdefault(identity,field['development_property_id']),field['development_property_id'])
            self.assertEqual(len(binding),len(set(binding.values())))

    def test_changed_original_scenario_bytes_and_unknown_pack_refuse(self):
        with self.assertRaises(ValueError):prepare('unknown')
        original_read=Path.read_bytes
        def changed(path):
            raw=original_read(path)
            return raw+b' ' if path.name=='pack.json' else raw
        with patch.object(Path,'read_bytes',changed):
            for pack in ('archaeology','ecology'):
                with self.assertRaises(ValueError):prepare(pack)

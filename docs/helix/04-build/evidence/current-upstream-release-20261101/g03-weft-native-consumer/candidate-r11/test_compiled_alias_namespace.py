"""Effect-free binding between Spark alias names and original Weft identifiers."""
import importlib.util
import json
from pathlib import Path
import unittest

SCRIPT = Path(__file__).with_name('prepare.py')
SPEC = importlib.util.spec_from_file_location('g03_native_prepare_namespace', SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

class CompiledAliasNamespaceTests(unittest.TestCase):
    def setUp(self):
        self.pub='local-configured-jsonl-123456789abc'
        self.namespace=MODULE.native_alias_namespace(self.pub)
        names=[['spark_catalog',self.namespace,r] for r in MODULE.ROLES]
        self.request={'target':{'bindingJson':json.dumps({'publication':{'id':self.pub,'tables':[{'name':n} for n in names]}})}}
        self.response={'sql':'SELECT * FROM '+' JOIN '.join('`'+'`.`'.join(n)+'` VERSION AS OF 2' for n in names)}

    def test_compiled_names_match_created_publication_aliases(self):
        self.assertEqual(self.namespace,'ashlar_g03_123456789abc')
        self.assertEqual(MODULE.assert_compiled_alias_binding(self.request,self.response,MODULE.ROLES),self.namespace)

    def test_grouped_and_page_artifacts_use_their_exact_role_subsets(self):
        binding=json.loads(self.request['target']['bindingJson'])
        object_name=binding['publication']['tables'][0]['name']
        edge_name=binding['publication']['tables'][1]['name']
        grouped={'sql':'SELECT * FROM `'+'`.`'.join(object_name)+'` VERSION AS OF 2 JOIN `'+'`.`'.join(edge_name)+'` VERSION AS OF 2'}
        self.assertEqual(MODULE.assert_compiled_alias_binding(self.request,grouped,('object_current','edge_current')),self.namespace)
        page={'sql':'SELECT * FROM `'+'`.`'.join(object_name)+'` VERSION AS OF 2'}
        self.assertEqual(MODULE.assert_compiled_alias_binding(self.request,page,('object_current',)),self.namespace)

    def test_installation_suffix_mismatch_is_refused(self):
        binding=json.loads(self.request['target']['bindingJson'])
        for table in binding['publication']['tables']:
            table['name'][1]='ashlar_g03_wronginstallation'
        self.request['target']['bindingJson']=json.dumps(binding)
        with self.assertRaisesRegex(RuntimeError,'publication-derived Spark namespace'):
            MODULE.assert_compiled_alias_binding(self.request,self.response,MODULE.ROLES)

if __name__ == '__main__':
    unittest.main()

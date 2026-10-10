"""Public explicit selection/original source controls; inert ports only."""
from copy import deepcopy
import unittest
from unittest.mock import patch
from ashlar_host import count_star_query as module
from ashlar_host.config import HostError
import test_host_paths_query as fixture
class CountStarHostWiringTests(unittest.TestCase):
    def test_fourteen_requests_preserve_original_sql_model_binding_bytes(self):
        manifest,registry,aliases=fixture.metadata()
        original=deepcopy((fixture.BINDINGS,manifest,registry,aliases))
        cases=module.commerce_path_cases()+module.commerce_count_star_cases()
        self.assertEqual(len(cases),14)
        for _,sql in cases:
            args=(sql,fixture.MODEL,fixture.BINDINGS,manifest,registry,aliases)
            before=module.original_commerce_path_request(*args,profile='paths-keys')
            expected=deepcopy(before);expected.update(interfaceVersion='weft-compile/0.4.1',dialect='weft-sql/0.4.1')
            expected['target']['backendVersion']='0.4.1-count-star-having-candidate'
            actual=module.commerce_count_star_request(*args)
            self.assertEqual(actual,expected);self.assertEqual(actual['sql'],sql)
            self.assertEqual(actual['modules'][0]['documentJson'].encode(),fixture.MODEL)
        self.assertEqual((fixture.BINDINGS,manifest,registry,aliases),original)
    def test_invalid_configuration_refuses_before_runtime_effects(self):
        with patch.object(module,'runtime_paths',side_effect=AssertionError('native-effect')):
            with self.assertRaises(HostError):module.query_commerce_count_star(object())
if __name__=='__main__':unittest.main()

import json
import unittest
from ashlar.origin import map_acceptance_origin, restore_asserted_origin, OriginMappingError

class OriginMappingTests(unittest.TestCase):
    def test_independent_exact_value_shape_and_literal_tags(self):
        original=b'{"z":[],"a":{"kind":"integer","text":"9007199254740993"},"empty":{},"nil":null,"flag":false}'
        mapped=map_acceptance_origin(original)
        expected={'kind':'map','entries':[
            {'key':'a','value':{'kind':'map','entries':[{'key':'kind','value':{'kind':'string','text':'integer'}},{'key':'text','value':{'kind':'string','text':'9007199254740993'}}]}},
            {'key':'empty','value':{'kind':'map','entries':[]}},
            {'key':'flag','value':{'kind':'boolean','value':False}},
            {'key':'nil','value':{'kind':'null'}},
            {'key':'z','value':{'kind':'sequence','items':[]}}]}
        self.assertEqual(json.loads(mapped.mapped_artifact),expected)
        self.assertEqual(restore_asserted_origin(mapped.mapped_artifact),b'{"a":{"kind":"integer","text":"9007199254740993"},"empty":{},"flag":false,"nil":null,"z":[]}')
        self.assertEqual(mapped.original,original)
    def test_controls_unicode_and_claimed_role_remain_asserted(self):
        original=json.dumps({'db_role':'claimed-admin','é':'e\u0301','nested':['\n',{},[]]},ensure_ascii=False).encode()
        mapped=map_acceptance_origin(original)
        self.assertEqual(restore_asserted_origin(mapped.mapped_artifact),mapped.canonical_tree)
        self.assertIn(b'\\u000a',mapped.canonical_tree)
        self.assertNotIn(b'databaseRole',mapped.mapped_artifact)
        self.assertIn(b'claimed-admin',mapped.mapped_artifact)
    def test_refuses_invalid_or_resource_exhausting_origin(self):
        for raw in [b'1',b'1.0',b'{"x":1,"x":2}',b'"\\ud800"',b'"\\u0000"',b'['*65+b'null'+b']'*65,b'['+b','.join([b'null']*1000)+b']',b' '*1048577]:
            with self.subTest(raw=raw[:30]):
                with self.assertRaises(ValueError):map_acceptance_origin(raw)
        for mapped in [{'kind':'boolean','value':1},{'kind':'integer','text':'1'}, {'kind':'map','entries':[{'key':'b','value':{'kind':'null'}},{'key':'a','value':{'kind':'null'}}]}, {'kind':'map','entries':[{'key':'a','value':{'kind':'null'}},{'key':'a','value':{'kind':'null'}}]}]:
            with self.assertRaises(OriginMappingError):restore_asserted_origin(json.dumps(mapped).encode())

if __name__=='__main__':unittest.main()

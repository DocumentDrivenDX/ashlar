import json
import unittest
from decimal import Decimal
from commerce_source_transaction import ROOT,build_transaction
from ashlar.whole_entity import changes_from_batch

class Tests(unittest.TestCase):
    def setUp(self):
        self.source=(ROOT/'ontology.json').read_bytes();self.graph=(ROOT/'graph/fixture.json').read_bytes()
    def test_original_rows_endpoints_and_exact_values(self):
        batch,binding=build_transaction(self.source,self.graph,source_system='commerce-fixture')
        changes=changes_from_batch(batch);self.assertEqual(len(changes),21)
        original=json.loads(self.graph);objects=original['objects'];edges=original['edges']
        by_original={b['originalKey']:c.state.key for b,c in zip(binding['entities'],changes)}
        for row,change in zip(objects+edges,changes):
            self.assertEqual(json.loads(change.state.retained_json)['original'],row)
            if 'values' in row:
                props=json.loads(change.state.props_json,parse_float=Decimal)
                for key,value in props.items():
                    self.assertEqual(str(value),row['values'][key])
            else:self.assertEqual(change.state.endpoints,tuple(by_original[row[k]] for k in ('source','target')))
    def test_deterministic_complete_transaction_and_source_isolation(self):
        a,ab=build_transaction(self.source,self.graph,source_system='A')
        again,bb=build_transaction(self.source,self.graph,source_system='A')
        b,_=build_transaction(self.source,self.graph,source_system='B')
        self.assertEqual(a,again);self.assertEqual(ab,bb)
        self.assertNotEqual(a.records_sha256,b.records_sha256)
        self.assertTrue(set(c.state.key for c in changes_from_batch(a)).isdisjoint(c.state.key for c in changes_from_batch(b)))
    def test_modified_originals_and_missing_namespace_refused(self):
        for s,g,n in [(self.source+b' ',self.graph,'A'),(self.source,self.graph+b' ','A'),(self.source,self.graph,'')]:
            with self.assertRaises(ValueError):build_transaction(s,g,source_system=n)

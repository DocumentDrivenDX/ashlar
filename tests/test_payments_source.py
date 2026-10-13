"""Original payments source correspondence, without publication authority."""
import json
import tempfile
import unittest
from pathlib import Path
from ashlar.payments_source import build_transaction, write_candidate
from ashlar.whole_entity import changes_from_batch

ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/payments/upstream'

class Tests(unittest.TestCase):
    def test_complete_original_exact_values_keys_endpoints_and_bytes(self):
        model=(ROOT/'ontology.json').read_bytes();graph=(ROOT/'graph/fixture.json').read_bytes()
        original=json.loads(graph)
        from ashlar_host.payments_oracle import original_oracle
        oracle=original_oracle(model,graph)
        self.assertEqual(oracle['objects'],original['objects'])
        self.assertEqual(oracle['edges'],original['edges'])
        self.assertEqual(oracle['scenarios'],{})
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'candidate'
            batch,binding=write_candidate(ROOT/'ontology.json',ROOT/'graph/fixture.json',output,source_system='payments-fixture')
            self.assertEqual((output/'ontology.json').read_bytes(),model)
            self.assertEqual((output/'graph.json').read_bytes(),graph)
            self.assertEqual(json.loads(model)['umf'],'0.8.0')
            changes=changes_from_batch(batch)
            self.assertEqual(len(changes),27)
            properties={p['property_id']:p['identity'][2] for p in binding['properties']}
            identities={b['originalKey']:c.state.key for b,c in zip(binding['entities'],changes)}
            decimal_seen=False;currency_seen=False
            for row,change in zip(original['objects']+original['edges'],changes):
                self.assertEqual(json.loads(change.state.retained_json)['original'],row)
                if 'values' in row:
                    actual=json.loads(change.state.props_json,parse_float=str,parse_int=str)
                    self.assertEqual({properties[k]:v for k,v in actual.items()},row['values'])
                    decimal_seen|=any(v=='1E+2' for v in actual.values())
                    currency_seen|='USD' in actual.values()
                else:
                    self.assertEqual(change.state.endpoints,tuple(identities[row[k]] for k in ('source','target')))
            self.assertTrue(decimal_seen);self.assertTrue(currency_seen)
            self.assertEqual(build_transaction(model,graph,source_system='payments-fixture'),(batch,binding))

    def test_conflicting_original_refused_before_output(self):
        model=(ROOT/'ontology.json').read_bytes();graph=json.loads((ROOT/'graph/fixture.json').read_bytes())
        graph['objects'][1]['key']=graph['objects'][0]['key']
        with self.assertRaises(ValueError):
            build_transaction(model,json.dumps(graph).encode(),source_system='payments-fixture')
        from ashlar_host.payments_oracle import original_oracle
        with self.assertRaises(ValueError):original_oracle(model,json.dumps(graph).encode())

if __name__=='__main__':unittest.main()

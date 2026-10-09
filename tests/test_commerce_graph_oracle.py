import copy
import json
from pathlib import Path
import unittest

from commerce_graph_oracle import binding_requests, decimal_scenarios, materialize_source_graph
from domain_pack_inventory import InventoryError
from prepare_commerce_values import prepare_values

ROOT=Path(__file__).resolve().parents[1]


class CommerceGraphOracleTests(unittest.TestCase):
    def setUp(self):
        source=ROOT/'examples/domain-packs/commerce/upstream'
        inventory=json.loads((ROOT/'examples/domain-packs/inventory.json').read_bytes())
        inspection=json.loads((ROOT/'docs/helix/04-build/evidence/commerce-domain-interpretation-20261009.json').read_bytes())
        self.prepared=prepare_values(source,inventory,inspection)
        self.model=json.loads((source/'ontology.json').read_bytes())
        self.tables={p.stem:json.loads(p.read_bytes()) for p in (source/'umf').glob('*.json')}
        self.requests,self.links=binding_requests(self.prepared,self.model,self.tables)
        # Test-only key doubles exercise source graph composition; these bytes are
        # deliberately NOT a UMF encoding proof. Actual public receipts live in evidence.
        self.public={'fields':[],'keys':[{'request':r,'receipt':{'operation':'encode-core-key-tuple','version':'3.0.0','profile':'umf-key-tuple-v1',
            'identity':r['identity'],'values':r['values'],'source':self.model,
            'bytesHex':r['values'][0]['string'].encode('utf-8').hex()}} for r in self.requests['keys']]}

    def graph(self):return materialize_source_graph(self.prepared,self.public,self.links)

    def test_request_membership_and_explicit_original_topology(self):
        self.assertEqual(len(self.requests['fields']),37)
        self.assertEqual(len(self.requests['keys']),21)
        graph=self.graph()
        self.assertEqual(graph['counts'],{'nodes':11,'edges':10})
        keys={n['key'] for n in graph['nodes']}
        self.assertTrue(all(e['source'] in keys and e['target'] in keys for e in graph['edges']))
        self.assertEqual(graph['relationship_counts']['fulfillments.line_id'],2)
        self.assertEqual(len(set(n['sourceRowKey'] for n in graph['nodes'])),11)

    def test_independent_decimal_scenarios_match_original_template_expectations(self):
        result=decimal_scenarios(self.prepared)
        pack=json.loads((ROOT/'examples/domain-packs/commerce/upstream/pack.json').read_bytes())
        for case in pack['scenario_checks']:
            self.assertEqual(result[case['id']],case['expected'])
        altered=copy.deepcopy(self.prepared)
        row=next(r for r in altered['rows'] if r['record']['element']=='refunds')
        next(f for f in row['fields'] if f['column']=='amount')['lexical']='25.01'
        self.assertEqual(decimal_scenarios(altered)['refund'],[])

    def test_unknown_or_mismatched_authored_fk_binding_refuses(self):
        self.tables['products']['relationships']['foreign_keys'][0]['references_column']='name'
        with self.assertRaises(InventoryError):binding_requests(self.prepared,self.model,self.tables)

    def test_missing_endpoint_and_duplicate_edge_refuse(self):
        index=self.links[0]['targetKeyRequestIndex'];self.public['keys'][index]['receipt']['bytesHex']='ffff'
        with self.assertRaises(InventoryError):self.graph()
        self.public['keys'][index]['receipt']['bytesHex']=self.public['keys'][index]['request']['values'][0]['string'].encode().hex()
        self.links.append(copy.deepcopy(self.links[0]))
        with self.assertRaises(InventoryError):self.graph()

    def test_stale_key_receipt_source_values_or_version_refuse(self):
        original=copy.deepcopy(self.public)
        for mutate in [lambda r:r.update(version='2.0.0'),lambda r:r.update(profile='other'),
                       lambda r:r['source'].update(umf='0.7.0'),lambda r:r.update(values=[{'string':'different'}])]:
            self.public=copy.deepcopy(original);mutate(self.public['keys'][0]['receipt'])
            with self.assertRaises(InventoryError):self.graph()

    def test_duplicate_record_key_refuses_without_allocating_identity(self):
        duplicate=copy.deepcopy(self.public['keys'][0]);self.public['keys'].append(duplicate)
        with self.assertRaises(InventoryError):self.graph()


if __name__=='__main__':unittest.main()

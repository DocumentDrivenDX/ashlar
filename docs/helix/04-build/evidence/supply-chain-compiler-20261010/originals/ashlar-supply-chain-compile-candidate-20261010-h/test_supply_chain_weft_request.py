import hashlib
import json
from pathlib import Path
import unittest
from supply_chain_weft_request import supply_chain_weft_request
from supply_chain_source_transaction import build_transaction
from prepare_supply_chain_query_cases import CASE_IDS

ROOT = Path(__file__).resolve().parents[1]


class SupplyChainWeftRequest(unittest.TestCase):
    def setUp(self):
        p = ROOT / 'examples/domain-packs/supply-chain/upstream'
        self.raw = tuple((p / n).read_bytes() for n in ('pack.json', 'ontology.json', 'graph/fixture.json'))
        report = json.loads((ROOT / 'docs/helix/04-build/evidence/supply-chain-native-publication-20261009.json').read_bytes())
        self.manifest = report['native_manifest']
        self.registry = report['table_registry']
        self.aliases = {name: name.replace('local.', 'spark_catalog.', 1) for name in json.loads(self.manifest['table_versions_json'])}
        _, self.bindings = build_transaction(self.raw[1], self.raw[2], source_system='private-original-supply-chain-fixture')

    def request(self, case):
        return supply_chain_weft_request(case, *self.raw, self.bindings, self.manifest, self.registry, self.aliases)

    def test_all_original_sql_model_pins_and_only_optional_encoding(self):
        scenarios = {c['id']: c['sql'] for c in json.loads(self.raw[0])['scenario_checks']}
        for case in CASE_IDS:
            r = self.request(case)
            self.assertEqual(r['sql'], scenarios[case])
            self.assertEqual(r['modules'][0]['documentJson'].encode(), self.raw[1])
            self.assertEqual(r['modules'][0]['pin']['umfVersion'], '0.8.0')
            self.assertEqual(r['interfaceVersion'], 'weft-compile/0.4.0')
            self.assertEqual(r['target']['backendId'], 'ashlar.databricks.paths-keys')
            self.assertEqual(set(r['target']), {'backendId', 'backendVersion', 'targetProfile', 'bindingJson', 'bindingSha256'})
            self.assertNotIn('interfaceVersion', r['target'])
            b = json.loads(r['target']['bindingJson'])
            encoded = [p for t in b['records'] for p in t['properties'] if 'encoding' in p['home']]
            self.assertEqual(len(encoded), 1)
            self.assertEqual(encoded[0]['logical']['element'], 'containers.parent_id')
            self.assertEqual(encoded[0]['home']['encoding'], 'ashlar-weft-json-native-null/0.1-candidate')
            self.assertEqual(r['target']['bindingSha256'], hashlib.sha256(r['target']['bindingJson'].encode()).hexdigest())
            self.assertEqual(b['publication']['id'], self.manifest['publication_id'])
            self.assertEqual(len(b['publication']['tables']), 4)

    def test_two_component_alias_refuses(self):
        self.aliases[next(iter(self.aliases))] = 'held_supply.object_current'
        with self.assertRaises(ValueError): self.request('sensor')

    def test_unknown_case_and_changed_original_bindings_refuse(self):
        with self.assertRaises(ValueError): self.request('replacement-query')
        self.bindings['properties'][0]['property_id'] = 'forged'
        with self.assertRaises(ValueError): self.request('sensor')


if __name__ == '__main__': unittest.main()

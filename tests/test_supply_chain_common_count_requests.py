"""Pure fixed original request/oracle controls; no compilation or native pass."""
from collections import Counter
import copy
import hashlib
import json
from pathlib import Path
import unittest
from ashlar.supply_chain_source import build_transaction
from ashlar_host.count_star_admission import BACKEND
from ashlar_host.graphframes_pack_scenarios import common_oracle
from ashlar_host.supply_chain_common_count_requests import prepare_supply_chain_common_count_requests
from ashlar_host.supply_chain_oracle import original_graph_oracle
from ashlar_host.supply_chain_request import supply_chain_count_star_request

ROOT=Path(__file__).resolve().parents[1]
class Controls(unittest.TestCase):
    def setUp(self):
        root=ROOT/'examples/domain-packs/supply-chain/upstream'
        self.raw=tuple((root/name).read_bytes()for name in ('pack.json','ontology.json','graph/fixture.json'))
        _,self.bindings=build_transaction(*self.raw[1:],source_system='private-original-supply-chain-fixture')
        retained=json.loads((ROOT/'docs/helix/04-build/evidence/supply-chain-native-publication-20261009.json').read_bytes())
        self.manifest,self.registry=retained['native_manifest'],retained['table_registry']
        self.aliases={table:table.replace('local.','spark_catalog.',1)for table in json.loads(self.manifest['table_versions_json'])}
    def prepare(self,family,part='complete'):
        return prepare_supply_chain_common_count_requests(*self.raw,self.bindings,self.manifest,self.registry,self.aliases,family=family,part=part)
    def test_complete_thirteen_partition_requests23occurrences_and_original_edge_ids(self):
        parts=[self.prepare('one-hop',part)for part in ('relationships-0-6','relationships-7-12')]
        queries=[q for result in parts for q in result['queries']]
        self.assertEqual([q['ordinal']for q in queries],list(range(13)))
        occurrences=[row for result in parts for row in result['expected_occurrences']]
        original=original_graph_oracle(*self.raw[1:]);common=common_oracle(original)
        self.assertEqual(Counter(map(tuple,occurrences)),Counter(map(tuple,common['one-hop'])))
        witnesses=[w for q in queries for w in q['original_edge_witnesses']]
        self.assertEqual(len(witnesses),23)
        self.assertEqual({w['edge_id']for w in witnesses},{r['id']for r in self.bindings['entities']if r['kind']=='edge'})
        self.assertEqual({w['original_edge']for w in witnesses},{e['key']for e in original['edges']})
        cells=[json.loads(row[1])for q in queries for row in q['expected_rows']]
        self.assertTrue(all(cell['truncated']is False for cell in cells));self.assertEqual(sum(len(c['items'])for c in cells),23)
        self.assertTrue(any(not c['items']for c in cells))
        self.assertTrue(all(', 24)'in q['request']['sql']for q in queries))
    def test_genuine_existing041_markers_full_original_binding_no_paths_relabel(self):
        baseline=supply_chain_count_star_request('sensor',*self.raw,self.bindings,self.manifest,self.registry,self.aliases)
        for result in (self.prepare('one-hop','relationships-0-6'),self.prepare('singleton'),self.prepare('filtered-limit')):
            for query in result['queries']:
                request=query['request'];self.assertEqual(request['interfaceVersion'],'weft-compile/0.4.1');self.assertEqual(request['dialect'],'weft-sql/0.4.1')
                self.assertEqual(request['modules'],baseline['modules'])
                self.assertEqual({k:request['target'][k]for k in ('backendId','backendVersion','targetProfile')},{k:BACKEND[k]for k in ('backendId','backendVersion','targetProfile')})
                binding=json.loads(request['target']['bindingJson']);relations=binding.pop('relationships')
                self.assertEqual(len(relations),13);self.assertEqual(binding,json.loads(baseline['target']['bindingJson']))
                self.assertEqual(hashlib.sha256(request['target']['bindingJson'].encode()).hexdigest(),request['target']['bindingSha256'])
    def test_singleton_complete_projection_native_null_expected_carrier(self):
        query=self.prepare('singleton')['queries'][0]
        self.assertEqual(len(query['expected_rows']),1);self.assertEqual(len(query['expected_rows'][0]),3)
        self.assertEqual(query['expected_rows'][0][0],'[42,0,"C1"]')
        self.assertEqual(json.loads(query['expected_rows'][0][1]),{'state':'null'})
        self.assertEqual(query['original_object_keys'],common_oracle(original_graph_oracle(*self.raw[1:]))['singleton'])
        self.assertIn('SELECT s.id, s.parent_id, s.kind',query['request']['sql'])
        self.assertIn("WHERE s.id='[42,0,\"C1\"]'",query['request']['sql'])
    def test_filtered_limit_nonvacuous_complete_total_and_first_record(self):
        queries=self.prepare('filtered-limit')['queries'];self.assertEqual(len(queries),2)
        self.assertEqual(queries[0]['expected_rows'],[['3']]);self.assertEqual(len(queries[0]['original_object_keys']),3)
        self.assertEqual(queries[1]['expected_rows'],self.prepare('singleton')['queries'][0]['expected_rows'])
        self.assertTrue(queries[1]['request']['sql'].endswith('ORDER BY s.id LIMIT 1'))
    def test_closed_family_partition_and_grouped_relation_gap_no_fallback(self):
        for family,part in ((True,'complete'),('one-hop',True),('one-hop','complete'),('singleton','relationships-0-6'),('raw-sql','complete'),('grouped-count','relationships-0-6'),('canonical-carriers','complete')):
            with self.subTest(family=family,part=part),self.assertRaises(ValueError):self.prepare(family,part)
        with self.assertRaises(TypeError):self.prepare('singleton',sql='SELECT arbitrary')
    def test_input_provenance_binding_alias_and_native_uuid_refuse_purely(self):
        for field in ('bindings','manifest','registry','aliases'):
            original=copy.deepcopy(getattr(self,field))
            if field=='bindings':self.bindings['entities'][0]['id']='999'
            if field=='manifest':self.manifest['table_versions_json']='{}'
            if field=='registry':self.registry[0]['uuid']='not-canonical'
            if field=='aliases':self.aliases[next(iter(self.aliases))]='invalid'
            with self.subTest(field=field),self.assertRaises(ValueError):self.prepare('singleton')
            setattr(self,field,original)
        self.raw=(self.raw[0]+b' ',*self.raw[1:])
        with self.assertRaises(ValueError):self.prepare('one-hop','relationships-0-6')
    def test_originals_not_borrowed_or_mutated_and_relationship_self_reference_retained(self):
        before=copy.deepcopy((self.bindings,self.manifest,self.registry,self.aliases))
        result=self.prepare('one-hop','relationships-0-6')
        self.assertEqual((self.bindings,self.manifest,self.registry,self.aliases),before)
        query=next(q for q in result['queries']if q['relationships']==['containers.parent_id'])
        binding=json.loads(query['request']['target']['bindingJson'])
        relation=next(r for r in binding['relationships']if r['logical']['relationship']=='containers.parent_id')
        self.assertEqual(relation['source'],relation['target'])
        request_before=copy.deepcopy(result['queries'][0]['request'])
        self.bindings['entities'][0]['id']='999';self.aliases.clear()
        self.assertEqual(result['queries'][0]['request'],request_before)

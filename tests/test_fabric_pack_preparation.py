# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure typed mapping checks; no OneLake or Fabric authority."""
import copy,unittest
from ashlar.graph_release import release_columns
from ashlar_host.fabric_pack_preparation import scenario_definition
import test_pack_scenario_puppygraph as fixtures
ROOT='abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/22222222-2222-2222-2222-222222222222/Tables/'
class Tests(unittest.TestCase):
    def test_original_profiles_have_exact_columns_types_and_unchanged_edges(self):
        for p in fixtures.ScenarioPuppyPreparationTests().profiles():
            d=scenario_definition(p,ROOT+p['pack']+'_nodes',ROOT+p['pack']+'_edges')
            node=d['graphType.json']['nodeTypes'][0];edge=d['graphType.json']['edgeTypes'][0]
            self.assertEqual(node['primaryKeyProperties'],['graph_id'])
            self.assertEqual([x['name']for x in node['properties']],list(p['nodes'][0]))
            self.assertEqual(sum(x['type']=='INT'for x in node['properties']),2 if p['pack']=='archaeology'else 1)
            self.assertEqual([x['name']for x in edge['properties']],list(release_columns('edge')))
            self.assertEqual(d['graphDefinition.json']['edgeTables'][0]['sourceNodeKeyColumns'],['src'])
    def test_collision_or_lossy_numeric_carrier_refuses(self):
        for value in (True,1.0,2**63):
            p=fixtures.ScenarioPuppyPreparationTests().profiles()[0]
            col=next(f['signed64_column']for f in p['fields']if f['signed64_column']);p['nodes'][0][col]=value
            with self.assertRaises(ValueError):scenario_definition(p,ROOT+'nodes',ROOT+'edges')
        p=fixtures.ScenarioPuppyPreparationTests().profiles()[0];p['fields'].append(copy.deepcopy(p['fields'][0]))
        with self.assertRaises(ValueError):scenario_definition(p,ROOT+'nodes',ROOT+'edges')
    def test_preparation_uses_real_file_custody_and_preserves_original_queries(self):
        import tempfile,json,hashlib
        from pathlib import Path
        import test_finite_graph_export as fixtures
        from ashlar_host.fabric_pack_preparation import prepare_fabric_pack
        from ashlar_host.local_graph_export import LocalGraphExportSelection
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();release,custody=fixtures.Tests().pair()
            (root/'release.json').write_bytes(release.payload);(root/'custody.json').write_bytes(custody)
            selection=LocalGraphExportSelection(root/'release.json',root/'custody.json',release.sha256,hashlib.sha256(custody).hexdigest())
            source=Path('examples/domain-packs/archaeology/upstream').resolve()
            args=dict(pack='archaeology',model=source/'ontology.json',graph=source/'graph/fixture.json',pack_file=source/'pack.json',selection=selection,nodes=ROOT+'nodes',edges=ROOT+'edges',output=root/'output')
            result=prepare_fabric_pack(**args);self.assertEqual(result['cases'],8)
            queries=json.loads((root/'output/queries.json').read_text());original=json.loads((source/'pack.json').read_text())['scenario_checks']
            self.assertEqual([q['original_sql']for q in queries],[q['sql']for q in original])
            with self.assertRaises(ValueError):prepare_fabric_pack(**args)
            (root/'custody.json').write_bytes(custody+b' ');args['output']=root/'refused'
            with self.assertRaises(ValueError):prepare_fabric_pack(**args)
            self.assertFalse((root/'refused').exists())

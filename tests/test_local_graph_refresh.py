import unittest
import hashlib
from test_graph_release_graphframes import fixture as release_fixture,encoded
from pathlib import Path
from dataclasses import FrozenInstanceError
from ashlar.graph_release import _carrier
from run_local_graph_refresh import fixture,Handle,bound_paths,validate_native_vector
from run_graph_release_graphframes import oracle
class Tests(unittest.TestCase):
    def test_explicit_profile_preserves_controls_and_successor_mutations(self):
        releases=fixture();expected={'R1':(3,3,3),'R2':(3,4,10)}
        for name,rows in releases.items():
            value={role:[_carrier(r,'node' if role=='nodes' else 'edge') for r in rs] for role,rs in rows.items()}
            counts=oracle(value)
            self.assertEqual((counts['nodes'],counts['edges'],counts['two_hop']),expected[name]);self.assertEqual(counts['isolates'],1)
            self.assertTrue(any(e['src']==e['dst'] for e in value['edges']))
        self.assertNotEqual(releases['R1']['nodes'][0]['props_json'],releases['R2']['nodes'][0]['props_json'])
        self.assertEqual(releases['R2']['nodes'][-1]['id'],'9223372036854775807')
    def test_handle_immutable_and_malformed_byte_binding_refused(self):
        h=Handle('0'*64,b'invalid',Path('/private/tmp')/('0'*64),(('nodes','u',0),('edges','v',0)))
        with self.assertRaises(FrozenInstanceError):h.sha256='changed'
        with self.assertRaises(ValueError):bound_paths(h)

    def test_whole_handle_vector_and_directory_binding(self):
        payload=encoded(release_fixture());sha=hashlib.sha256(payload).hexdigest()
        h=Handle(sha,payload,Path('/private/tmp')/sha,(('nodes','node-uuid',0),('edges','edge-uuid',0)))
        self.assertEqual(set(bound_paths(h)),{'nodes','edges'})
        for candidate in [Handle(sha,payload,Path('/private/tmp/other'),h.targets),
                          Handle(sha,payload,h.directory,h.targets[:1]),
                          Handle(sha,payload,h.directory,(h.targets[0],h.targets[0])),
                          Handle(sha,payload,h.directory,(('nodes','node-uuid',True),h.targets[1]))]:
            with self.assertRaises(ValueError):bound_paths(candidate)

    def test_same_rows_node_replacement_while_edge_query_requires_closing_vector(self):
        from types import SimpleNamespace
        payload=encoded(release_fixture());sha=hashlib.sha256(payload).hexdigest()
        handle=Handle(sha,payload,Path('/private/tmp')/sha,(('nodes','node-uuid',0),('edges','edge-uuid',0)))
        paths=bound_paths(handle)
        class Native:
            node_uuid='node-uuid'
            unchanged_rows=['identical original carriers']
            def sql(self,query):
                uuid=self.node_uuid if str(paths['nodes']) in query else 'edge-uuid'
                return SimpleNamespace(first=lambda:SimpleNamespace(id=uuid))
            def edge_query(self):
                self.node_uuid='replacement-uuid'
                return self.unchanged_rows
        native=Native();validate_native_vector(native,handle,paths)
        self.assertEqual(native.edge_query(),native.unchanged_rows)
        with self.assertRaises(ValueError):validate_native_vector(native,handle,paths)

    def test_execute_refuses_replacement_during_edge_read_even_with_same_rows(self):
        from types import SimpleNamespace,ModuleType
        from unittest.mock import MagicMock,patch
        from run_local_graph_refresh import Refresh
        payload=encoded(release_fixture());sha=hashlib.sha256(payload).hexdigest();value=release_fixture()
        handle=Handle(sha,payload,Path('/private/tmp')/sha,(('nodes','node-uuid',0),('edges','edge-uuid',0)))
        paths=bound_paths(handle);native=SimpleNamespace(node_uuid='node-uuid');spark=MagicMock()
        spark.sql.side_effect=lambda q:SimpleNamespace(first=lambda:SimpleNamespace(id=native.node_uuid if str(paths['nodes']) in q else 'edge-uuid'))
        frames={r:MagicMock() for r in ['nodes','edges']}
        for role in frames:
            frames[role].withColumnRenamed.return_value.withColumnRenamed.return_value=frames[role]
        frames['nodes'].collect.return_value=[SimpleNamespace(asDict=lambda r=r:r) for r in value['nodes']]
        def edge_read():
            native.node_uuid='same-values-new-uuid'
            return [SimpleNamespace(asDict=lambda r=r:r) for r in value['edges']]
        frames['edges'].collect.side_effect=edge_read
        spark.read.format.return_value.option.return_value.load.side_effect=lambda p:frames[Path(p).name]
        graph=MagicMock();graph.vertices.count.return_value=3;graph.edges.count.return_value=3
        graph.vertices.join.return_value.count.return_value=1
        motifs=MagicMock();motifs.count.return_value=3;graph.find.return_value=motifs
        nodes={n['graph_id']:n for n in value['nodes']}
        pairs=[{'source':e['src'],'edge':e['graph_id'],'target':e['dst'],'source_props':nodes[e['src']]['props_json'],'edge_props':e['props_json'],'target_props':nodes[e['dst']]['props_json']} for e in value['edges']]
        motifs.select.return_value.collect.return_value=[SimpleNamespace(asDict=lambda r=r:r) for r in pairs]
        gf=ModuleType('graphframes');gf.GraphFrame=lambda *_:graph
        sql=ModuleType('pyspark.sql');sql.functions=SimpleNamespace(col=lambda _:MagicMock())
        ps=ModuleType('pyspark');ps.sql=sql
        with patch.dict('sys.modules',{'graphframes':gf,'pyspark':ps,'pyspark.sql':sql}):
            with self.assertRaisesRegex(ValueError,'Complete local graph native identity vector changed'):
                Refresh(spark,Path('/private/tmp')).execute(handle)
        self.assertEqual(graph.find.call_count,3)

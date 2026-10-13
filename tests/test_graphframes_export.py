"""Two small generic engine-port controls; no native GraphFrames claim."""
import json,sys,unittest
from types import SimpleNamespace,ModuleType
from unittest.mock import patch
from ashlar_host.graphframes_export import query_graphframes_export

class Tests(unittest.TestCase):
    def test_canonical_frames_one_hop_degree_and_isolated_vertex(self):
        self.check(False)
    def test_wrong_native_bag_refuses(self):
        self.check(True)
    def check(self,wrong):
        graph={'publication':{'publication_id':'original'},'nodes':[{'id':'1','graph_id':'a','props_json':'{}'},{'id':'2','graph_id':'b','props_json':'{}'},{'id':'3','graph_id':'isolated','props_json':'{}'}],
            'edges':[{'id':'7','graph_id':'edge7','src':'a','dst':'b','order_key':None,'retained_json':'{}'}]}
        types=ModuleType('pyspark.sql.types');types.StructType=lambda fields:fields;types.StructField=lambda name,*args:name;types.StringType=lambda:None
        frames=[]
        class Frame:
            def __init__(self,rows,names):self.rows=rows;self.names=names
            def limit(self,bound):return self
            def collect(self):return [SimpleNamespace(asDict=lambda row=row:dict(zip(self.names,row)))for row in self.rows]
            def withColumnRenamed(self,old,new):self.names=[new if name==old else name for name in self.names];return self
        def create(rows,schema):
            frame=Frame(rows,schema);frames.append(frame);return frame
        class Native:
            def __init__(self,vertices,edges):pass
            def find(self,pattern):
                self_pattern.assertEqual(pattern,'(a)-[e]->(b)')
                return SimpleNamespace(selectExpr=lambda *args:SimpleNamespace(limit=lambda bound:SimpleNamespace(collect=lambda:[]if wrong else[{'src':'a','edge':'edge7','dst':'b'}])))
            @property
            def outDegrees(self):return SimpleNamespace(limit=lambda bound:SimpleNamespace(collect=lambda:[{'id':'a','outDegree':1}]))
        self_pattern=self
        with patch.dict(sys.modules,{'pyspark.sql.types':types}):
            if wrong:
                with self.assertRaisesRegex(ValueError,'occurrence bag differs'):query_graphframes_export(SimpleNamespace(createDataFrame=create),Native,SimpleNamespace(payload=json.dumps(graph).encode()))
            else:
                result=query_graphframes_export(SimpleNamespace(createDataFrame=create),Native,SimpleNamespace(payload=json.dumps(graph).encode()))
                self.assertEqual(result['out_degrees'],[{'id':'a','out_degree':1},{'id':'b','out_degree':0},{'id':'isolated','out_degree':0}])
                self.assertIn('carrier_id',frames[0].names);self.assertIn('props_json',frames[0].names);self.assertIn('order_key',frames[1].names)
                self.assertEqual(frames[0].rows[0][frames[0].names.index('id')],'a')

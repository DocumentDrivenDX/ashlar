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

    def test_two_hop_counts_distinguish_paths_from_destinations_and_refuse_bad_counts(self):
        # Diamond plus distinct parallel edges and self-loop: repeated edge/vertex
        # use is allowed, while each original edge identity remains observable.
        edges=[{'id':str(index),'graph_id':'e'+str(index),'src':source,'dst':target}for index,(source,target)in enumerate((('a','b'),('a','b'),('a','c'),('b','d'),('c','d'),('d','d')),1)]
        graph={'publication':{'publication_id':'original'},'nodes':[{'id':name,'graph_id':name}for name in ('a','b','c','d','isolated')],'edges':edges}
        paths=[{'src':first['src'],'edge1':first['graph_id'],'middle':first['dst'],'edge2':second['graph_id'],'dst':second['dst']}for first in edges for second in edges if first['dst']==second['src']]
        types=ModuleType('pyspark.sql.types');types.StructType=lambda fields:fields;types.StructField=lambda name,*args:name;types.StringType=lambda:None
        functions=ModuleType('pyspark.sql.functions');functions.count=lambda value:SimpleNamespace(alias=lambda name:name);functions.countDistinct=functions.count
        class Frame:
            def __init__(self,rows,names):self.rows=rows;self.names=names
            def limit(self,bound):return self
            def collect(self):return [SimpleNamespace(asDict=lambda row=row:dict(zip(self.names,row)))for row in self.rows]
            def withColumnRenamed(self,old,new):self.names=[new if name==old else name for name in self.names];return self
        bad=[False]
        class Paths:
            def selectExpr(self,*args):return self
            def limit(self,bound):return self
            def collect(self):return paths
            def groupBy(self,source):return self
            def agg(self,*args):
                counts=[]
                for source in ('a','b','c','d'):
                    selected=[row for row in paths if row['src']==source]
                    counts.append({'src':source,'path_count':len(selected)+(1 if bad[0]else 0),'distinct_destinations':len({row['dst']for row in selected})})
                return SimpleNamespace(limit=lambda bound:SimpleNamespace(collect=lambda:counts))
        class Native:
            def __init__(self,*args):pass
            def find(self,pattern):
                if pattern=='(a)-[e1]->(b); (b)-[e2]->(c)':return Paths()
                return SimpleNamespace(selectExpr=lambda *args:SimpleNamespace(limit=lambda bound:SimpleNamespace(collect=lambda:[{'src':row['src'],'edge':row['graph_id'],'dst':row['dst']}for row in edges])))
            @property
            def outDegrees(self):return SimpleNamespace(limit=lambda bound:SimpleNamespace(collect=lambda:[{'id':source,'outDegree':sum(row['src']==source for row in edges)}for source in ('a','b','c','d')]))
        spark=SimpleNamespace(createDataFrame=lambda rows,schema:Frame(rows,schema))
        with patch.dict(sys.modules,{'pyspark.sql.types':types,'pyspark.sql.functions':functions}):
            release=SimpleNamespace(payload=json.dumps(graph).encode())
            result=query_graphframes_export(spark,Native,release,maximum_hops=2)
            source=next(row for row in result['two_hop_counts']if row['id']=='a')
            self.assertEqual(source,{'id':'a','path_count':3,'distinct_destinations':1})
            self.assertIn({'src':'d','edge1':'e6','middle':'d','edge2':'e6','dst':'d'},result['two_hop'])
            self.assertEqual(result['two_hop_counts'][-1],{'id':'isolated','path_count':0,'distinct_destinations':0})
            bad[0]=True
            with self.assertRaisesRegex(ValueError,'path/distinct-destination counts differ'):query_graphframes_export(spark,Native,release,maximum_hops=2)

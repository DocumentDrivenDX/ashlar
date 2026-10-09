import copy,gzip,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from run_pack_release_graphframes import query_oracle,original,validate_vector,closing_parity,finish_native,native_filter_and_isolates,admit_runtime,VERSIONS,JARS
from run_graph_release_graphframes import graph_rows
ROOT=Path(__file__).resolve().parents[1]
EVIDENCE=ROOT/'docs/helix/04-build/evidence/original-pack-graph-exports-20261009'

class PackReleaseGraphFramesTests(unittest.TestCase):
    def graph(self):
        t={'module':'m','element':'record'};rel={'module':'m','id':'relationship'}
        return {'objects':[{'key':key,'type':t}for key in ['a','b','isolated']], 'edges':[
          {'key':'parallel1','source':'a','target':'b','relationship':rel},
          {'key':'parallel2','source':'a','target':'b','relationship':rel},
          {'key':'loop','source':'b','target':'b','relationship':rel}]}
    def test_oracle_retains_parallel_self_loop_isolate_and_nonvacuous_limit(self):
        q=query_oracle(self.graph());self.assertEqual(len(q['one-hop']),3);self.assertEqual(len(q['two-hop']),3)
        self.assertEqual(q['isolates'],['isolated']);self.assertEqual(q['filtered-limit'],['a']);self.assertEqual(q['filtered-total'],3)
        self.assertIn(['a','parallel1','b','loop','b'],q['two-hop']);self.assertIn(['a','parallel2','b','loop','b'],q['two-hop'])
        self.assertEqual(sorted(row[2:]for row in q['grouped-count']),[[1,1],[2,1]])
        with self.assertRaises(ValueError):query_oracle({'objects':[self.graph()['objects'][0]],'edges':[]})
    def test_archived_original_source_mismatches_refuse_before_graph_construction(self):
        value=json.loads((EVIDENCE/'archaeology/release.graph.json').read_bytes())
        admission=json.loads(value['publication']['validation_report_json'])['source_admission']
        import base64
        with tempfile.TemporaryDirectory()as temporary:
            directory=Path(temporary)
            for name,filename in [('model','original-ontology.json'),('graph','original-graph.json'),('bindings','development-bindings.json')]:
                (directory/filename).write_bytes(base64.b64decode(admission['original_'+name+'_base64']))
            (directory/'original-ontology.json').write_bytes((directory/'original-ontology.json').read_bytes()+b' ')
            with self.assertRaisesRegex(ValueError,'source custody'):original('archaeology',value,directory)
            with self.assertRaisesRegex(ValueError,'Explicit original'):original('other',value,directory)
    def fake_spark(self,*,uuid='nodes-uuid',version=0):
        state={'nodes':uuid,'edges':'edges-uuid','version':version,'calls':[]}
        class Query:
            def __init__(self,kind,role):self.kind=kind;self.role=role
            def first(self):return SimpleNamespace(asDict=lambda:{'id':state[self.role],'location':'file:/tmp/'+self.role})
            def select(self,*a):return self
            def collect(self):return [{'version':state['version']}]
        class Spark:
            def sql(self,sql):
                state['calls'].append(sql);return Query('detail'if 'DETAIL'in sql else'history','nodes'if '/nodes'in sql else'edges')
        return Spark(),state
    def test_complete_closing_vector_catches_replacement_during_other_table_read(self):
        spark,state=self.fake_spark();paths={'nodes':Path('/tmp/nodes'),'edges':Path('/tmp/edges')};snapshots={r:{'uuid':state[r],'version':0}for r in paths}
        value={'nodes':[{'id':'native','graph_id':'graph'}],'edges':[]}
        class Frame:
            def __init__(self,role):self.role=role
            def collect(self):
                if self.role=='edges':state['nodes']='replacement-same-rows'
                return [SimpleNamespace(asDict=lambda row=row:row)for row in graph_rows(value[self.role])]
        frames={r:Frame(r)for r in paths}
        with self.assertRaisesRegex(ValueError,'UUID'):closing_parity(spark,paths,snapshots,frames,value)
        self.assertGreaterEqual(len(state['calls']),5)
    def test_changed_version_and_carrier_bag_refuse(self):
        spark,state=self.fake_spark(version=1);paths={'nodes':Path('/tmp/nodes')};snapshots={'nodes':{'uuid':'nodes-uuid','version':0}}
        with self.assertRaisesRegex(ValueError,'version'):validate_vector(spark,paths,snapshots)
        state['version']=0
        with self.assertRaisesRegex(ValueError,'carrier parity'):closing_parity(spark,paths,snapshots,{'nodes':SimpleNamespace(collect=lambda:[])},{'nodes':[{'id':'native','graph_id':'graph'}]})

    def test_native_filter_and_isolate_wiring_uses_native_columns(self):
        calls=[]
        class Expression:
            def __init__(self,value):self.value=value
            def __eq__(self,value):return Expression(('equal',self.value,value))
            def __and__(self,other):return Expression(('and',self.value,other.value))
            def alias(self,name):return Expression(('alias',self.value,name))
        class Frame:
            def __init__(self,name):self.name=name
            def filter(self,expression):calls.append(('filter',self.name,expression.value));return Frame('filtered')
            def select(self,expression):calls.append(('select',self.name,expression.value));return Frame('endpoints')
            def union(self,other):calls.append(('union',self.name,other.name));return self
            def distinct(self):calls.append(('distinct',self.name));return self
            def join(self,other,key,kind):calls.append(('join',self.name,other.name,key,kind));return Frame('isolates')
        native_filter_and_isolates(Frame('nodes'),Frame('edges'),SimpleNamespace(col=lambda name:Expression(('column',name))),'source','7')
        self.assertEqual(calls[0],('filter','nodes',('and',('equal',('column','source_system'),'source'),('equal',('column','type_id'),'7'))))
        self.assertIn(('select','edges',('alias',('column','src'),'id')),calls);self.assertIn(('select','edges',('alias',('column','dst'),'id')),calls)
        self.assertIn(('join','nodes','endpoints','id','left_anti'),calls)

    def test_unqualified_runtime_or_replaced_jars_refuse(self):
        with tempfile.TemporaryDirectory()as temporary:
            with self.assertRaisesRegex(ValueError,'runtime'):admit_runtime(temporary,{**VERSIONS,'pyspark':'4.0.1'})
            for name in JARS:(Path(temporary)/name).write_bytes(b'replaced jar')
            with self.assertRaisesRegex(ValueError,'fingerprints'):admit_runtime(temporary,VERSIONS)

    def test_native_cleanup_failure_withholds_report(self):
        with tempfile.TemporaryDirectory()as temporary:
            directory=Path(temporary)
            def fail():raise ValueError('Spark stop failed')
            with self.assertRaisesRegex(ValueError,'stop failed'):finish_native(SimpleNamespace(stop=fail),directory,{'provisional':'never released'})
            self.assertFalse((directory/'report.json').exists())
            calls=[];finish_native(SimpleNamespace(stop=lambda:calls.append('stopped')),directory,{'closed':True})
            self.assertEqual(calls,['stopped']);self.assertEqual(json.loads((directory/'report.json').read_bytes()),{'closed':True})

if __name__=='__main__':unittest.main()

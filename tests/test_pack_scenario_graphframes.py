import base64,json,unittest
from pathlib import Path
from run_pack_scenario_graphframes import validate_case,ARITY,expressions
from prepare_pack_graph_scenarios import project
import test_pack_graph_scenarios as profile_tests

class Expression:
    def __init__(self,text):self.text=text
    def alias(self,name):return Expression(self.text+' AS '+name)
    def __eq__(self,other):return Expression('('+self.text+' = '+repr(other)+')')
    def __ne__(self,other):return Expression('('+self.text+' != '+repr(other)+')')
    def __lt__(self,other):return Expression('('+self.text+' < '+repr(other)+')')
    def __le__(self,other):return Expression('('+self.text+' <= '+repr(other)+')')
    def __gt__(self,other):return Expression('('+self.text+' > '+repr(other)+')')
    def __and__(self,other):return Expression('('+self.text+' AND '+repr(other)+')')
    def __repr__(self):return self.text
    def isNull(self):return Expression(self.text+' IS NULL')
    def isNotNull(self):return Expression(self.text+' IS NOT NULL')
    def isin(self,*args):return Expression(self.text+' IN '+repr(args))
class Frame:
    def __init__(self,log=()):self.log=list(log)
    def op(self,name,*args):return Frame(self.log+[(name,args)])
    def where(self,*a):return self.op('where',*a)
    def alias(self,*a):return self.op('alias',*a)
    def join(self,other,*a):return Frame(self.log+other.log+[('join',a)])
    def groupBy(self,*a):return self.op('group',*a)
    def agg(self,*a):return self.op('agg',*a)
    def select(self,*a):return self.op('select',*a)
    def distinct(self):return self.op('distinct')
    def orderBy(self,*a):return self.op('order',*a)

class ScenarioNativeTests(unittest.TestCase):
    def profile(self,pack):
        v,g,n,a=profile_tests.ScenarioProfileTests().fixture(pack);return project(pack,v,g,n,a)
    def test_native_operator_plan_contains_all_original_cases(self):
        from unittest.mock import patch
        import types,sys
        f=types.SimpleNamespace(col=lambda n:Expression(n),countDistinct=lambda x:Expression('COUNT DISTINCT '+repr(x)))
        module=types.ModuleType('pyspark.sql');module.functions=f
        with patch.dict(sys.modules,{'pyspark':types.ModuleType('pyspark'),'pyspark.sql':module}):
            for pack in ('archaeology','ecology'):
                plans=expressions(types.SimpleNamespace(vertices=Frame()),self.profile(pack))
                self.assertEqual(set(plans),set(ARITY[pack]))
                if pack=='archaeology':
                    self.assertEqual(sum(type(a[-1])is str and a[-1]=='left'for name,a in plans['evidence-links'].log if name=='join'),2)
                    self.assertTrue(any(name=='group'for name,a in plans['media'].log))
                    self.assertIn('integer_',str(plans['dating'].log))
                else:
                    self.assertTrue(any(name=='distinct'for name,a in plans['comparability'].log))
                    self.assertIn('integer_',str(plans['zero'].log))
                    self.assertIn('IS NULL',str(plans['censor'].log))
                    self.assertTrue(any(n=='order'for n,a in plans['connected-measurements'].log))
    def test_aggregate_counts_and_schema_are_exact(self):
        p=self.profile('archaeology');schema=[{'name':'result_0','type':'string'},{'name':'result_1','type':'bigint'}]
        expected=next(c['original_graph_expected']for c in p['cases']if c['original_scenario']['id']=='media');raw=[[r[0],int(r[1])]for r in expected]
        self.assertEqual(validate_case(p,'media',schema,raw),expected)
        for bad in ([[raw[0][0],True]],[[raw[0][0],float(raw[0][1])]]):
            with self.assertRaises(ValueError):validate_case(p,'media',schema,bad)
        with self.assertRaises(ValueError):validate_case(p,'media',list(reversed(schema)),raw)
    def test_optional_witness_distinguishes_unmatched(self):
        p=self.profile('archaeology');graph=json.loads(base64.b64decode(p['source_admission']['original_graph_base64']))
        objects={o['key']:o for o in graph['objects']};rows=[]
        for e in [o for o in objects.values()if o['type']['element']=='interpretation_evidence']:
            i=next(o for o in objects.values()if o['type']['element']=='interpretations'and o['values']['interpretations.id']==e['values']['interpretation_evidence.interpretation_id'])
            right=[]
            for record,field,idfield in [('pottery_results','form','pottery_result_id'),('fauna_results','taxon','fauna_result_id')]:
                match=next((o for o in objects.values()if o['type']['element']==record and o['values'][record+'.id']==e['values']['interpretation_evidence.'+idfield]),None)
                right.append((None,None,None)if match is None else(match['values'][record+'.'+field],match['key'],'present-null'if match['values'][record+'.'+field]is None else'present'))
            rows.append([i['values']['interpretations.author'],right[0][0],right[1][0],i['key'],e['key'],i['values']['interpretations.id'],right[0][1],right[1][1],right[0][2],right[1][2]])
        rows.sort(key=lambda r:r[5])
        names=['result_0','result_1','result_2','interpretation_key','evidence_key','interpretation_order_token','matched_pottery_key','matched_fauna_key','pottery_form_presence','fauna_taxon_presence'];schema=[{'name':n,'type':'string'}for n in names]
        validate_case(p,'evidence-links',schema,rows)
        bad=[r[:]for r in rows];bad[0][8]='nonmember'
        with self.assertRaises(ValueError):validate_case(p,'evidence-links',schema,bad)
        with self.assertRaises(ValueError):validate_case(p,'evidence-links',schema,[r[:3]for r in rows])
        with self.assertRaisesRegex(ValueError,'ORDER BY'):validate_case(p,'evidence-links',schema,list(reversed(rows)))
        bad=[r[:]for r in rows];bad[0][4]=rows[-1][4]
        with self.assertRaises(ValueError):validate_case(p,'evidence-links',schema,bad)
    def test_stop_failure_withholds_complete_report(self):
        from run_pack_scenario_graphframes import run
        from unittest.mock import patch
        import tempfile,types,sys,os
        class Builder:
            def master(self,*a):return self
            def appName(self,*a):return self
            def config(self,*a):return self
            def getOrCreate(self):return spark
        class Spark:
            sparkContext=types.SimpleNamespace(setLogLevel=lambda *a:None)
            def stop(self):raise RuntimeError('stop failed')
        spark=Spark();module=types.ModuleType('pyspark.sql');module.SparkSession=types.SimpleNamespace(builder=Builder())
        from run_graph_release_graphframes import VERSIONS
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);publication=root/'publication';publication.mkdir()
            for name in ('original-ontology.json','original-graph.json','development-bindings.json','public-dataset.json','source.jsonl','report.json'):(publication/name).write_bytes(b'original')
            release=root/'release';custody=root/'custody';release.write_bytes(b'release');custody.write_bytes(b'custody')
            import hashlib
            candidates=[dict(pack=p,publication=str(publication),release=str(release),release_sha256=hashlib.sha256(release.read_bytes()).hexdigest(),custody=str(custody),custody_sha256=hashlib.sha256(custody.read_bytes()).hexdigest())for p in ('archaeology','ecology')]
            with patch.dict(sys.modules,{'pyspark':types.ModuleType('pyspark'),'pyspark.sql':module}),patch('sys.version_info',(3,11)),patch.dict(os.environ,{'PYSPARK_PYTHON':sys.executable,'PYSPARK_DRIVER_PYTHON':sys.executable}),patch('importlib.metadata.version',side_effect=lambda n:VERSIONS[n]),patch('run_pack_release_graphframes.admit_runtime',return_value=[]),patch('prepare_pack_graph_scenarios.prepare',side_effect=lambda c,u:{'pack':c['pack']}),patch('run_pack_scenario_graphframes.execute',return_value={}):
                with self.assertRaisesRegex(RuntimeError,'stop failed'):run(candidates,'not-invoked',root/'output','not-invoked')
                self.assertFalse((root/'output/report.json').exists())
    def test_grouped_order_is_native_and_verified(self):
        p=self.profile('ecology');expected=next(c['original_graph_expected']for c in p['cases']if c['original_scenario']['id']=='connected-measurements')
        schema=[{'name':'result_0','type':'string'},{'name':'result_1','type':'bigint'}];raw=[[r[0],int(r[1])]for r in expected]
        self.assertGreater(len(raw),1);self.assertEqual(validate_case(p,'connected-measurements',schema,raw),expected)
        with self.assertRaisesRegex(ValueError,'ORDER BY'):validate_case(p,'connected-measurements',schema,list(reversed(raw)))
    def test_preparation_mutation_refuses_before_spark(self):
        from run_pack_scenario_graphframes import run
        from unittest.mock import patch,MagicMock
        import tempfile,types,sys,os,hashlib
        from run_graph_release_graphframes import VERSIONS
        module=types.ModuleType('pyspark.sql');builder=MagicMock();module.SparkSession=types.SimpleNamespace(builder=builder)
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);publication=root/'publication';publication.mkdir()
            for name in ('original-ontology.json','original-graph.json','development-bindings.json','public-dataset.json','source.jsonl','report.json'):(publication/name).write_bytes(b'original')
            release=root/'release';custody=root/'custody';release.write_bytes(b'release');custody.write_bytes(b'custody')
            candidates=[dict(pack=p,publication=str(publication),release=str(release),release_sha256=hashlib.sha256(release.read_bytes()).hexdigest(),custody=str(custody),custody_sha256=hashlib.sha256(custody.read_bytes()).hexdigest())for p in ('archaeology','ecology')]
            def mutation(c,u):
                (publication/'original-graph.json').write_bytes(b'changed during preparation');return {'pack':c['pack']}
            with patch.dict(sys.modules,{'pyspark':types.ModuleType('pyspark'),'pyspark.sql':module}),patch('sys.version_info',(3,11)),patch.dict(os.environ,{'PYSPARK_PYTHON':sys.executable,'PYSPARK_DRIVER_PYTHON':sys.executable}),patch('importlib.metadata.version',side_effect=lambda n:VERSIONS[n]),patch('run_pack_release_graphframes.admit_runtime',return_value=[]),patch('prepare_pack_graph_scenarios.prepare',side_effect=mutation):
                with self.assertRaisesRegex(ValueError,'changed'):run(candidates,'not-invoked',root/'output','not-invoked')
                self.assertFalse((root/'output/report.json').exists());builder.master.assert_not_called()

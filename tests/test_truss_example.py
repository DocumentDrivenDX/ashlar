"""Finite portable command preflight and public-component lifetime controls."""
import importlib,json,os,sys,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_example import run_truss_example_command

class Tests(unittest.TestCase):
    def configuration(self,root):
        for name in ('bun','delta.jar','storage.jar','index.json'):(root/name).write_text('selected')
        for name in ('jdk','weft'):(root/name).mkdir()
        (root/'jdk/bin').mkdir();(root/'jdk/bin/java').write_text('selected-java')
        config={'profile':'ashlar-local-truss-example/0.1','bun':str(root/'bun'),'java_home':str(root/'jdk'),'jars':[str(root/'delta.jar'),str(root/'storage.jar')],'weft_index':str(root/'index.json'),'weft_installation':str(root/'weft')}
        path=root/'configuration.json';path.write_text(json.dumps(config));return path
    def test_invalid_selection_refuses_before_optional_runtime_effects(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();configuration=self.configuration(root)
            config=json.loads(configuration.read_text());config['unexpected']='refuse';configuration.write_text(json.dumps(config))
            with patch('ashlar_host.truss_runtime.require_truss_psycopg_runtime')as runtime:
                with self.assertRaises(ValueError):run_truss_example_command(configuration,root/'output')
                runtime.assert_not_called();self.assertFalse((root/'output').exists())
                del config['unexpected'];configuration.write_text(json.dumps(config));(root/'jdk/bin/java').unlink()
                with self.assertRaises(ValueError):run_truss_example_command(configuration,root/'output')
                runtime.assert_not_called();self.assertFalse((root/'output').exists())
    def test_public_pipeline_result_only_after_close_and_environment_restored(self):
        modules={name:importlib.import_module("ashlar_host."+name)for name in ("truss_runtime","truss_local_setup","truss_graph_mutation","truss_note_cli","truss_query_cli","truss_note_publication")}
        for export_graph,fail_close,missing_pair in ((False,False,False),(True,False,False),(True,False,True),(False,True,False)):
            with self.subTest(export_graph=export_graph,fail_close=fail_close,missing_pair=missing_pair),tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();configuration=self.configuration(root);output=root/'output';events=[]
                if export_graph:
                    value=json.loads(configuration.read_text());value['export_graph']=True;configuration.write_text(json.dumps(value))
                class Runtime:
                    def __init__(self,path):self.info=SimpleNamespace(connection_uri='private-selected-uri')
                    def __enter__(self):events.append('open');return self
                    def __exit__(self,*args):
                        events.append('close');self_test.assertFalse((output/'result.json').exists())
                        if fail_close:raise RuntimeError('owned close failed')
                self_test=self
                psycopg=SimpleNamespace(conninfo=SimpleNamespace(conninfo_to_dict=lambda uri:{'user':'administrator'},make_conninfo=lambda **kwargs:'private-'+kwargs['user']),connect=lambda *args,**kwargs:None)
                def publish(config,destination):
                    events.append('publish');destination.mkdir()
                    (destination/'publication-intent-2.json').write_text(json.dumps({'feed':'example-truss','source_page_path':str(destination/'page.json'),'source_page_sha256':'a'*64,'request_digest':'b'*64}))
                    value=json.loads(config.read_text());self.assertEqual(value.get('export_graph',False),export_graph)
                    result={'publications':[{},{}]}
                    if export_graph and not missing_pair:result['graph_release']={'path':str(destination/'graph-release.json'),'sha256':'a'*64,'custody_path':str(destination/'graph-custody.json'),'custody_sha256':'b'*64,'qualification':'actual original capture'}
                    return result
                def query(*args,**kwargs):events.append('query');return {'publication_id':'actual-selected','rows':[{'label':'Beta','related':{'items':[['Value-A','Alpha']],'truncated':False}}]}
                values={'objects':[{'values':{'A-value':'Value-A','A-label':'Alpha'}},{'values':{'B-value':'Value-B','B-label':'Beta'}}]}
                with patch.dict(sys.modules,{'psycopg':psycopg,'truss':SimpleNamespace(LocalPostgres=Runtime)}), \
                     patch.dict(os.environ,{'JAVA_HOME':'original-java','PYSPARK_PYTHON':'original-python'}), \
                     patch.object(modules['truss_runtime'],'require_truss_psycopg_runtime'), \
                     patch.object(modules['truss_local_setup'],'run_setup_truss_local_command',return_value={'roles':{'consumer':'ordinary','reader':'reader'}}), \
                     patch.object(modules['truss_graph_mutation'],'run_mutate_truss_graph_command',return_value=values), \
                     patch.object(modules['truss_note_cli'],'run_publish_truss_graph_command',side_effect=publish), \
                     patch.object(modules['truss_query_cli'],'run_query_truss_graph_command',side_effect=query), \
                     patch.object(modules['truss_note_publication'],'read_page',return_value=SimpleNamespace(prior=SimpleNamespace(content=b'actual-checkpoint'))):
                    if missing_pair:
                        with self.assertRaisesRegex(ValueError,"export pair missing"):run_truss_example_command(configuration,output)
                        self.assertFalse((output/"result.json").exists())
                    elif fail_close:
                        with self.assertRaisesRegex(RuntimeError,'owned close failed'):run_truss_example_command(configuration,output)
                        self.assertFalse((output/'result.json').exists())
                    else:
                        result=run_truss_example_command(configuration,output)
                        self.assertEqual(result['publications'],2);self.assertTrue((output/'result.json').exists())
                        self.assertNotIn('private-selected-uri',json.dumps(result))
                        if export_graph:
                            self.assertEqual(result['release'],{'path':str(output/'publication/graph-release.json'),'sha256':'a'*64})
                            self.assertEqual(result['custody'],{'path':str(output/'publication/graph-custody.json'),'sha256':'b'*64})
                    self.assertEqual(events,['open','publish','query','close'])
                    self.assertEqual(os.environ['JAVA_HOME'],'original-java');self.assertEqual(os.environ['PYSPARK_PYTHON'],'original-python')
                    self.assertEqual((output/'mutation-configuration.json').stat().st_mode&0o777,0o600)

import importlib,json,os,sys,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from contextlib import ExitStack
from ashlar_host.truss_record_example import run_truss_record_example_command,PROFILE

class Tests(unittest.TestCase):
    def configuration(self,root):
        for name in ('bun','delta.jar','storage.jar'):(root/name).write_text('selected')
        (root/'jdk/bin').mkdir(parents=True);(root/'jdk/bin/java').write_text('selected')
        value={'profile':PROFILE,'bun':str(root/'bun'),'java_home':str(root/'jdk'),'jars':[str(root/'delta.jar'),str(root/'storage.jar')]}
        path=root/'configuration';path.write_text(json.dumps(value));return path,value
    def test_malformed_before_runtime(self):
        with tempfile.TemporaryDirectory()as d:
            root=Path(d).resolve();path,value=self.configuration(root)
            for invalid in ({**value,'weft_index':'unwanted'},{**value,'jars':[]},{**value,'profile':'wrong'}):
                path.write_text(json.dumps(invalid))
                with patch('ashlar_host.truss_runtime.require_truss_psycopg_runtime')as runtime:
                    with self.assertRaises(ValueError):run_truss_record_example_command(path,root/'output')
                    runtime.assert_not_called();self.assertFalse((root/'output').exists())
    def test_composition_identity_exact_values_close_and_restore(self):
        for name in ('truss_runtime','truss_local_setup','truss_record_mutation','truss_note_cli','truss_resume_setup','truss_record_read','truss_note_publication'):importlib.import_module('ashlar_host.'+name)
        for fail_close in (False,True):
            with self.subTest(fail_close=fail_close),tempfile.TemporaryDirectory()as d:
                root=Path(d).resolve();path,value=self.configuration(root);output=root/'output';events=[];self_test=self
                class Runtime:
                    def __init__(self,path):self.info=SimpleNamespace(connection_uri='private')
                    def __enter__(self):events.append('open');return self
                    def __exit__(self,*args):
                        events.append('close');self_test.assertFalse((output/'result.json').exists())
                        if fail_close:raise RuntimeError('close failed')
                fields=[{'field_id':name,'state':state,'value':value}for name,state,value in [('record-id','value','R1'),('quantity','value',{'integer_token':'9007199254740993'}),('amount','value',{'decimal_token':'1.2300'}),('active','value',True),('note','absent',None),('tag','absent',None)]]
                def create(config,result):
                    selected=json.loads(config.read_text());self.assertEqual(selected['values']['amount']['text'],'1.2300');self.assertEqual(selected['values']['quantity']['text'],'9007199254740993');events.append('create');return {'object':{'type_id':41,'id':73}}
                def publish(config,result):
                    selected=json.loads(config.read_text());self.assertEqual(selected['maximum_pages'],2);self.assertEqual(selected['feed'],'example-typed');events.append('publish');return {'publications':[{'manifest':{'publication_id':'catalog'}},{'manifest':{'publication_id':'actual'}}]}
                def configure(config,intent,destination):self.assertEqual(intent,output/'publication/publication-intent-2.json');events.append('configure')
                def read(config,result,**kwargs):
                    self.assertEqual(kwargs,{'type_id':41,'entity_id':73});events.append('read');return {'publication_id':'actual',**kwargs,'record':{'document_id':'typed-record','module_id':'m','record_id':'TypedRecord','fields':fields}}
                psycopg=SimpleNamespace(conninfo=SimpleNamespace(conninfo_to_dict=lambda uri:{'user':'administrator'},make_conninfo=lambda **kwargs:'private-'+kwargs['user']),connect=lambda *a,**k:None)
                patches={'truss_runtime.require_truss_psycopg_runtime':None,'truss_local_setup.run_setup_truss_local_command':{'roles':{'consumer':'ordinary','reader':'reader'}},'truss_note_publication.read_page':SimpleNamespace(prior=SimpleNamespace(content=b'checkpoint'))}
                with ExitStack()as stack:
                    stack.enter_context(patch.dict(sys.modules,{'psycopg':psycopg,'truss':SimpleNamespace(LocalPostgres=Runtime)}));stack.enter_context(patch.dict(os.environ,{'JAVA_HOME':'original','PYSPARK_PYTHON':'original-python'}))
                    for name,result in patches.items():stack.enter_context(patch('ashlar_host.'+name,return_value=result))
                    for name,function in [('truss_record_mutation.run_create_truss_record_command',create),('truss_note_cli.run_publish_truss_graph_command',publish),('truss_resume_setup.run_configure_truss_resume_command',configure),('truss_record_read.run_read_truss_record_command',read)]:stack.enter_context(patch('ashlar_host.'+name,side_effect=function))
                    if fail_close:
                        with self.assertRaisesRegex(RuntimeError,'close failed'):run_truss_record_example_command(path,output)
                        self.assertFalse((output/'result.json').exists())
                    else:
                        result=run_truss_record_example_command(path,output);self.assertEqual(result['entity_id'],73);self.assertEqual((output/'result.json').stat().st_mode&0o777,0o600);self.assertEqual(output.stat().st_mode&0o777,0o700)
                    self.assertEqual(events,['open','create','publish','configure','read','close']);self.assertEqual(os.environ['JAVA_HOME'],'original');self.assertEqual(os.environ['PYSPARK_PYTHON'],'original-python')

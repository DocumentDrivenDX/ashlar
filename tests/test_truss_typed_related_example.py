"""Focused five-page typed relationship composition ports; no native qualification."""
import importlib,json,os,sys,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from contextlib import ExitStack
from unittest.mock import patch
from ashlar_host.truss_typed_related_example import run_truss_typed_related_example_command,PROFILE

class Tests(unittest.TestCase):
 def configuration(self,root):
  for name in ('bun','delta.jar','storage.jar','compiler'):(root/name).write_text('selected')
  (root/'jdk/bin').mkdir(parents=True);(root/'jdk/bin/java').write_text('selected')
  raw={'profile':PROFILE,'bun':str(root/'bun'),'java_home':str(root/'jdk'),'jars':[str(root/'delta.jar'),str(root/'storage.jar')],'compiler':str(root/'compiler')};path=root/'configuration';path.write_text(json.dumps(raw));return path
 def test_two_records_two_edges_same_publication_and_close_failure(self):
  for module in ('truss_runtime','truss_local_setup','truss_record_mutation','truss_typed_edge_command','truss_note_cli','truss_resume_setup','truss_record_read','truss_typed_related_query_cli','truss_note_publication'):importlib.import_module('ashlar_host.'+module)
  for topology,fail_close in (('forward-self-loop',False),('forward-self-loop',True),('parallel-self-loops',False)):
   with self.subTest(fail_close=fail_close),tempfile.TemporaryDirectory()as directory:
    root=Path(directory).resolve();configuration=self.configuration(root);output=root/'output';events=[];objects=[];self_test=self
    class Runtime:
     def __init__(self,path):self.info=SimpleNamespace(connection_uri='private')
     def __enter__(self):events.append('open');return self
     def __exit__(self,*args):
      events.append('close');self_test.assertFalse((output/'result.json').exists())
      if fail_close:raise RuntimeError('close failed')
    def create_record(configuration,result):
     raw=json.loads(configuration.read_text());self.assertEqual(Path(raw['document_path']).read_bytes(),Path('examples/truss-native/typed-related-record.umf.json').read_bytes());objects.append(raw['values']);events.append('record');return {'object':{'id':len(objects),'type_id':1}}
    def create_edge(configuration,result):
     raw=json.loads(configuration.read_text());self.assertEqual(raw['target']['entity_id'],1 if topology=='parallel-self-loops'else 2);self.assertIn(raw['source']['entity_id'],(1,)if topology=='parallel-self-loops'else(1,2));self.assertEqual(raw['values'],{});events.append('edge');return {'edge':{'identity':{'id':str(events.count('edge'))}}}
    def publish(configuration,result):
     selected=json.loads(configuration.read_text());self.assertEqual(selected['maximum_pages'],5);self.assertEqual(selected['maximum_page_bytes'],2097152);events.append('publish');return {'publications':[{'manifest':{'publication_id':'actual'}}for _ in range(5)]}
    def read(configuration,result,*,type_id,entity_id):
     vals=objects[entity_id-1];fields=[]
     for field,tag in vals.items():
      kind=tag['kind'];value=tag['value']if kind=='boolean'else{kind+'_token':tag['text']}if kind in ('integer','decimal')else tag['text'];fields.append({'field_id':field,'state':'value','value':value})
     fields.extend({'field_id':name,'state':'absent','value':None}for name in ('note','tag'));events.append('read');return {'publication_id':'actual','type_id':type_id,'entity_id':entity_id,'record':{'document_id':'typed-related-record','module_id':'m','record_id':'TypedRecord','fields':fields}}
    def query(configuration,result,**kwargs):
     inverse='incoming'in kwargs['sql'];self.assertEqual(kwargs['maximum_rows'],2);events.append('query');
     if topology=='parallel-self-loops':
      return {'publication_id':'actual','rows':[{'id':'R1','targets':{'items':[['R1'],['R1']],'truncated':False}},{'id':'R2','targets':{'items':[],'truncated':False}}]}
     return {'publication_id':'actual','rows':[{'id':'R1','targets':{'items':[]if inverse else[['R2']],'truncated':False}},{'id':'R2','targets':{'items':[['R1'],['R2']]if inverse else[['R2']],'truncated':False}}]}
    def checkpoint(*args,**kwargs):
     self.assertEqual(kwargs,{'maximum_bytes':2097152});return SimpleNamespace(prior=SimpleNamespace(content=b'checkpoint'))
    psycopg=SimpleNamespace(conninfo=SimpleNamespace(conninfo_to_dict=lambda uri:{'user':'admin'},make_conninfo=lambda **kw:'private-'+kw['user']),connect=lambda *a,**kw:None)
    with ExitStack()as stack:
     stack.enter_context(patch.dict(sys.modules,{'psycopg':psycopg,'truss':SimpleNamespace(LocalPostgres=Runtime)}));stack.enter_context(patch.dict(os.environ,{'JAVA_HOME':'original'}))
     for name,result in [('truss_runtime.require_truss_psycopg_runtime',None),('truss_typed_related_example.require_parallel_runtime',None),('truss_local_setup.run_setup_truss_local_command',{'roles':{'consumer':'ordinary','reader':'reader'},'catalog_status':{'layout_profile':{'identity':'truss-layout','version':'1.0','sha256':'a'*64}}}),('truss_note_publication.read_page',SimpleNamespace(prior=SimpleNamespace(content=b'checkpoint'))),('truss_resume_setup.run_configure_truss_resume_command',None)]:stack.enter_context(patch('ashlar_host.'+name,**({'side_effect':checkpoint}if name=='truss_note_publication.read_page'else{'return_value':result})))
     for name,operation in [('truss_record_mutation.run_create_truss_record_command',create_record),('truss_typed_edge_command.run_create_truss_edge_command',create_edge),('truss_note_cli.run_publish_truss_graph_command',publish),('truss_record_read.run_read_truss_record_command',read),('truss_typed_related_query_cli.run_query_truss_typed_related_keys_command',query)]:stack.enter_context(patch('ashlar_host.'+name,side_effect=operation))
     if fail_close:
      with self.assertRaisesRegex(RuntimeError,'close failed'):run_truss_typed_related_example_command(configuration,output,topology=topology)
      self.assertFalse((output/'result.json').exists())
     else:
      result=run_truss_typed_related_example_command(configuration,output,topology=topology);self.assertEqual(result['publications'],5);self.assertEqual(len(result['edges']),2);self.assertEqual((output/'result.json').stat().st_mode&0o777,0o600)
     self.assertEqual(os.environ['JAVA_HOME'],'original');self.assertEqual(events,['open','record','record','edge','edge','publish','read','read','query','query','close'])

 def test_public_example_dispatch(self):
  import io
  from contextlib import redirect_stdout
  from ashlar.cli import main
  with patch('sys.argv',['ashlar','run-truss-typed-related-example','--configuration','/configuration','--output','/output']),patch('ashlar_host.truss_typed_related_example.run_truss_typed_related_example_command',return_value={'publication_id':'original'})as command,redirect_stdout(io.StringIO()):main()
  command.assert_called_once_with('/configuration','/output')

 def test_parallel_preflight_and_actual_layout_gate(self):
  from ashlar_host.truss_typed_related_example import require_parallel_layout,require_parallel_runtime,topology_rows
  with patch('importlib.metadata.version',return_value='0.0.14'),patch('ashlar_host.truss_typed_related_example.preflight')as preflight:
   with self.assertRaises(ValueError):run_truss_typed_related_example_command('/not-read','/not-created',topology='parallel-self-loops')
   preflight.assert_not_called()
  with patch('importlib.metadata.version',return_value='0.0.15'):require_parallel_runtime()
  for pin in ({'identity':'truss-layout','version':'0.16','sha256':'a'*64},{'identity':'foreign','version':'1.0','sha256':'a'*64}):
   with self.assertRaises(ValueError):require_parallel_layout({'catalog_status':{'layout_profile':pin}})
  self.assertEqual(topology_rows('parallel-self-loops')['forward'][0]['targets']['items'],[['R1'],['R1']])

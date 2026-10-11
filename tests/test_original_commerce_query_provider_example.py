"""Self-contained public provider dispatch and bounded selection regressions."""
import importlib.util,unittest,tempfile,json,hashlib
from pathlib import Path
from dataclasses import replace
from unittest import mock
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('template', ROOT/'examples/original-commerce-local/query-provider.template.py');p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
from ashlar_host.config import ProducerConfig
from ashlar_host.implicit_arithmetic_configuration import QueryCommerceImplicitArithmeticConfig,ExplicitCommercePostgresEndpoint
from ashlar_host.path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig
class Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.file=Path(self.tmp.name).resolve()/'publication.json';template=json.loads((ROOT/'examples/original-commerce-local/publish.template.json').read_bytes())
  def replace_explicit(value):
   if type(value)is dict:return {k:replace_explicit(v)for k,v in value.items()}
   if type(value)is str and value.startswith('/absolute/'):
    return str(self.file.parent/value.rsplit('/',1)[-1].replace('REPLACE_WITH_','selected_'))
   if type(value)is str and value.startswith('REPLACE_WITH_64_'):return '0'*64
   return value
  template=replace_explicit(template)
  template['source'].update(port=25435,source_schema='selected_source',writer_role='selected_writer',reader_role='selected_reader',ack_role='selected_ack')
  template['source']['scope'].update(service_schema='ashlar_ack_selected_service',scope_id='07c04255-73c9-40f3-a16a-000000000002',installation_id='07c04255-73c9-40f3-a16a-000000000001',consumer='selected_consumer')
  self.file.write_text(json.dumps(template))
  self.config=QueryCommerceImplicitArithmeticConfig(*(self.file.parent/n for n in ['index','installation','publication','output','jars','model','graph']),ProducerConfig(self.file.parent/'umf',self.file.parent/'bun',self.file.parent/'git',30,1048576,4194304),ExplicitCommercePostgresEndpoint('selected-container','127.0.0.1',25435,'truss_e2e'),16777216,PathCaptureConfig(1000,16777216,67108864),PathDecodeConfig(16777216))
 def configured(self):return mock.patch.multiple(p,CONFIGURATION=self.file,CONFIGURATION_SHA256=hashlib.sha256(self.file.read_bytes()).hexdigest(),CONTAINER='selected-container')
 def test_unfilled_refuses_before_configuration_read(self):
  with mock.patch.object(Path,'open',side_effect=AssertionError('must not read')):
   with self.assertRaises(ValueError):p.selected(self.config)
 def test_actual_typed_ports_and_exact_callbacks(self):
  from ashlar_host.commerce_query_sources import CommerceQuerySourceFactory
  with self.configured():
   ports=p.selected(self.config);self.assertIs(type(p.commerce_query_source_factory(self.config)),CommerceQuerySourceFactory)
  calls=[];r=object();a=object();source=replace(ports.registration.source,connection_factory=lambda c:(calls.append(('reader',c)),r)[1]);reg=replace(ports.registration,source=source,metadata_factory=lambda c:(calls.append(('ack',c)),a)[1])
  with mock.patch.object(p,'selected',return_value=replace(ports,registration=reg)):
   f=p.ordinary_query_connection_factory(self.config);self.assertIs(f.open(reg.source_role),r);self.assertIs(f.open(reg.ack_role),a)
   for role in (ports.writer_role,'foreign',True):
    with self.assertRaises(ValueError):f.open(role)
  self.assertEqual(calls,[('reader',None),('ack',None)])
 def test_actual_callback_error_cancel_identity(self):
  with self.configured():ports=p.selected(self.config)
  for field in ('reader','ack'):
   for error in (ValueError('fixed-test'),KeyboardInterrupt()):
    def fail(c):raise error
    reg=replace(ports.registration,metadata_factory=fail) if field=='ack' else replace(ports.registration,source=replace(ports.registration.source,connection_factory=fail))
    with mock.patch.object(p,'selected',return_value=replace(ports,registration=reg)):
     with self.assertRaises(type(error))as caught:p.ordinary_query_connection_factory(self.config).open(reg.ack_role if field=='ack' else reg.source_role)
     self.assertIs(caught.exception,error)
 def test_current_configuration_mutation_refuses_before_factories_return(self):
  from ashlar_host.original_commerce_local_setup import original_commerce_local_source_ports
  def mutate(source):
   ports=original_commerce_local_source_ports(source)
   self.file.write_bytes(self.file.read_bytes()+b' ')
   return ports
  with self.configured(),mock.patch('ashlar_host.original_commerce_local_setup.original_commerce_local_source_ports',side_effect=mutate):
   with self.assertRaises(Exception):p.selected(self.config)
 def test_wrong_endpoint_digest_symlink_oversize_refuse(self):
  with self.configured():
   with self.assertRaises(ValueError):p.selected(replace(self.config,postgres=replace(self.config.postgres,port=25434)))
   with mock.patch.object(p,'CONFIGURATION_SHA256','0'*64):
    with self.assertRaises(ValueError):p.selected(self.config)
   alias=self.file.parent/'alias';alias.symlink_to(self.file)
   with mock.patch.object(p,'CONFIGURATION',alias):
    with self.assertRaises(Exception):p.selected(self.config)
   self.file.write_bytes(b'x'*262145)
   with self.assertRaises(Exception):p.selected(self.config)
if __name__=='__main__':unittest.main()

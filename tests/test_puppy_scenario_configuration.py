"""Closed immutable original17 configuration; no file/SDK acquisition."""
import json,pickle,unittest
from dataclasses import FrozenInstanceError
from ashlar_host.puppy_scenario_configuration import (parse_puppy_scenario_configuration,
    with_puppy_scenario_credentials,MAP_PUPPY)

class Tests(unittest.TestCase):
 def value(self):
  return {'profile':'ashlar-original17-puppygraph-invocation/0.1','packs':[{'name':name,'model':'/input/'+name+'/model','graph':'/input/'+name+'/graph','pack':'/input/'+name+'/pack','export':{'release_path':'/input/'+name+'/release','custody_path':'/input/'+name+'/custody','release_sha256':'a'*64,'custody_sha256':'b'*64}}for name in ('archaeology','ecology')],'producer':{'source':'/umf','bun':'/bin/bun','git':'/bin/git','timeout_seconds':60,'maximum_output_bytes':1048576,'maximum_receipt_bytes':4194304},'runtime':{'docker':'/bin/docker','docker_host':'unix:///socket/docker.sock','image':'sha256:'+'c'*64,'name':'ashlar-original17-owned','protocol':'Cypher','http_port':18081,'bolt_port':17687},'credentials':{'username':'private-user','password':'private-password'},'output':'/owned/fresh-output','bounds':{'native_seconds':120,'whole_seconds':300,'stdout_bytes':1048576,'stderr_bytes':1048576,'maximum_rows':1000,'maximum_bytes':1048576}}
 def parse(self,v):return parse_puppy_scenario_configuration(json.dumps(v).encode())
 def test_immutable_selection_redacts_and_explicit_transport_port(self):
  v=self.value();c=self.parse(v)
  self.assertIsInstance(c.packs,tuple);self.assertEqual(tuple(p.name for p in c.packs),('archaeology','ecology'))
  self.assertNotIn('private-password',repr(c));self.assertNotIn('private-user',repr(c))
  with self.assertRaises(FrozenInstanceError):c.packs[0].name='foreign'
  with self.assertRaises(AttributeError):c.credentials.password.value='foreign'
  with self.assertRaises(TypeError):pickle.dumps(c.credentials)
  self.assertEqual(with_puppy_scenario_credentials(c.credentials,lambda u,p:(u,p)),('private-user','private-password'))
  v['runtime'].update(protocol='Gremlin',gremlin_port=18182,native_map_profile=MAP_PUPPY)
  self.assertEqual(self.parse(v).runtime.gremlin_port,18182)
 def test_unknown_types_protocol_ports_and_bounds_refuse(self):
  for change in ('extra','bool-port','bool-bound','oversize','protocol','map','duplicate-port','order','relative','overlap'):
   v=self.value()
   if change=='extra':v['runtime']['ambient_endpoint']='foreign'
   elif change=='bool-port':v['runtime']['http_port']=True
   elif change=='bool-bound':v['bounds']['native_seconds']=True
   elif change=='oversize':v['bounds']['maximum_rows']=1001
   elif change=='protocol':v['runtime']['protocol']='SQL'
   elif change=='map':v['runtime'].update(protocol='Gremlin',gremlin_port=18182,native_map_profile='unknown')
   elif change=='duplicate-port':v['runtime']['bolt_port']=v['runtime']['http_port']
   elif change=='order':v['packs'].reverse()
   elif change=='relative':v['packs'][0]['model']='relative'
   else:v['output']='/input'
   with self.subTest(change=change),self.assertRaises(ValueError):self.parse(v)
  v=self.value();v['runtime']['gremlin_port']=18182
  with self.assertRaises(ValueError):self.parse(v)
  raw=json.dumps(self.value()).replace('"native_seconds": 120','"native_seconds": 120,"native_seconds": 120').encode()
  with self.assertRaises(ValueError):parse_puppy_scenario_configuration(raw)

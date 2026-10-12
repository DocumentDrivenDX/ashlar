"""Actual owner checks over inert Docker observations; no container/SDK/source run."""
import unittest
from pathlib import Path
from ashlar_host.puppy_finite_local import OwnedPuppyFiniteLocal,PuppyFiniteLoopbackConfig
class Tests(unittest.TestCase):
 def owner(self):
  from types import SimpleNamespace
  owner=object.__new__(OwnedPuppyFiniteLocal);owner._container_id='a'*64;owner.database=Path('/private/tmp/carrier.duckdb')
  owner._config=SimpleNamespace(image='sha256:'+'b'*64,name='ashlar-inert-finite',http_port=25450,bolt_port=25451)
  row={'Id':owner.container_id,'Image':owner.config.image,'Name':'/'+owner.config.name,'State':{'Running':True},'HostConfig':{'NanoCpus':2000000000,'Memory':3*1024**3},'Mounts':[{'Destination':'/tmp/carrier.duckdb','Source':str(owner.database),'RW':False}],'NetworkSettings':{'Ports':{'8081/tcp':[{'HostIp':'127.0.0.1','HostPort':'25450'}],'7687/tcp':[{'HostIp':'127.0.0.1','HostPort':'25451'}]}}}
  import json
  owner._docker=lambda args:json.dumps([row]).encode()
  return owner,row
 def test_exact_owned_container_image_resources_mount_and_ports(self):
  owner,row=self.owner();owner._container()
  from unittest.mock import patch
  import time
  owner._config.docker=Path('/usr/bin/true');owner._config.prepared=Path('/private/tmp');owner._config.docker_host='unix:///private/tmp/inert.sock'
  owner.deadline=time.monotonic()+20;owner.docker_diagnostics=[]
  with patch('ashlar_host.puppy_finite_local.capture',return_value=(b'a'*64+b'\n',b'platform warning')):
   # Use actual method, not the inert inspect port assigned by fixture.
   self.assertEqual(OwnedPuppyFiniteLocal._docker(owner,['create']),b'a'*64+b'\n')
  self.assertEqual(owner.docker_diagnostics,[b'platform warning'])
  row['Mounts'][0]['RW']=True
  with self.assertRaises(PermissionError):owner._container()
 def test_foreign_container_or_public_endpoint_refuses(self):
  owner,row=self.owner();row['Id']='c'*64
  with self.assertRaises(PermissionError):owner._container()
  row['Id']=owner.container_id;row['NetworkSettings']['Ports']['8081/tcp'][0]['HostIp']='0.0.0.0'
  with self.assertRaises(PermissionError):owner._container()
 def test_schema_omission_is_explicit_and_unknown_content_retained(self):
  import json
  from unittest.mock import patch
  owner,row=self.owner();owner.closed=False;owner.loaded=True;owner.original_database=b'db';owner.original_model=b'model';owner.observations=[];owner.config.prepared=Path('/private/tmp')
  owner.database=Path(__file__).resolve();owner.original_database=owner.database.read_bytes()
  owner.requested_model={'catalog':[{'name':'selected'}],'node':[{'label':'FinitePackNode'}],'edge':[{'label':'FinitePackEdge'}]}
  observed={'node':owner.requested_model['node'],'edge':owner.requested_model['edge'],'unknown':{'kept':True}};owner._http=lambda path:json.dumps(observed).encode();owner._container=lambda:None
  with patch('ashlar_host.puppy_finite_local.FiniteExportPreparationPolicy.renew'),patch('ashlar_host.puppy_finite_local.snapshot',side_effect=lambda path,bound:owner.original_database if path==owner.database else b'model'),patch.object(Path,'stat',return_value=type('Info',(),{'st_mode':0o444})()):
   owner.renew();self.assertEqual(owner.catalog_visibility,'omitted');self.assertEqual(json.loads(owner.observations[-1][1])['unknown'],{'kept':True})
   observed['node']=[]
   with self.assertRaises(PermissionError):owner.renew()

 def test_startup_transients_check_owned_status_but_auth_fails_fast(self):
  import http.client,time,urllib.error
  from unittest.mock import patch
  owner,row=self.owner();owner.deadline=time.monotonic()+10;owner.observations=[]
  checks=[];owner._container=lambda:checks.append('owned-running')
  replies=iter((http.client.RemoteDisconnected('startup'),TimeoutError('socket'),b'{}'))
  def response(path):
   value=next(replies)
   if isinstance(value,BaseException):raise value
   return value
  owner._http=response
  with patch('ashlar_host.puppy_finite_local.time.sleep'):owner._wait_ready()
  self.assertEqual(checks,['owned-running','owned-running']);self.assertEqual(owner.observations,[('before-model',b'{}')])
  owner._http=lambda path:(_ for _ in ()).throw(urllib.error.HTTPError('http://127.0.0.1',401,'auth',{},None))
  with self.assertRaises(urllib.error.HTTPError):owner._wait_ready()
  self.assertEqual(len(checks),2)
  owner._http=lambda path:(_ for _ in ()).throw(http.client.RemoteDisconnected('startup'))
  owner._container=lambda:(_ for _ in ()).throw(PermissionError('owned engine exited'))
  with self.assertRaises(PermissionError):owner._wait_ready()

 def test_submission_has_one_bounded_socket_allowance_not_post_retry(self):
  import io,time
  owner,row=self.owner();owner.deadline=time.monotonic()+100
  owner._config.username='selected';owner._config.password='private'
  observed=[]
  class Opener:
   def open(self,request,timeout):
    observed.append(timeout);response=io.BytesIO(b'{"ok":true}');response.status=200;return response
  owner.opener=Opener()
  owner._http('/schema?postUploadBehavior=none',b'{}');self.assertEqual(observed,[30])
  owner.deadline=time.monotonic()+3
  owner._http('/schema?postUploadBehavior=none',b'{}');self.assertTrue(0<observed[-1]<=3)
  owner._http('/schemajson');self.assertTrue(0<observed[-1]<=3)

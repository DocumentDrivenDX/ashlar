# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pinned optional Neo4j readiness controls; all network/runtime ports inert."""
import unittest

class Tests(unittest.TestCase):
 def owner(self):
  from test_puppy_finite_local import Tests as OwnerControls
  return OwnerControls.owner(self)

 def test_actual_neo4j_transient_readiness_witness_and_fatal_controls(self):
  import time,json
  from importlib.metadata import version
  self.assertEqual(version('neo4j'),'6.4.1')
  from types import SimpleNamespace
  from unittest.mock import patch
  from neo4j import GraphDatabase
  from neo4j.exceptions import TransientError,AuthError,UnsupportedServerProduct
  for mode in ('witness','auth','security','product','invalid','cleanup'):
   with self.subTest(mode=mode):
    owner,row=self.owner();owner.deadline=time.monotonic()+10;owner.config.username='operator';owner.config.password='never-record-this';owner.observations=[];checks=[];calls=[];owner._container=lambda:checks.append('owned')
    # Neo4j 6.4.1 assigns this synthetic TransientError its generic database
    # error code; the readiness record preserves the SDK's actual value.
    first=TransientError('never-record-this');second=TransientError('never-record-this')
    if mode=='auth':first=AuthError('never-record-this')
    if mode=='product':first=UnsupportedServerProduct('never-record-this')
    if mode=='security':
     class SecurityTransientError(TransientError):
      @property
      def code(self):return 'Neo.TransientError.Security.AuthorizationExpired'
     first=SecurityTransientError('never-record-this')
    replies=iter(([{'ready':'invalid'}],)if mode=='invalid'else(first,second,[{'ready':1}]))
    class Session:
     def run(self,query):
      calls.append(('query',query.text,query.timeout));response=next(replies)
      if isinstance(response,BaseException):raise response
      return [SimpleNamespace(data=lambda row=row:row)for row in response]
     def close(self):
      calls.append(('session-close',))
      if mode=='cleanup':raise OSError('private-cleanup-message')
    class Driver:
     def session(self):return Session()
     def close(self):calls.append(('driver-close',))
    with patch.object(GraphDatabase,'driver',return_value=Driver()),patch('ashlar_host.puppy_finite_local.time.sleep'):
     if mode=='witness':owner._wait_backend_ready()
     else:
      expected=ValueError if mode=='invalid'else type(first)
      with self.assertRaises(expected):owner._wait_backend_ready()
    self.assertEqual([c[0]for c in calls],['query','session-close','driver-close']*(3 if mode=='witness'else 1))
    self.assertEqual(len(checks),2 if mode=='witness'else 0)
    records=[json.loads(raw)for name,raw in owner.observations if name=='backend-readiness']
    self.assertEqual(len(records),3 if mode=='witness'else 1);self.assertNotIn('never-record-this',json.dumps(records))
    if mode=='witness':
     self.assertEqual(records[-1]['rows'],[{'ready':1}]);self.assertEqual(records[0]['error_code'],'Neo.DatabaseError.General.UnknownError')
     self.assertEqual(records[0]['error_type'],'neo4j.exceptions.TransientError')
    if mode=='security':self.assertEqual(records[0]['error_code'],'Neo.TransientError.Security.AuthorizationExpired')

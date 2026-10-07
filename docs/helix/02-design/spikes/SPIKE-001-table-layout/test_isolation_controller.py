import unittest
from unittest.mock import patch
from threading import Lock
from isolation_controller import compare_readers
class FakeClient:
 instances=[];lock=Lock()
 def __init__(self,out,warehouse_id):
  self.out=out;self.warehouse_id=warehouse_id;self.closed=False
  with self.lock:self.instances.append(self)
 def history(self):pass
 def close(self):self.closed=True
class FakeReader:
 def __init__(self,c,v,r):self.client=c
 def preflight(self):
  if self.client.out=='fail':raise RuntimeError('preflight failed')
 def read(self,phase,i):pass
 def bounded_load(self,stop,max_reads,max_seconds):stop.wait(1);return 1
class Controller(unittest.TestCase):
 def setUp(self):FakeClient.instances=[]
 def test_both_targets_closed_and_publication_once(self):
  calls=[]
  with patch('isolation_controller.ExactReader',FakeReader):
   r=compare_readers([('shared','a','one'),('isolated','b','two')],{},[],lambda stop:calls.append('publish'),client_factory=FakeClient,idle_reads=1)
  self.assertEqual(calls,['publish']);self.assertEqual(len(r['readers']),2)
  self.assertTrue(all(c.closed for c in FakeClient.instances))
 def test_preflight_failure_prevents_publication(self):
  calls=[]
  with patch('isolation_controller.ExactReader',FakeReader):
   with self.assertRaises(RuntimeError):compare_readers([('shared','a','fail'),('isolated','b','two')],{},[],lambda stop:calls.append('publish'),client_factory=FakeClient,idle_reads=1)
  self.assertEqual(calls,[]);self.assertTrue(all(c.closed for c in FakeClient.instances))
 def test_publisher_failure_closes_readers_without_retry(self):
  calls=[]
  def fail(stop):calls.append(1);raise RuntimeError('write observation failed')
  with patch('isolation_controller.ExactReader',FakeReader):
   with self.assertRaises(RuntimeError):compare_readers([('shared','a','one'),('isolated','b','two')],{},[],fail,client_factory=FakeClient,idle_reads=1)
  self.assertEqual(calls,[1]);self.assertTrue(all(c.closed for c in FakeClient.instances))
 def test_same_compute_rejected(self):
  with self.assertRaises(AssertionError):compare_readers([('shared','a','one'),('isolated','a','two')],{},[],lambda stop:None)
if __name__=='__main__':unittest.main()

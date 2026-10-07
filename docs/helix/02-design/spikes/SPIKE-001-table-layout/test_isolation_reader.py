"""Failure controls for the exact reader used in the pending isolation run."""
import unittest
from unittest.mock import Mock
from threading import Event
from pathlib import Path
from isolation_reader import ExactReader,expected_r103,E
class ReaderGuards(unittest.TestCase):
 def setUp(self):
  self.versions,self.rows=expected_r103(Path(__file__).resolve().parent)
  self.client=Mock();self.reader=ExactReader(self.client,self.versions,self.rows)
 def test_pinned_full_identity_and_exact_row(self):
  self.client.sql.return_value=[self.rows[0]];self.reader.read('idle',0)
  args=self.client.sql.call_args
  self.assertIn('VERSION AS OF '+str(self.versions[E]),args.args[1])
  self.assertEqual(args.kwargs['parameters']['id'],self.rows[0][2])
  self.assertIn('source_system=:source',args.args[1])
 def test_missing_extra_or_changed_carrier_fails(self):
  corrupt=list(self.rows[0]);corrupt[9]='{}'
  for returned in [[],[self.rows[0],self.rows[0]],[corrupt]]:
   self.client.sql.return_value=returned
   with self.assertRaises(AssertionError):self.reader.read('load',0)
 def test_failure_stops_other_lanes(self):
  stop=Event();self.client.sql.return_value=[]
  with self.assertRaises(AssertionError):self.reader.bounded_load(stop)
  self.assertTrue(stop.is_set())
 def test_stop_and_count_bounds(self):
  stop=Event();stop.set();self.assertEqual(self.reader.bounded_load(stop),0)
  self.client.sql.side_effect=[[self.rows[0]],[self.rows[1]]]
  self.assertEqual(self.reader.bounded_load(Event(),max_reads=2),2)
if __name__=='__main__':unittest.main()

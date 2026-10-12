"""Basic export composition controls; public reader/hold ports inert, no native."""
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
import json,unittest
import test_private_graph_custody as fixture
from ashlar_host.commerce_graph_release import read_commerce_export

class Tests(unittest.TestCase):
 def setUp(self):
  self.release,self.receipt=fixture.Tests().fixture();value=json.loads(self.receipt);ack=value['ack'];self.events=[];self.fail_outer=False;self.fail_hold=False
  self.capture={'format':'ashlar-private-local-protected-ack-interval/0.1','scope':ack['scope'],'source_schema':ack['opening']['session']['source_schema'],'source_signature_sha256':ack['opening']['session']['source_signature_sha256'],'original_request_hex':ack['requestHex'],'original_manifest_hex':ack['manifestHex'],'opening_ack':ack['opening'],'closing_ack':ack['closing'],'qualification':'inert actual-port control'}
  source=json.loads(value['originalManifest']['validation_report_json'])['source_admission']
  self.opened=SimpleNamespace(provider=SimpleNamespace(closed_interval_custody=lambda context:self.capture),context=object(),admission=SimpleNamespace(metadata=lambda:source),original_native_files={'native':'original'})
 @contextmanager
 def factory(self):
  self.events.append('reader-open')
  try:yield self.opened
  finally:
   self.events.append('reader-close')
   if self.fail_outer:raise PermissionError('inert original outer closing refusal')
 @contextmanager
 def hold(self,opened):
  self.assertIs(opened,self.opened);self.events.append('hold-open')
  try:
   if self.fail_hold:raise PermissionError('inert native read refusal')
   yield SimpleNamespace(release=self.release)
  finally:self.events.append('hold-close')
 def test_exact_pair_returned_after_both_actual_lifecycle_ports_close(self):
  with patch('ashlar_host.commerce_graph_release.hold_original_commerce_graph_release',self.hold):
   release,custody,native=read_commerce_export(self.factory)
  self.assertEqual(release,self.release);self.assertEqual(custody,self.receipt);self.assertEqual(native,{'native':'original'})
  self.assertEqual(self.events,['reader-open','hold-open','hold-close','reader-close'])
 def test_outer_closing_refusal_withholds_pair(self):
  self.fail_outer=True
  with patch('ashlar_host.commerce_graph_release.hold_original_commerce_graph_release',self.hold),self.assertRaises(PermissionError):read_commerce_export(self.factory)
  self.assertEqual(self.events[-1],'reader-close')
 def test_native_read_refusal_still_closes_outer_reader(self):
  self.fail_hold=True
  with patch('ashlar_host.commerce_graph_release.hold_original_commerce_graph_release',self.hold),self.assertRaises(PermissionError):read_commerce_export(self.factory)
  self.assertEqual(self.events[-2:],['hold-close','reader-close'])

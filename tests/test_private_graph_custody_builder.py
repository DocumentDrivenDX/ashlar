# SPDX-License-Identifier: MIT OR Apache-2.0
"""Basic public receipt builder correspondence; fixture observations are inert."""
import hashlib,json,unittest
import test_private_graph_custody as fixture
from ashlar_host.private_graph_custody import build_private_custody,load_private_graph_release

class Tests(unittest.TestCase):
 def setUp(self):
  self.release,self.original=fixture.Tests().fixture();self.value=json.loads(self.original)
  self.source=json.loads(json.loads(self.release.payload)['publication']['validation_report_json'])['source_admission']
 def test_original_tool_receipt_byte_parity_and_public_loader(self):
  payload=build_private_custody(self.release,self.source,self.value['ack'])
  self.assertEqual(payload,self.original)
  self.assertEqual(load_private_graph_release(self.release.payload,self.release.sha256,payload,hashlib.sha256(payload).hexdigest()),json.loads(self.release.payload))
 def test_changed_source_and_ack_refuse(self):
  with self.assertRaises(ValueError):build_private_custody(self.release,{'different':'source'},self.value['ack'])
  self.value['ack']['closing']['observation']['receipt_hex']='00'
  with self.assertRaises(ValueError):build_private_custody(self.release,self.source,self.value['ack'])
 def test_missing_closing_observation_and_changed_release_digest_refuse(self):
  self.value['ack'].pop('closing')
  with self.assertRaises(ValueError):build_private_custody(self.release,self.source,self.value['ack'])
  from ashlar.graph_release import GraphRelease
  with self.assertRaises(ValueError):build_private_custody(GraphRelease(self.release.payload,'0'*64),self.source,{})

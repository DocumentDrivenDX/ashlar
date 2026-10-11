"""Retained original export correspondence; no current authority or PG access."""
import copy
import gzip
import hashlib
import json
from pathlib import Path
import unittest
from ashlar_host.private_graph_custody import load_private_graph_release, admit_private_custody

ROOT=Path(__file__).resolve().parents[1]/'docs/helix/04-build/evidence/original17-graphframes-native-20261009/original'
def digest(raw):return hashlib.sha256(raw).hexdigest()
class Tests(unittest.TestCase):
 def fixtures(self):
  for receipt,release in [('0-custody.json.gz','1-release.graph.json'),('8-custody.json.gz','9-release.graph.json')]:
   yield (ROOT/release).read_bytes(),gzip.decompress((ROOT/receipt).read_bytes())
 def load(self,raw,receipt):return load_private_graph_release(raw,digest(raw),receipt,digest(receipt))
 def test_original_two_complete_exports_preserve_every_carrier(self):
  for raw,receipt in self.fixtures():
   with self.subTest(release=digest(raw)):
    value=self.load(raw,receipt)
    self.assertEqual(value,json.loads(raw))
    self.assertGreater(len(value['nodes']),30)
    self.assertGreater(len(value['edges']),40)
 def test_ack_session_and_complete_vector_remain_mandatory(self):
  for raw,receipt in self.fixtures():
   for change in [lambda x:x.pop('ack'),lambda x:x['ack']['closing']['session'].update(backend_pid='999'),lambda x:x['snapshots'].pop(next(iter(x['snapshots']))),lambda x:x['interval'].update(closingSucceeded=1)]:
    value=json.loads(receipt);change(value)
    changed=(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
    with self.assertRaises(ValueError):self.load(raw,changed)
 def test_both_independent_digest_inputs_required(self):
  raw,receipt=next(self.fixtures())
  for release_sha,receipt_sha in [('0'*64,digest(receipt)),(digest(raw),'0'*64)]:
   with self.assertRaises(ValueError):load_private_graph_release(raw,release_sha,receipt,receipt_sha)
 def test_overloaded_original_carriers_refuse_before_equality(self):
  class Spoof(dict):
   def __eq__(self,other):raise AssertionError('Executable comparison invoked')
  raw,receipt=next(self.fixtures());value=json.loads(raw)
  value['publication']=Spoof(value['publication'])
  with self.assertRaises(ValueError):admit_private_custody(value,receipt,digest(receipt),digest(raw))
 def test_duplicate_and_giant_integer_refuse(self):
  raw,receipt=next(self.fixtures())
  for changed in [receipt.replace(b'"format":',b'"format":"duplicate","format":',1),receipt.replace(b'"version":',b'"version":'+b'9'*1000+b',"other":',1)]:
   with self.assertRaises(ValueError):self.load(raw,changed)
 def test_direct_cycles_depth_and_nonfinite_nested_inventory_refuse(self):
  raw,receipt=next(self.fixtures());cyclic=[];cyclic.append(cyclic)
  for bad in [cyclic,{'publication':{'huge':2**64}},{'publication':{'unknown':float('nan')}}]:
   with self.assertRaises(ValueError):admit_private_custody(bad,receipt,digest(receipt),digest(raw))
 def test_full_carrier_roles_and_dangling_endpoints_refuse(self):
  raw,receipt=next(self.fixtures())
  for change in [lambda x:x['roles'].update(edges=x['roles']['nodes']),lambda x:x['edges'][0].update(dst='unknown'),lambda x:x['nodes'][0].update(graph_id='forged'),lambda x:x['nodes'][0].update(unknown='field')]:
   value=json.loads(raw);change(value)
   altered=(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
   custody=json.loads(receipt);custody['releaseSha256']=digest(altered);custody['roles']=value['roles']
   rebound=(json.dumps(custody,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
   with self.assertRaises(ValueError):self.load(altered,rebound)
 def test_present_malformed_retention_never_rescues_original_ack(self):
  raw,receipt=next(self.fixtures())
  for retention in [None,False,{'targets':None},{'targets':{}}]:
   value=json.loads(raw);report=json.loads(value['publication']['validation_report_json']);report['retention']=retention
   value['publication']['validation_report_json']=json.dumps(report,ensure_ascii=False,sort_keys=True,separators=(',',':'))
   altered=(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
   custody=json.loads(receipt);custody['releaseSha256']=digest(altered)
   rebound=(json.dumps(custody,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
   with self.assertRaises(ValueError):self.load(altered,rebound)
if __name__=='__main__':unittest.main()

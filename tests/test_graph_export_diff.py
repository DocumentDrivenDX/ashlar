# SPDX-License-Identifier: MIT OR Apache-2.0
"""Two actual owned export-pair comparisons; no source or engine qualification."""
import hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import test_private_graph_custody as fixture
from ashlar_host.local_graph_export import LocalGraphExportUse
from ashlar_host.graph_export_diff import run_compare_graph_exports_command

class Tests(unittest.TestCase):
 def fixture(self):
  temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup);root=Path(temporary.name).resolve();root.chmod(0o700)
  release,custody=fixture.Tests().fixture();original=json.loads(release.payload);changed=json.loads(release.payload)
  changed['nodes'][0]['props_json']=' {"opaque":"changed"} '
  selections=[]
  for label,graph in (('first',original),('second',changed)):
   raw=(json.dumps(graph,sort_keys=True,separators=(',',':'),ensure_ascii=False)+'\n').encode()
   pin=hashlib.sha256(raw).hexdigest();receipt=json.loads(custody);receipt['releaseSha256']=pin
   receipt_raw=(json.dumps(receipt,sort_keys=True,separators=(',',':'),ensure_ascii=False)+'\n').encode()
   release_path=root/(label+'-release.json');custody_path=root/(label+'-custody.json')
   release_path.write_bytes(raw);custody_path.write_bytes(receipt_raw)
   config={'release_path':str(release_path),'custody_path':str(custody_path),'release_sha256':pin,'custody_sha256':hashlib.sha256(receipt_raw).hexdigest()}
   path=root/(label+'-configuration.json');path.write_text(json.dumps(config));selections.append(path)
  return root,selections,original,changed
 def test_stable_ids_exact_property_carriers_and_edges_after_owner_close(self):
  root,configs,before,after=self.fixture();output=root/'diff.json'
  result=run_compare_graph_exports_command(*configs,output)
  self.assertEqual(result,json.loads(output.read_bytes()));self.assertEqual(output.stat().st_mode&0o777,0o600)
  self.assertEqual(result['nodes']['added'],[]);self.assertEqual(result['nodes']['removed'],[])
  change=result['nodes']['changed'][0];self.assertEqual(change['graph_id'],before['nodes'][0]['graph_id'])
  self.assertEqual(change['changed_columns'],['props_json']);self.assertEqual(change['before'],before['nodes'][0]);self.assertEqual(change['after'],after['nodes'][0])
  self.assertEqual(result['edges'],{'added':[],'removed':[],'changed':[]})
 def test_wrong_digest_and_final_close_failure_withhold_report(self):
  root,configs,_,_=self.fixture();output=root/'refused.json';original_config=configs[0].read_bytes()
  value=json.loads(original_config);value['release_sha256']='0'*64;configs[0].write_text(json.dumps(value))
  with self.assertRaises(ValueError):run_compare_graph_exports_command(*configs,output)
  self.assertFalse(output.exists());configs[0].write_bytes(original_config)
  original=LocalGraphExportUse.renew;seen=set()
  def renewal(owner):
   original(owner)
   if owner._active:seen.add(owner.context)
   elif owner.context in seen:raise PermissionError('Final original file-use close refused')
  with patch.object(LocalGraphExportUse,'renew',renewal):
   with self.assertRaises(PermissionError):run_compare_graph_exports_command(*configs,output)
  self.assertFalse(output.exists());self.assertEqual(len(seen),2)

# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib,json,unittest
from unittest.mock import patch
import test_graph_export_diff as pairs
from ashlar_host.graph_export_setup import run_configure_graph_export_command
from ashlar_host.graph_export_setup import GraphExportReportFile

class Tests(unittest.TestCase):
 def fixture(self):
  helper=pairs.Tests();root,configs,_,_=helper.fixture();self.addCleanup(helper.doCleanups)
  selected=json.loads(configs[0].read_bytes());report=root/'export-report.json'
  raw=json.dumps({'release':{'path':selected['release_path'],'sha256':selected['release_sha256']},'custody':{'path':selected['custody_path'],'sha256':selected['custody_sha256']},'benign_padding':'x'*70000}).encode();report.write_bytes(raw)
  return root,report,hashlib.sha256(raw).hexdigest(),selected
 def test_exact_shared_selection_preserves_report_digests(self):
  root,report,pin,selected=self.fixture();output=root/'selection.json'
  run_configure_graph_export_command(str(report),pin,output)
  self.assertEqual(json.loads(output.read_bytes()),selected);self.assertEqual(output.stat().st_mode&0o777,0o600)
 def test_wrong_report_digest_and_final_report_close_withhold_output(self):
  root,report,pin,_=self.fixture();output=root/'selection.json'
  with self.assertRaises(ValueError):run_configure_graph_export_command(str(report),'0'*64,output)
  self.assertFalse(output.exists());renew=GraphExportReportFile.renew;calls=[]
  def close(owner):
   renew(owner);calls.append(owner)
   if len(calls)==2:raise PermissionError('Original report close refused')
  with patch.object(GraphExportReportFile,'renew',close):
   with self.assertRaises(PermissionError):run_configure_graph_export_command(str(report),pin,output)
  self.assertFalse(output.exists())

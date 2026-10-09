"""Structural trusted-host export receipts; fixture sessions confer no authority."""
import copy,hashlib,json,unittest
from ashlar.graph_release import GraphRelease
from private_graph_custody import PROFILE,build_private_custody,admit_private_custody
from run_graph_release_graphframes import load_release
from test_graph_release_graphframes import fixture,encoded as release_bytes
from test_protected_outbox_ack import original
from protected_outbox_ack import AckScope,receipt_bytes
from local_delta_custody import encoded

class Tests(unittest.TestCase):
 def fixture(self):
  request,manifest=original();source={'original':'source admission custody'};report=json.loads(manifest['validation_report_json']);report['source_admission']=source;manifest['validation_report_json']=encoded(report)
  value=fixture();value['publication']=manifest;tables=list(json.loads(manifest['table_versions_json']));value['roles']={'nodes':tables[0],'edges':tables[1]};value['snapshots']={tables[0]:{'uuid':'00000000-0000-0000-0000-000000000001','version':6},tables[1]:{'uuid':'00000000-0000-0000-0000-000000000002','version':2}}
  payload=release_bytes(value);release=GraphRelease(payload,hashlib.sha256(payload).hexdigest());scope=AckScope('ashlar_ack_test','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002','consumer','feed','epoch');req=encoded(request).encode();man=encoded(manifest).encode();_,_,checkpoint,protected=receipt_bytes(scope,req,man)
  observation={'position':checkpoint['position'],'request_hex':req.hex(),'manifest_hex':man.hex(),'receipt_hex':protected.hex()}
  session={'current_user':'operator','session_user':'operator','database':'local','server_address':None,'server_port':None,'backend_pid':'123','source_schema':'original_source','source_signature_sha256':'a'*64,'connection_route':'private-local-postgresql'}
  ack={'scope':scope.__dict__,'requestHex':req.hex(),'manifestHex':man.hex(),'opening':{'session':session,'observation':observation},'closing':{'session':copy.deepcopy(session),'observation':copy.deepcopy(observation)}}
  receipt=build_private_custody(release,source,ack);return release,receipt
 def load(self,release,receipt):return load_release(release.payload,release.sha256,custody_profile=PROFILE,custody_payload=receipt,trusted_custody_sha256=hashlib.sha256(receipt).hexdigest())
 def test_explicit_trusted_pair_preserves_every_original_manifest_string(self):
  release,receipt=self.fixture();value=self.load(release,receipt);self.assertEqual(value,json.loads(release.payload));self.assertNotIn('retention',json.loads(value['publication']['validation_report_json']))
  with self.assertRaises(ValueError):load_release(release.payload,release.sha256)
 def test_retained_original_ack_may_be_observed_under_a_later_head(self):
  release,receipt=self.fixture();value=json.loads(receipt)
  value['ack']['opening']['observation']['position']='2';value['ack']['closing']['observation']['position']='3'
  self.load(release,(encoded(value)+'\n').encode())
  for bad in [1,True,'01','-1',str(2**63),'99999999999999999999999']:
   altered=copy.deepcopy(value);altered['ack']['closing']['observation']['position']=bad
   with self.subTest(position=bad),self.assertRaises(ValueError):self.load(release,(encoded(altered)+'\n').encode())
 def test_partial_unknown_untrusted_or_duplicate_profile_refuses(self):
  release,receipt=self.fixture()
  for args in [{'custody_profile':PROFILE},{'custody_payload':receipt},{'custody_profile':'unknown','custody_payload':receipt,'trusted_custody_sha256':hashlib.sha256(receipt).hexdigest()},{'custody_profile':PROFILE,'custody_payload':receipt,'trusted_custody_sha256':'0'*64}]:
   with self.assertRaises(ValueError):load_release(release.payload,release.sha256,**args)
  changed=receipt.replace(b'"format":',b'"format":"duplicate","format":',1)
  with self.assertRaises(ValueError):self.load(release,changed)
 def test_changed_closed_custody_and_numeric_flags_are_refused(self):
  changes=[lambda x:x.update(extra='unknown'),lambda x:x.pop('ack'),lambda x:x.update(releaseSha256='0'*64),lambda x:x.update(manifestSha256='0'*64),lambda x:x.update(sourceAdmissionSha256='0'*64),lambda x:x['originalManifest'].update(recorded_at='changed'),lambda x:x['roles'].update(edges=x['roles']['nodes']),lambda x:x['snapshots'].pop(next(iter(x['snapshots']))),lambda x:next(iter(x['snapshots'].values())).update(uuid='changed'),lambda x:next(iter(x['snapshots'].values())).update(version=True),lambda x:x['ack']['scope'].update(epoch='changed'),lambda x:x['ack']['opening']['observation'].update(position='0'),lambda x:x['ack']['closing']['observation'].update(receipt_hex='00'),lambda x:x['ack']['closing']['session'].update(current_user='changed'),lambda x:x['ack']['opening']['session'].update(backend_pid=True),lambda x:x['interval'].update(wholeVectorHeld=False),lambda x:x['interval'].update(wholeVectorHeld=1),lambda x:x['interval'].update(closingSucceeded=False),lambda x:x['interval'].update(closingSucceeded=1)]
  for i,change in enumerate(changes):
   release,receipt=self.fixture();value=json.loads(receipt);change(value);changed=(encoded(value)+'\n').encode()
   with self.subTest(case=i),self.assertRaises(ValueError):self.load(release,changed)
 def test_present_retention_inventory_never_uses_private_fallback(self):
  for retention in [None,False,{}, {'targets':None},{'targets':{}},{'targets':{'future':'bad'}}]:
   release,receipt=self.fixture();value=json.loads(release.payload);report=json.loads(value['publication']['validation_report_json']);report['retention']=retention;value['publication']['validation_report_json']=encoded(report);raw=release_bytes(value);changed=GraphRelease(raw,hashlib.sha256(raw).hexdigest());custody=json.loads(receipt);custody['originalManifest']=value['publication'];custody['releaseSha256']=changed.sha256;custody['manifestSha256']=hashlib.sha256(encoded(value['publication']).encode()).hexdigest();payload=(encoded(custody)+'\n').encode()
   # A dict with targets absent is allowed only if its original ACK also matches;
   # this altered-original test still refuses exact ACK custody.
   with self.subTest(retention=retention),self.assertRaises(ValueError):self.load(changed,payload)
if __name__=='__main__':unittest.main()

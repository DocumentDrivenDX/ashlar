# SPDX-License-Identifier: MIT OR Apache-2.0
"""Named native page capacity; JSONL and retained artifact limits stay fixed."""
import base64,hashlib,json,unittest
from dataclasses import replace
from ashlar.truss_native_feed import native_feed_batch,MAX_NATIVE_PAGE_BYTES,NativeFeedError
from ashlar.staging import batch_row,batch_from_row,StagingError

class Tests(unittest.TestCase):
 def page(self,size):
  before={'profile':'truss-caller-asserted-feed/0.1','sourceEpoch':'epoch','installationId':'installation','consumerId':'consumer','registrationId':'registration','nextXid':'0'}
  after={**before,'nextXid':'43'}
  payload={'interfaceVersion':'truss-native-feed/0.1','prior':before,'next':after,'writerXid':'42',
   'configuration':{},'catalogPrerequisites':[],'revisions':[],'changes':[],'sources':[],'reservations':[],'unknownContent':''}
  encode=lambda x:json.dumps(x,separators=(',',':')).encode()
  payload['unknownContent']='x'*(size-len(encode(payload)));raw=encode(payload);self.assertEqual(len(raw),size)
  return raw,{'sha256':hashlib.sha256(raw).hexdigest(),'prior':encode(before),'next':encode(after),'feed':'selected'}
 def test_native_above_one_mib_roundtrip_preserves_all_unknown_bytes(self):
  raw,args=self.page(1048577);batch=native_feed_batch(raw,**args);row=batch_row(batch)
  self.assertEqual(batch_from_row(row),batch);self.assertEqual(batch_from_row(row).begin,raw)
  self.assertEqual(base64.b64decode(json.loads(row['batch_json'])['begin_base64']),raw)
 def test_jsonl_remains_one_mib_on_write_and_read(self):
  raw,args=self.page(1048577);batch=native_feed_batch(raw,**args)
  with self.assertRaises(StagingError):batch_row(replace(batch,profile='ashlar-jsonl-transactions/0.1'))
  row=batch_row(batch);row['source_profile']='ashlar-jsonl-transactions/0.1'
  with self.assertRaises(StagingError):batch_from_row(row)
 def test_oversized_native_refuses_at_source_write_and_retained_read(self):
  raw,args=self.page(MAX_NATIVE_PAGE_BYTES+1)
  with self.assertRaises(NativeFeedError):native_feed_batch(raw,**args)
  small,small_args=self.page(1024);batch=native_feed_batch(small,**small_args)
  with self.assertRaises(StagingError):batch_row(replace(batch,begin=raw))
  row=batch_row(batch);value=json.loads(row['batch_json']);value['begin_base64']=base64.b64encode(raw).decode()
  row['batch_json']=json.dumps(value,separators=(',',':'),sort_keys=True);row['batch_digest']=hashlib.sha256(row['batch_json'].encode()).hexdigest()
  with self.assertRaises(StagingError):batch_from_row(row)

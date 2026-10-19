# SPDX-License-Identifier: MIT OR Apache-2.0
import base64,hashlib,json,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.schema import SchemaIntake
from ashlar.semantic_policy import StringRecordPolicy
from ashlar_host.config import ProducerConfig
from ashlar_host.managed_record_admission import admit_managed_records,RECORD_CHECK_PIN

class ManagedRecordTests(unittest.TestCase):
    def test_original_receipt_inventory_and_closing_on_mismatch(self):
        source=b'{"id":"original"}';sha=hashlib.sha256(source).hexdigest()
        intake=SchemaIntake('original','1',source,b'artifact',sha,'x','a'*40,True)
        policy=object.__new__(StringRecordPolicy);policy.source_sha256=sha;policy.document_revision='1'
        batch=SimpleNamespace(records=[SimpleNamespace(delivery_id='d1',sha256='b'*64)])
        config=ProducerConfig(Path('/producer'),Path('/bun'),Path('/git'),30,1048576,1048576)
        change=SimpleNamespace(operation='upsert')
        selected={'identity':{'module':'original','element':'thing'},'values':[]}
        def transport(argv,**kwargs):
            request=Path(argv[-2]).read_bytes()
            self.assertEqual(argv[-1],'/git')
            result={'format':'ashlar-umf-record-check/0.1','producerRevision':RECORD_CHECK_PIN,'sourceSha256':sha,'sourceBase64':base64.b64encode(source).decode(),'requestSha256':hashlib.sha256(request).hexdigest(),'records':[{'deliveryId':'d1','recordSha256':'b'*64,'result':{'validation':{'valid':True,'complete':True}}}]}
            if bad[0]=='cancel':raise cancellation
            if bad[0]:result['records'][0]['deliveryId']='foreign'
            return json.dumps(result).encode(),b''
        bad=[False];cancellation=KeyboardInterrupt()
        with patch('ashlar_host.managed_record_admission._source',return_value=('held',))as custody,patch.object(SchemaIntake,'read',return_value=intake),patch.object(StringRecordPolicy,'record_value_request',return_value=selected),patch('ashlar_host.managed_record_admission.changes_from_batch',return_value=[change]),patch('ashlar_host.managed_record_admission.capture',side_effect=transport):
            self.assertEqual(len(admit_managed_records(config,intake,policy,(batch,))),1)
            bad[0]=True
            with self.assertRaises(ValueError):admit_managed_records(config,intake,policy,(batch,))
            bad[0]='cancel'
            with self.assertRaises(KeyboardInterrupt)as raised:admit_managed_records(config,intake,policy,(batch,))
            self.assertIs(raised.exception,cancellation)
            self.assertEqual(custody.call_count,6)

# SPDX-License-Identifier: MIT OR Apache-2.0
"""Configured read composition with actual original intake, inert native acquisition."""
from contextlib import contextmanager
from dataclasses import replace
import hashlib,json,unittest
from pathlib import Path
from unittest.mock import patch
import test_original_medical_local_publication as publications
import ashlar_host.original_medical_local_read as owner
from ashlar.medical_source import build_transaction,SOURCE_SHA
from ashlar.outbox import OutboxTransaction
from ashlar_host.medical_publication_plan import STREAM,PREDECESSOR,FEED
from ashlar_host.publication_planning import request_for
from ashlar_host.original_medical_local_configuration import OriginalMedicalReadConfig,MedicalPinnedFile

class Tests(unittest.TestCase):
    def configured(self):
        fixture=publications.Tests();config,selection,capture=fixture.installation();self.addCleanup(fixture.doCleanups)
        batch,_=build_transaction(*(p.path.read_bytes()for p in (config.inputs.model,config.inputs.graph,config.inputs.baseline)),source_system=FEED)
        payload=batch.begin+b''.join(r.raw for r in batch.records)+batch.commit
        tr=OutboxTransaction('ashlar-postgresql-outbox/0.1',batch.feed,batch.epoch,'0','1',hashlib.sha256(payload).hexdigest(),batch)
        request=request_for(STREAM,tr,PREDECESSOR,{FEED:SOURCE_SHA})
        def pin(name,value):
            raw=(owner.encoded(value)+'\n').encode();path=config.output.parent/name;path.write_bytes(raw);path.chmod(0o600);return MedicalPinnedFile(path,hashlib.sha256(raw).hexdigest(),len(raw))
        values=dict(vars(config));values['profile']='ashlar-original-medical-local-read/0.1'
        result=OriginalMedicalReadConfig(**values,request=pin('read-request.json',request),publication=pin('read-publication.json',{'publication_id':'original-medical-publication-1'}))
        result.installation.path.chmod(0o600)
        return result,selection,capture
    def test_nominal_public_read_refuses_before_resources(self):
        with patch.object(owner,'OriginalMedicalRuntimeCustody')as runtime:
            with self.assertRaises(ValueError):owner.read_original_medical_local(object())
            runtime.open.assert_not_called()
    def test_missing_journal_never_initializes_repairs_or_acquires_native(self):
        config,selection,capture=self.configured();config.journal.unlink()
        with patch.object(owner,'OriginalMedicalRuntimeCustody')as runtime:
            with self.assertRaises(Exception):owner.read_original_medical_local(config)
            runtime.open.assert_not_called()
        self.assertFalse(config.journal.exists());self.assertFalse((config.output/'receipt.json').exists())
    def test_actual_intake_native_open_cancel_has_no_source_transport_or_receipt(self):
        config,selection,capture=self.configured();before=config.journal.read_bytes();primary=KeyboardInterrupt('native acquisition')
        @contextmanager
        def native(*args,**kwargs):raise primary;yield
        with patch('ashlar_host.medical_producer.capture',side_effect=capture),patch.object(owner,'original_medical_native_session',side_effect=native),patch.object(owner,'original_medical_local_read_sources')as sources,patch.object(owner.ReadOnlyTransport,'open')as transport:
            with self.assertRaises(KeyboardInterrupt)as caught:owner.read_original_medical_local(config)
        self.assertIs(caught.exception,primary);sources.assert_not_called();transport.assert_not_called()
        self.assertEqual(config.journal.read_bytes(),before);self.assertFalse((config.output/'receipt.json').exists())
        self.assertTrue((config.output/'public-revalidation.json').exists())

if __name__=='__main__':unittest.main()

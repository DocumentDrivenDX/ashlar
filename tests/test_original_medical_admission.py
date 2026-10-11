"""Original medical public-operation seams and independent full-row oracle controls."""
import base64
import decimal
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ashlar.medical_source import build_transaction,UMF_PIN
from ashlar.whole_entity import changes_from_batch
from ashlar_host.config import ProducerConfig
from ashlar_host.medical_producer import HeldMedicalDataset,MedicalFile,verify_medical_revalidation,same_medical_json
from ashlar_host.original_medical_publication import MedicalAdmission,MedicalAdmissionEvidence,original_medical_oracle
from ashlar_host.schema_rows import fixture_columns
from ashlar_host.resources import RESOURCE_ROOT

ROOT=Path(__file__).resolve().parents[1]

class MedicalAdmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model=(ROOT/'examples/domain-packs/medical/historical/archive/schemas/ontology.json').read_bytes()
        cls.graph=(ROOT/'examples/domain-packs/medical/upstream/graph/fixture.json').read_bytes()
        cls.baseline=(ROOT/'docs/helix/04-build/evidence/medical-historical-admission-20261009/public-receipt.json').read_bytes()
        cls.public=json.loads(cls.baseline)
        cls.batch,cls.bindings=build_transaction(cls.model,cls.graph,cls.baseline,source_system='isolated-medical')
        cls.columns=fixture_columns(RESOURCE_ROOT)
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve()
        self.paths=tuple(self.root/name for name in ('model.json','graph.json','baseline.json'))
        for p,raw in zip(self.paths,(self.model,self.graph,self.baseline)):p.write_bytes(raw)
        self.cfg=ProducerConfig(self.root/'umf',self.root/'bun',self.root/'git',2,1048576,4194304)
        self.calls=[]
    def observed(self):
        return json.dumps({'profile':'ashlar-original-medical-public-revalidation/0.1','umfRevision':UMF_PIN,'modelSha256':hashlib.sha256(self.model).hexdigest(),'graphSha256':hashlib.sha256(self.graph).hexdigest(),'baselineSha256':hashlib.sha256(self.baseline).hexdigest(),'receipt':self.public['receipt'],'booleanReceipts':self.public['booleanReceipts']},ensure_ascii=False,separators=(',',':')).encode()
    def capture(self,argv,**kwargs):
        self.calls.append((argv,kwargs))
        if argv[0]==str(self.cfg.git):return ((UMF_PIN+'\n').encode()if argv[-1]=='HEAD'else b''),b''
        Path(argv[3]).write_bytes(self.observed());return b'original public output\n',b''
    def held(self):
        with patch('ashlar_host.medical_producer.capture',side_effect=self.capture):return HeldMedicalDataset(self.cfg,*self.paths,self.root/'actual.json')
    def admission(self):
        held=self.held()
        with patch('ashlar_host.medical_producer.capture',side_effect=self.capture):return MedicalAdmission(self.batch,self.bindings,*self.paths,held)
    def test_public_producer_preserves_all_raw_and_original_declared_inputs(self):
        held=self.held();argv,kwargs=next((a,k)for a,k in self.calls if a[0]==str(self.cfg.bun))
        self.assertEqual(argv[4:7],list(map(str,self.paths)));self.assertNotIn('PGPASSWORD',kwargs['environment']);self.assertNotIn('HOME',kwargs['environment'])
        with patch('ashlar_host.medical_producer.capture',side_effect=self.capture):
            self.assertEqual(held(*held.originals),self.observed());meta=held.metadata()
        self.assertEqual(meta['actual_revalidation']['receipt']['residuals'],self.public['receipt']['residuals']);self.assertEqual(meta['actual_revalidation']['booleanReceipts'],self.public['booleanReceipts'])
        self.assertEqual(held.originals,(self.model,self.graph,self.baseline))
    def test_admission_full113_and_original_unknown_warning_custody(self):
        owner=self.admission()
        with patch('ashlar_host.medical_producer.capture',side_effect=self.capture):
            meta=owner.metadata();evidence=owner.evidence()
            for change in changes_from_batch(self.batch):owner.admit(change)
        self.assertEqual(len(owner.changes),113);self.assertEqual((evidence.model,evidence.graph,evidence.baseline),(self.model,self.graph,self.baseline));self.assertEqual(evidence.revalidation,self.observed())
        self.assertEqual(json.loads(evidence.baseline)['booleanReceipts'],self.public['booleanReceipts']);self.assertEqual(json.loads(evidence.revalidation)['receipt']['provenance'],self.public['receipt']['provenance'])
        self.assertEqual(meta['record_count'],51);self.assertLess(len(json.dumps(meta,separators=(',',':')).encode()),1048576)
        for item,raw in zip(meta['evidence']['files'],(evidence.model,evidence.graph,evidence.baseline,evidence.bindings,evidence.revalidation)):
            self.assertEqual(item['bytes'],len(raw));self.assertEqual(item['sha256'],hashlib.sha256(raw).hexdigest())
    def test_aggregate_original_inputs_refuse_before_read_or_public_call(self):
        for path in self.paths:path.write_bytes(b'x'*1500000)
        with patch.object(MedicalFile,'read')as read,patch('ashlar_host.medical_producer.capture')as capture,self.assertRaises(ValueError):HeldMedicalDataset(self.cfg,*self.paths,self.root/'actual.json')
        read.assert_not_called();capture.assert_not_called()

    def test_changed_original_refuses_before_public_call(self):
        self.paths[0].write_bytes(self.model+b' ')
        with patch('ashlar_host.medical_producer.capture')as capture,self.assertRaises(PermissionError):HeldMedicalDataset(self.cfg,*self.paths,self.root/'actual.json')
        capture.assert_not_called()
    def test_incomplete_changed_input_boolean_endpoint_result_refuses(self):
        for mutate in (lambda v:v['receipt']['datasetValidation'].update(complete=False),lambda v:v['receipt']['input']['scope'].update(id='other'),lambda v:v['receipt']['relationships'][0].update(targetInstanceId='other'),lambda v:v['booleanReceipts'][0]['request'].update(token='False'),lambda v:v['receipt']['records'].pop()):
            value=json.loads(self.observed());mutate(value)
            with self.assertRaises(PermissionError):verify_medical_revalidation(self.model,self.graph,self.baseline,json.dumps(value).encode())
    def test_provenance_difference_retained_without_raw_equality_grant(self):
        value=json.loads(self.observed());value['receipt']['provenance']={'current':'separately emitted observation'}
        raw=json.dumps(value).encode();result=verify_medical_revalidation(self.model,self.graph,self.baseline,raw)
        self.assertEqual(result['receipt']['provenance'],value['receipt']['provenance'])
    def test_selected_revision_or_dirty_source_refuses(self):
        for response in (b'other\n',b' M original\n'):
            def fake(argv,**kwargs):return (response if response.startswith(b'other')or argv[-1]=='--porcelain'else(UMF_PIN+'\n').encode()),b''
            with patch('ashlar_host.medical_producer.capture',side_effect=fake),self.assertRaises(PermissionError):HeldMedicalDataset(self.cfg,*self.paths,self.root/'actual.json')
    def test_wrong_typed_producer_and_batch_bindings_refuse(self):
        with self.assertRaises(ValueError):MedicalAdmission(self.batch,self.bindings,*self.paths,lambda *a:self.observed())
        held=self.held();changed=json.loads(json.dumps(self.bindings));changed['entities'][0]['id']='999'
        with patch('ashlar_host.medical_producer.capture',side_effect=self.capture),self.assertRaises(PermissionError):MedicalAdmission(self.batch,changed,*self.paths,held)
    def test_named_substitution_hardlink_and_receipt_drift_refuse(self):
        f=MedicalFile(self.paths[0],4194304);self.paths[0].rename(self.root/'old');self.paths[0].write_bytes(self.model)
        with self.assertRaises(ValueError):f.read()
        alias=self.root/'alias';os.link(self.paths[1],alias)
        with self.assertRaises(ValueError):MedicalFile(alias,4194304)
        alias.unlink();held=self.held();held.output.write_bytes(self.observed()[:-1]+b' ')
        with patch('ashlar_host.medical_producer.capture',side_effect=self.capture),self.assertRaises(PermissionError):held.renew()
    def test_primary_cancel_and_independent_closing_inputs_attempted(self):
        primary=KeyboardInterrupt('private sentinel');observed=[]
        def fake(argv,**kwargs):
            if argv[0]==str(self.cfg.bun):raise primary
            return self.capture(argv,**kwargs)
        def closing(owner):observed.append(True);raise OSError('secondary')
        with patch('ashlar_host.medical_producer.capture',side_effect=fake),patch.object(HeldMedicalDataset,'_inputs',closing),self.assertRaises(KeyboardInterrupt)as error:HeldMedicalDataset(self.cfg,*self.paths,self.root/'actual.json')
        self.assertIs(error.exception,primary);self.assertEqual(observed,[True]);self.assertFalse((self.root/'actual.json').exists())
    def test_real_verifier_refuses_boolean_numeric_carrier_aliases(self):
        for mutate in (lambda v:v['booleanReceipts'][0]['value'].update(boolean=1),lambda v:v['receipt']['records'][0]['result']['validation'].update(valid=1)):
            value=json.loads(self.observed());mutate(value)
            with self.assertRaises(PermissionError):verify_medical_revalidation(self.model,self.graph,self.baseline,json.dumps(value).encode())
        self.assertFalse(same_medical_json({'number':1},{'number':True}))
        self.assertFalse(same_medical_json(decimal.Decimal('1'),1))
        self.assertFalse(same_medical_json(decimal.Decimal('1.0'),decimal.Decimal('1.00')))
    def test_opening_pin_cancel_closes_every_acquired_input_script_and_parent(self):
        primary=KeyboardInterrupt('private opening');calls=[];pins=[];read=MedicalFile.read
        def pin(owner):
            pins.append(True)
            if len(pins)==1:raise primary
            raise OSError('secondary pin')
        def observed(file):calls.append(file.path);return read(file)
        with patch.object(HeldMedicalDataset,'_pin',pin),patch.object(MedicalFile,'read',observed),self.assertRaises(KeyboardInterrupt)as error:
            HeldMedicalDataset(self.cfg,*self.paths,self.root/'actual.json')
        self.assertIs(error.exception,primary);self.assertEqual(len(pins),2)
        self.assertEqual(calls,list(self.paths)+[RESOURCE_ROOT/'check_medical_dataset.ts']+list(self.paths)+[RESOURCE_ROOT/'check_medical_dataset.ts'])
    def test_input_closing_failure_still_attempts_parent_other_inputs_and_script(self):
        held=self.held();calls=[];read=MedicalFile.read;primary=KeyboardInterrupt('input close')
        held.parent_identity=(0,0,0,0)
        def observed(file):
            calls.append(file.path)
            if file.path==self.paths[0]:raise primary
            return read(file)
        with patch.object(MedicalFile,'read',observed),self.assertRaises(KeyboardInterrupt)as error:held._inputs()
        self.assertIs(error.exception,primary);self.assertEqual(calls,list(self.paths)+[RESOURCE_ROOT/'check_medical_dataset.ts'])
    def test_admission_input_failure_still_attempts_all_inputs_and_producer(self):
        owner=self.admission();calls=[];read=MedicalFile.read;primary=KeyboardInterrupt('admission close')
        def observed(file):
            calls.append(file.path)
            if file.path==self.paths[0]:raise primary
            return read(file)
        def producer(_):calls.append('producer');raise OSError('secondary producer')
        with patch.object(MedicalFile,'read',observed),patch.object(HeldMedicalDataset,'renew',producer),self.assertRaises(KeyboardInterrupt)as error:owner.renew()
        self.assertIs(error.exception,primary);self.assertEqual(calls,list(self.paths)+['producer'])
    def test_raw_evidence_closed_carriers_and_aggregate_limit(self):
        with self.assertRaises(ValueError):MedicalAdmissionEvidence(b'a',b'b',b'c',b'd','foreign')
        with self.assertRaises(ValueError):MedicalAdmissionEvidence(b'a',b'b',b'',b'd',b'e')
        with self.assertRaises(ValueError):MedicalAdmissionEvidence(b'x'*12582912,b'b',b'c',b'd',b'e')
    def oracle(self,**kwargs):
        return original_medical_oracle(self.model,self.graph,self.bindings,feed=self.batch.feed,epoch=self.batch.epoch,batch_id=self.batch.batch_id,materialized_at='2026-10-09T12:00:00.123456+00:00',columns=self.columns,**kwargs)
    def test_independent_full_bags_all_fields_history_raw_and_microseconds(self):
        bags=self.oracle();self.assertEqual({k:len(v)for k,v in bags.items()},{'object_current':51,'edge_current':62,'tombstone':0,'whole_source_history':113})
        expected_raw=[record.raw for record in self.batch.records]
        self.assertEqual([base64.b64decode(row['raw_base64'])for row in bags['whole_source_history']],expected_raw)
        for role in ('object_current','edge_current'):
            self.assertTrue(all(set(row)=={name for name,_ in self.columns[role]}for row in bags[role]));self.assertTrue(all(row['source_cursor_json']==json.dumps({'profile':self.batch.profile,'offset':self.batch.cursor_after},separators=(',',':'))for row in bags[role]));self.assertTrue(all(row['published_at'].endswith('123456')for row in bags[role]))
        self.assertEqual(len({(r['rel_type_id'],r['id'])for r in bags['edge_current']}),62)
        for expected,row in zip(changes_from_batch(self.batch),bags['whole_source_history']):
            actual=json.loads(row['change_json']);self.assertEqual(actual['state']['props_json'],expected.state.props_json);self.assertEqual(actual['state']['retained_json'],expected.state.retained_json);self.assertEqual(row['digest'],expected.raw_digest)
    def test_oracle_wrong_bindings_roles_timestamp_and_original_refuse(self):
        changed=json.loads(json.dumps(self.bindings));changed['properties'][0]['property_id']=changed['properties'][1]['property_id']
        with self.assertRaises(ValueError):original_medical_oracle(self.model,self.graph,changed,feed=self.batch.feed,epoch=self.batch.epoch,batch_id=self.batch.batch_id,materialized_at='2026-10-09T12:00:00+00:00',columns=self.columns)
        with self.assertRaises(ValueError):original_medical_oracle(self.model,self.graph,self.bindings,feed=self.batch.feed,epoch=self.batch.epoch,batch_id=self.batch.batch_id,materialized_at='2026-10-09T12:00:00',columns=self.columns)
        with self.assertRaises(ValueError):original_medical_oracle(self.model+b' ',self.graph,self.bindings,feed=self.batch.feed,epoch=self.batch.epoch,batch_id=self.batch.batch_id,materialized_at='2026-10-09T12:00:00+00:00',columns=self.columns)

if __name__=='__main__':unittest.main()

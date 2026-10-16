"""Write-ahead medical plans; actual journal, explicit inert native ports."""
from contextlib import ExitStack
import json,unittest
from unittest.mock import patch
import test_medical_effect_proof as proofs
from ashlar_host.delta_custody import encoded
from ashlar_host.original_medical_local_publication import _MedicalPreparedBackend
from ashlar.stored_publisher import StoredPublisherBackend

class Tests(unittest.TestCase):
    def fixture(self,stack):
        helper=proofs.Tests();self.addCleanup(helper.doCleanups)
        f,d,b,v,a,ids,source,native=helper.fixture(stack)
        for table in ('local_operation','local_plan','local_source_plan'):d.transport.db.execute('DELETE FROM '+table)
        d.transport.db.commit();native.clear()
        stack.enter_context(patch.object(d.transport,'mutation',side_effect=AssertionError('Preparation cannot submit native effects')))
        return d,b,v,ids,native
    def prepare(self,d,b):
        with d.writer('original',d.context),d.transport.bounded_observations(b):
            return d.prepare_original_medical_apply(d._original_medical_publication_binding['plan'],context=d.context,bounds=b)
    def test_original_plans_are_durable_without_any_submitted_ordinal(self):
        with ExitStack()as stack:
            d,b,v,ids,native=self.fixture(stack)
            self.assertIsNone(self.prepare(d,b));before=list(d.transport.db.iterdump())
            self.assertEqual(d.transport.db.execute('SELECT count(*) FROM local_source_plan').fetchone()[0],1)
            self.assertEqual(d.transport.db.execute('SELECT count(*) FROM local_plan').fetchone()[0],1)
            self.assertEqual(d.transport.db.execute('SELECT count(*) FROM local_operation').fetchone()[0],0)
            self.prepare(d,b);self.assertEqual(before,list(d.transport.db.iterdump()))
    def test_original_context_plan_and_bounds_are_required_before_retention(self):
        with ExitStack()as stack:
            d,b,v,ids,native=self.fixture(stack);plan=d._original_medical_publication_binding['plan']
            with d.writer('original',d.context),d.transport.bounded_observations(b):
                for p,context,bounds in [(object(),d.context,b),(plan,object(),b),(plan,d.context,object())]:
                    with self.assertRaises(PermissionError):d.prepare_original_medical_apply(p,context=context,bounds=bounds)
            self.assertEqual(d.transport.db.execute('SELECT count(*) FROM local_source_plan').fetchone()[0],0)
    def test_native_prior_change_and_started_ordinal_refuse(self):
        with ExitStack()as stack:
            d,b,v,ids,native=self.fixture(stack);self.prepare(d,b)
            target=d.transport.targets[d.tables['object_current']];native[target.table]=[{'version':3},{'version':1}]
            with self.assertRaisesRegex(Exception,'native prior changed'):self.prepare(d,b)
        with ExitStack()as stack:
            d,b,v,ids,native=self.fixture(stack);self.prepare(d,b)
            d.transport.db.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,?)',(ids[0],'{}','x',1,'submitted',None));d.transport.db.execute('DELETE FROM local_source_plan');d.transport.db.commit()
            with self.assertRaisesRegex(Exception,'custody missing or already started'):self.prepare(d,b)
            self.assertEqual(d.transport.db.execute('SELECT count(*) FROM local_source_plan').fetchone()[0],0)
    def test_backend_prepares_before_phase_append_and_preserves_cancellation(self):
        class Driver:
            def prepare_original_apply(self,request,context):events.append('plans')
        events=[];backend=_MedicalPreparedBackend(None,Driver(),None,context=object())
        with patch.object(backend,'observe',side_effect=lambda *a:events.append('observe')),patch.object(StoredPublisherBackend,'prepare',side_effect=lambda *a:events.append('prepared')):
            backend.prepare('s',{'batch_id':'b'});self.assertEqual(events,['observe','plans','prepared'])
            primary=KeyboardInterrupt()
            with patch.object(backend.driver,'prepare_original_apply',side_effect=primary),patch.object(StoredPublisherBackend,'prepare')as append:
                with self.assertRaises(KeyboardInterrupt)as caught:backend.prepare('s',{'batch_id':'b'})
                self.assertIs(caught.exception,primary);append.assert_not_called()

    def test_existing_attempt_never_reconstructs_missing_preparation(self):
        class Driver:
            def prepare_original_apply(self,*args):raise AssertionError('Existing attempt cannot create plans')
        backend=_MedicalPreparedBackend(None,Driver(),None,context=object())
        with patch.object(backend,'observe',return_value=object()),patch.object(StoredPublisherBackend,'prepare')as append:
            with self.assertRaisesRegex(Exception,'cannot be prepared again'):backend.prepare('s',{'batch_id':'b'})
            append.assert_not_called()

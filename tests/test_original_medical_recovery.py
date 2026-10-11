"""Closed recovery selections/shared phases; inert lower ports, no native claim."""
import copy
from contextlib import contextmanager, ExitStack
import hashlib
import json
from dataclasses import replace, fields
import unittest
from unittest.mock import patch
import test_original_medical_local_configuration as selections
import test_stored_publisher as phases
import test_original_medical_local_publication as composition
import test_medical_publication_sources as source_owners
from ashlar_host.original_medical_local_configuration import OriginalMedicalRecoveryConfig, parse_original_medical_local
from ashlar_host.original_medical_installation_handoff import validate_original_medical_installation_handoff
import test_original_medical_installation_handoff as handoffs
import ashlar_host.original_medical_local_publication as owner

class Tests(unittest.TestCase):
    def setUp(self):
        self.fixture=selections.OriginalMedicalConfigurationTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
    def selection(self):
        value=self.fixture.configuration('publication');value['profile']='ashlar-original-medical-local-recovery/0.1';value['request']=self.fixture.pin('retained/request.json');return value
    def test_closed_recovery_type_retains_request_without_publication_or_effects(self):
        with patch('os.open')as opened:
            selected=self.fixture.parse(self.selection())
        self.assertIs(type(selected),OriginalMedicalRecoveryConfig);self.assertFalse(hasattr(selected,'publication'));opened.assert_not_called()
    def test_same_complete_handoff_admits_exact_recovery_without_invented_publication(self):
        fixture=handoffs.Tests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        raw,publication=fixture.selected()
        values={field.name:getattr(publication,field.name)for field in fields(publication)}
        values['profile']='ashlar-original-medical-local-recovery/0.1'
        values['request']=publication.installation.__class__(publication.installation.path.parent/'request.json','b'*64,382102)
        recovery=OriginalMedicalRecoveryConfig(**values)
        selected=validate_original_medical_installation_handoff(raw,recovery)
        self.assertEqual(selected.installation_id,recovery.installation_id)
        self.assertEqual(len(selected.targets),6)
        with self.assertRaises(ValueError):validate_original_medical_installation_handoff(raw,object())

    def test_missing_added_and_aliased_originals_refuse(self):
        value=self.selection()
        for change in ('missing','publication','unknown','alias','same-installation'):
            v=copy.deepcopy(value)
            if change=='missing':v.pop('request')
            elif change=='publication':v['publication']=self.fixture.pin('invented.json')
            elif change=='unknown':v['resume_latest']=True
            elif change=='alias':v['request']['path']=str(self.fixture.root/'x/../request.json')
            else:v['request']=v['installation']
            with self.subTest(change=change),self.assertRaises(ValueError):self.fixture.parse(v)
    def test_distinct_public_entry_selects_recovery_not_replay_or_fresh(self):
        config=self.fixture.parse(self.selection());phase=object()
        with patch.object(owner,'_operate',return_value='result')as operate:
            self.assertEqual(owner.recover_original_medical_local(config,supervision=phase),'result')
        operate.assert_called_once_with(config,False,phase,recovery=True)
        with patch.object(owner._Output,'acquire')as acquire:
            with self.assertRaises(ValueError):owner.publish_original_medical_local(config)
            with self.assertRaises(ValueError):owner.replay_original_medical_local(config)
        acquire.assert_not_called()
    def test_actual_recovery_runtime_lease_open_cancel_never_reaches_source_or_receipt(self):
        fixture=composition.Tests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        publication,selection,capture=fixture.installation()
        path=publication.output.parent/'original-request.json';raw=b'original';path.write_bytes(raw)
        values={field.name:getattr(publication,field.name)for field in fields(publication)}
        values.update(profile='ashlar-original-medical-local-recovery/0.1',
            request=publication.installation.__class__(path,hashlib.sha256(raw).hexdigest(),len(raw)))
        config=OriginalMedicalRecoveryConfig(**values);primary=KeyboardInterrupt()
        @contextmanager
        def native(custody):
            raise primary
            yield
        with patch('ashlar_host.medical_producer.capture',side_effect=capture),patch.object(owner,'original_medical_native_session',side_effect=native),patch.object(owner,'original_medical_local_source_ports')as source:
            with self.assertRaises(KeyboardInterrupt)as caught:owner.recover_original_medical_local(config)
        self.assertIs(caught.exception,primary);source.assert_not_called()
        self.assertFalse((config.output/'receipt.json').exists())
        with owner.OriginalMedicalInstallationLease.open(selection,object()):pass

    def test_actual_original_source_route_recovery_never_appends_or_commits(self):
        fixture=source_owners.Tests();fixture.setUp();self.addCleanup(fixture.doCleanups);fixture.head=1
        with ExitStack()as stack:
            source,_=fixture.owner(stack)
            stack.enter_context(patch.object(source,'append_original',side_effect=AssertionError('Recovery never appends')))
            transaction=owner._original_transaction(source,fixture.batch,context=fixture.context,existing=True)
            self.assertEqual(transaction.position,'1');self.assertEqual(transaction.batch,fixture.batch)
        self.assertFalse(any(c.role=='medical_writer'for c in fixture.connections))
        self.assertEqual(sum(c.commits for c in fixture.connections),0)
        self.assertTrue(all(c.closes==1 for c in fixture.connections))

    def test_shared_original_apply_recovery_retains_request_and_descriptor(self):
        fixture=phases.StoredPublisherTests()
        with fixture.fixture()as (store,driver,policy,transport,api):
            backend=fixture.backend(store,driver,transport,policy);context=object();driver.fail='apply'
            with self.assertRaises(RuntimeError):fixture.publish(backend,context)
            original=list(store.rows);driver.fail=None
            descriptor=fixture.publish(backend,context)
            self.assertEqual(store.rows[:len(original)],original)
            self.assertIn('recover-apply',driver.calls)
            self.assertIsNotNone(descriptor)

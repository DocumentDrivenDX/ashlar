# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual core/nominal host guards with inert native/ordinary session fixtures."""
from contextlib import contextmanager
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import test_evolution_verification as verification
from ashlar_host.evolution_singleton import EvolutionSingletonSelection, read_evolution_singletons, _BorrowedPins, _SingletonPolicy
from ashlar_host.evolution_run import EvolutionRunLedgerView
from ashlar_host.evolution_operator import ExistingEvolutionLease
from ashlar_host.evolution_composition import NativeEvolutionRunPolicy
from ashlar_host.evolution_verification_types import VerifyEvolutionInvocation, VerifyCommerceEvolutionConfig, VerificationSupervision
from ashlar_host.config import EvolutionAdmissionConfig
from ashlar_host.delta_custody import LocalDeltaTransport
from ashlar.pins import PinVector


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):verification.DriverTests.setUpClass()
    def setUp(self):
        self.f=verification.DriverTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.f.complete(0);self.f.complete(1)
        self.driver=self.f.driver;self.context=self.driver.context
        self.inventory=EvolutionRunLedgerView(tuple(self.f.slots))
        root=self.driver.transport.journal_path.parent
        producer=EvolutionAdmissionConfig(root,root/'bun',root/'git',1,1024,4194304)
        self.invocation=VerifyEvolutionInvocation(root/'run.sqlite',root/'answer.json',producer,self.f.original.sha256,1,self.f.bounds,
            VerificationSupervision(600,self.f.bounds.capture_bytes,'operator-owned-process-supervisor/0.1'))
    @contextmanager
    def held(self):
        with LocalDeltaTransport.bounded_observations(self.driver.transport,self.f.bounds):
            with self.driver.hold_evolution_publication(self.f.original,self.inventory,1,context=self.context,policy=self.f.policy,bounds=self.f.bounds)as held:
                yield held
    def test_actual_minted_borrowed_pin_fullvector_context_and_expiry(self):
        with self.held()as held:
            self.assertEqual(len(held.vector.targets),4)
            pins=_BorrowedPins(held)
            with pins.hold(held.vector,context=self.context):held.require(self.context,held.vector)
            with self.assertRaises(PermissionError):
                with pins.hold(held.vector,context=object()):self.fail('foreign context entered')
            target=dict(held.vector.targets);target.pop(next(iter(target)))
            partial=PinVector(held.vector.authority,held.vector.scope_kind,held.vector.scope_id,held.vector.custody_digest,target)
            with self.assertRaises(PermissionError):
                with pins.hold(partial,context=self.context):self.fail('partial vector entered')
            original=self.driver._held_original_vector;self.driver._held_original_vector=SimpleNamespace(__eq__=lambda value:True)
            with self.assertRaises(PermissionError):held.require(self.context,original)
            self.driver._held_original_vector=original
            table=next(iter(held.vector.targets));target=self.driver.transport.targets[table]
            self.driver.transport.targets[table]=SimpleNamespace(uuid='foreign')
            with self.assertRaises(PermissionError):held.require(self.context,original)
            self.driver.transport.targets[table]=target
        with self.assertRaises(PermissionError):held.require(self.context,held.vector)
        self.assertFalse(self.driver.pin_held)
    def test_complete_sourcequalified_rows_absence_and_foreign_row(self):
        with self.held()as held:
            rows=held.facts['expected']['object_current'];a=next(r for r in rows if r['source_system']=='A');matches=[r for r in rows if(r['type_id'],r['id'])==(a['type_id'],a['id'])]
            self.assertEqual({r['source_system']for r in matches},{'A','B'})
            selection=EvolutionSingletonSelection(self.invocation,'object',int(a['type_id']),int(a['id']))
            desc=SimpleNamespace(raw=held.descriptor);table=self.driver.tables['object_current']
            for row in matches:_SingletonPolicy(held,selection,row['source_system']).authorize_row(desc,table,row,self.context)
            with self.assertRaises(PermissionError):_SingletonPolicy(held,selection,'A').authorize_row(desc,table,next(r for r in matches if r['source_system']=='B'),self.context)
            with self.assertRaises(PermissionError):_SingletonPolicy(held,selection,'A').authorize_row(desc,table,None,self.context)
            missing=EvolutionSingletonSelection(self.invocation,'object',int(a['type_id']),-(2**63))
            _SingletonPolicy(held,missing,'A').authorize_row(desc,table,None,self.context)
    def run_public(self,closing_error=None,body_error=None):
        # Exact nominal lease class, inert original authority methods only. No OS/PG authority claim.
        old=self.f.policy.authority;lease=object.__new__(ExistingEvolutionLease);lease.context=self.context;lease.invocation=self.invocation
        for name in ('admit_run','admit_ledger','admit_attempt'):setattr(lease,name,getattr(old,name))
        lease.inspect_ledger=lambda original,context:self.inventory
        policy=NativeEvolutionRunPolicy(self.driver,lease)
        cfg=VerifyCommerceEvolutionConfig(self.driver,self.invocation.ledger_path,policy,self.invocation.producer,self.context,
            self.f.original.sha256,1,self.f.bounds,self.invocation.supervision,self.f.original)
        events=[];self.last_events=events
        @contextmanager
        def provider(invocation):
            self.assertIs(invocation,self.invocation)
            try:
                with LocalDeltaTransport.bounded_observations(self.driver.transport,self.f.bounds):yield cfg
            finally:
                events.append('provider-closing')
                if closing_error is not None:raise closing_error
        def read(executor,backend,pins,vector,row_policy,**kw):
            with pins.hold(vector,context=kw['context']):
                held=row_policy.held;row=next(r for r in held.facts['expected']['object_current']if
                    (r['source_system'],r['type_id'],r['id'])==(kw['source'],str(kw['type_id']),str(kw['entity_id'])))
                row_policy.authorize_row(SimpleNamespace(raw=held.descriptor),kw['table'],row,kw['context'])
                events.append(('row',kw['source']))
                if body_error is not None:raise body_error
                return row
        self.driver.held=False
        from ashlar_host.evolution_composition import OriginalRunCorrespondence
        rows=OriginalRunCorrespondence(self.f.original,self.driver.source_admission).at(1)['expected']['object_current'];a=next(r for r in rows if r['source_system']=='A')
        selection=EvolutionSingletonSelection(self.invocation,'object',int(a['type_id']),int(a['id']))
        with patch('ashlar_host.evolution_singleton.read_singleton',side_effect=read):
            value=read_evolution_singletons(selection,SimpleNamespace(open_verification=provider))
        self.assertEqual(events[-1],'provider-closing');return json.loads(value)
    def test_public_release_follows_provider_closing_and_missingack_refuses(self):
        value=self.run_public();self.assertEqual([r['source_system']for r in value['records']],['A','B'])
        self.f.ack_mock.return_value=None
        with self.assertRaises(PermissionError):self.run_public()
    def test_small_receipt_bound_refuses_only_after_all_closing_without_output(self):
        from dataclasses import replace
        from ashlar_host.exact_report import report_body
        sample={'text':'雪\n','number':-(2**63),'nullable':None,'flag':True}
        self.assertEqual(json.loads(report_body(sample,1024)),json.loads(__import__('ashlar_host.delta_custody',fromlist=['encoded']).encoded(sample)))
        self.f.bounds=replace(self.f.bounds,receipt_bytes=128)
        self.invocation=replace(self.invocation,bounds=self.f.bounds)
        with self.assertRaises(ValueError):self.run_public()
        self.assertEqual(self.last_events[-1],'provider-closing')
        self.assertEqual([event for event in self.last_events if type(event)is tuple],[('row','A'),('row','B')])

    def test_closing_failure_withholds_success_and_cancel_keeps_primary(self):
        with self.assertRaises(OSError):self.run_public(closing_error=OSError('close'))
        primary=KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt)as raised:self.run_public(closing_error=OSError('close'),body_error=primary)
        self.assertIs(raised.exception,primary)

if __name__=='__main__':unittest.main()

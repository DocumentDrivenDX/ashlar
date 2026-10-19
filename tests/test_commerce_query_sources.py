# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual private lock/registry controls on inert snapshots; no PostgreSQL proof."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace,MappingProxyType
from ashlar.publication import Snapshot
from ashlar.native import SQLResult
from ashlar_host.postgres import Session
from unittest.mock import patch
from ashlar_host.ack import AckScope
from ashlar_host.commerce_query_sources import CommerceQuerySourceFactory
from ashlar_host.existing_outbox_authority import OutboxAuthorityRegistration
from ashlar_host.source_sessions import OutboxSourceRegistration
from ashlar_host.driver import NativeDriver
from ashlar_host.read_only_transport import ReadOnlyDeltaTransport
from ashlar_host.delta_custody import encoded,DeltaTarget


def factory():
    scope=AckScope('ashlar_ack_existing','11111111-1111-1111-1111-111111111111',
        '22222222-2222-2222-2222-222222222222','ordinary','A','epoch')
    source=OutboxSourceRegistration(scope,'original_source','a'*64,lambda context:None)
    return CommerceQuerySourceFactory(OutboxAuthorityRegistration(source,'reader','ack_role','test_db',lambda context:None))


class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name);(root/'.pipeline-writer.lock').touch()
        self.f=factory();r=self.f.registration
        t=object.__new__(ReadOnlyDeltaTransport);t.journal_path=root/'journal.sqlite'
        t.db=sqlite3.connect(t.journal_path);self.addCleanup(t.db.close)
        registry={'targets':{'local.commerce.nodes':'original-uuid'}}
        t.reservation=encoded({'registry':registry})
        t.db.execute('CREATE TABLE local_installation(id INTEGER,original TEXT)')
        t.db.execute('INSERT INTO local_installation VALUES(1,?)',(encoded(registry),));t.db.commit()
        (root/'local.commerce.nodes').mkdir()
        t.targets=MappingProxyType({'local.commerce.nodes':DeltaTarget('local.commerce.nodes',root/'local.commerce.nodes','original-uuid')})
        d=object.__new__(NativeDriver);d.transport=t;d.held=False;d.pin_held=False;d.source_sessions=None;d.context=object()
        request={'source_checkpoint_json':encoded({'feed':'A','epoch':'epoch'})}
        d.policy=SimpleNamespace(active={'request':request,'manifest':{'publication_id':'original','profile_version':'ashlar-delta/0.3',
            'table_versions_json':encoded({'local.commerce.nodes':4}),'source_progress_json':encoded({'A':{}}),
            'schema_revisions_json':encoded({'A':'original-schema'}),'validation_report_json':encoded({'complete':True}),'recorded_at':'0'}})
        d.scope_ports={'A':{'scope':r.source.scope,'source':r.source.source_schema,'signature':'a'*64,'reader':'reader','role':'ack_role'}}
        d.admission_facts=lambda:{'profile':'ashlar-original-commerce-public-dataset-admission/0.1'}
        self.inspected=[]
        def inspect(table,version):self.inspected.append(table);return Snapshot(table,t.targets[table].uuid,version)
        d.inspect_snapshot=inspect;d.bind_query_sources(self.f);self.d=d

    def renew(self):
        self.d.source_sessions.policy.writer.admit_source_writer(self.f.registration.source,self.d.context)

    def test_original_lock_registry_and_snapshot_are_required(self):
        with self.d.query_read_interval(self.d.context):
            self.d.pin_held=True;self.renew();self.assertEqual(self.inspected,['local.commerce.nodes'])
        self.assertFalse(self.d.held)
        with self.assertRaises(PermissionError):self.renew()

    def test_real_authority_renews_ordinary_roles_and_exact_scope_binding(self):
        from test_existing_outbox_authority import Tests as AuthorityControls
        observer=object.__new__(AuthorityControls)
        observer.calls=[];observer.fail=None;observer.scope=self.f.registration.source.scope
        observer.source=self.f.registration.source
        class Connection:autocommit=False
        session=Session(Connection())
        def query(sql,parameters):
            rows=AuthorityControls.observe(observer,sql,parameters,'ack_role').rows
            if 'pg_catalog.pg_roles' in sql:rows[0]['database']='test_db'
            return SQLResult(rows)
        session.query=query
        with self.d.query_read_interval(self.d.context):
            self.d.pin_held=True
            self.d.source_sessions.policy.admit_scope(observer.scope,session,self.d.context)
            observer.fail=lambda row,sql:row.update(rolsuper=True) if 'pg_catalog.pg_roles' in sql else None
            with self.assertRaises(ValueError):self.d.source_sessions.policy.admit_scope(observer.scope,session,self.d.context)
            observer.fail=lambda row,sql:row['binding'].update(source_signature_sha256='b'*64) if 'scope_binding(CAST' in sql else None
            with self.assertRaises(ValueError):self.d.source_sessions.policy.admit_scope(observer.scope,session,self.d.context)
        self.assertGreaterEqual(len(self.inspected),2)

    def test_matching_substituted_target_refuses_before_snapshot_read(self):
        with self.d.query_read_interval(self.d.context):
            self.d.pin_held=True;object.__setattr__(self.d.transport.targets['local.commerce.nodes'],'uuid','substituted')
            with self.assertRaises(PermissionError):self.renew()
        self.assertEqual(self.inspected,[])

    def test_journal_registry_mutation_refuses(self):
        with self.d.query_read_interval(self.d.context):
            self.d.pin_held=True;self.d.transport.db.execute("UPDATE local_installation SET original='{}'")
            with self.assertRaises(PermissionError):self.renew()
        self.assertEqual(self.inspected,[])

    def test_missing_pins_and_checkpoint_or_source_signature_drift_refuse(self):
        with self.d.query_read_interval(self.d.context):
            with self.assertRaises(PermissionError):self.renew()
            self.d.pin_held=True
            original=self.d.policy.active['request']['source_checkpoint_json']
            self.d.policy.active['request']['source_checkpoint_json']=encoded({'feed':'A','epoch':'other'})
            with self.assertRaises(PermissionError):self.renew()
            self.d.policy.active['request']['source_checkpoint_json']=original
            source=self.f.registration.source
            object.__setattr__(source,'source_signature_sha256','b'*64)
            with self.assertRaises(PermissionError):self.renew()
        self.assertEqual(self.inspected,[])

    def test_initial_checkpoint_role_schema_and_signature_mismatch_refuse(self):
        selected=self.f.registration
        for change in ('checkpoint','epoch','role','ack-role','schema','signature'):
            with self.subTest(change=change):
                self.d.source_sessions=None;self.d.query_source_factory=None
                candidate=factory()
                if change=='checkpoint':
                    self.d.policy.active['request']['source_checkpoint_json']=encoded({'feed':'other','epoch':'epoch'})
                elif change=='epoch':self.d.policy.active['request']['source_checkpoint_json']=encoded({'feed':'A','epoch':'other_epoch'})
                elif change=='role':object.__setattr__(candidate.registration,'source_role','other_reader')
                elif change=='ack-role':object.__setattr__(candidate.registration,'ack_role','other_ack')
                elif change=='schema':object.__setattr__(candidate.registration.source,'source_schema','other_source')
                else:object.__setattr__(candidate.registration.source,'source_signature_sha256','b'*64)
                with self.assertRaises(PermissionError):self.d.bind_query_sources(candidate)
                self.assertIsNone(self.d.source_sessions)
                self.d.policy.active['request']['source_checkpoint_json']=encoded({'feed':'A','epoch':'epoch'})
        self.assertEqual(self.inspected,[])

    def test_cancellation_identity_and_lock_cleanup(self):
        primary=KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt) as caught:
            with self.d.query_read_interval(self.d.context):raise primary
        self.assertIs(caught.exception,primary);self.assertFalse(self.d.held)
        with self.d.query_read_interval(self.d.context):pass

    def provider(self):
        from ashlar_host.publication_reader import PublicationProvider
        from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
        events=[];outer=self
        class Connection:
            autocommit=False
            def rollback(self):events.append('rollback')
            def close(self):events.append('close')
        provider=PublicationProvider(self.d,{},self.d.scope_ports['A'],b'request',b'manifest',{},
            context=self.d.context,ordinary_session=OrdinaryQueryConnectionFactory(lambda role:Connection()))
        def validate(row,context):
            events.append('manifest')
            outer.assertTrue(self.d.held);outer.assertTrue(self.d.pin_held)
            self.renew()
        def ack(session):
            events.append('ack')
            self.renew()
            return {'inert':True}
        self.d.validate_manifest=validate;provider._ack=ack
        return provider,events

    def test_actual_provider_and_pin_context_close_source_before_releasing_gate(self):
        provider,events=self.provider()
        with provider.interval(self.d.context):
            events.append('body');self.assertTrue(provider.active)
        self.assertEqual(events,['manifest','ack','body','ack','rollback','close','manifest'])
        self.assertFalse(provider.active);self.assertFalse(self.d.held);self.assertFalse(self.d.pin_held)
        self.assertIsNotNone(provider.closed_interval_custody(self.d.context))

    def test_body_cancellation_closing_source_drift_checks_both_contexts_and_releases(self):
        provider,events=self.provider();primary=KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt) as caught:
            with provider.interval(self.d.context):
                self.d.transport.db.execute("UPDATE local_installation SET original='{}'")
                raise primary
        self.assertIs(caught.exception,primary)
        self.assertEqual(events,['manifest','ack','ack','rollback','close','manifest'])
        self.assertFalse(self.d.held);self.assertFalse(self.d.pin_held);self.assertFalse(provider.active)
        with self.assertRaises(PermissionError):provider.closed_interval_custody(self.d.context)

    def test_closing_ack_cancellation_outranks_ordinary_body_and_still_renews_pins(self):
        provider,events=self.provider();original_ack=provider._ack;cancel=SystemExit()
        def ack(session):
            if events.count('ack')==1:
                events.append('closing-cancel');self.assertTrue(provider.active);raise cancel
            return original_ack(session)
        provider._ack=ack
        with self.assertRaises(SystemExit) as caught:
            with provider.interval(self.d.context):raise ValueError('ordinary body')
        self.assertIs(caught.exception,cancel)
        self.assertEqual(events,['manifest','ack','closing-cancel','rollback','close','manifest'])
        self.assertFalse(self.d.held);self.assertFalse(self.d.pin_held)
        with self.assertRaises(PermissionError):provider.closed_interval_custody(self.d.context)

    def test_failed_opening_manifest_does_not_acquire_source_or_ack(self):
        provider,events=self.provider();primary=KeyboardInterrupt()
        def refuse(row,context):events.append('opening-refusal');raise primary
        self.d.validate_manifest=refuse
        with self.assertRaises(KeyboardInterrupt) as caught:
            with provider.interval(self.d.context):self.fail('body reached')
        self.assertIs(caught.exception,primary);self.assertEqual(events,['opening-refusal'])
        self.assertFalse(self.d.pin_held);self.assertFalse(self.d.held)

    def test_failed_opening_ack_closes_connection_without_second_ack(self):
        provider,events=self.provider();primary=KeyboardInterrupt()
        def refuse(session):events.append('opening-ack-refusal');raise primary
        provider._ack=refuse
        with self.assertRaises(KeyboardInterrupt) as caught:
            with provider.interval(self.d.context):self.fail('body reached')
        self.assertIs(caught.exception,primary)
        self.assertEqual(events,['manifest','opening-ack-refusal','rollback','close','manifest'])
        self.assertFalse(self.d.pin_held);self.assertFalse(self.d.held)

    def test_read_only_compatibility_factory_returns_stable_exact_leaf(self):
        from ashlar_host.publication_reader import ReadOnlyTransport
        with patch.object(ReadOnlyDeltaTransport,'__init__',return_value=None) as initialize:
            selected=ReadOnlyTransport.open('spark','journal','installation',(),object(),capacity=7)
            self.assertIs(type(selected),ReadOnlyDeltaTransport)
            self.assertEqual(initialize.call_args.kwargs,{'capacity':7})
        for operation in ('mutation','recover'):
            with self.assertRaises(ValueError):getattr(selected,operation)(None,object(),lambda:None)

    def test_missing_registration_cannot_bind_and_native_writes_remain_refused(self):
        with self.assertRaises(PermissionError):self.d.bind_query_sources(object())
        with self.assertRaises(PermissionError):self.d.acknowledge({}, {},self.d.context)

    def test_factory_refuses_subclass_registration(self):
        class Forged(OutboxAuthorityRegistration):pass
        r=self.f.registration
        with self.assertRaises(ValueError):CommerceQuerySourceFactory(Forged(r.source,r.source_role,r.ack_role,r.database,r.metadata_factory))

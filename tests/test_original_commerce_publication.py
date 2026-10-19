# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure original source composition controls; no native session or PG authority."""
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace,MappingProxyType
import unittest
from unittest.mock import patch,Mock
from ashlar_host.ack import AckScope
from ashlar_host.ack_sessions import RegisteredOutboxAcks,OutboxAckRegistration
from ashlar_host.commerce_publication_sources import OriginalCommerceSourcePorts,OriginalCommerceSourceOwner,FEED,EPOCH
from ashlar_host.original_commerce_publication import OriginalCommerceInstallationLease,OriginalCommercePublicationOperator,OriginalCommercePublicationConfig,publish_original_commerce,ROLES
from ashlar_host.source_sessions import OutboxSourceRegistration,RegisteredOutboxSources
from ashlar_host.existing_outbox_authority import OutboxAuthorityRegistration
from ashlar_host.delta_custody import LocalDeltaTransport,DeltaTarget,encoded
from ashlar_host.driver import NativeDriver,PrivatePolicy
from ashlar_host.config import ProducerConfig
from ashlar.commerce_source import build_transaction

class OriginalPublicationTests(unittest.TestCase):
    def ports(self,**changes):
        scope=AckScope('ashlar_ack_original','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002','original',FEED,EPOCH)
        reader=Mock();ack=Mock();writer=Mock()
        source=OutboxSourceRegistration(scope,'ashlar_original_source','a'*64,reader)
        registration=OutboxAuthorityRegistration(source,'original_reader','original_ack','isolated',ack)
        return OriginalCommerceSourcePorts(registration,changes.get('writer_role','original_writer'),writer)
    def test_closed_original_roles_and_feed(self):
        p=self.ports();self.assertEqual(p.metadata()['source']['scope']['feed'],FEED)
        for v in ('original_reader','original_ack',True):
            with self.assertRaises(ValueError):self.ports(writer_role=v)
        source=self.ports().registration.source
        bad=OutboxSourceRegistration(AckScope(source.scope.service_schema,source.scope.scope_id,source.scope.installation_id,'original','A','evolution'),source.source_schema,source.source_signature_sha256,source.connection_factory)
        with self.assertRaises(ValueError):OriginalCommerceSourcePorts(OutboxAuthorityRegistration(bad,'reader','ack','db',Mock()),'writer',Mock())
    def test_common_factory_original_profile_no_legacy_ports(self):
        ports=self.ports();context=object();db=sqlite3.connect(':memory:');transport=object.__new__(LocalDeltaTransport);transport.db=db
        for table in ('local_plan','local_operation','local_publication_artifact','local_source_plan'):db.execute('CREATE TABLE '+table+' (x TEXT)')
        class Admission:
            def metadata(self):return {'profile':'ashlar-original-commerce-public-dataset-admission/0.1','qualification':'pure constructor correspondence only'}
            def admit(self,*args):pass
        class Policy:
            def admit_source(self,*args):pass
            def admit_scope(self,*args):pass
            def admit_publication(self,*args):pass
        policy=Policy();source=ports.registration.source
        try:
            driver=NativeDriver.with_registered_evolution(transport,policy,context,{},(),{},source_admission=Admission(),source_sessions=RegisteredOutboxSources((source,),policy),ack_factory=lambda d:RegisteredOutboxAcks((OutboxAckRegistration(source,Mock(),policy),)))
            self.assertIs(type(driver),NativeDriver);self.assertEqual(driver.scope_ports,{})
            self.assertEqual(driver.original_source_sessions,encoded(driver.source_sessions.metadata()));self.assertIs(type(driver.ack_sessions),RegisteredOutboxAcks)
        finally:db.close()
    def test_common_factory_mismatched_ack_refuses_closes(self):
        ports=self.ports();db=sqlite3.connect(':memory:');transport=SimpleNamespace(db=db,close=Mock())
        for t in ('local_plan','local_operation','local_publication_artifact','local_source_plan'):db.execute('CREATE TABLE '+t+' (x TEXT)')
        class Admission:
            def metadata(self):return {'profile':'ashlar-original-commerce-public-dataset-admission/0.1','qualification':'pure'}
            def admit(self,*args):pass
        policy=SimpleNamespace(admit_source=lambda *a:None)
        try:
            with self.assertRaises(PermissionError):NativeDriver.with_registered_evolution(transport,policy,object(),{},(),{},source_admission=Admission(),source_sessions=RegisteredOutboxSources((ports.registration.source,),policy),ack_factory=lambda d:None)
            transport.close.assert_called_once()
        finally:db.close()
    def operator(self,root):
        targets=[]
        for role in sorted(ROLES):
            p=root/role;p.mkdir();targets.append(DeltaTarget('local.commerce.'+role,p,role+'-uuid'))
        journal=root/'operations.sqlite';journal.touch()
        return OriginalCommercePublicationOperator('original',journal,tuple(targets),self.ports(),None,Mock())
    def test_actual_lease_complete_registry_and_substitution(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();root.chmod(0o700);o=self.operator(root);context=object()
            registry={'installation':o.installation_id,'targets':[{'table':x.table,'path':str(x.path),'uuid':x.uuid} for x in sorted(o.targets,key=lambda x:x.table)]}
            reservation=encoded({'profile':'ashlar-private-native-installation-reservation/0.1','registry':registry,'original_journal_path':str(o.journal)})+'\n'
            for x in o.targets:(x.path/'.ashlar-local-installation-reservation.json').write_text(reservation)
            transport=object.__new__(LocalDeltaTransport);transport.journal_path=o.journal;transport.reservation=reservation;transport.targets=MappingProxyType({x.table:x for x in o.targets});transport.db=sqlite3.connect(':memory:');transport.db.execute('CREATE TABLE local_installation(id INTEGER,original TEXT)');transport.db.execute('INSERT INTO local_installation VALUES(1,?)',(encoded(registry),))
            try:
                with OriginalCommerceInstallationLease.open(o,context) as lease:
                    lease.verify(context,transport)
                    first=o.targets[0];bad=DeltaTarget(first.table,first.path,'forged-uuid');transport.targets=MappingProxyType({**dict(transport.targets),first.table:bad})
                    with self.assertRaises(PermissionError):lease.verify(context,transport)
                    transport.targets=MappingProxyType({x.table:x for x in o.targets})
            finally:transport.db.close()
    def test_lease_closing_cancel_original_identity(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();root.chmod(0o700);o=self.operator(root);context=object();cancel=KeyboardInterrupt('original')
            with patch.object(OriginalCommerceInstallationLease,'renew',side_effect=[None,OSError('closing')]):
                try:
                    with OriginalCommerceInstallationLease.open(o,context):raise cancel
                except KeyboardInterrupt as observed:self.assertIs(observed,cancel)
                else:self.fail('Original cancellation lost')
    def test_lease_entry_rejects_executable_carrier_before_effects(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();callback=Mock()
            forged=SimpleNamespace(journal=root/'journal',__post_init__=callback)
            with self.assertRaises(ValueError):
                with OriginalCommerceInstallationLease.open(forged,object()):self.fail('Admitted forged owner')
            callback.assert_not_called();self.assertEqual(list(root.iterdir()),[])

    def test_shared_installation_lock_conflicts_with_evolution_owner(self):
        from ashlar_host.evolution_operator import ExistingEvolutionLease
        from ashlar_host.evolution_cli import FreshEvolutionInvocation
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();root.chmod(0o700);operator=self.operator(root)
            invocation=object.__new__(FreshEvolutionInvocation)
            object.__setattr__(invocation,'ledger_path',root/'fresh-ledger')
            # A second real owner reaches the canonical flock before any SDK,
            # source or reservation effects. Its other fields are never used.
            evolution=SimpleNamespace(journal_path=operator.journal)
            with patch.object(OriginalCommerceInstallationLease,'renew'):
                with OriginalCommerceInstallationLease.open(operator,object()):
                    with self.assertRaises(BlockingIOError):
                        with ExistingEvolutionLease.open(evolution,invocation,object()):self.fail('Conflicting installation owner acquired')
    def test_entry_refusal_before_runtime_or_source(self):
        with patch('ashlar_host.commerce.runtime_paths') as runtime:
            with self.assertRaises(ValueError):publish_original_commerce(object(),operator=object())
            runtime.assert_not_called()
    def append_fixture(self):
        root=Path(__file__).resolve().parents[1]
        batch,_=build_transaction((root/'src/ashlar_host/resources/evolution/original-model.json').read_bytes(),(root/'examples/domain-packs/commerce/upstream/graph/fixture.json').read_bytes(),source_system=FEED)
        ports=self.ports();lease=object.__new__(OriginalCommerceInstallationLease)
        owner=OriginalCommerceSourceOwner(ports,lease);owner.driver=object.__new__(NativeDriver)
        import hashlib
        facts={'profile':'ashlar-original-commerce-public-dataset-admission/0.1','qualification':'pure fixed original source bytes','source_transaction_sha256':hashlib.sha256(batch.begin+b''.join(x.raw for x in batch.records)+batch.commit).hexdigest()}
        owner.driver.source_admission=SimpleNamespace(batch=batch,metadata=lambda:dict(facts));owner.driver.original_admission=encoded(facts)
        reader1=Mock();reader2=Mock();writer=Mock();metadata=Mock()
        ports.registration.source.connection_factory.side_effect=[reader1,reader2]
        ports.writer_connection.return_value=writer;ports.registration.metadata_factory.return_value=metadata
        transaction=SimpleNamespace(position='1',batch=batch)
        def one(session,sql,parameters,fields):
            if fields==('position',):
                if '.append(' in sql:
                    self.assertEqual(parameters,{'batch':EPOCH,'payload':(batch.begin+b''.join(x.raw for x in batch.records)+batch.commit).decode()})
                    return {'position':1}
                return {'position':0}
            return {'append':True}
        return owner,batch,transaction,writer,reader1,reader2,metadata,one
    def test_actual_append_function_exactbytes_commit_readback_cleanup(self):
        owner,batch,transaction,writer,r1,r2,metadata,one=self.append_fixture()
        with patch.object(owner,'admit_source_writer'),patch('ashlar_host.existing_outbox_authority.ExistingOutboxAuthority.admit_scope'),patch('ashlar_host.commerce_publication_sources.ordinary'),patch('ashlar_host.commerce_publication_sources.privileges'),patch('ashlar_host.commerce_publication_sources.one',side_effect=one),patch('ashlar_host.commerce_publication_sources.PostgresOutbox') as outbox:
            outbox.return_value.read.return_value=[transaction]
            self.assertIs(owner.append_original(batch,context=object()),transaction)
        writer.commit.assert_called_once()
        for connection in (writer,r1,r2):connection.rollback.assert_called_once();connection.close.assert_called_once()
        self.assertEqual(metadata.rollback.call_count,2);self.assertEqual(metadata.close.call_count,2)
    def test_committed_original_readback_difference_refuses(self):
        owner,batch,transaction,writer,r1,r2,metadata,one=self.append_fixture()
        with patch.object(owner,'admit_source_writer'),patch('ashlar_host.existing_outbox_authority.ExistingOutboxAuthority.admit_scope'),patch('ashlar_host.commerce_publication_sources.ordinary'),patch('ashlar_host.commerce_publication_sources.privileges'),patch('ashlar_host.commerce_publication_sources.one',side_effect=one),patch('ashlar_host.commerce_publication_sources.PostgresOutbox') as outbox:
            outbox.return_value.read.return_value=[SimpleNamespace(position='1',batch=None)]
            with self.assertRaises(ValueError):owner.append_original(batch,context=object())
        self.assertEqual(writer.commit.call_count,1);r2.close.assert_called_once()
    def test_append_cancel_preserved_despite_rollback_failure(self):
        owner,batch,transaction,writer,r1,r2,metadata,one=self.append_fixture();cancel=KeyboardInterrupt('commit')
        writer.commit.side_effect=cancel;writer.rollback.side_effect=OSError('cleanup')
        with patch.object(owner,'admit_source_writer'),patch('ashlar_host.existing_outbox_authority.ExistingOutboxAuthority.admit_scope'),patch('ashlar_host.commerce_publication_sources.ordinary'),patch('ashlar_host.commerce_publication_sources.privileges'),patch('ashlar_host.commerce_publication_sources.one',side_effect=one):
            try:owner.append_original(batch,context=object())
            except KeyboardInterrupt as observed:self.assertIs(observed,cancel)
            else:self.fail('Original cancellation missing')
        writer.close.assert_called_once();r2.close.assert_not_called()
    def test_actual_append_privilege_refusal_before_commit(self):
        owner,batch,transaction,writer,r1,r2,metadata,one=self.append_fixture()
        with patch.object(owner,'admit_source_writer'),patch('ashlar_host.commerce_publication_sources.ordinary'),patch('ashlar_host.commerce_publication_sources.one',return_value={'append':False}):
            with self.assertRaises(PermissionError):owner.admit_append(SimpleNamespace(),object())
        writer.commit.assert_not_called()
    def test_real_ordinary_writer_role_flags_refuse(self):
        from ashlar_host.postgres import Session
        from ashlar.native import SQLResult
        owner,batch,transaction,writer,r1,r2,metadata,one=self.append_fixture();connection=SimpleNamespace(autocommit=False)
        observed={'session_user':'original_writer','current_user':'original_writer','database':'isolated','isolation':'read committed','rolsuper':True,'rolcreaterole':False,'rolcreatedb':False,'rolreplication':False,'rolbypassrls':False}
        with patch.object(Session,'query',return_value=SQLResult([observed])):
            with self.assertRaises(ValueError):owner.admit_append(Session(connection),object())
        owner.ports.writer_connection.assert_not_called()
    def test_current_registration_signature_changed_refuses(self):
        owner,batch,transaction,writer,r1,r2,metadata,one=self.append_fixture();context=object();owner.driver.context=context;owner.driver.held=True;owner.driver.transport=object()
        with patch.object(OriginalCommerceInstallationLease,'verify'):
            owner.admit_source_writer(owner.ports.registration.source,context)
            object.__setattr__(owner.ports.registration.source,'source_signature_sha256','b'*64)
            with self.assertRaises(PermissionError):owner.admit_source_writer(owner.ports.registration.source,context)
        owner.ports.writer_connection.assert_not_called()
    def test_lost_append_reply_reuses_complete_original_without_write(self):
        owner,batch,transaction,writer,r1,r2,metadata,one=self.append_fixture()
        def already(session,sql,parameters,fields):return {'position':1}
        with patch.object(owner,'admit_source_writer'),patch('ashlar_host.existing_outbox_authority.ExistingOutboxAuthority.admit_scope'),patch('ashlar_host.commerce_publication_sources.ordinary'),patch('ashlar_host.commerce_publication_sources.privileges'),patch('ashlar_host.commerce_publication_sources.one',side_effect=already),patch('ashlar_host.commerce_publication_sources.PostgresOutbox') as outbox:
            outbox.return_value.read.return_value=[transaction]
            self.assertIs(owner.append_original(batch,context=object()),transaction)
        owner.ports.writer_connection.assert_not_called();writer.commit.assert_not_called();r1.close.assert_called_once()
    def test_current_missing_writer_hold_refuses_before_connections(self):
        owner,batch,transaction,writer,r1,r2,metadata,one=self.append_fixture();context=object();owner.driver.context=context;owner.driver.held=False
        with self.assertRaises(PermissionError):owner.append_original(batch,context=context)
        owner.ports.writer_connection.assert_not_called();owner.ports.registration.metadata_factory.assert_not_called()
    def test_explicit_reader_roles_match_factory_legacy_preserved(self):
        from ashlar_host.publication_reader import original_commerce_reader_roles
        from ashlar_host.commerce_query_sources import CommerceQuerySourceFactory
        ports=self.ports();r=ports.registration
        report={'protected_ack_scope':r.source.metadata()['scope'],'source_schema':r.source.source_schema,'source_ports_profile':'ashlar-explicit-original-commerce-source/0.1','source_role':r.source_role,'ack_role':r.ack_role}
        factory=CommerceQuerySourceFactory(r)
        self.assertEqual(original_commerce_reader_roles(report,factory),(r.source_role,r.ack_role))
        with self.assertRaises(ValueError):original_commerce_reader_roles(report,None)
        with self.assertRaises(ValueError):original_commerce_reader_roles(dict(report,ack_role='foreign_ack'),factory)
        with self.assertRaises(ValueError):original_commerce_reader_roles(dict(report,source_ports_profile='unknown'),factory)
        old={'protected_ack_scope':{'service_schema':'ashlar_pipeline_original'},'source_schema':'original_source'}
        self.assertEqual(original_commerce_reader_roles(old,None),('original_source_reader','ashlar_operator_original'))
    def test_closing_source_runtime_on_cancel(self):
        from ashlar_host.original_commerce_publication import original_publication_custody
        config=SimpleNamespace(model=Path('/model'),graph=Path('/graph'));admission=Mock();cancel=KeyboardInterrupt('body')
        with patch('ashlar_host.original_commerce_publication.original_inputs') as inputs,patch('ashlar_host.commerce.runtime_paths',side_effect=OSError('closing')) as runtime:
            try:
                with original_publication_custody(config,admission):raise cancel
            except KeyboardInterrupt as observed:self.assertIs(observed,cancel)
            else:self.fail('Cancellation missing')
        admission.metadata.assert_called_once();inputs.assert_called_once();runtime.assert_called_once()
    def test_pipeline_closing_source_live_before_transport_and_cancel(self):
        from contextlib import contextmanager
        from ashlar_host.original_commerce_publication import close_original_publication
        events=[];context=object();cancel=KeyboardInterrupt('body');held=[False]
        @contextmanager
        def writer(stream,supplied):
            self.assertIs(supplied,context);held[0]=True;events.append('writer-open')
            try:yield
            finally:events.append('writer-close');held[0]=False
        def renew(supplied):
            self.assertTrue(held[0]);self.assertIs(supplied,context);events.append('source-close')
            raise PermissionError('Current signature revoked')
        owner=SimpleNamespace(renew_scope=renew);driver=SimpleNamespace(writer=writer)
        lease=SimpleNamespace(verify=lambda *a:events.append('lease-close'))
        transport=SimpleNamespace(close=lambda:events.append('transport-close'))
        with self.assertRaises(PermissionError):close_original_publication(None,owner,driver,context,lease,transport)
        self.assertEqual(events,['writer-open','source-close','writer-close','lease-close','transport-close'])
        events.clear()
        try:close_original_publication(cancel,owner,driver,context,lease,transport)
        except KeyboardInterrupt as observed:self.assertIs(observed,cancel)
        else:self.fail('Original cancellation missing')
        self.assertEqual(events,['writer-open','source-close','writer-close','lease-close','transport-close'])

    def test_explicit_existing_journal_layout_refuses_alias_missing(self):
        from ashlar_host.publication_reader import original_commerce_reader_installation
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();journal=root/'actual.sqlite';journal.touch()
            report={'source_ports_profile':'ashlar-explicit-original-commerce-source/0.1','original_journal_path':str(journal),'installation_id':'exact-original'}
            self.assertEqual(original_commerce_reader_installation(report,root),(journal,'exact-original'))
            alias=root/'alias.sqlite';alias.symlink_to(journal)
            for value in (str(alias),str(root/'missing.sqlite'),True,'relative.sqlite'):
                with self.assertRaises(ValueError):original_commerce_reader_installation(dict(report,original_journal_path=value),root)
            self.assertEqual(original_commerce_reader_installation({},root),(root/'operations.sqlite','private-original-commerce'))
    def test_all_six_actual_native_counts_before_append(self):
        from ashlar_host.original_commerce_publication import require_fresh_original_native
        from ashlar.publication import Snapshot
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();operator=self.operator(root);driver=object.__new__(NativeDriver);driver.transport=SimpleNamespace(original_history=lambda target:[{'version':1}])
            spark=Mock();spark.sql.return_value.collect.return_value=[{'n':0}]
            with patch.object(driver,'inspect_snapshot',side_effect=lambda table,version:Snapshot(table,dict((x.table,x.uuid) for x in operator.targets)[table],version)):
                require_fresh_original_native(spark,driver,operator.targets)
                self.assertEqual(spark.sql.call_count,6)
                for count in (1,True):
                    spark.sql.return_value.collect.return_value=[{'n':count}]
                    with self.assertRaises(PermissionError):require_fresh_original_native(spark,driver,operator.targets)
    def test_original21_byte_inventory_preserved(self):
        root=Path(__file__).resolve().parents[1]
        model=(root/'src/ashlar_host/resources/evolution/original-model.json').read_bytes();graph=(root/'examples/domain-packs/commerce/upstream/graph/fixture.json').read_bytes()
        batch,binding=build_transaction(model,graph,source_system=FEED)
        self.assertEqual((batch.feed,batch.epoch,batch.batch_id),(FEED,EPOCH,EPOCH));self.assertEqual(len(batch.records),21)
        raw=batch.begin+b''.join(r.raw for r in batch.records)+batch.commit
        import hashlib
        self.assertEqual(hashlib.sha256(raw).hexdigest(),'e7b488a318d562916f55f875f430087077ab263131e9c71754721d637c3c9482')

if __name__=='__main__':unittest.main()

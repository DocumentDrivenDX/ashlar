"""Real medical lease/source owners over inert native and ordinary SQL ports."""
from contextlib import ExitStack
from dataclasses import replace
import hashlib,json,os,re,sqlite3,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.native import SQLResult
from ashlar.medical_source import build_transaction
from ashlar_host.medical_installation import OriginalMedicalInstallationSelection,OriginalMedicalInstallationLease,ROLES
from ashlar_host.medical_publication_sources import OriginalMedicalSourcePorts,OriginalMedicalSourceOwner,FEED,EPOCH
from ashlar_host.ack import AckScope
from ashlar_host.source_sessions import OutboxSourceRegistration
from ashlar_host.existing_outbox_authority import OutboxAuthorityRegistration
from ashlar_host.delta_custody import DeltaTarget,LocalDeltaTransport,encoded
from ashlar_host.driver import NativeDriver
from ashlar_host.postgres import Session
import test_original_medical_admission as admission_fixture

class Connection:
    autocommit=False
    def __init__(self,role):self.role=role;self.rollbacks=0;self.closes=0;self.commits=0;self.cancel=None;self.cleanup=None
    def rollback(self):
        self.rollbacks+=1
        if self.cleanup:raise self.cleanup
    def close(self):self.closes+=1
    def commit(self):
        self.commits+=1
        if self.cancel:raise self.cancel

class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve();self.root.chmod(0o700)
        self.context=object();self.selection=self.make_selection();self.connections=[];self.head=0;self.revoked=False;self.binding_drift=False;self.writer_cancel=None
        self.scope=AckScope('ashlar_ack_medical','11111111-1111-1111-1111-111111111111',self.selection.installation_id,'medical-consumer',FEED,EPOCH)
        def connect(role):
            c=Connection(role)
            if role=='medical_writer'and self.writer_cancel:c.cancel=self.writer_cancel;c.cleanup=OSError('rollback')
            self.connections.append(c);return c
        source=OutboxSourceRegistration(self.scope,'medical_source','a'*64,lambda context:connect('medical_reader'))
        registration=OutboxAuthorityRegistration(source,'medical_reader','medical_ack','medical_db',lambda context:connect('medical_ack'))
        self.ports=OriginalMedicalSourcePorts(registration,'medical_writer',lambda context:connect('medical_writer'))
        admission_fixture.MedicalAdmissionTests.setUpClass();self.fixture=admission_fixture.MedicalAdmissionTests();self.fixture.setUp();self.addCleanup(self.fixture.tmp.cleanup)
        self.fixture.batch,self.fixture.bindings=build_transaction(self.fixture.model,self.fixture.graph,self.fixture.baseline,source_system=FEED)
        self.admission=self.fixture.admission();self.batch=self.admission.batch
        self.raw=self.batch.begin+b''.join(r.raw for r in self.batch.records)+self.batch.commit
    def make_selection(self):
        targets=[]
        for role in sorted(ROLES):
            p=self.root/role;p.mkdir(mode=0o700);targets.append(DeltaTarget('local.medical.'+role,p,role+'-uuid'))
        journal=self.root/'journal.sqlite';db=sqlite3.connect(journal);
        for sql in ('CREATE TABLE local_installation(id INTEGER PRIMARY KEY CHECK(id=1), original TEXT NOT NULL)','CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)','CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)','CREATE TABLE local_publication_artifact (request_digest TEXT PRIMARY KEY,request TEXT NOT NULL,artifact TEXT NOT NULL)','CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)'):db.execute(sql)
        db.commit();db.close()
        return OriginalMedicalInstallationSelection('22222222-2222-2222-2222-222222222222',journal,tuple(targets))
    def setup_reservations(self):
        registry=encoded({'installation':self.selection.installation_id,'targets':[{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in sorted(self.selection.targets,key=lambda t:t.table)]})
        reservation=encoded({'profile':'ashlar-private-native-installation-reservation/0.1','registry':json.loads(registry),'original_journal_path':str(self.selection.journal)})+'\n'
        for t in self.selection.targets:(t.path/'.ashlar-local-installation-reservation.json').write_text(reservation)
        db=sqlite3.connect(self.selection.journal);db.execute('INSERT INTO local_installation VALUES(1,?)',(registry,));db.commit();return db,reservation
    def sql(self,session,statement,parameters):
        role=session.connection.role
        if 'FROM pg_catalog.pg_roles' in statement:
            return SQLResult([dict(session_user=role,current_user=role,database='medical_db',isolation='read committed',rolsuper=self.revoked,rolcreaterole=False,rolcreatedb=False,rolreplication=False,rolbypassrls=False)])
        if 'SELECT c.relname AS kind' in statement:return SQLResult([{'kind':k,'oid':i}for i,k in enumerate(('batch','head','append'),10)])
        if 'has_schema_privilege' in statement:
            fields=('usage','create','binding','observe','ack','installation_write','batch_write','head_write','append','source_owner','scope_write','receipt_write','owner')if role=='medical_ack'else('usage','create','batch_read','head_read','batch_write','head_write','append','owner')
            yes={'usage','binding','observe','ack'}if role=='medical_ack'else{'usage','batch_read','head_read'}
            return SQLResult([{k:k in yes for k in fields}])
        if 'scope_binding' in statement:
            v=self.ports.registration.source.metadata()['scope'];v.pop('service_schema');v.update(source_schema='medical_source',source_signature_sha256=('b'if self.binding_drift else'a')*64);return SQLResult([{'binding':v}])
        if 'has_function_privilege' in statement:return SQLResult([{'append':True}])
        if '.append(' in statement:
            self.assertEqual(parameters,{'batch':EPOCH,'payload':self.raw.decode()});self.head=1;return SQLResult([{'position':1}])
        if '.head WHERE' in statement:return SQLResult([{'position':str(self.head)if '::text' in statement else self.head}])
        if '.batch WHERE' in statement:return SQLResult([{'position':'1','batch_id':EPOCH,'payload':self.raw.decode(),'digest':hashlib.sha256(self.raw).hexdigest()}])
        raise AssertionError(statement)
    def owner(self,stack):
        db,reservation=self.setup_reservations();stack.callback(db.close)
        lease=stack.enter_context(OriginalMedicalInstallationLease.open(self.selection,self.context))
        transport=object.__new__(LocalDeltaTransport);transport.journal_path=self.selection.journal;transport.reservation=reservation;transport.targets={t.table:t for t in self.selection.targets};transport.db=db
        driver=object.__new__(NativeDriver);driver.context=self.context;driver.held=True;driver.transport=transport;driver.source_admission=self.admission
        stack.enter_context(patch('ashlar_host.medical_producer.capture',side_effect=self.fixture.capture));driver.original_admission=encoded(self.admission.metadata())
        owner=OriginalMedicalSourceOwner(self.ports,lease);acks=owner.bind(driver)
        stack.enter_context(patch.object(Session,'query',lambda session,statement,parameters:self.sql(session,statement,parameters)))
        return owner,acks
    def test_head_zero_append_actual_owners_and_complete_readback(self):
        with ExitStack()as stack:
            owner,acks=self.owner(stack);result=owner.append_original(self.batch,context=self.context)
            self.assertEqual(result.batch,self.batch);self.assertEqual(result.position,'1');self.assertIs(acks.registrations[0].connection_factory,self.ports.registration.metadata_factory)
        self.assertEqual(sum(c.commits for c in self.connections),1)
        self.assertTrue(all(c.closes==1 and c.rollbacks==1 for c in self.connections))
    def test_head_one_reply_loss_readback_never_appends_or_commits(self):
        self.head=1
        with ExitStack()as stack:
            owner,_=self.owner(stack);self.assertEqual(owner.append_original(self.batch,context=self.context).batch,self.batch)
        self.assertEqual(sum(c.commits for c in self.connections),0);self.assertFalse(any(c.role=='medical_writer'for c in self.connections))
    def test_settled_read_has_complete_original_transaction_and_no_writer(self):
        self.head=1
        with ExitStack()as stack:
            owner,_=self.owner(stack)
            stack.enter_context(patch.object(owner,'append_original',side_effect=AssertionError('No append route')))
            result=owner.read_original(self.batch,context=self.context)
            self.assertEqual(result.batch,self.batch);self.assertEqual(result.payload_digest,hashlib.sha256(self.raw).hexdigest())
        self.assertEqual(sum(c.commits for c in self.connections),0)
        self.assertFalse(any(c.role=='medical_writer'for c in self.connections))
        self.assertTrue(all(c.closes==1 and c.rollbacks==1 for c in self.connections))
    def test_settled_read_missing_or_later_head_refuses_without_append(self):
        for head in (0,2):
            with self.subTest(head=head),ExitStack()as stack:
                db=sqlite3.connect(self.selection.journal);db.execute('DELETE FROM local_installation');db.commit();db.close()
                owner,_=self.owner(stack);self.head=head
                with self.assertRaises(ValueError):owner.read_original(self.batch,context=self.context)
        self.assertFalse(any(c.role=='medical_writer'for c in self.connections))
        self.assertTrue(all(c.closes==1 for c in self.connections))
    def test_settled_read_closing_payload_drift_refuses(self):
        self.head=1
        with ExitStack()as stack:
            owner,_=self.owner(stack);original=self.sql;reads=[]
            def sql(session,statement,parameters):
                result=original(session,statement,parameters)
                if '.batch WHERE' in statement:
                    reads.append(statement)
                    if len(reads)>1:result.rows[0]['payload']=self.raw.decode().replace('Patient','patient',1)
                return result
            stack.enter_context(patch.object(Session,'query',lambda session,statement,parameters:sql(session,statement,parameters)))
            with self.assertRaises(ValueError):owner.read_original(self.batch,context=self.context)
        self.assertGreaterEqual(len(reads),2);self.assertTrue(all(c.closes==1 for c in self.connections))
    def test_settled_read_cancel_attempts_ack_closing_and_preserves_primary(self):
        self.head=1;cancel=KeyboardInterrupt('read')
        with ExitStack()as stack:
            owner,_=self.owner(stack);original=self.sql;reads=[]
            def sql(session,statement,parameters):
                if '.head WHERE' in statement:
                    reads.append(statement)
                    session.connection.cleanup=OSError('secondary rollback')
                    raise cancel
                return original(session,statement,parameters)
            stack.enter_context(patch.object(Session,'query',lambda session,statement,parameters:sql(session,statement,parameters)))
            with self.assertRaises(KeyboardInterrupt)as caught:owner.read_original(self.batch,context=self.context)
            self.assertIs(caught.exception,cancel)
        self.assertEqual(len(reads),2);self.assertEqual(sum(c.role=='medical_ack'for c in self.connections),2)
        self.assertTrue(all(c.closes==1 for c in self.connections));self.assertFalse(any(c.role=='medical_writer'for c in self.connections))
    def test_role_signature_and_head_drift_refuse_with_cleanup(self):
        for field,value in (('revoked',True),('binding_drift',True),('head',2)):
            with self.subTest(field=field),ExitStack()as stack:
                # Each owner has a separately empty journal registry row.
                db=sqlite3.connect(self.selection.journal);db.execute('DELETE FROM local_installation');db.commit();db.close()
                owner,_=self.owner(stack);setattr(self,field,value)
                with self.assertRaises((ValueError,PermissionError)):owner.append_original(self.batch,context=self.context)
                setattr(self,field,False if field!='head'else 0)
        self.assertTrue(all(c.closes==1 for c in self.connections));self.assertEqual(sum(c.commits for c in self.connections),0)
    def test_commit_cancel_exact_identity_with_independent_cleanup(self):
        cancel=KeyboardInterrupt('commit')
        with ExitStack()as stack:
            owner,_=self.owner(stack);self.writer_cancel=cancel
            try:owner.append_original(self.batch,context=self.context)
            except KeyboardInterrupt as error:self.assertIs(error,cancel)
            else:self.fail('Missing cancellation')
        self.assertTrue(all(c.closes==1 for c in self.connections))
    def test_lease_reservation_drift_and_lock_exclusion(self):
        db,_=self.setup_reservations();db.close()
        with OriginalMedicalInstallationLease.open(self.selection,self.context)as lease:
            with self.assertRaises(BlockingIOError):
                with OriginalMedicalInstallationLease.open(self.selection,self.context):self.fail('Second held lease')
            with self.assertRaises(PermissionError):lease.renew(object())
        with self.assertRaises(PermissionError):lease.renew(self.context)
        p=self.selection.targets[0].path/'.ashlar-local-installation-reservation.json';p.write_text('{}')
        with self.assertRaises(PermissionError):
            with OriginalMedicalInstallationLease.open(self.selection,self.context):self.fail('Changed reservation admitted')
    def test_closed_feed_role_types_and_batch_refuse(self):
        r=self.ports.registration
        with self.assertRaises(ValueError):OriginalMedicalSourcePorts(r,r.source_role,lambda context:None)
        wrong=replace(r.source,scope=replace(self.scope,feed='other'))
        with self.assertRaises(ValueError):OriginalMedicalSourcePorts(replace(r,source=wrong),'medical_writer',lambda context:None)
        with ExitStack()as stack:
            owner,_=self.owner(stack)
            with self.assertRaises(ValueError):owner.append_original(replace(self.batch,feed='other'),context=self.context)
        self.assertFalse(self.connections)

    def test_held_admission_factory_and_native_registry_drift_refuse(self):
        with ExitStack()as stack:
            owner,_=self.owner(stack)
            owner.driver.held=False
            with self.assertRaises(PermissionError):owner.admit_source_writer(self.ports.registration.source,self.context)
            owner.driver.held=True
            old=owner.driver.source_admission;owner.driver.source_admission=object()
            with self.assertRaises(PermissionError):owner.admit_source_writer(self.ports.registration.source,self.context)
            owner.driver.source_admission=old
            object.__setattr__(self.ports,'writer_connection',lambda context:None)
            with self.assertRaises(PermissionError):owner.admit_source_writer(self.ports.registration.source,self.context)
        self.assertFalse(self.connections)
    def test_opening_cancel_still_renews_scope_and_closes_connection(self):
        cancel=GeneratorExit('opening')
        with ExitStack()as stack:
            owner,_=self.owner(stack);calls=[];original=self.sql
            def sql(session,statement,parameters):
                calls.append(statement)
                if len(calls)==1:
                    session.connection.cleanup=OSError('rollback');raise cancel
                return original(session,statement,parameters)
            stack.enter_context(patch.object(Session,'query',lambda session,statement,parameters:sql(session,statement,parameters)))
            try:owner.renew_scope(self.context)
            except GeneratorExit as error:self.assertIs(error,cancel)
            else:self.fail('Missing exact cancellation')
            self.assertGreater(len(calls),1)
        self.assertEqual(len(self.connections),1);self.assertEqual(self.connections[0].closes,1)

    def test_incomplete_and_malformed_journal_refuse_without_repair(self):
        db,_=self.setup_reservations();db.close()
        for malformed in (False,True):
            db=sqlite3.connect(self.selection.journal)
            if not malformed:db.execute('DROP TABLE local_source_plan')
            else:db.execute('CREATE TABLE local_source_plan(request_digest TEXT)')
            db.commit();before=list(db.execute('SELECT name,sql FROM sqlite_master ORDER BY name'));db.close()
            with patch.object(OriginalMedicalInstallationLease,'__init__',side_effect=AssertionError('No lease construction'))as construct:
                with self.assertRaises(ValueError):
                    with OriginalMedicalInstallationLease.open(self.selection,self.context):self.fail('Incomplete journal admitted')
                construct.assert_not_called()
            db=sqlite3.connect(self.selection.journal);self.assertEqual(list(db.execute('SELECT name,sql FROM sqlite_master ORDER BY name')),before);db.close()
    def test_acquisition_and_constructor_cancellation_cleanup(self):
        db,_=self.setup_reservations();db.close();cancel=KeyboardInterrupt('acquisition')
        with patch('ashlar_host.medical_installation.os.open',side_effect=cancel),patch('ashlar_host.medical_installation.os.close')as close:
            try:
                with OriginalMedicalInstallationLease.open(self.selection,self.context):self.fail('Unexpected acquisition')
            except KeyboardInterrupt as error:self.assertIs(error,cancel)
            close.assert_not_called()
        original_close=os.close;closed=[]
        def close(fd):closed.append(fd);original_close(fd)
        with patch.object(OriginalMedicalInstallationLease,'__init__',side_effect=cancel),patch('ashlar_host.medical_installation.os.close',side_effect=close):
            try:
                with OriginalMedicalInstallationLease.open(self.selection,self.context):self.fail('Unexpected construction')
            except KeyboardInterrupt as error:self.assertIs(error,cancel)
        self.assertEqual(len(closed),1)
        with self.assertRaises(OSError):os.fstat(closed[0])
        with OriginalMedicalInstallationLease.open(self.selection,self.context):pass
    def test_closing_journal_schema_refusal_still_releases_lock(self):
        db,_=self.setup_reservations();db.close()
        with self.assertRaises(ValueError):
            with OriginalMedicalInstallationLease.open(self.selection,self.context):
                db=sqlite3.connect(self.selection.journal);db.execute('DROP TABLE local_source_plan');db.commit();db.close()
        fd=os.open(self.root/'.evolution-operator.lock',os.O_RDWR)
        try:
            import fcntl
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finally:os.close(fd)

"""Explicit original-medical ordinary sessions; no provisioning or discovery."""
from dataclasses import dataclass, field
import json
import hashlib
from ashlar.source import SourceBatch, SourceRecord
import re
from typing import Callable
from ashlar.outbox import PostgresOutbox, OutboxTransaction
from .ack import AckScope
from .ack_sessions import OutboxAckRegistration, RegisteredOutboxAcks
from .source_sessions import OutboxSourceRegistration, RegisteredOutboxSources
from .existing_outbox_authority import OutboxAuthorityRegistration, ExistingOutboxAuthority, ordinary, one, privileges
from .driver import NativeDriver
from .delta_custody import DeltaTarget, LocalDeltaTransport, encoded
from .lifecycle import finish
from .postgres import Session

FEED='private-original-medical-fixture'
EPOCH='original-medical-v1'

@dataclass(frozen=True, repr=False)
class OriginalMedicalSourcePorts:
    registration: OutboxAuthorityRegistration
    writer_role: str
    writer_connection: Callable = field(compare=False,repr=False)

    def __post_init__(self):
        if type(self.registration) is not OutboxAuthorityRegistration:
            raise ValueError('Exact ordinary original-source registration required')
        r=self.registration
        object.__setattr__(self,'registration',OutboxAuthorityRegistration(r.source,r.source_role,r.ack_role,r.database,r.metadata_factory))
        if (type(self.writer_role) is not str or not re.fullmatch('[a-z][a-z0-9_]{0,62}',self.writer_role)
                or self.writer_role in (r.source_role,r.ack_role) or not callable(self.writer_connection)
                or (r.source.scope.feed,r.source.scope.epoch)!=(FEED,EPOCH)):
            raise ValueError('Separate original writer/reader/ACK roles and exact feed/epoch required')

    def metadata(self):
        self.__post_init__()
        r=self.registration
        return {'source':r.source.metadata(),'source_role':r.source_role,'ack_role':r.ack_role,
                'database':r.database,'writer_role':self.writer_role}

class OriginalMedicalSourceOwner:
    """Construction-bound actual driver; current held native/source correspondence."""
    def __init__(self,ports,lease):
        if type(ports) is not OriginalMedicalSourcePorts:
            raise ValueError('Exact original ports required')
        ports.__post_init__()
        # Lease is the dedicated public owner, imported lazily to avoid a cycle.
        from .medical_installation import OriginalMedicalInstallationLease
        if type(lease) is not OriginalMedicalInstallationLease:
            raise ValueError('Actual original installation lease required')
        self.ports=ports;self.lease=lease;self.original=encoded(ports.metadata());self.driver=None
        self.factories=(ports.writer_connection,ports.registration.source.connection_factory,ports.registration.metadata_factory)
        self._original_factories=self.factories
        r=ports.registration
        self._original_registration=OutboxAuthorityRegistration(r.source,r.source_role,r.ack_role,r.database,r.metadata_factory)
        self.authority=ExistingOutboxAuthority((ports.registration,),self)

    def bind(self,driver):
        if self.driver is not None or type(driver) is not NativeDriver or type(driver.transport) is not LocalDeltaTransport:
            raise ValueError('Single actual driver construction required')
        from .original_medical_publication import MedicalAdmission
        if type(driver.source_admission) is not MedicalAdmission or driver.admission_facts()['profile']!='ashlar-original-medical-public-dataset-admission/0.1':raise ValueError('Actual original public medical admission required')
        if driver.source_admission.batch.feed!=FEED or self.ports.registration.source.scope.installation_id!=self.lease.selection.installation_id:raise PermissionError('Dedicated medical source and installation correspondence required')
        self.lease.verify(driver.context,driver.transport)
        self.driver=driver;self.admission=driver.source_admission
        self._original_driver=driver;self._original_admission=driver.source_admission
        self._original_context=driver.context
        owner=self
        class AckPolicy:
            def admit_scope(self,scope,session,context):
                owner.admit_source_writer(owner.ports.registration.source,context)
                owner.authority.admit_scope(scope,session,context)
            def admit_publication(self,scope,request,resolved,session,context):
                owner.authority.admit_scope(scope,session,context)
                owner.driver.admit_publication(scope,request,resolved,session,context)
        return RegisteredOutboxAcks((OutboxAckRegistration(self.ports.registration.source,
            self.ports.registration.metadata_factory,AckPolicy()),))

    def admit_source_writer(self,registration,context):
        if type(registration) is not OutboxSourceRegistration or self.driver is None:
            raise PermissionError('Complete construction-bound source owner required')
        registration.__post_init__();self.driver.require(context)
        if self.driver.source_admission is not self.admission or self.driver.admission_facts()['profile']!='ashlar-original-medical-public-dataset-admission/0.1':raise PermissionError('Current original medical admission required')
        self.lease.verify(context,self.driver.transport)
        if any(a is not b for a,b in zip(self.factories,(self.ports.writer_connection,self.ports.registration.source.connection_factory,self.ports.registration.metadata_factory))) or encoded(self.ports.metadata())!=self.original or registration.metadata()!=self.ports.registration.source.metadata():
            raise PermissionError('Original ordinary source registration changed')

    def sources(self):
        return RegisteredOutboxSources((self.ports.registration.source,),self.authority)

    def renew_scope(self,context):
        # Current registration drift must never substitute the closing login.
        r=self._original_registration;connection=self._original_factories[2](context);primary=None;
        try:
            self.authority.admit_scope(r.source.scope,Session(connection),context)
        except BaseException as error:primary=error
        finish(primary,[lambda:self.authority.admit_scope(r.source.scope,Session(connection),context)]+[connection.rollback,connection.close])

    def renew_original(self,batch,*,context):
        """Independently renew the captured original reader/ACK on every exit.

        Current binding drift still refuses. It cannot select a foreign factory
        or suppress the independent original ordinary observations and cleanup.
        This readback port grants no append, publication or ACK submission.
        """
        if not hasattr(self,'_original_driver') or not hasattr(self,'_original_admission'):
            raise PermissionError('Actual acquired medical driver required before renewal')
        if context is not self._original_context:
            raise PermissionError('Exact original acquired medical context required')
        r=self._original_registration;owner=self
        class ClosingWriter:
            def admit_source_writer(self,registration,supplied):
                driver=owner._original_driver;admission=owner._original_admission
                if (type(registration)is not OutboxSourceRegistration
                        or registration.metadata()!=r.source.metadata()
                        or type(driver)is not NativeDriver
                        or driver.source_admission is not admission):
                    raise PermissionError('Original acquired medical source binding required')
                driver.require(supplied);owner.lease.verify(supplied,driver.transport)
                if admission.metadata()['profile']!='ashlar-original-medical-public-dataset-admission/0.1':
                    raise PermissionError('Current original medical admission required')
        authority=ExistingOutboxAuthority((r,),ClosingWriter())
        def current():
            if self.driver is not self._original_driver or self.admission is not self._original_admission:
                raise PermissionError('Current acquired medical source owner changed')
            if type(self.factories)is not tuple or len(self.factories)!=3 or any(a is not b for a,b in zip(self.factories,self._original_factories)):
                raise PermissionError('Original acquired medical factories changed')
            self.admit_source_writer(r.source,context)
        def raw():
            if (type(batch)is not SourceBatch or batch is not self._original_admission.batch
                    or type(batch.records)is not tuple or len(batch.records)!=113
                    or type(batch.begin)is not bytes or type(batch.commit)is not bytes
                    or any(type(record)is not SourceRecord or type(record.raw)is not bytes for record in batch.records)
                    or (batch.feed,batch.epoch,batch.batch_id)!=(FEED,EPOCH,EPOCH)):
                raise PermissionError('Actual original medical closing transaction required')
            original=batch.begin+b''.join(record.raw for record in batch.records)+batch.commit
            if len(original)>1048576 or hashlib.sha256(original).hexdigest()!=self._original_admission.metadata()['source_transaction_sha256']:
                raise PermissionError('Complete original medical closing bytes changed')
            return original
        def reader():
            connection=None;session=None;primary=None
            def read():
                if session is None:return
                original=raw()
                self._read_retained(session,r,batch,hashlib.sha256(original).hexdigest())
                ClosingWriter().admit_source_writer(r.source,context)
            try:
                connection=self._original_factories[1](context);session=Session(connection);read()
            except BaseException as error:primary=error
            finish(primary,[read]+([] if connection is None else [connection.rollback,connection.close]))
        def ack():
            connection=None;session=None;primary=None
            def observe():
                if session is not None:authority.admit_scope(r.source.scope,session,context)
            try:
                connection=self._original_factories[2](context);session=Session(connection);observe()
            except BaseException as error:primary=error
            finish(primary,[observe]+([] if connection is None else [connection.rollback,connection.close]))
        finish(None,[current,reader,ack,current])

    @staticmethod
    def _read_retained(session,r,batch,digest):
        ordinary(session,r.source_role,r.database);privileges(session,r,ack=False)
        head=one(session,'SELECT position FROM "'+r.source.source_schema+'".head WHERE id=1',{},('position',))
        if type(head['position']) is not int or head['position']!=1:
            raise ValueError('Settled exact original head one required')
        transactions=PostgresOutbox(session,feed=FEED,epoch=EPOCH,schema=r.source.source_schema).read('0',limit=1)
        if (len(transactions)!=1 or type(transactions[0]) is not OutboxTransaction
                or transactions[0].position!='1' or transactions[0].previous!='0'
                or (transactions[0].profile,transactions[0].feed,transactions[0].epoch)!=('ashlar-postgresql-outbox/0.1',FEED,EPOCH)
                or transactions[0].payload_digest!=digest or transactions[0].batch!=batch):
            raise ValueError('Retained original complete transaction differs')
        return transactions[0]

    def admit_append(self,session,context):
        r=self.ports.registration
        ordinary(session,self.ports.writer_role,r.database)
        allowed=one(session,"SELECT has_function_privilege(current_user,:append,'EXECUTE') AS append",
            {'append':r.source.source_schema+'.append(text,text)'},('append',))
        if type(allowed['append']) is not bool or allowed['append'] is not True:
            raise PermissionError('Current ordinary append privilege required')
        self.admit_source_writer(r.source,context)

    def read_original(self,batch,*,context):
        """Read the settled original transaction; never acquire the append writer."""
        self.admit_source_writer(self.ports.registration.source,context)
        if (type(batch) is not SourceBatch or batch is not self.driver.source_admission.batch
                or type(batch.records) is not tuple or len(batch.records)!=113
                or type(batch.begin) is not bytes or type(batch.commit) is not bytes
                or any(type(record) is not SourceRecord or type(record.raw) is not bytes for record in batch.records)
                or (batch.feed,batch.epoch,batch.batch_id)!=(FEED,EPOCH,EPOCH)):
            raise ValueError('Actual original admitted transaction required')
        raw=batch.begin+b''.join(record.raw for record in batch.records)+batch.commit
        digest=hashlib.sha256(raw).hexdigest()
        if len(raw)>1048576 or digest!=self.driver.admission_facts()['source_transaction_sha256']:
            raise PermissionError('Original admitted complete source bytes changed')
        r=self.ports.registration
        self.renew_scope(context)
        connection=r.source.connection_factory(context);primary=None;transaction=None
        session=None
        def read():
            if session is None:return
            return self._read_retained(session,r,batch,digest)
        try:
            session=Session(connection)
            transaction=read()
            self.admit_source_writer(r.source,context)
        except BaseException as error:primary=error
        finish(primary,[read,lambda:self.admit_source_writer(r.source,context),
            lambda:self.renew_scope(context),connection.rollback,connection.close])
        return transaction

    def append_original(self,batch,*,context):
        # Exact original admission batch is owned by the driver, never caller proof.
        self.admit_source_writer(self.ports.registration.source,context)
        if (type(batch) is not SourceBatch or any(type(x)is not str for x in (batch.feed,batch.epoch,batch.batch_id)) or type(batch.begin)is not bytes or type(batch.commit)is not bytes or type(batch.records)is not tuple or len(batch.records)!=113 or any(type(x)is not SourceRecord or type(x.raw)is not bytes for x in batch.records) or batch is not self.driver.source_admission.batch or (batch.feed,batch.epoch,batch.batch_id)!=(FEED,EPOCH,EPOCH)):
            raise ValueError('Actual original admitted transaction required')
        raw=batch.begin+b''.join(record.raw for record in batch.records)+batch.commit
        if len(raw)>1048576 or hashlib.sha256(raw).hexdigest()!=self.driver.admission_facts()['source_transaction_sha256']:
            raise PermissionError('Original admitted complete source bytes changed')
        r=self.ports.registration
        self.renew_scope(context)
        reader=r.source.connection_factory(context);primary=None;existing=None
        try:
            session=Session(reader);ordinary(session,r.source_role,r.database);privileges(session,r,ack=False)
            head=one(session,'SELECT position FROM "'+r.source.source_schema+'".head WHERE id=1',{},('position',))
            if type(head['position']) is not int or head['position'] not in (0,1):raise ValueError('Only fresh or exact original appended source allowed')
            if head['position']==1:
                transactions=PostgresOutbox(session,feed=FEED,epoch=EPOCH,schema=r.source.source_schema).read('0',limit=1)
                if len(transactions)!=1 or type(transactions[0].position) is not str or transactions[0].position!='1' or transactions[0].batch!=batch:raise ValueError('Retained original append differs')
                existing=transactions[0]
            ordinary(session,r.source_role,r.database);privileges(session,r,ack=False)
            self.admit_source_writer(r.source,context)
        except BaseException as error:primary=error
        finish(primary,[lambda:ordinary(session,r.source_role,r.database),lambda:privileges(session,r,ack=False),lambda:self.admit_source_writer(r.source,context),lambda:self.renew_scope(context),reader.rollback,reader.close])
        if existing is not None:return existing
        connection=self.ports.writer_connection(context);primary=None
        try:
            session=Session(connection)
            self.admit_append(session,context)
            result=one(session,'SELECT "'+r.source.source_schema+'".append(:batch,:payload) AS position',
                {'batch':batch.batch_id,'payload':raw.decode('utf-8')},('position',))
            if type(result['position']) is not int or result['position']!=1:raise ValueError('Exact first original append required')
            self.admit_append(session,context)
            connection.commit();position=1
            self.admit_append(session,context)
        except BaseException as error:primary=error
        finish(primary,[lambda:self.admit_append(session,context),lambda:self.renew_scope(context),connection.rollback,connection.close])
        connection=r.source.connection_factory(context);primary=None;transaction=None
        try:
            session=Session(connection);ordinary(session,r.source_role,r.database);privileges(session,r,ack=False)
            transactions=PostgresOutbox(session,feed=FEED,epoch=EPOCH,schema=r.source.source_schema).read('0',limit=1)
            if len(transactions)!=1 or transactions[0].position!='1' or transactions[0].batch!=batch:
                raise ValueError('Complete committed original source bytes differ')
            transaction=transactions[0]
            ordinary(session,r.source_role,r.database);privileges(session,r,ack=False)
            self.admit_source_writer(r.source,context)
        except BaseException as error:primary=error
        finish(primary,[lambda:ordinary(session,r.source_role,r.database),lambda:privileges(session,r,ack=False),lambda:self.admit_source_writer(r.source,context),lambda:self.renew_scope(context),connection.rollback,connection.close])
        return transaction

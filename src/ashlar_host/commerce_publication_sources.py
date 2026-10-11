"""Explicit original-commerce ordinary sessions; no provisioning or discovery."""
from dataclasses import dataclass, field
import json
import hashlib
from ashlar.source import SourceBatch, SourceRecord
import re
from typing import Callable
from ashlar.outbox import PostgresOutbox
from .ack import AckScope
from .ack_sessions import OutboxAckRegistration, RegisteredOutboxAcks
from .source_sessions import OutboxSourceRegistration, RegisteredOutboxSources
from .existing_outbox_authority import OutboxAuthorityRegistration, ExistingOutboxAuthority, ordinary, one, privileges
from .driver import NativeDriver
from .delta_custody import DeltaTarget, LocalDeltaTransport, encoded
from .lifecycle import finish
from .postgres import Session

FEED='private-original-commerce-fixture'
EPOCH='original-commerce-v1'

@dataclass(frozen=True, repr=False)
class OriginalCommerceSourcePorts:
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

class OriginalCommerceSourceOwner:
    """Construction-bound actual driver; current held native/source correspondence."""
    def __init__(self,ports,lease):
        if type(ports) is not OriginalCommerceSourcePorts:
            raise ValueError('Exact original ports required')
        ports.__post_init__()
        # Lease is the dedicated public owner, imported lazily to avoid a cycle.
        from .original_commerce_publication import OriginalCommerceInstallationLease
        if type(lease) is not OriginalCommerceInstallationLease:
            raise ValueError('Actual original installation lease required')
        self.ports=ports;self.lease=lease;self.original=encoded(ports.metadata());self.driver=None
        self.authority=ExistingOutboxAuthority((ports.registration,),self)

    def bind(self,driver):
        if self.driver is not None or type(driver) is not NativeDriver or type(driver.transport) is not LocalDeltaTransport:
            raise ValueError('Single actual driver construction required')
        from .commerce_admission import CommerceAdmission
        if type(driver.source_admission) is not CommerceAdmission or driver.admission_facts()['profile']!='ashlar-original-commerce-public-dataset-admission/0.1':raise ValueError('Actual original public commerce admission required')
        self.lease.verify(driver.context,driver.transport)
        self.driver=driver
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
        self.lease.verify(context,self.driver.transport)
        if encoded(self.ports.metadata())!=self.original or registration.metadata()!=self.ports.registration.source.metadata():
            raise PermissionError('Original ordinary source registration changed')

    def sources(self):
        return RegisteredOutboxSources((self.ports.registration.source,),self.authority)

    def renew_scope(self,context):
        r=self.ports.registration;connection=r.metadata_factory(context);primary=None;admitted=False
        try:
            self.authority.admit_scope(r.source.scope,Session(connection),context);admitted=True
        except BaseException as error:primary=error
        finish(primary,([lambda:self.authority.admit_scope(r.source.scope,Session(connection),context)] if admitted else [])+[connection.rollback,connection.close])

    def admit_append(self,session,context):
        r=self.ports.registration
        ordinary(session,self.ports.writer_role,r.database)
        allowed=one(session,"SELECT has_function_privilege(current_user,:append,'EXECUTE') AS append",
            {'append':r.source.source_schema+'.append(text,text)'},('append',))
        if type(allowed['append']) is not bool or allowed['append'] is not True:
            raise PermissionError('Current ordinary append privilege required')
        self.admit_source_writer(r.source,context)

    def append_original(self,batch,*,context):
        # Exact original admission batch is owned by the driver, never caller proof.
        self.admit_source_writer(self.ports.registration.source,context)
        if (type(batch) is not SourceBatch or any(type(x)is not str for x in (batch.feed,batch.epoch,batch.batch_id)) or type(batch.begin)is not bytes or type(batch.commit)is not bytes or type(batch.records)is not tuple or len(batch.records)!=21 or any(type(x)is not SourceRecord or type(x.raw)is not bytes for x in batch.records) or batch is not self.driver.source_admission.batch or (batch.feed,batch.epoch,batch.batch_id)!=(FEED,EPOCH,EPOCH)):
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
        finish(primary,[reader.rollback,reader.close])
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
        finish(primary,[connection.rollback,connection.close])
        return transaction

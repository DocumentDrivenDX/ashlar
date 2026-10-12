"""Ordinary public Truss feed sessions and publication-before-ACK composition.

Truss ACK asserts delivery; it does not retain a destination receipt or release
history. Already-advanced source replay is separate from exact-page admission.
"""
import hashlib
from contextlib import contextmanager
from dataclasses import dataclass,field
from types import MappingProxyType
from ashlar.attempt_store import verify_request_digest
from ashlar.manifest import bind_manifest_pins
from ashlar.publication import resolve_publication
from ashlar.schema import decode_retained_json
from ashlar.source_checkpoint import bind_source_descriptor,truss_checkpoint
from ashlar.staging import batch_row
from ashlar.truss_native_feed import native_feed_batch
from .ack import AckOutcomeUncertain
from .lifecycle import finish,owned_context


class TrussSessionError(ValueError):pass


def _sdk():
    from truss import Ok,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host
    from truss.feed import FeedPage,FeedAcknowledged
    return Ok,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host,FeedPage,FeedAcknowledged


def _value(result):
    if not isinstance(result,_sdk()[0]):raise TrussSessionError('Public Truss operation refused')
    return result.value


@contextmanager
def _assembly(connection,readonly):
    _,Configuration,create,adopt,_,_=_sdk()
    if connection.autocommit or connection.info.transaction_status!=0:
        raise TrussSessionError('Fresh ordinary non-autocommit connection required')
    connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
    if readonly:connection.execute('SET TRANSACTION READ ONLY')
    connection.execute("SET LOCAL statement_timeout = '15000ms'")
    host=adopt(connection);assembly=None;primary=None
    try:
        tx=_value(host.adopt_transaction(access_mode='read_only'if readonly else'read_write'))
        assembly=_value(create(Configuration(),host))
        yield assembly,tx
    except BaseException as error:primary=error
    def detach():
        if not host.detach():raise TrussSessionError('Public host detach refused')
    finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])


@dataclass(frozen=True)
class TrussFeedSessions:
    """One original page, explicit ordinary factory and fixed host context.

    No credentials, roles or consumer registrations are discovered or created.
    The destination supplies real pin custody and a publication resolver.
    """
    page:object
    feed:str
    connection_factory:object=field(repr=False,compare=False)
    context:object=field(repr=False,compare=False)

    def __post_init__(self):
        if type(self.page)is not _sdk()[4] or self.page.outcome!='transaction' or not callable(self.connection_factory):
            raise TrussSessionError('Actual complete Truss transaction page and ordinary connection factory required')
        batch=self.batch()
        cursor=decode_retained_json(self.page.prior.content)
        if self.page.consumer_id!=cursor['consumerId'] or self.page.registration_id!=cursor['registrationId'] or self.page.writer_xid!=batch.batch_id:
            raise TrussSessionError('Original page registration/writer differs')

    def batch(self):
        return native_feed_batch(self.page.content,sha256=self.page.sha256,
            prior=self.page.prior.content,next=self.page.next.content,feed=self.feed)

    def metadata(self):
        batch=self.batch()
        return {'profile':'ashlar-truss-feed-sessions/0.1','feed':self.feed,'epoch':batch.epoch,
                'consumer_id':self.page.consumer_id,'registration_id':self.page.registration_id,
                'page_sha256':self.page.sha256,'checkpoint':truss_checkpoint(batch)}

    def _request(self,request,context):
        if context is not self.context:raise PermissionError('Original Truss host context required')
        if type(request)not in (dict,MappingProxyType) or len(request)!=8 or any(type(key)is not str or type(value)is not str or len(value)>4194304 for key,value in request.items()):
            raise TrussSessionError('Exact bounded original request strings required')
        request=dict(request)
        verify_request_digest(request)
        row=batch_row(self.batch())
        if request.get('source_batch_json')!=row['batch_json'] or request.get('source_batch_digest')!=row['batch_digest'] or request.get('source_checkpoint_json')!=truss_checkpoint(self.batch()):
            raise TrussSessionError('Publication request differs from original Truss page')

    def _read(self,connection):
        with _assembly(connection,True)as (assembly,tx):
            page=_value(assembly.read_feed_in_transaction(tx,consumer_id=self.page.consumer_id,
                maximum_facts=4096,maximum_bytes=8388608))
            if type(page)is not _sdk()[4] or page.consumer_id!=self.page.consumer_id or page.registration_id!=self.page.registration_id:
                raise TrussSessionError('Ordinary public source registration differs')
            return page

    def admit(self,request,*,context):
        """Read the same original committed page under current source permissions."""
        self._request(request,context)
        connection=self.connection_factory(context);primary=None
        try:
            current=self._read(connection)
            # The safe frontier is a fresh observation outside original payload
            # bytes; another committed writer may advance it between polls.
            fields=('content','sha256','prior','next','consumer_id','registration_id','writer_xid','outcome')
            if any(getattr(current,key)!=getattr(self.page,key)for key in fields):
                raise TrussSessionError('Current source page differs; advanced replay needs retained interval admission')
        except BaseException as error:primary=error
        finish(primary,[connection.rollback,connection.close])

    def _resolve(self,request,manifest,vector,context,pins,publication_backend,supported_profiles,supported_revisions):
        resolved=resolve_publication(publication_backend,manifest['publication_id'],
            {table:target[0]for table,target in vector.targets.items()},context=context,
            supported_profiles=supported_profiles,supported_revisions=supported_revisions)
        if dict(resolved.descriptor.raw)!=manifest or any(resolved.snapshots[table].version!=version for table,(_,version)in vector.targets.items()):
            raise TrussSessionError('Whole authoritative destination publication differs')
        bind_manifest_pins(resolved.descriptor,vector,{table:target[0]for table,target in vector.targets.items()},authority=vector.authority)
        bind_source_descriptor(request,resolved.descriptor,expected_publication_id=manifest['publication_id'])
        if publication_backend.validate_manifest(manifest,context)is not None:
            raise TrussSessionError('Current destination effects admission incomplete')

    def acknowledge(self,request_bytes,manifest_bytes,vector,*,context,pins,publication_backend,supported_profiles,supported_revisions):
        return self._perform(request_bytes,manifest_bytes,vector,context,pins,publication_backend,
            supported_profiles,supported_revisions,reconcile=False)

    def reconcile(self,request_bytes,manifest_bytes,vector,*,context,pins,publication_backend,supported_profiles,supported_revisions):
        """Observe the exact current cursor on a fresh session; never retry a write."""
        return self._perform(request_bytes,manifest_bytes,vector,context,pins,publication_backend,
            supported_profiles,supported_revisions,reconcile=True)

    def _perform(self,request_bytes,manifest_bytes,vector,context,pins,backend,profiles,revisions,*,reconcile):
        if any(type(raw)is not bytes or not 0<len(raw)<=4194304 for raw in (request_bytes,manifest_bytes)):
            raise TrussSessionError('Bounded original publication bytes required')
        request=decode_retained_json(request_bytes);manifest=decode_retained_json(manifest_bytes)
        self._request(request,context)
        connection=None;primary=None;commit_attempted=False;commit_observed=False;answer=None
        try:
            with owned_context(pins.hold(vector,context=context)):
                self._resolve(request,manifest,vector,context,pins,backend,profiles,revisions)
                connection=self.connection_factory(context)
                if reconcile:
                    current=self._read(connection)
                    if current.prior.content==self.page.prior.content:answer=None
                    elif current.prior.content==self.page.next.content:answer={'checkpoint_observed':True}
                    else:raise TrussSessionError('Source cursor does not prove the exact ACK outcome')
                else:
                    with _assembly(connection,False)as (assembly,tx):
                        acknowledged=_value(assembly.acknowledge_feed_in_transaction(tx,self.page))
                        if type(acknowledged)is not _sdk()[5] or acknowledged.checkpoint!=self.page.next or acknowledged.outcome not in ('advanced','equal'):
                            raise TrussSessionError('Actual source ACK correspondence differs')
                        self._resolve(request,manifest,vector,context,pins,backend,profiles,revisions)
                    commit_attempted=True
                    connection.commit();commit_observed=True
                    answer={'outcome':acknowledged.outcome,'caller_commit_confirmed':True}
                if reconcile:self._resolve(request,manifest,vector,context,pins,backend,profiles,revisions)
        except BaseException as error:primary=error
        try:finish(primary,([connection.rollback,connection.close]if connection is not None else []))
        except BaseException as error:
            if commit_attempted:
                uncertain=AckOutcomeUncertain(request_bytes,manifest_bytes,self.metadata(),
                    commit_attempted=True,commit_observed=commit_observed)
                if not isinstance(error,Exception):
                    error.ack_outcome_uncertain=uncertain
                    raise
                raise uncertain from error
            raise
        if answer is not None:
            answer.update(page_sha256=self.page.sha256,publication_id=manifest['publication_id'],
                request_sha256=hashlib.sha256(request_bytes).hexdigest(),
                scope='Caller-asserted Truss delivery ACK; no destination receipt retention or history release.')
        return answer

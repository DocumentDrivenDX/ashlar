"""Ordinary public Truss feed sessions and publication-before-ACK composition.

Plain Truss ACK asserts delivery without destination receipt retention. Explicit
receipt mode retains exact opaque destination evidence after consumer-owned
publication verification; it does not release history or verify Delta for Truss.
Already-advanced source replay remains separate from exact-page admission.
"""
import base64,gzip,hashlib,io,json
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


def encode_truss_destination_receipt(request_bytes,manifest_bytes,vector):
    """Exact original receipt, or deterministic lossless gzip envelope.

    Small historical 0.1 bytes stay unchanged. The source retains opaque bytes;
    compression never substitutes digest-only evidence or grants authority.
    """
    if any(type(raw)is not bytes or not 0<len(raw)<=4194304 for raw in (request_bytes,manifest_bytes)):
        raise TrussSessionError('Bounded original publication bytes required')
    if not 1<=len(vector.targets)<=64 or any(type(name)is not str or len(name.encode())>4096 for name in vector.targets):
        raise TrussSessionError('Bounded complete original vector required')
    vector_payload={'authority':vector.authority,'scope_kind':vector.scope_kind,'scope_id':vector.scope_id,'custody_digest':vector.custody_digest,
        'targets':{table:list(target)for table,target in vector.targets.items()}}
    vector_raw=json.dumps(vector_payload,sort_keys=True,separators=(',',':')).encode()
    if len(vector_raw)>1048576:raise TrussSessionError('Complete original vector exceeds receipt budget')
    raw=(json.dumps({'profile':'ashlar-truss-destination-receipt/0.1',
        'request_base64':base64.b64encode(request_bytes).decode(),'manifest_base64':base64.b64encode(manifest_bytes).decode(),
        'vector':vector_payload},sort_keys=True,separators=(',',':'))+'\n').encode()
    if len(raw)>16*1048576:raise TrussSessionError('Complete original receipt exceeds raw budget')
    if len(raw)<=1048576:return raw
    output=io.BytesIO()
    with gzip.GzipFile(filename='',mode='wb',fileobj=output,compresslevel=9,mtime=0)as stream:
        stream.write(raw)
    envelope=(json.dumps({'profile':'ashlar-truss-destination-receipt-gzip-base64/0.1',
        'original_bytes':len(raw),'original_sha256':hashlib.sha256(raw).hexdigest(),
        'gzip_base64':base64.b64encode(output.getvalue()).decode()},sort_keys=True,separators=(',',':'))+'\n').encode()
    if len(envelope)>1048576:raise TrussSessionError('Complete compressed destination receipt exceeds public SDK bound')
    return envelope


def receipt_destination_id(value):
    if value is None:return None
    if type(value)is not str or not value or '\0'in value or len(value.encode('utf8'))>1024:
        raise TrussSessionError('Explicit bounded destination receipt identity required')
    return value


def require_truss_receipt_sdk():
    try:
        from truss import FeedAckReceipt,FeedReceiptAcknowledged,FeedAckReconciled
    except ImportError:
        raise TrussSessionError('Selected public Truss SDK lacks destination receipt capability')from None
    return FeedAckReceipt,FeedReceiptAcknowledged,FeedAckReconciled


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
    destination_receipts:bool=False
    destination_id:object=None
    _pending_checkpoint:object=field(default=None,init=False,repr=False,compare=False)
    _committed_checkpoint:object=field(default=None,init=False,repr=False,compare=False)

    def __post_init__(self):
        if type(self.destination_receipts)is not bool:raise TrussSessionError('Explicit destination receipt mode required')
        if self.destination_receipts:
            if receipt_destination_id(self.destination_id)is None:raise TrussSessionError('Explicit destination identity required')
            require_truss_receipt_sdk()
        elif self.destination_id is not None:raise TrussSessionError('Destination identity requires receipt mode')
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
        return {**({'ack_receipt_profile':'ashlar-truss-destination-receipt/0.1','destination_id':self.destination_id}if self.destination_receipts else {}),'profile':'ashlar-truss-feed-sessions/0.1','feed':self.feed,'epoch':batch.epoch,
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

    def _read(self,connection,*,retained=False):
        with _assembly(connection,True)as (assembly,tx):
            page=_value(assembly.read_feed_in_transaction(tx,consumer_id=self.page.consumer_id,
                maximum_facts=4096,maximum_bytes=8388608))
            if type(page)is not _sdk()[4] or page.consumer_id!=self.page.consumer_id or page.registration_id!=self.page.registration_id:
                raise TrussSessionError('Ordinary public source registration differs')
            fields=('content','sha256','prior','next','consumer_id','registration_id','writer_xid','outcome')
            reread=getattr(assembly,'reread_feed_in_transaction',None)
            if retained and any(getattr(page,key)!=getattr(self.page,key)for key in fields) and callable(reread):
                page=_value(reread(tx,self.page,maximum_facts=4096,maximum_bytes=8388608))
                if type(page)is not _sdk()[4] or any(getattr(page,key)!=getattr(self.page,key)for key in fields):
                    raise TrussSessionError('Original retained public Truss page unavailable or differs')
            return page

    def observe_checkpoint(self, *, context):
        """Read the actual current checkpoint; do not reproduce a historical page."""
        if context is not self.context:raise PermissionError('Original Truss host context required')
        connection=self.connection_factory(context);primary=None;checkpoint=None
        try:checkpoint=self._read(connection).prior.content
        except BaseException as error:primary=error
        finish(primary,[connection.rollback,connection.close])
        return checkpoint

    def admit(self,request,*,context):
        """Read the same original committed page under current source permissions."""
        self._request(request,context)
        connection=self.connection_factory(context);primary=None
        try:
            current=self._read(connection,retained=True)
            # The safe frontier is a fresh observation outside original payload
            # bytes; another committed writer may advance it between polls.
            fields=('content','sha256','prior','next','consumer_id','registration_id','writer_xid','outcome')
            committed=(self._committed_checkpoint is not None and current.prior.content==self._committed_checkpoint)
            if not committed and any(getattr(current,key)!=getattr(self.page,key)for key in fields):
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

    def adopt_ack_attempt(self,driver,request,manifest,vector):
        """Adopt only an original live native committed caller-asserted intent."""
        from .truss_ack_journal import admit_truss_ack_attempt
        if driver.source_sessions is not self or driver.ack_sessions is not self or driver.context is not self.context:
            raise TrussSessionError('Original actual ACK driver/session correspondence required')
        admit_truss_ack_attempt(driver,request,manifest,vector)
        if self.observe_checkpoint(context=self.context)!=self.page.next.content:
            raise TrussSessionError('Original advanced ACK checkpoint not observed')
        object.__setattr__(self,'_pending_checkpoint',self.page.next.content)

    def acknowledge(self,request_bytes,manifest_bytes,vector,*,context,pins,publication_backend,supported_profiles,supported_revisions):
        return self._perform(request_bytes,manifest_bytes,vector,context,pins,publication_backend,
            supported_profiles,supported_revisions,reconcile=False)

    def reconcile(self,request_bytes,manifest_bytes,vector,*,context,pins,publication_backend,supported_profiles,supported_revisions):
        """Observe the exact current cursor on a fresh session; never retry a write."""
        return self._perform(request_bytes,manifest_bytes,vector,context,pins,publication_backend,
            supported_profiles,supported_revisions,reconcile=True)

    def _perform(self,request_bytes,manifest_bytes,vector,context,pins,backend,profiles,revisions,*,reconcile):
        if self.destination_receipts:
            return self._perform_receipt(request_bytes,manifest_bytes,vector,context,pins,backend,profiles,revisions,reconcile=reconcile)
        if any(type(raw)is not bytes or not 0<len(raw)<=4194304 for raw in (request_bytes,manifest_bytes)):
            raise TrussSessionError('Bounded original publication bytes required')
        request=decode_retained_json(request_bytes);manifest=decode_retained_json(manifest_bytes)
        self._request(request,context)
        connection=None;primary=None;commit_attempted=False;commit_observed=False;answer=None
        try:
            if reconcile:
                if self._pending_checkpoint is None:
                    raise TrussSessionError('Original actual ACK attempt required; retained-interval replay unavailable')
                connection=self.connection_factory(context)
                current=self._read(connection)
                if current.prior.content==self._pending_checkpoint:
                    object.__setattr__(self,'_committed_checkpoint',self._pending_checkpoint)
                elif current.prior.content!=self.page.prior.content:
                    raise TrussSessionError('Source cursor does not prove the original ACK attempt')
                connection.rollback();connection.close();connection=None
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
                    object.__setattr__(self,'_pending_checkpoint',self.page.next.content)
                    commit_attempted=True
                    connection.commit();commit_observed=True
                    object.__setattr__(self,'_committed_checkpoint',self.page.next.content)
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


    def _perform_receipt(self,request_bytes,manifest_bytes,vector,context,pins,backend,profiles,revisions,*,reconcile):
        if any(type(raw)is not bytes or not 0<len(raw)<=4194304 for raw in (request_bytes,manifest_bytes)):
            raise TrussSessionError('Bounded original publication bytes required')
        request=decode_retained_json(request_bytes);manifest=decode_retained_json(manifest_bytes)
        self._request(request,context)
        destination=self.destination_id
        if type(destination)is not str or not destination or len(destination.encode())>1024:
            raise TrussSessionError('Exact destination installation identity required')
        receipt=encode_truss_destination_receipt(request_bytes,manifest_bytes,vector)
        request_sha=hashlib.sha256(request_bytes).hexdigest();receipt_sha=hashlib.sha256(receipt).hexdigest()
        cursor=decode_retained_json(self.page.prior.content);receipt_type,ack_type,reconcile_type=require_truss_receipt_sdk()
        def exact(retained):
            if (type(retained)is not receipt_type or retained.installation_id!=cursor['installationId']or retained.source_epoch!=cursor['sourceEpoch']
                    or retained.consumer_id!=self.page.consumer_id or retained.registration_id!=self.page.registration_id
                    or retained.prior!=self.page.prior or retained.next!=self.page.next or retained.feed_sha256!=self.page.sha256
                    or retained.destination_id!=destination or retained.request_sha256!=request_sha or retained.publication_id!=manifest['publication_id']
                    or retained.receipt_bytes!=receipt or retained.receipt_sha256!=receipt_sha):raise TrussSessionError('Exact original source-retained destination receipt differs')
        connection=None;primary=None;answer=None;commit_attempted=False;commit_observed=False
        try:
            if reconcile and self._pending_checkpoint is None:raise TrussSessionError('Original actual receipt ACK attempt required')
            with owned_context(pins.hold(vector,context=context)):
                self._resolve(request,manifest,vector,context,pins,backend,profiles,revisions)
                connection=self.connection_factory(context)
                with _assembly(connection,reconcile)as (assembly,tx):
                    method=getattr(assembly,'reconcile_feed_ack_in_transaction'if reconcile else'acknowledge_feed_with_receipt_in_transaction',None)
                    if not callable(method):raise TrussSessionError('Actual source release lacks receipt ACK capability')
                    observed=_value(method(tx,self.page,destination_id=destination,request_sha256=request_sha,publication_id=manifest['publication_id'],receipt_bytes=receipt))
                    if reconcile:
                        if type(observed)is not reconcile_type or observed.outcome not in ('matched','missing','conflicting'):
                            raise TrussSessionError('Actual receipt reconciliation result required')
                        if observed.outcome=='missing':
                            if observed.receipt is not None or observed.checkpoint!=self.page.prior:raise TrussSessionError('Missing original receipt cannot prove original ACK outcome')
                        elif observed.outcome=='conflicting':raise TrussSessionError('Original source ACK receipt conflict')
                        else:exact(observed.receipt);answer={'receipt_observed':True}
                    else:
                        if type(observed)is not ack_type or observed.checkpoint!=self.page.next or observed.outcome not in ('advanced','equal'):
                            raise TrussSessionError('Actual pending receipt ACK correspondence differs')
                        exact(observed.receipt)
                    self._resolve(request,manifest,vector,context,pins,backend,profiles,revisions)
                if not reconcile:
                    object.__setattr__(self,'_pending_checkpoint',self.page.next.content);commit_attempted=True
                    connection.commit();commit_observed=True
                    object.__setattr__(self,'_committed_checkpoint',self.page.next.content)
                    answer={'outcome':observed.outcome,'caller_commit_confirmed':True}
                elif answer is not None:object.__setattr__(self,'_committed_checkpoint',self.page.next.content)
        except BaseException as error:primary=error
        try:finish(primary,([connection.rollback,connection.close]if connection is not None else []))
        except BaseException as error:
            if commit_attempted:
                uncertain=AckOutcomeUncertain(request_bytes,manifest_bytes,self.metadata(),commit_attempted=True,commit_observed=commit_observed)
                if not isinstance(error,Exception):error.ack_outcome_uncertain=uncertain;raise
                raise uncertain from error
            raise
        if answer is not None:answer.update(page_sha256=self.page.sha256,publication_id=manifest['publication_id'],request_sha256=request_sha,
            source_receipt_sha256=receipt_sha,source_receipt_retained=True,
            scope='Exact source-retained opaque destination receipt after consumer-verified durable publication; caller controls source commit; no history release.')
        return answer

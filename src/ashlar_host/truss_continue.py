# SPDX-License-Identifier: MIT OR Apache-2.0
"""Append contiguous public Truss feed pages to an existing owned publication."""
import datetime
import base64
import hashlib
import json
import os
import stat
import time
from pathlib import Path
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.manifest import manifest_pin_vector
from .truss_backend import TrussPublisherBackend
from ashlar.truss_publication import publish_truss_transaction
from .delta_custody import encoded
from .driver import NativeDriver,AttemptExecutor,CarrierPolicy,ManifestPort,progress_union
from .graph_sql import graph_sql_plan
from .lifecycle import owned_context
from .publication_planning import local_effect_plan,truss_request_for
from .truss_ack_journal import admit_truss_ack_attempt
from .truss_catalog_reader import read_historical_catalog_views
from .truss_feed_sessions import TrussFeedSessions
from .truss_graph_admission import TrussGraphAdmission,TrussEmptyIntervalAdmission
from .truss_note_publication import bind_ordinary_connection,read_next_page,validate_graph_edge_orders,graph_expected_rows,native_xxhash64
from .truss_page_archive import encode_truss_page
from .truss_recovery import restore_truss_apply_state


def admit_truss_continuation_head(driver, *, allowed_digest=None):
    """Existing restored head, native committed phase and actual source cursor."""
    active=driver.policy.active;request=active['request'];manifest=active['manifest']
    vector=manifest_pin_vector(manifest,{table:driver.transport.targets[table].uuid
        for table in json.loads(manifest['table_versions_json'])},authority='private-local-process')
    admit_truss_ack_attempt(driver,request,manifest,vector)
    if driver.source_sessions.observe_checkpoint(context=driver.context)!=driver.source_sessions.page.next.content:
        raise ValueError('Original predecessor source checkpoint is not committed')
    if driver.source_sessions.destination_receipts:
        source=driver.source_sessions
        source.adopt_ack_attempt(driver,request,manifest,vector)
        observed=source.reconcile(encoded(request).encode(),encoded(manifest).encode(),vector,
            context=driver.context,pins=driver,publication_backend=driver,
            supported_profiles=[manifest['profile_version']],
            supported_revisions={key:[value]for key,value in json.loads(manifest['schema_revisions_json']).items()})
        if type(observed)is not dict or observed.get('receipt_observed')is not True or observed.get('source_receipt_retained')is not True:
            raise ValueError('Original predecessor destination receipt not retained')
    # Any original source plan without its applied artifact requires resume.
    pending_plan=driver.transport.db.execute('SELECT 1 FROM local_source_plan WHERE request_digest NOT IN (SELECT request_digest FROM local_publication_artifact) AND (? IS NULL OR request_digest!=?) LIMIT 1',(allowed_digest,allowed_digest)).fetchone()
    if pending_plan:
        raise ValueError('Pending original source attempt requires resume')
    token='"predecessor":'+encoded(manifest['publication_id'])+','
    if driver.transport.db.execute('SELECT request_digest FROM local_publication_artifact WHERE instr(request,?)>0 LIMIT 1',(token,)).fetchone():
        raise ValueError('Selected publication already has an original successor')
    table='`'+'`.`'.join(driver.tables['attempts'].split('.'))+'`'
    pending=driver.transport.query('SELECT batch_id,request_digest FROM '+table+' WHERE stream=:stream GROUP BY batch_id,request_digest HAVING NOT array_contains(collect_set(phase),\'committed\') LIMIT 1',{'stream':request['stream']}).rows
    if pending:raise ValueError('Pending original native publication phase requires resume')
    return restore_truss_apply_state(active['expected'])


def _persist(path,raw):
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        if stream.write(raw)!=len(raw):raise OSError('Complete original continuation carrier write required')
        stream.flush();os.fsync(stream.fileno())
    directory_flags=os.O_RDONLY|getattr(os,'O_DIRECTORY',0)
    directory=os.open(Path(path).parent,directory_flags)
    try:os.fsync(directory)
    finally:os.close(directory)


def _private_carrier_bytes(path, *, maximum_bytes):
    descriptor=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
    try:
        info=os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid()
                or stat.S_IMODE(info.st_mode)!=0o600 or not 0<=info.st_size<=maximum_bytes):
            raise ValueError('Existing continuation carrier is not an exact private prepublication byte')
        observed=bytearray()
        while len(observed)<=info.st_size:
            chunk=os.read(descriptor,min(65536,info.st_size+1-len(observed)))
            if not chunk:break
            observed.extend(chunk)
        if len(observed)!=info.st_size:
            raise ValueError('Existing continuation carrier changed during bounded read')
        return bytes(observed)
    finally:os.close(descriptor)


def _persist_or_reuse_exact_prepublication_pair(page_path,page_raw,intent_path,intent_raw,*,allow_reuse,page=None,feed=None):
    """Persist a carrier pair; narrowly reuse a canonical transaction identity with a newer safe frontier."""
    page_path=Path(page_path);intent_path=Path(intent_path)
    page_present=page_path.exists() or page_path.is_symlink()
    intent_present=intent_path.exists() or intent_path.is_symlink()
    if page_present or intent_present:
        if not allow_reuse or not (page_present and intent_present):
            raise ValueError('Original continuation carriers already exist; inspect original attempt')
        try:
            stored_page=_private_carrier_bytes(page_path,maximum_bytes=3*1024*1024)
            stored_intent=_private_carrier_bytes(intent_path,maximum_bytes=65536)
        except OSError as error:
            raise ValueError('Existing continuation carrier is not the exact private prepublication byte') from error
        if page is None or type(feed)is not str or not feed:
            if stored_page!=page_raw or stored_intent!=intent_raw:
                raise ValueError('Existing continuation carrier is not the exact private prepublication byte')
            return stored_page,stored_intent
        from .truss_page_archive import decode_truss_page
        archived=decode_truss_page(stored_page,hashlib.sha256(stored_page).hexdigest(),feed)
        stable_fields=('content','sha256','consumer_id','registration_id','writer_xid','outcome')
        if (archived.outcome!='transaction' or page.outcome!='transaction'
                or any(getattr(archived,name)!=getattr(page,name) for name in stable_fields)
                or archived.prior.content!=page.prior.content or archived.next.content!=page.next.content):
            raise ValueError('Existing continuation archive is not the same transaction page identity')
        def frontier(value):
            if (type(value)is not str or not value.isascii() or not value.isdecimal()
                    or str(int(value))!=value or len(value)>20 or int(value)>18446744073709551615):
                raise ValueError('Canonical bounded Truss safe frontier required')
            return int(value)
        if frontier(page.safe_watermark_xid)<frontier(archived.safe_watermark_xid):
            raise ValueError('Current Truss safe frontier precedes retained transaction-page observation')
        def unique_pairs(items):
            selected={}
            for key,value in items:
                if key in selected:raise ValueError('Duplicate continuation intent member')
                selected[key]=value
            return selected
        expected=json.loads(intent_raw,object_pairs_hook=unique_pairs)
        observed=json.loads(stored_intent,object_pairs_hook=unique_pairs)
        if type(expected)is not dict or type(observed)is not dict:
            raise ValueError('Closed continuation intent required')
        expected['source_page_sha256']=hashlib.sha256(stored_page).hexdigest()
        canonical=(encoded(observed)+'\n').encode()
        if observed!=expected or canonical!=stored_intent:
            raise ValueError('Existing continuation intent does not bind retained page bytes')
        return stored_page,stored_intent
    _persist(page_path,page_raw)
    _persist(intent_path,intent_raw)
    return page_raw,intent_raw


def continue_truss_graph(driver,connection_factory,*,maximum_pages=1,wait_seconds=0,maximum_page_bytes=1048576,archives=(),defer_source_acknowledgement=False,catalog_connection_factory=None,prepublication_source_custody=None,reuse_identical_prepublication_intent=False,export_graph=False):
    """Continue a restored explicit committed predecessor; never initialize/reset.

    Caller holds the existing installation lease and outer transport/Spark/source
    lifetime. Results remain provisional until those owners close successfully.
    """
    if (type(driver)is not NativeDriver or type(driver.source_admission) not in (TrussGraphAdmission,TrussEmptyIntervalAdmission)
            or type(driver.source_sessions)is not TrussFeedSessions or driver.ack_sessions is not driver.source_sessions
            or not callable(connection_factory)or type(driver.policy.active)is not dict):
        raise PermissionError('Actual restored Truss graph continuation owner required')
    if driver.lookup_hash_profile=='spark-xxhash64-typed-key/0.1':
        from .local_current_carrier_profile import XXHASH64_PROFILE,verify_local_current_profile
        from .truss_current_catalog import CURRENT_TRUSS_048_VERSION, CURRENT_TRUSS_049_VERSION, CURRENT_TRUSS_050_VERSION, CURRENT_TRUSS_051_VERSION, CURRENT_TRUSS_052_CANDIDATE_VERSION, CURRENT_TRUSS_053_FRESH_VERSION
        profile=getattr(driver.transport,'installation_profile',None)
        if (not driver.source_admission.current
                or driver.source_admission.current_truss_version not in (
                    CURRENT_TRUSS_048_VERSION, CURRENT_TRUSS_049_VERSION, CURRENT_TRUSS_050_VERSION, CURRENT_TRUSS_051_VERSION, CURRENT_TRUSS_052_CANDIDATE_VERSION, CURRENT_TRUSS_053_FRESH_VERSION)
                or type(profile)is not dict or profile.get('physical_profile')!=XXHASH64_PROFILE
                or profile.get('lookup_hash_profile')!=driver.lookup_hash_profile):
            raise PermissionError('Exact admitted local `.4` continuation profile required')
        verify_local_current_profile(driver.transport.spark,profile,
            tuple(driver.transport.targets.values()),XXHASH64_PROFILE)
    elif driver.lookup_hash_profile!='sha256-json-v1':
        raise PermissionError('Exact supported local continuation hash profile required')
    if type(maximum_pages)is not int or not 1<=maximum_pages<=8 or type(wait_seconds)is not int or not 0<=wait_seconds<=60:
        raise ValueError('Bounded continuation pages1..8 and wait0..60 required')
    if type(maximum_page_bytes)is not int or not 1<=maximum_page_bytes<=2097152:
        raise ValueError('Explicit bounded continuation page bytes required')
    if type(defer_source_acknowledgement)is not bool:
        raise ValueError('Explicit boolean continuation ACK mode required')
    if type(export_graph)is not bool:
        raise ValueError('Explicit boolean original graph export selection required')
    if type(reuse_identical_prepublication_intent)is not bool:
        raise ValueError('Explicit boolean prepublication-intent recovery selection required')
    if defer_source_acknowledgement and maximum_pages!=1:
        raise ValueError('Deferred-ACK continuation is limited to one page')
    if prepublication_source_custody is not None and (
            not callable(prepublication_source_custody) or not defer_source_acknowledgement
            or maximum_pages!=1 or not driver.source_sessions.destination_receipts
            or not driver.source_sessions.destination_id):
        raise ValueError('Per-page protected custody requires one deferred-ACK destination-backed continuation')
    if export_graph and (maximum_pages != 1 or not defer_source_acknowledgement
                         or prepublication_source_custody is None):
        raise ValueError('Original graph export requires one protected deferred-ACK continuation page')
    if reuse_identical_prepublication_intent and (
            not defer_source_acknowledgement or maximum_pages!=1 or prepublication_source_custody is None
            or not driver.source_sessions.destination_receipts or not driver.source_sessions.destination_id
            or driver.lookup_hash_profile!='spark-xxhash64-typed-key/0.1'):
        raise ValueError('Identical prepublication carrier reuse is limited to one protected local `.4` page')
    context=driver.context;source=driver.source_sessions
    with owned_context(driver.writer(driver.policy.active['request']['stream'],context)):
        state=_ready(driver)
        expected=json.loads(encoded(driver.policy.active['expected']))
        progress=json.loads(encoded(driver.policy.active['progress']))
        predecessor=driver.policy.active['publication_id']
    original_predecessor=predecessor;cursor=source.page.next.content
    current_revisions=json.loads(driver.policy.active['manifest']['schema_revisions_json'])
    original_cursor=json.loads(cursor);consumer=source.page.consumer_id;feed=source.feed
    ordinary=bind_ordinary_connection(connection_factory,context);transport=driver.transport
    root=transport.journal_path.parent;deadline=time.monotonic()+wait_seconds;results=[]
    for _ in range(maximum_pages):
        page=read_next_page(connection_factory,consumer,wait_seconds,deadline=deadline,maximum_bytes=maximum_page_bytes)
        if page.prior.content!=cursor or page.consumer_id!=consumer or page.registration_id!=source.page.registration_id:
            raise ValueError('Original contiguous Truss consumer cursor differs')
        current=json.loads(page.prior.content)
        if any(current[key]!=original_cursor[key]for key in ('installationId','sourceEpoch','registrationId','consumerId')):
            raise ValueError('Original Truss installation/source epoch changed')
        if page.outcome=='empty_interval' and page.next.content==page.prior.content:break
        sessions=TrussFeedSessions(page,feed,ordinary,context,destination_receipts=source.destination_receipts,destination_id=source.destination_id);batch=sessions.batch()
        if page.outcome=='empty_interval':
            admission=TrussEmptyIntervalAdmission(batch)
        else:
            from .truss_catalog_reader import read_graph_catalog_evidence
            admission=TrussGraphAdmission(batch,read_graph_catalog_evidence(
                connection_factory,batch,archives,catalog_connection_factory=catalog_connection_factory))
        if driver.lookup_hash_profile=='spark-xxhash64-typed-key/0.1':
            from .truss_current_catalog import CURRENT_TRUSS_048_VERSION, CURRENT_TRUSS_049_VERSION, CURRENT_TRUSS_050_VERSION, CURRENT_TRUSS_051_VERSION, CURRENT_TRUSS_052_CANDIDATE_VERSION, CURRENT_TRUSS_053_FRESH_VERSION
            if not admission.current or admission.current_truss_version not in (
                    CURRENT_TRUSS_048_VERSION, CURRENT_TRUSS_049_VERSION, CURRENT_TRUSS_050_VERSION, CURRENT_TRUSS_051_VERSION, CURRENT_TRUSS_052_CANDIDATE_VERSION, CURRENT_TRUSS_053_FRESH_VERSION):
                raise PermissionError('Local `.4` continuation page requires exact supported current Truss catalog evidence')
        revisions=(current_revisions if page.outcome=='empty_interval' else
            {current['installationId']:str(max(int(value)for value in admission.revisions))}if admission.revisions else {current['installationId']:'0'})
        request=truss_request_for(feed,batch,predecessor,revisions)
        digest=request['request_digest'];publication_id='truss-graph-'+digest
        if transport.db.execute('SELECT 1 FROM local_source_plan WHERE request_digest=?',(digest,)).fetchone():
            raise ValueError('Original continuation attempt already exists; use resume')
        page_path=root/('source-page-'+digest+'.json');page_raw=encode_truss_page(page,feed)
        intent_path=root/('publication-intent-'+digest+'.json')
        recovery_path=None;recovery_sha256=None
        readback_marker_path=None;readback_marker_sha256=None
        intent_raw=(encoded({'request_digest':digest,'publication_id':publication_id,
            'source_page_path':str(page_path),'source_page_sha256':hashlib.sha256(page_raw).hexdigest(),'feed':feed})+'\n').encode()
        page_raw,intent_raw=_persist_or_reuse_exact_prepublication_pair(
            page_path,page_raw,intent_path,intent_raw,allow_reuse=reuse_identical_prepublication_intent,
            page=page if reuse_identical_prepublication_intent else None,
            feed=feed if reuse_identical_prepublication_intent else None)
        selection={'path':str(intent_path),'sha256':hashlib.sha256(intent_raw).hexdigest()}
        try:
            following_driver=NativeDriver.from_existing_truss(transport,driver.policy,context,driver.tables,
                admission.changes,driver.columns,source_admission=admission,source_sessions=sessions,ack_sessions=sessions,
                lookup_hash_profile=driver.lookup_hash_profile)
            stamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
            graph_tables={role:driver.tables[role]for role in driver.columns}
            validate_graph_edge_orders(expected,admission.changes,admission.parsed.event_bytes)
            after,generated=graph_sql_plan(state,batch,graph_tables,materialized_at=stamp,
                schema_policy=admission.admit,schema_transition_policy=admission.admit_transition,native_scalar_profile='typed'if admission.typed else'string',
                lookup_hash_profile=driver.lookup_hash_profile)
            hashes=(native_xxhash64(transport.spark,admission.changes,after)
                    if driver.lookup_hash_profile=='spark-xxhash64-typed-key/0.1' else None)
            following=graph_expected_rows(expected,after,admission.changes,batch,driver.columns,stamp,admission.parsed.event_bytes,admission.parsed.edge_order_keys,lookup_hashes=hashes)
            steps,elisions=local_effect_plan(generated,expected,graph_tables,lookup_hash_profile=driver.lookup_hash_profile)
            following_driver.policy.active={'request':request,'steps':steps,'expected':following,'publication_id':publication_id,
                'previous_progress':progress,'progress':progress_union(progress,json.loads(request['source_checkpoint_json'])),
                'previous_expected':expected,'generated_steps':generated,'elisions':elisions,
                'recorded_at':str((datetime.datetime.fromisoformat(stamp)-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc))//datetime.timedelta(microseconds=1))}
            target=transport.targets[driver.tables['attempts']]
            backend=TrussPublisherBackend(DeltaAttemptStore(AttemptExecutor(following_driver),CarrierPolicy(following_driver,'attempts'),target.table,target.uuid),following_driver,
                lambda original,supplied:ManifestPort(following_driver,original),context=context)
            native_sink=None
            if prepublication_source_custody is not None:
                native_sink=prepublication_source_custody(page=page,feed=feed,batch=batch,
                    admission=admission,request=request,
                    publication_id=publication_id,destination_transport=transport,
                    context=context,source_sessions=sessions)
                if not callable(native_sink):raise ValueError('Continuation source selection must return its postpublication custody sink')
            descriptor=publish_truss_transaction(backend,feed,batch,predecessor=predecessor,schema_revisions_json=request['schema_revisions_json'],context=context,acknowledge_source=not defer_source_acknowledgement)
            manifest=dict(descriptor.raw);result={'manifest':manifest,
                'source_page':{'path':str(page_path),'sha256':hashlib.sha256(page_raw).hexdigest()},
                'publication_intent':selection}
            if defer_source_acknowledgement:
                from .truss_note_publication import validate_deferred_ack_pair
                digest=validate_deferred_ack_pair(request,descriptor,expected_publication_id=publication_id)
                request_raw=encoded(request).encode();manifest_raw=encoded(manifest).encode()
                target_uuids={table:transport.targets[table].uuid
                    for table in json.loads(manifest['table_versions_json'])}
                vector=manifest_pin_vector(manifest,target_uuids,authority='private-local-process')
                from .truss_feed_sessions import encode_truss_destination_receipt
                receipt_raw=encode_truss_destination_receipt(request_raw,manifest_raw,vector)
                recovery_path=root/('publication-readback-pending-'+digest+'.json')
                if recovery_path.exists()or recovery_path.is_symlink():
                    raise ValueError('Original postpublication recovery carrier already exists; inspect original attempt')
                recovery={'profile':'ashlar-truss-deferred-publication-recovery/0.1',
                    'status':'publication_returned_native_readback_pending',
                    'acknowledgement':'not_attempted',
                    'qualification':'Publisher returned a committed destination descriptor. Independent native readback may still be pending; do not ACK or replay. Reopen only under explicit original-attempt recovery.',
                    'consumer_id':consumer,'feed':feed,'destination_id':source.destination_id,
                    'publication_id':descriptor.publication_id,'request_digest':digest,
                    'source_page':{'path':str(page_path),'archive_sha256':hashlib.sha256(page_raw).hexdigest(),
                        'page_sha256':page.sha256,'content_sha256':hashlib.sha256(page.content).hexdigest(),
                        'prior_sha256':hashlib.sha256(page.prior.content).hexdigest(),
                        'next_sha256':hashlib.sha256(page.next.content).hexdigest(),
                        'registration_id':page.registration_id,'writer_xid':page.writer_xid},
                    'request_bytes_base64':base64.b64encode(request_raw).decode(),
                    'request_sha256':hashlib.sha256(request_raw).hexdigest(),
                    'manifest_bytes_base64':base64.b64encode(manifest_raw).decode(),
                    'manifest_sha256':hashlib.sha256(manifest_raw).hexdigest(),'manifest':manifest,
                    'receipt_bytes_base64':base64.b64encode(receipt_raw).decode(),
                    'receipt_sha256':hashlib.sha256(receipt_raw).hexdigest(),
                    'publication_vector':{'authority':vector.authority,'scope_kind':vector.scope_kind,
                        'scope_id':vector.scope_id,'custody_digest':vector.custody_digest,
                        'targets':{table:list(value) for table,value in vector.targets.items()}}}
                recovery_raw=(encoded(recovery)+'\n').encode();_persist(recovery_path,recovery_raw)
                recovery_sha256=hashlib.sha256(recovery_raw).hexdigest()
                result['publication_recovery_receipt']={'path':str(recovery_path),
                    'sha256':recovery_sha256,'status':recovery['status']}
                if native_sink is not None:
                    from .truss_source_custody_selection import _mint_expected_native_post_state
                    native_sink(expected_post_state=_mint_expected_native_post_state(
                        publication_id=publication_id,request_digest=request['request_digest'],
                        following=following,carrier_profile=('ashlar-delta/0.4-xxhash64'
                            if driver.lookup_hash_profile=='spark-xxhash64-typed-key/0.1'
                            else 'ashlar-delta/0.3')),
                        destination_transport=transport,descriptor=descriptor,
                        manifest_bytes=manifest_raw,vector=vector,receipt_bytes=receipt_raw)
                intent={'profile':'ashlar-truss-deferred-destination-receipt-ack/0.1',
                    'acknowledgement':'deferred_not_attempted','consumer_id':consumer,'feed':feed,
                    'destination_id':source.destination_id,'request_digest':digest,
                    'publication_id':descriptor.publication_id,
                    'source_page':{'path':str(page_path),'archive_sha256':hashlib.sha256(page_raw).hexdigest(),
                        'page_sha256':page.sha256,'content_sha256':hashlib.sha256(page.content).hexdigest(),
                        'prior_sha256':hashlib.sha256(page.prior.content).hexdigest(),
                        'next_sha256':hashlib.sha256(page.next.content).hexdigest(),
                        'registration_id':page.registration_id,'writer_xid':page.writer_xid},
                    'request_bytes_base64':base64.b64encode(request_raw).decode(),
                    'request_sha256':hashlib.sha256(request_raw).hexdigest(),
                    'manifest_bytes_base64':base64.b64encode(manifest_raw).decode(),
                    'manifest_sha256':hashlib.sha256(manifest_raw).hexdigest(),'manifest':manifest,
                    'destination_receipt_bytes_base64':base64.b64encode(receipt_raw).decode(),
                    'destination_receipt_sha256':hashlib.sha256(receipt_raw).hexdigest(),
                    'publication_vector':{'authority':vector.authority,'scope_kind':vector.scope_kind,
                        'scope_id':vector.scope_id,'custody_digest':vector.custody_digest,
                        'targets':{table:list(value) for table,value in vector.targets.items()}}}
                deferred_path=root/('deferred-ack-'+digest+'.json')
                if deferred_path.exists()or deferred_path.is_symlink():
                    raise ValueError('Original deferred continuation ACK intent already exists; inspect original attempt')
                intent_raw=(encoded(intent)+'\n').encode();_persist(deferred_path,intent_raw)
                result['deferred_ack_intent']={'path':str(deferred_path),
                    'sha256':hashlib.sha256(intent_raw).hexdigest()}
                if native_sink is not None:
                    marker={'profile':'ashlar-truss-publication-native-readback-complete/0.1',
                        'status':'native_readback_verified_deferred_ack',
                        'acknowledgement':'not_attempted',
                        'supersedes_pending_recovery_only_when_hashes_match':True,
                        'publication_id':descriptor.publication_id,'request_digest':digest,
                        'pending_recovery_receipt':{'path':str(recovery_path),'sha256':recovery_sha256},
                        'deferred_ack_intent':{'path':str(deferred_path),
                            'sha256':hashlib.sha256(intent_raw).hexdigest()},
                        'qualification':'Strict native readback completed and the deferred ACK intent is durable. This marker supersedes the matching pending carrier; it does not assert or perform ACK.'}
                    readback_marker_path=root/('publication-native-readback-complete-'+digest+'.json')
                    if readback_marker_path.exists()or readback_marker_path.is_symlink():
                        raise ValueError('Original native readback completion marker exists; inspect original attempt')
                    marker_raw=(encoded(marker)+'\n').encode();_persist(readback_marker_path,marker_raw)
                    readback_marker_sha256=hashlib.sha256(marker_raw).hexdigest()
                    result['publication_readback_completion']={'path':str(readback_marker_path),
                        'sha256':readback_marker_sha256,'status':marker['status']}
            results.append(result)
            state=after;expected=following;progress=following_driver.policy.active['progress'];predecessor=publication_id;cursor=page.next.content;current_revisions=revisions
        except BaseException as error:
            try:
                error.ashlar_truss_continuation_interruption={
                    'format':'ashlar-truss-continuation-interruption/0.1','outcome':'unconfirmed',
                    'publication_intent':selection,'request_digest':digest,'publication_id':publication_id,
                    'completed_publication_ids':[item['manifest']['publication_id']for item in results],
                    'publication_recovery_receipt':({'path':str(recovery_path),'sha256':recovery_sha256,
                        'status':'publication_returned_native_readback_pending'} if recovery_path is not None
                        else None),
                    'publication_readback_completion':({'path':str(readback_marker_path),
                        'sha256':readback_marker_sha256,'status':'native_readback_verified_deferred_ack'}
                        if readback_marker_path is not None else None),
                    'qualification':'No rollback or automatic retry is asserted. If the recovery receipt exists, the publisher returned: preserve the original attempt and require an explicit readback/reconciliation path before any ACK. An absent receipt leaves publication outcome unconfirmed.'}
            except BaseException:
                pass  # Diagnostic capture never replaces the original failure.
            raise
    result={'predecessor':original_predecessor,'publications':results,
        'source_acknowledgement':('not_attempted_empty_interval' if not results else
            'deferred_not_attempted' if defer_source_acknowledgement else 'completed_by_public_truss_api'),
        'scope':('No continuation page was available; no source ACK was attempted.' if not results else
            'One contiguous page appended to the existing private local Delta installation; source ACK explicitly not attempted and no protected ACK is claimed.' if defer_source_acknowledgement else
            ('Contiguous public Truss pages appended with consumer-verified durable publication before exact source-retained destination receipt ACK; caller controls commit; no history release or Unity Catalog qualification.'if source.destination_receipts else'Contiguous public Truss pages appended to the existing private local Delta installation with publication-before-caller-asserted ACK; no reset/new registration or Unity Catalog qualification.'))}
    if export_graph:
        if not results or 'publication_readback_completion' not in results[-1]:
            raise ValueError('Final continuation has no completed independent native readback')
        from .truss_graph_export import read_original_truss_graph_export
        release,custody=read_original_truss_graph_export(following_driver,with_custody=True)
        result['original_graph_export']={
            'release_base64':base64.b64encode(release.payload).decode('ascii'),
            'release_sha256':release.sha256,
            'custody_base64':base64.b64encode(custody).decode('ascii'),
            'custody_sha256':hashlib.sha256(custody).hexdigest(),
            'engine_execution':False,
            'qualification':'Canonical immutable `.4` release and custody bytes read through the original live Truss owner; output is returned only after the caller closes its owners.'}
    return result


def _ready(driver):
    """Compatibility alias for the public retained-head admission."""
    return admit_truss_continuation_head(driver)

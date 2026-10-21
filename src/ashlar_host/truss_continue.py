# SPDX-License-Identifier: MIT OR Apache-2.0
"""Append contiguous public Truss feed pages to an existing owned publication."""
import datetime
import hashlib
import json
import os
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
from .truss_graph_admission import TrussGraphAdmission
from .truss_note_publication import bind_ordinary_connection,read_next_page,validate_graph_edge_orders,graph_expected_rows
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


def continue_truss_graph(driver,connection_factory,*,maximum_pages=1,wait_seconds=0,maximum_page_bytes=1048576,archives=()):
    """Continue a restored explicit committed predecessor; never initialize/reset.

    Caller holds the existing installation lease and outer transport/Spark/source
    lifetime. Results remain provisional until those owners close successfully.
    """
    if (type(driver)is not NativeDriver or type(driver.source_admission)is not TrussGraphAdmission
            or type(driver.source_sessions)is not TrussFeedSessions or driver.ack_sessions is not driver.source_sessions
            or not callable(connection_factory)or type(driver.policy.active)is not dict):
        raise PermissionError('Actual restored Truss graph continuation owner required')
    if type(maximum_pages)is not int or not 1<=maximum_pages<=8 or type(wait_seconds)is not int or not 0<=wait_seconds<=60:
        raise ValueError('Bounded continuation pages1..8 and wait0..60 required')
    if type(maximum_page_bytes)is not int or not 1<=maximum_page_bytes<=2097152:
        raise ValueError('Explicit bounded continuation page bytes required')
    context=driver.context;source=driver.source_sessions
    with owned_context(driver.writer(driver.policy.active['request']['stream'],context)):
        state=_ready(driver)
        expected=json.loads(encoded(driver.policy.active['expected']))
        progress=json.loads(encoded(driver.policy.active['progress']))
        predecessor=driver.policy.active['publication_id']
    original_predecessor=predecessor;cursor=source.page.next.content
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
        if page.outcome=='empty_interval':break
        sessions=TrussFeedSessions(page,feed,ordinary,context,destination_receipts=source.destination_receipts,destination_id=source.destination_id);batch=sessions.batch()
        from .truss_catalog_reader import read_graph_catalog_evidence
        admission=TrussGraphAdmission(batch,read_graph_catalog_evidence(connection_factory,batch,archives))
        revisions={current['installationId']:str(max(int(value)for value in admission.revisions))}if admission.revisions else {current['installationId']:'0'}
        request=truss_request_for(feed,batch,predecessor,revisions)
        digest=request['request_digest'];publication_id='truss-graph-'+digest
        if transport.db.execute('SELECT 1 FROM local_source_plan WHERE request_digest=?',(digest,)).fetchone():
            raise ValueError('Original continuation attempt already exists; use resume')
        page_path=root/('source-page-'+digest+'.json');page_raw=encode_truss_page(page,feed)
        intent_path=root/('publication-intent-'+digest+'.json')
        if page_path.exists()or page_path.is_symlink()or intent_path.exists()or intent_path.is_symlink():
            raise ValueError('Original continuation carriers already exist; inspect original attempt')
        _persist(page_path,page_raw)
        intent_raw=(encoded({'request_digest':digest,'publication_id':publication_id,
            'source_page_path':str(page_path),'source_page_sha256':hashlib.sha256(page_raw).hexdigest(),'feed':feed})+'\n').encode()
        _persist(intent_path,intent_raw)
        selection={'path':str(intent_path),'sha256':hashlib.sha256(intent_raw).hexdigest()}
        try:
            following_driver=NativeDriver.from_existing_truss(transport,driver.policy,context,driver.tables,
                admission.changes,driver.columns,source_admission=admission,source_sessions=sessions,ack_sessions=sessions)
            stamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
            graph_tables={role:driver.tables[role]for role in driver.columns}
            validate_graph_edge_orders(expected,admission.changes,admission.parsed.event_bytes)
            after,generated=graph_sql_plan(state,batch,graph_tables,materialized_at=stamp,
                schema_policy=admission.admit,schema_transition_policy=admission.admit_transition,native_scalar_profile='typed'if admission.typed else'string')
            following=graph_expected_rows(expected,after,admission.changes,batch,driver.columns,stamp,admission.parsed.event_bytes,admission.parsed.edge_order_keys)
            steps,elisions=local_effect_plan(generated,expected,graph_tables)
            following_driver.policy.active={'request':request,'steps':steps,'expected':following,'publication_id':publication_id,
                'previous_progress':progress,'progress':progress_union(progress,json.loads(request['source_checkpoint_json'])),
                'previous_expected':expected,'generated_steps':generated,'elisions':elisions,
                'recorded_at':str((datetime.datetime.fromisoformat(stamp)-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc))//datetime.timedelta(microseconds=1))}
            target=transport.targets[driver.tables['attempts']]
            backend=TrussPublisherBackend(DeltaAttemptStore(AttemptExecutor(following_driver),CarrierPolicy(following_driver,'attempts'),target.table,target.uuid),following_driver,
                lambda original,supplied:ManifestPort(following_driver,original),context=context)
            descriptor=publish_truss_transaction(backend,feed,batch,predecessor=predecessor,schema_revisions_json=request['schema_revisions_json'],context=context)
            results.append({'manifest':dict(descriptor.raw),'source_page':{'path':str(page_path),'sha256':hashlib.sha256(page_raw).hexdigest()},
                'publication_intent':selection})
            state=after;expected=following;progress=following_driver.policy.active['progress'];predecessor=publication_id;cursor=page.next.content
        except BaseException as error:
            try:
                error.ashlar_truss_continuation_interruption={
                    'format':'ashlar-truss-continuation-interruption/0.1','outcome':'unconfirmed',
                    'publication_intent':selection,'request_digest':digest,'publication_id':publication_id,
                    'completed_publication_ids':[item['manifest']['publication_id']for item in results],
                    'qualification':'Saved original selection only; publication/ACK outcome requires actual original resume admission. No rollback or recovery authority is asserted.'}
            except BaseException:
                pass  # Diagnostic capture never replaces the original failure.
            raise
    return {'predecessor':original_predecessor,'publications':results,'scope':('Contiguous public Truss pages appended with consumer-verified durable publication before exact source-retained destination receipt ACK; caller controls commit; no history release or Unity Catalog qualification.'if source.destination_receipts else'Contiguous public Truss pages appended to the existing private local Delta installation with publication-before-caller-asserted ACK; no reset/new registration or Unity Catalog qualification.')}


def _ready(driver):
    """Compatibility alias for the public retained-head admission."""
    return admit_truss_continuation_head(driver)

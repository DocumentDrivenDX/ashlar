# SPDX-License-Identifier: MIT OR Apache-2.0
"""First-plan publication of an original saved, pre-phase Truss selection."""
import datetime,hashlib,json,os
from pathlib import Path
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.truss_publication import publish_truss_transaction
from .delta_custody import encoded
from .driver import NativeDriver,AttemptExecutor,CarrierPolicy,ManifestPort,progress_union
from .evolution_producer import snapshot
from .graph_sql import graph_sql_plan
from .lifecycle import owned_context
from .private_graph_custody import decode_custody_bytes,require_custody_digest
from .publication_planning import truss_request_for,local_effect_plan
from .truss_backend import TrussPublisherBackend
from .truss_catalog_reader import read_historical_catalog_views
from .truss_continue import admit_truss_continuation_head
from .truss_feed_sessions import TrussFeedSessions
from .truss_graph_admission import TrussGraphAdmission
from .truss_note_publication import bind_ordinary_connection,validate_graph_edge_orders,graph_expected_rows
from .truss_page_archive import decode_truss_page, MAXIMUM_ARCHIVE_BYTES


def retain_first_truss_planning(path,selection,request,predecessor):
    """Retain the first planning timestamp; never replace a retained candidate."""
    required={'profile','selection','request','predecessor','recorded_at'}
    value={'profile':'ashlar-truss-first-planning/0.1','selection':selection,'request':request,'predecessor':predecessor}
    if path.exists()or path.is_symlink():
        original=decode_custody_bytes(snapshot(path,1048576))
        if (type(original)is not dict or set(original)!=required or any(original[key]!=item for key,item in value.items())
                or type(original['recorded_at'])is not str or not original['recorded_at'].isdigit()or len(original['recorded_at'])>20):
            raise ValueError('Original retained first Truss planning differs')
        return original
    stamp=datetime.datetime.now(datetime.timezone.utc)
    value['recorded_at']=str((stamp-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc))//datetime.timedelta(microseconds=1))
    raw=(encoded(value)+'\n').encode()
    if len(raw)>1048576:raise ValueError('Bounded original first planning required')
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(raw)!=len(raw):raise OSError('Complete original first planning write required')
        stream.flush();os.fsync(stream.fileno())
    return value


def require_saved_planning_for_existing_plans(transport,digest,path):
    """A retained first plan cannot acquire a replacement planning timestamp."""
    existing=transport.db.execute('SELECT 1 FROM local_source_plan WHERE request_digest=?',(digest,)).fetchone()or transport.db.execute('SELECT 1 FROM local_plan WHERE operation=?',('effects:'+digest,)).fetchone()
    if existing and (not path.is_file()or path.is_symlink()):
        raise ValueError('Existing original plans require retained first planning companion')
    return bool(existing)


def publish_saved_truss_selection(driver,connection_factory,intent_path,intent_sha256,*,archives=()):
    """First planning under positive prior/source admission; not plan recovery.

    Caller owns the existing installation lease and source/transport/Spark. A
    Retained phases require normal resume. Exact pre-phase plans require their
    original planning companion and unchanged native prior; no replacement.
    The archived page and selection are never recreated or replaced.
    """
    if type(driver)is not NativeDriver or not callable(connection_factory):raise PermissionError('Actual committed predecessor owner required')
    require_custody_digest(intent_sha256);intent_path=Path(intent_path);root=driver.transport.journal_path.parent
    if not intent_path.is_absolute()or intent_path.resolve()!=intent_path or intent_path.parent!=root:
        raise ValueError('Original canonical saved publication intent required')
    raw=snapshot(intent_path,65536)
    if hashlib.sha256(raw).hexdigest()!=intent_sha256:raise ValueError('Original saved publication intent digest differs')
    intent=decode_custody_bytes(raw)
    if type(intent)is not dict or set(intent)!={'request_digest','publication_id','source_page_path','source_page_sha256','feed'}:
        raise ValueError('Exact original saved publication selection required')
    require_custody_digest(intent['request_digest']);require_custody_digest(intent['source_page_sha256'])
    if intent['publication_id']!='truss-graph-'+intent['request_digest']:raise ValueError('Original continuation publication identity differs')
    page_path=Path(intent['source_page_path'])
    if not page_path.is_absolute()or page_path.resolve()!=page_path or page_path.parent!=root:raise ValueError('Original canonical saved page required')
    page=decode_truss_page(snapshot(page_path,MAXIMUM_ARCHIVE_BYTES),intent['source_page_sha256'],intent['feed'])
    context=driver.context;source=driver.source_sessions;transport=driver.transport
    planning_path=Path(str(intent_path)+'-planning.json')
    require_saved_planning_for_existing_plans(transport,intent['request_digest'],planning_path)
    with owned_context(driver.writer(driver.policy.active['request']['stream'],context)):
        state=admit_truss_continuation_head(driver,allowed_digest=intent['request_digest'])
        previous=json.loads(encoded(driver.policy.active['expected']));progress=json.loads(encoded(driver.policy.active['progress']))
        predecessor=driver.policy.active['publication_id'];manifest=driver.policy.active['manifest']
        if (page.prior.content!=source.page.next.content or page.consumer_id!=source.page.consumer_id
                or page.registration_id!=source.page.registration_id or intent['feed']!=source.feed):raise ValueError('Original saved contiguous source page differs')
        # Equality to a positively admitted immutable predecessor is required;
        # missing phase/journal records alone never authorize a first submission.
        for table,version in json.loads(manifest['table_versions_json']).items():
            if int(transport.original_history(transport.targets[table])[0]['version'])!=version:
                raise ValueError('Original saved predecessor current native vector differs')
    sessions=TrussFeedSessions(page,intent['feed'],bind_ordinary_connection(connection_factory,context),context)
    from .truss_catalog_reader import read_graph_catalog_evidence
    admission=TrussGraphAdmission(sessions.batch(),read_graph_catalog_evidence(connection_factory,sessions.batch(),archives))
    installation=json.loads(page.prior.content)['installationId']
    revisions={installation:str(max(int(value)for value in admission.revisions))}if admission.revisions else {installation:'0'}
    request=truss_request_for(intent['feed'],sessions.batch(),predecessor,revisions)
    if request['request_digest']!=intent['request_digest']:raise ValueError('Original saved request/predecessor differs')
    following=NativeDriver.from_existing_truss(transport,driver.policy,context,driver.tables,admission.changes,driver.columns,
        source_admission=admission,source_sessions=sessions,ack_sessions=sessions)
    target=transport.targets[driver.tables['attempts']]
    attempts=DeltaAttemptStore(AttemptExecutor(following),CarrierPolicy(following,'attempts'),target.table,target.uuid)
    with owned_context(following.writer(intent['feed'],context)):
        for table,version in json.loads(manifest['table_versions_json']).items():
            if int(transport.original_history(transport.targets[table])[0]['version'])!=version:
                raise ValueError('Original saved predecessor current native vector differs')
        if sessions.observe_checkpoint(context=context)!=page.prior.content:raise ValueError('Saved first-plan source checkpoint is no longer pending')
        if sessions.admit(request,context=context)is not None:raise PermissionError('Original saved live source admission incomplete')
        with owned_context(attempts.session(context))as held:
            if held.read(request['stream'],request['batch_id']):raise ValueError('Original saved phase exists; use resume')
        require_saved_planning_for_existing_plans(transport,intent['request_digest'],planning_path)
        planning=retain_first_truss_planning(planning_path,{'path':str(intent_path),'sha256':intent_sha256},request,predecessor)
        stamp=(datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)+datetime.timedelta(microseconds=int(planning['recorded_at']))).isoformat()
        graph_tables={role:driver.tables[role]for role in driver.columns}
        validate_graph_edge_orders(previous,admission.changes,admission.parsed.event_bytes)
        after,generated=graph_sql_plan(state,sessions.batch(),graph_tables,materialized_at=stamp,schema_policy=admission.admit,schema_transition_policy=admission.admit_transition)
        expected=graph_expected_rows(previous,after,admission.changes,sessions.batch(),driver.columns,stamp,admission.parsed.event_bytes,admission.parsed.edge_order_keys)
        steps,elisions=local_effect_plan(generated,previous,graph_tables)
        following.policy.active={'request':request,'steps':steps,'generated_steps':generated,'elisions':elisions,'previous_expected':previous,
            'expected':expected,'previous_progress':progress,'progress':progress_union(progress,json.loads(request['source_checkpoint_json'])),
            'publication_id':intent['publication_id'],'recorded_at':planning['recorded_at']}
        following.prepare_truss_apply(request,context)
    backend=TrussPublisherBackend(attempts,following,lambda original,supplied:ManifestPort(following,original),context=context)
    return publish_truss_transaction(backend,intent['feed'],sessions.batch(),predecessor=predecessor,schema_revisions_json=request['schema_revisions_json'],context=context)

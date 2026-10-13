"""Applied Truss artifact restoration; original plans only, no new source authority."""
import base64
import datetime
import json
from types import MappingProxyType
from ashlar.apply import ApplyState, Change, EntityKey, EntityState, plan_apply
from ashlar.stored_publisher import validate_applied_artifact
from .delta_custody import encoded, sha, LocalDeltaEffects
from .driver import NativeDriver, progress_union
from .private_graph_custody import decode_custody_bytes, require_custody_digest
from .truss_graph_admission import TrussGraphAdmission
from .truss_note_publication import graph_expected_rows


def _retained_state(previous):
    current = {}; history = {}; tombstones = {}; deliveries = {}
    for row in previous['whole_source_history']:
        value = decode_custody_bytes(row['change_json'].encode()); state = value['state']; key = EntityKey(**state['key'])
        endpoints = state['endpoints']
        entity = EntityState(key, state['version'], state['schema_revision'], state['props_json'], state['retained_json'],
            None if endpoints is None else tuple(EntityKey(**item) for item in endpoints))
        change = Change(value['feed'], value['epoch'], value['delivery_id'], value['raw_digest'], value['operation'], entity)
        raw = base64.b64decode(row['raw_base64'], validate=True)
        if (sha(raw.decode()) != change.raw_digest or row['digest'] != change.raw_digest
                or (row['feed'],row['epoch'],row['delivery_id']) != (change.feed,change.epoch,change.delivery_id)):
            raise ValueError('Original prior lifecycle history digest differs')
        delivery = (change.feed,change.epoch,change.delivery_id)
        if delivery in deliveries or (key,entity.version) in history:
            raise ValueError('Duplicate original prior lifecycle history')
        history[key,entity.version] = change; deliveries[delivery] = change
        if change.operation == 'delete': current.pop(key,None); tombstones[key] = change
        else: current[key] = entity
    physical = {}
    for kind, role, typed in (('object','object_current','type_id'),('edge','edge_current','rel_type_id')):
        for row in previous[role]:
            key = EntityKey(row['source_system'],kind,int(row[typed]),int(row['id']))
            endpoints = None if kind == 'object' else tuple(EntityKey(key.source,'object',int(row[prefix+'_type']),int(row[prefix+'_id'])) for prefix in ('source','target'))
            entity = EntityState(key,int(row['entity_version']),row['schema_revision'],row['props_json'],row['retained_json'],endpoints)
            if key in physical: raise ValueError('Duplicate retained current entity')
            physical[key] = entity
    if physical != current: raise ValueError('Original complete prior history/current differs')
    return ApplyState(*[MappingProxyType(item) for item in (current,history,tombstones,deliveries)])


def restore_truss_apply_state(expected):
    """Reconstruct retained semantic state after the owner validates native bags.

    Original history/current correspondence alone grants no source or publication
    authority. Call only inside an actual validated publication owner interval.
    """
    return _retained_state(expected)


def _restore_truss_publication(driver, request_digest, *, interrupted=False, prepared=False):
    """Restore policy from retained applied custody; live validation remains required.

    Prepared restoration observes only; interrupted restoration recovers original
    effects. Neither path publishes or acknowledges.
    """
    require_custody_digest(request_digest)
    if type(driver) is not NativeDriver or type(driver.source_admission) is not TrussGraphAdmission:
        raise PermissionError('Actual reopened Truss graph driver required')
    driver.require(driver.context)
    db = driver.transport.db
    def selected(table, columns):
        rows = db.execute('SELECT '+columns+' FROM '+table+' WHERE request_digest=?',(request_digest,)).fetchall()
        if len(rows) != 1 or any(type(item) is not str or len(item.encode()) > 4194304 for item in rows[0]):
            raise ValueError('Complete bounded original applied Truss custody required')
        return rows[0]
    plan_raw, = selected('local_source_plan','original')
    plan = decode_custody_bytes(plan_raw.encode())
    required = {'request','generated_steps','selected_steps','zero_match_elisions','complete_prior_oracle','observed_native_prior'}
    if set(plan) not in (required, required | {'publication_intent'}):
        raise ValueError('Original Truss source plan inventory differs')
    intent = plan.get('publication_intent')
    if interrupted:
        if db.execute('SELECT 1 FROM local_publication_artifact WHERE request_digest=?',(request_digest,)).fetchone():
            raise ValueError('Applied artifact requires applied restoration')
        if (type(intent) is not dict or set(intent) != {'profile','publication_id','recorded_at','previous_progress','progress','source_admission','source_oracle_sha256'}
                or intent['profile'] != 'ashlar-truss-publication-intent/0.1'):
            raise ValueError('Original durable Truss publication intent required')
        request = plan['request']
        manifest = {'publication_id':intent['publication_id'],'recorded_at':intent['recorded_at'],
            'source_progress_json':encoded(intent['progress'])}
        report = {'original_source_plan_sha256':sha(plan_raw),'source_admission':intent['source_admission'],
            'source_oracle_sha256':intent['source_oracle_sha256'],'zero_match_elisions':plan['zero_match_elisions']}
        descriptor = None
    else:
        request_raw, artifact_raw = selected('local_publication_artifact','request,artifact')
        request = decode_custody_bytes(request_raw.encode())
        artifact, descriptor = validate_applied_artifact(artifact_raw,request)
        manifest = artifact['manifest']; report = json.loads(manifest['validation_report_json'])
    if plan['request'] != request or request['request_digest'] != request_digest:
        raise ValueError('Original Truss source plan/request differs')
    driver.source_sessions._request(request,driver.context)
    if report['original_source_plan_sha256'] != sha(plan_raw) or report['source_admission'] != driver.admission_facts():
        raise ValueError('Original applied source plan/admission differs')
    original, digest, _ = LocalDeltaEffects(driver.transport).original_plan('effects:'+request_digest,request_digest,plan['selected_steps'])
    if db.execute('SELECT original,digest FROM local_plan WHERE operation=?',('effects:'+request_digest,)).fetchall() != [(original,digest)]:
        raise ValueError('Original complete operation plan differs')
    previous = plan['complete_prior_oracle']; prior = _retained_state(previous)
    admission = driver.source_admission
    after = plan_apply(prior, admission.changes, schema_policy=admission.admit, schema_transition_policy=admission.admit_transition)
    stamp = datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)+datetime.timedelta(microseconds=int(manifest['recorded_at']))
    following = graph_expected_rows(previous,after,admission.changes,admission.batch,driver.columns,stamp.isoformat(),admission.parsed.event_bytes,admission.parsed.edge_order_keys)
    if report['source_oracle_sha256'] != sha(encoded(following)) or report['zero_match_elisions'] != plan['zero_match_elisions']:
        raise ValueError('Independently restored original source oracle differs')
    previous_progress = {}
    if interrupted:
        previous_progress = intent['previous_progress']
    elif request['predecessor'] != 'explicit-truss-genesis':
        candidates = db.execute(
            "SELECT artifact FROM local_publication_artifact WHERE json_extract(artifact,'$.manifest.publication_id')=? LIMIT 2",
            (request['predecessor'],)).fetchall()
        if len(candidates) != 1 or type(candidates[0][0]) is not str or len(candidates[0][0].encode()) > 4194304:
            raise ValueError('Original predecessor publication custody required')
        previous_progress = json.loads(json.loads(candidates[0][0])['manifest']['source_progress_json'])
    progress = progress_union(previous_progress,json.loads(request['source_checkpoint_json']))
    if progress != json.loads(manifest['source_progress_json']): raise ValueError('Original complete source progress differs')
    active = {'request':request,'steps':plan['selected_steps'],'generated_steps':plan['generated_steps'],
        'elisions':plan['zero_match_elisions'],'previous_expected':previous,'expected':following,
        'previous_progress':previous_progress,'progress':progress,'publication_id':manifest['publication_id'],
        'recorded_at':manifest['recorded_at']}
    if not interrupted: active['manifest'] = manifest
    previous_active = driver.policy.active
    driver.policy.active = active
    try:
        driver.source_admit(request)
        if prepared:
            driver.observe_prepared_truss_apply(request, driver.context)
        elif interrupted:
            artifact_raw = driver.recover_apply(request, driver.context)
            _, descriptor = validate_applied_artifact(artifact_raw, request)
        else:
            driver.validate_manifest(manifest, driver.context)
    except BaseException:
        driver.policy.active = previous_active
        raise
    return descriptor


def restore_applied_truss_publication(driver, request_digest):
    """Restore an original applied artifact; never execute or acknowledge."""
    return _restore_truss_publication(driver, request_digest)


def restore_prepared_truss_publication(driver, request_digest):
    """Restore retained intent and unchanged native prior without effects."""
    return _restore_truss_publication(driver, request_digest, interrupted=True, prepared=True)


def recover_interrupted_truss_apply(driver, request_digest):
    """Recover only originally retained effects inside the caller's live writer.

    Requires a durable original publication intent and operation plan. Never
    manufactures a replacement source/effect plan, publishes, or acknowledges.
    """
    return _restore_truss_publication(driver, request_digest, interrupted=True)

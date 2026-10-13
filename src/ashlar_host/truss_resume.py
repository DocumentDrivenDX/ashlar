"""Resume an original Truss publication while its source checkpoint is pending."""
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.stored_publisher import StoredPublisherBackend
from ashlar.truss_publication import publish_truss_transaction
from .driver import NativeDriver, AttemptExecutor, CarrierPolicy, ManifestPort
from .lifecycle import owned_context
from .private_graph_custody import decode_custody_bytes, require_custody_digest
from .truss_feed_sessions import TrussFeedSessions
from .truss_recovery import restore_applied_truss_publication, recover_interrupted_truss_apply, restore_prepared_truss_publication


def resume_truss_publication(driver, request_digest):
    """Use retained phases/plans and genuine coordinator, never a fresh attempt.

    Advanced checkpoints require exact retained ACK intent and native committed
    custody. Caller owns the transport and enclosing source lifetime.
    """
    require_custody_digest(request_digest)
    if (type(driver) is not NativeDriver or type(driver.source_sessions) is not TrussFeedSessions
            or driver.ack_sessions is not driver.source_sessions):
        raise PermissionError('Original reopened Truss source and ACK owner required')
    rows = driver.transport.db.execute('SELECT original FROM local_source_plan WHERE request_digest=?',(request_digest,)).fetchall()
    if len(rows) != 1 or type(rows[0][0]) is not str or len(rows[0][0].encode()) > 4194304:
        raise ValueError('Original bounded Truss source plan required')
    request = decode_custody_bytes(rows[0][0].encode())['request']
    if request['request_digest'] != request_digest:
        raise ValueError('Selected original Truss request differs')
    source = driver.source_sessions
    source._request(request,driver.context)
    target = driver.transport.targets[driver.tables['attempts']]
    attempts = DeltaAttemptStore(AttemptExecutor(driver),CarrierPolicy(driver,'attempts'),target.table,target.uuid)
    with owned_context(driver.writer(request['stream'],driver.context)):
        # Observe the actual current cursor, never a historical-page reproduction.
        checkpoint = source.observe_checkpoint(context=driver.context)
        if checkpoint not in (source.page.prior.content,source.page.next.content):
            raise ValueError('Original Truss source checkpoint differs')
        with owned_context(attempts.session(driver.context)) as session:
            records = session.read(request['stream'],request['batch_id'])
        if not records or records[-1].phase not in ('prepared','applying','applied','committing','committed'):
            raise ValueError('Original interrupted publication phase required')
        if any(record.request_digest != request_digest or decode_custody_bytes(record.payload_json.encode())['request'] != request for record in records):
            raise ValueError('Original retained publication phases differ')
        if checkpoint == source.page.next.content and records[-1].phase != 'committed':
            raise ValueError('Advanced Truss checkpoint requires original committed phase')
        applied = driver.transport.db.execute('SELECT 1 FROM local_publication_artifact WHERE request_digest=?',(request_digest,)).fetchone()
        if checkpoint == source.page.next.content and not applied:
            raise ValueError('Advanced Truss checkpoint requires original applied artifact')
        if records[-1].phase == 'prepared':
            if applied: raise ValueError('Prepared phase contradicts original applied artifact')
            restore_prepared_truss_publication(driver,request_digest)
        elif applied: restore_applied_truss_publication(driver,request_digest)
        else: recover_interrupted_truss_apply(driver,request_digest)
        if checkpoint == source.page.next.content:
            from ashlar.manifest import manifest_pin_vector
            manifest = driver.policy.active['manifest']
            import json
            vector = manifest_pin_vector(manifest,{table:driver.transport.targets[table].uuid for table in json.loads(manifest['table_versions_json'])},authority='private-local-process')
            source.adopt_ack_attempt(driver,request,manifest,vector)
    # The coordinator acquires its own writer; no nested ownership interval.
    backend = StoredPublisherBackend(attempts,driver,lambda original,supplied:ManifestPort(driver,original))
    return publish_truss_transaction(backend,request['stream'],source.batch(),
        predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=driver.context)

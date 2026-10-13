"""Retained caller-asserted Truss ACK intent; never a protected ACK receipt."""
from ashlar.attempt_store import DeltaAttemptStore
from .delta_custody import encoded, sha
from .lifecycle import owned_context
from .private_graph_custody import decode_custody_bytes


def _intent(driver, request, manifest, vector):
    from .driver import NativeDriver
    from .truss_feed_sessions import TrussFeedSessions
    if (type(driver) is not NativeDriver or type(driver.source_sessions) is not TrussFeedSessions
            or driver.ack_sessions is not driver.source_sessions):
        raise PermissionError('Original actual Truss ACK owner required')
    driver.require(driver.context)
    return {'profile':'ashlar-truss-caller-asserted-ack-intent/0.1','request':dict(request),
        'manifest':dict(manifest),'vector':{'authority':vector.authority,'scope_kind':vector.scope_kind,
        'scope_id':vector.scope_id,'custody_digest':vector.custody_digest,
        'targets':{table:list(target) for table,target in vector.targets.items()}},
        'sessions':driver.source_sessions.metadata()}


def retain_truss_ack_attempt(driver, request, manifest, vector):
    """Commit original intent before SDK ACK, refusing replacement bytes."""
    original = encoded(_intent(driver,request,manifest,vector))
    if len(original.encode()) > 4194304: raise ValueError('Bounded original Truss ACK intent required')
    key='truss-ack:'+request['request_digest'];db=driver.transport.db
    rows=db.execute('SELECT original,digest FROM local_plan WHERE operation=?',(key,)).fetchall()
    if not rows:
        with db: db.execute('INSERT INTO local_plan VALUES(?,?,?)',(key,original,sha(original)))
    elif rows != [(original,sha(original))]: raise ValueError('Original Truss ACK intent differs')


def admit_truss_ack_attempt(driver, request, manifest, vector):
    """Require real committed native phase/manifest and exact retained intent."""
    from .driver import AttemptExecutor,CarrierPolicy
    original=encoded(_intent(driver,request,manifest,vector))
    if driver.transport.db.execute('SELECT original,digest FROM local_plan WHERE operation=?',
            ('truss-ack:'+request['request_digest'],)).fetchall() != [(original,sha(original))]:
        raise ValueError('Original durable Truss ACK attempt required')
    driver.source_sessions._request(request,driver.context)
    target=driver.transport.targets[driver.tables['attempts']]
    store=DeltaAttemptStore(AttemptExecutor(driver),CarrierPolicy(driver,'attempts'),target.table,target.uuid)
    with owned_context(store.session(driver.context)) as session:
        records=session.read(request['stream'],request['batch_id'])
    if not records or records[-1].phase!='committed' or records[-1].request_digest!=request['request_digest']:
        raise ValueError('Actual original native committed phase required')
    payload=decode_custody_bytes(records[-1].payload_json.encode())
    if payload['request']!=request or decode_custody_bytes(payload['descriptor_json'].encode())!=manifest:
        raise ValueError('Actual original committed manifest differs')
    driver.source_admit(request)
    driver.validate_manifest(manifest,driver.context)

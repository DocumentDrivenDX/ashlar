from .resources import RESOURCE_ROOT
"""Selected local host implementation; native qualification remains version-scoped."""
from dataclasses import dataclass
import hashlib, json, re, sys
from pathlib import Path
from uuid import UUID
from ashlar.attempt_store import verify_request_digest
from ashlar.manifest import bind_manifest_pins
from ashlar.publication import resolve_publication
from ashlar.schema import decode_retained_json
from ashlar.source_checkpoint import bind_outbox_descriptor
from .postgres import Session

class AckError(ValueError):
    pass

class AckOutcomeUncertain(AckError):

    def __init__(self, original_request, original_manifest, scope, *, commit_attempted, commit_observed):
        super().__init__('Original source ACK outcome requires fresh reconciliation; retain exact original custody')
        self.original_request = original_request
        self.original_manifest = original_manifest
        self.scope = scope
        self.commit_attempted = commit_attempted
        self.commit_observed = commit_observed

@dataclass(frozen=True)
class AckScope:
    service_schema: str
    scope_id: str
    installation_id: str
    consumer: str
    feed: str
    epoch: str

    def __post_init__(self):
        if type(self.service_schema) is not str or len(self.service_schema) > 63 or (not re.fullmatch('ashlar_ack_[a-z0-9_]+', self.service_schema)):
            raise AckError('Explicit dedicated service namespace required')
        for value in (self.scope_id, self.installation_id):
            if str(UUID(value)) != value:
                raise AckError('Canonical original scope/installation UUID required')
        if any((type(v) is not str or not v or '\x00' in v or (len(v.encode()) > 1024) for v in (self.consumer, self.feed, self.epoch))):
            raise AckError('Explicit original consumer/feed/epoch required')

def render_ddl(namespace, role):
    if any((type(v) is not str or len(v) > 63 or (not re.fullmatch('ashlar_ack_[a-z0-9_]+', v)) for v in (namespace, role))):
        raise AckError('Fresh safe private service identifiers required')
    source = RESOURCE_ROOT / 'sql/ashlar-source-ack/01-postgresql.sql'
    return source.read_text().replace('__ACK_SCHEMA__', namespace).replace('__ACK_ROLE__', role)

def receipt_bytes(scope, request_bytes, manifest_bytes):
    if any((type(v) is not bytes or not 0 < len(v) <= 4194304 for v in (request_bytes, manifest_bytes))):
        raise AckError('Bounded original request/manifest bytes required')
    request = decode_retained_json(request_bytes)
    verify_request_digest(request)
    manifest = decode_retained_json(manifest_bytes)
    checkpoint = decode_retained_json(request['source_checkpoint_json'].encode())
    if checkpoint.get('profile') != 'ashlar-postgresql-outbox/0.1' or (checkpoint['feed'], checkpoint['epoch']) != (scope.feed, scope.epoch):
        raise AckError('Original registered outbox scope required')
    publication_id = manifest.get('publication_id')
    if type(publication_id) is not str or not publication_id:
        raise AckError('Original publication identity required')
    receipt = {'format': 'ashlar-protected-source-ack/0.1', 'scope_id': scope.scope_id, 'installation_id': scope.installation_id, 'consumer': scope.consumer, 'feed': scope.feed, 'epoch': scope.epoch, **{k: checkpoint[k] for k in ('previous', 'position', 'batch_id', 'payload_digest')}, 'request_sha256': hashlib.sha256(request_bytes).hexdigest(), 'manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(), 'publication_id': publication_id}
    return (request, manifest, checkpoint, (json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode())

class ProtectedOutboxAck:

    def __init__(self, connection_factory, pins, publication_backend, policy, scope, *, supported_profiles, supported_revisions):
        if not isinstance(scope, AckScope) or not callable(connection_factory) or (not supported_profiles) or (not supported_revisions):
            raise AckError('Explicit authenticated native ports/scope required')
        for name in ('admit_scope', 'admit_publication'):
            if not callable(getattr(policy, name, None)):
                raise AckError('Mandatory current source/publication/ancestry host policy required')
        self.factory = connection_factory
        self.pins = pins
        self.backend = publication_backend
        self.policy = policy
        self.scope = scope
        self.profiles = tuple(supported_profiles)
        self.revisions = supported_revisions

    def _resolve(self, request, manifest, vector, session, context):
        rows = session.query('SELECT "' + self.scope.service_schema + '".scope_binding(CAST(:scope AS uuid)) AS binding', {'scope': self.scope.scope_id}).rows
        if len(rows) != 1 or not isinstance(rows[0].get('binding'), dict):
            raise AckError('Native immutable source scope unavailable')
        binding = rows[0]['binding']
        expected = {k: getattr(self.scope, k) for k in ('scope_id', 'installation_id', 'consumer', 'feed', 'epoch')}
        if any((binding.get(k) != v for (k, v) in expected.items())) or not isinstance(binding.get('source_schema'), str) or (not re.fullmatch('[0-9a-f]{64}', binding.get('source_signature_sha256', ''))):
            raise AckError('Native original consumer/feed/epoch/installation differs')
        if self.policy.admit_scope(self.scope, session, context) is not None:
            raise AckError('Native source scope/authority admission incomplete')
        resolved = resolve_publication(self.backend, manifest['publication_id'], {t: p[0] for (t, p) in vector.targets.items()}, context=context, supported_profiles=self.profiles, supported_revisions=self.revisions)
        if dict(resolved.descriptor.raw) != manifest or any((resolved.snapshots[t].version != v for (t, (u, v)) in vector.targets.items())):
            raise AckError('Original authoritative whole publication differs')
        bind_manifest_pins(resolved.descriptor, vector, {t: p[0] for (t, p) in vector.targets.items()}, authority=vector.authority)
        bind_outbox_descriptor(request, resolved.descriptor, expected_publication_id=manifest['publication_id'])
        if self.policy.admit_publication(self.scope, request, resolved, session, context) is not None:
            raise AckError('Current native effects/source/global ancestry admission incomplete')
        return resolved

    def acknowledge(self, request_bytes, manifest_bytes, vector, *, context):
        return self._perform(request_bytes, manifest_bytes, vector, context, reconcile=False)

    def reconcile(self, request_bytes, manifest_bytes, vector, *, context):
        """Fresh connection exact observation, never a mutation or replacement ACK."""
        return self._perform(request_bytes, manifest_bytes, vector, context, reconcile=True)

    def _perform(self, request_bytes, manifest_bytes, vector, context, *, reconcile):
        (request, manifest, checkpoint, receipt) = receipt_bytes(self.scope, request_bytes, manifest_bytes)
        commit_attempted = False
        commit_observed = False
        write_attempted = False
        transaction_closed = False
        completed = False
        connection = None
        session = None
        answer = None
        try:
            with self.pins.hold(vector, context=context):
                connection = self.factory(context)
                if connection.autocommit or connection.info.transaction_status != 0:
                    raise AckError('Fresh non-autocommit source connection required')
                session = Session(connection)
                self._resolve(request, manifest, vector, session, context)
                if reconcile:
                    rows = session.query('SELECT * FROM "' + self.scope.service_schema + '".observe(CAST(:scope AS uuid),CAST(:position AS bigint))', {'scope': self.scope.scope_id, 'position': checkpoint['position']}).rows
                    if len(rows) != 1:
                        raise AckError('Original source scope missing or ambiguous')
                    row = rows[0]
                    if row['receipt_hex'] is None:
                        if any((row[k] is not None for k in ('request_hex', 'manifest_hex'))) or row['position'] != checkpoint['previous']:
                            raise AckError('Missing original ACK cannot be reconciled as absent')
                        answer = None
                    else:
                        if row['request_hex'] != request_bytes.hex() or row['manifest_hex'] != manifest_bytes.hex() or row['receipt_hex'] != receipt.hex() or (int(row['position']) < int(checkpoint['position'])):
                            raise AckError('Retained exact original ACK conflict')
                        answer = receipt
                else:
                    params = {'scope': self.scope.scope_id, 'previous': checkpoint['previous'], 'position': checkpoint['position'], 'batch': checkpoint['batch_id'], 'digest': checkpoint['payload_digest'], 'request': request_bytes.hex(), 'manifest': manifest_bytes.hex(), 'receipt': receipt.hex()}
                    sql = 'SELECT encode("' + self.scope.service_schema + '".ack(CAST(:scope AS uuid),CAST(:previous AS bigint),CAST(:position AS bigint),:batch,decode(:digest,\'hex\'),decode(:request,\'hex\'),decode(:manifest,\'hex\'),decode(:receipt,\'hex\')),\'hex\') AS receipt_hex'
                    write_attempted = True
                    rows = session.query(sql, params).rows
                    if len(rows) != 1 or rows[0].get('receipt_hex') != receipt.hex():
                        raise AckError('Native exact ACK receipt readback differs')
                    answer = receipt
                self._resolve(request, manifest, vector, session, context)
                if connection.info.transaction_status != 2:
                    transaction_closed = True
                    raise AckError('Source transaction unexpectedly closed before controlled commit')
                commit_attempted = True
                connection.commit()
                commit_observed = True
            completed = True
        except BaseException as error:
            if connection is not None:
                if write_attempted:
                    try:
                        if connection.info.transaction_status not in (2, 3):
                            transaction_closed = True
                    except BaseException:
                        transaction_closed = True
                try:
                    connection.rollback()
                except BaseException:
                    transaction_closed = True
            if write_attempted and (commit_attempted or transaction_closed):
                raise AckOutcomeUncertain(request_bytes, manifest_bytes, self.scope, commit_attempted=commit_attempted, commit_observed=commit_observed) from error
            raise
        finally:
            original_error = sys.exc_info()[1]
            cleanup_error = None
            if session is not None:
                session.active = False
            if connection is not None:
                try:
                    if write_attempted and (not commit_observed) and (connection.info.transaction_status == 0):
                        transaction_closed = True
                except BaseException as error:
                    cleanup_error = error
                    if write_attempted:
                        transaction_closed = True
                try:
                    connection.close()
                except BaseException as error:
                    if cleanup_error is None:
                        cleanup_error = error
                if cleanup_error is not None:
                    if write_attempted and (commit_attempted or commit_observed or transaction_closed):
                        raise AckOutcomeUncertain(request_bytes, manifest_bytes, self.scope, commit_attempted=commit_attempted, commit_observed=commit_observed) from original_error or cleanup_error
                    raise cleanup_error from original_error
        if not completed or not commit_observed:
            if write_attempted and (commit_attempted or transaction_closed):
                raise AckOutcomeUncertain(request_bytes, manifest_bytes, self.scope, commit_attempted=commit_attempted, commit_observed=commit_observed)
            raise AckError('ACK completion suppressed or unobserved')
        return answer

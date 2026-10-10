"""Publisher composition over immutable phase custody and original native ports.

No credentials, permissive source policy or replacement-write recovery lives here.
The driver must retain actual effects and one complete proposed manifest before
returning an applied artifact. Manifest recovery is a separate mandatory port.
"""
from contextlib import contextmanager
import json
from .publisher import Attempt, PublicationError
from .schema import _json
from .manifest import validate_manifest_row
from .publication import Descriptor, _decode, _freeze
from .source_checkpoint import bind_source_descriptor


def _artifact(text, request):
    if not isinstance(text, str) or not text or len(text.encode()) > 3 * 1024 * 1024:
        raise PublicationError('Bounded original applied artifact required')
    value = _json(text.encode())
    if not isinstance(value, dict) or set(value) != {'effects', 'manifest'} or not isinstance(value['effects'], dict):
        raise PublicationError('Complete effects and proposed manifest required')
    row = value['manifest']
    if not isinstance(row, dict):raise PublicationError('Exact original manifest row required')
    validate_manifest_row(row)
    descriptor = Descriptor(row['publication_id'], row['profile_version'],
        _freeze(_decode(row['table_versions_json'])), _freeze(_decode(row['schema_revisions_json'])),
        _freeze(_decode(row['source_progress_json'])), _freeze(_decode(row['validation_report_json'])), _freeze(row))
    if descriptor.revisions != _decode(request['schema_revisions_json']) or descriptor.validation_report.get('request_digest') != request['request_digest']:
        raise PublicationError('Manifest does not bind original request and revisions')
    if 'source_checkpoint_json' in request:
        bind_source_descriptor(request, descriptor, expected_publication_id=row['publication_id'])
    return value, descriptor


def validate_applied_artifact(text, request):
    """Decode the complete original applied artifact and bound descriptor.

    This is correspondence validation only. Native effect, manifest, source and
    protected ACK authority remain independently owned host gates.
    """
    return _artifact(text, request)


class StoredPublisherBackend:
    """Connect publish_batch to DeltaAttemptStore and an explicit native driver.

    Driver: writer(stream, context), apply/recover_apply(request, context) ->
    original artifact JSON, validate(request, artifact_text, descriptor, context),
    acknowledge(request, descriptor, context). Validation/ACK must return None.
    Manifest factory(request, context) -> store with commit(row, context=...) and
    recover(row, context=...). Recovery must inspect the original submission,
    never start a replacement. Sessions are intentionally single-use/nonreentrant.
    """
    def __init__(self, attempts, driver, manifest_factory):
        self.attempts = attempts
        self.driver = driver
        self.manifest_factory = manifest_factory
        self._session = None
        self._context = None
        self._stream = None

    @contextmanager
    def writer(self, stream, context):
        if self._session is not None:raise PublicationError('Publisher session already held')
        with self.driver.writer(stream, context) as permit:
            if permit is not None:raise PublicationError('Source/writer admission incomplete')
            with self.attempts.session(context) as session:
                self._session, self._context, self._stream = session, context, stream
                try:yield
                finally:self._session = self._context = self._stream = None

    def _records(self, stream, batch_id):
        if self._session is None or stream != self._stream:
            raise PublicationError('Original held publisher session required')
        return self._session.read(stream, batch_id)

    def _original(self, stream, batch_id, digest=None):
        records = self._records(stream, batch_id)
        if not records:raise PublicationError('Missing original prepared attempt')
        last = records[-1]
        if digest is not None and last.request_digest != digest:
            raise PublicationError('Original attempt digest conflict')
        return last, _json(last.payload_json.encode())

    def _attempt(self, record, payload):
        descriptor = None
        result = payload['result_json']
        if result is not None:
            _, proposed = _artifact(result, payload['request'])
            if record.phase == 'committed':
                actual = _json(payload['descriptor_json'].encode())
                if actual != dict(proposed.raw):raise PublicationError('Committed manifest differs from retained proposal')
                descriptor = proposed
        return Attempt(record.request_digest, record.phase, result, descriptor)

    def observe(self, stream, batch_id):
        records = self._records(stream, batch_id)
        if not records:return None
        last = records[-1]
        return self._attempt(last, _json(last.payload_json.encode()))

    def prepare(self, stream, request):
        self._records(stream, request['batch_id'])
        record = self._session.append(dict(request), 'prepared')
        return self._attempt(record, _json(record.payload_json.encode()))

    def start_apply(self, stream, batch_id, digest):
        _, payload = self._original(stream, batch_id, digest)
        self._session.append(payload['request'], 'applying')

    def _effects(self, stream, request, recovery):
        last, original = self._original(stream, request['batch_id'], request['request_digest'])
        if last.phase != 'applying' or dict(request) != original['request']:
            raise PublicationError('Original applying request required')
        function = self.driver.recover_apply if recovery else self.driver.apply
        result = function(dict(original['request']), self._context)
        _artifact(result, original['request'])
        return result

    def apply(self, stream, request):return self._effects(stream, request, False)
    def recover_apply(self, stream, request):return self._effects(stream, request, True)

    def retain_applied(self, stream, batch_id, digest, result):
        _, payload = self._original(stream, batch_id, digest)
        _artifact(result, payload['request'])
        record = self._session.append(payload['request'], 'applied', result_json=result)
        return self._attempt(record, _json(record.payload_json.encode()))

    def validate(self, stream, request, result, context):
        last, original = self._original(stream, request['batch_id'], request['request_digest'])
        if context is not self._context or last.phase != 'applied' or dict(request) != original['request'] or result != original['result_json']:
            raise PublicationError('Original applied custody/context required')
        _, descriptor = _artifact(result, original['request'])
        if self.driver.validate(dict(original['request']), result, descriptor, context) is not None:
            raise PublicationError('Complete effect/source/pin admission incomplete')

    def start_commit(self, stream, batch_id, digest):
        _, payload = self._original(stream, batch_id, digest)
        self._session.append(payload['request'], 'committing', result_json=payload['result_json'])

    def _commit(self, stream, request, result, recovery):
        last, original = self._original(stream, request['batch_id'], request['request_digest'])
        if last.phase != 'committing' or dict(request) != original['request'] or result != original['result_json']:
            raise PublicationError('Original committing custody required')
        _, descriptor = _artifact(result, original['request'])
        # Recovery must renew complete admission, including finite retention.
        if self.driver.validate(dict(original['request']), result, descriptor, self._context) is not None:
            raise PublicationError('Current commit admission incomplete')
        store = self.manifest_factory(dict(original['request']), self._context)
        function = store.recover if recovery else store.commit
        actual = function(dict(descriptor.raw), context=self._context)
        if not isinstance(actual, dict) or actual != dict(descriptor.raw):
            raise PublicationError('Native commit differs from original proposed manifest')
        text = json.dumps(actual, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        record = self._session.append(original['request'], 'committed', result_json=result, descriptor_json=text)
        return self._attempt(record, _json(record.payload_json.encode()))

    def commit(self, stream, request, result):return self._commit(stream, request, result, False)
    def recover_commit(self, stream, request, result, context):
        if context is not self._context:raise PublicationError('Original commit context required')
        return self._commit(stream, request, result, True)

    def acknowledge(self, stream, request, descriptor, context):
        last, original = self._original(stream, request['batch_id'], request['request_digest'])
        attempt = self._attempt(last, original)
        if context is not self._context or last.phase != 'committed' or dict(request) != original['request'] or descriptor != attempt.descriptor:
            raise PublicationError('Only original committed descriptor may reach ACK')
        if self.driver.acknowledge(dict(original['request']), attempt.descriptor, context) is not None:
            raise PublicationError('Current source acknowledgement incomplete')

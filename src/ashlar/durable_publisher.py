"""Publisher coordinator backed by original Delta phase custody.

Effects must supply native apply/commit/recovery and admission; no defaults.
Exact result/descriptor JSON strings cross this boundary without reserialization.
"""
from contextlib import contextmanager
from .publisher import Attempt, PublicationError
from .schema import _json


class DurablePublisher:
    def __init__(self, store, effects):
        self.store = store
        self.effects = effects
        self._session = None
        self._stream = None
        self._context = None

    @contextmanager
    def writer(self, stream, context):
        if self._session is not None:
            raise PublicationError('Publisher session already active')
        with self.effects.writer(stream, context) as permit:
            if permit is not None:
                raise PublicationError('Source/effect writer authority incomplete')
            with self.store.session(context) as session:
                self._session, self._stream, self._context = session, stream, context
                try:
                    yield
                finally:
                    self._session = self._stream = self._context = None

    def _records(self, stream, batch_id):
        if self._session is None or stream != self._stream:
            raise PublicationError('No matching active publisher session')
        return self._session.read(stream, batch_id)

    def _original(self, stream, batch_id, digest):
        records = self._records(stream, batch_id)
        if not records or records[-1].request_digest != digest:
            raise PublicationError('Missing or conflicting original attempt')
        return _json(records[-1].payload_json.encode())

    def observe(self, stream, batch_id):
        records = self._records(stream, batch_id)
        return self._attempt(records[-1]) if records else None

    @staticmethod
    def _attempt(record):
        value = _json(record.payload_json.encode())
        return Attempt(record.request_digest, record.phase,
                       value['result_json'], value['descriptor_json'])

    def _append(self, stream, request, phase, result=None, descriptor=None):
        self._records(stream, request['batch_id'])
        return self._attempt(self._session.append(request, phase,
                             result_json=result, descriptor_json=descriptor))

    def prepare(self, stream, request):
        if request['stream'] != stream:
            raise PublicationError('Request stream mismatch')
        return self._append(stream, request, 'prepared')

    def start_apply(self, stream, batch_id, digest):
        original = self._original(stream, batch_id, digest)
        self._append(stream, original['request'], 'applying')

    def apply(self, stream, request):
        return self.effects.apply(stream, request, self._context)

    def recover_apply(self, stream, request):
        return self.effects.recover_apply(stream, request, self._context)

    def retain_applied(self, stream, batch_id, digest, result):
        original = self._original(stream, batch_id, digest)
        return self._append(stream, original['request'], 'applied', result)

    def validate(self, stream, request, result, context):
        return self.effects.validate(stream, request, result, context)

    def start_commit(self, stream, batch_id, digest):
        original = self._original(stream, batch_id, digest)
        self._append(stream, original['request'], 'committing', original['result_json'])

    def commit(self, stream, request, result):
        descriptor = self.effects.commit(stream, request, result, self._context)
        return self._append(stream, request, 'committed', result, descriptor)

    def recover_commit(self, stream, request, result, context):
        descriptor = self.effects.recover_commit(stream, request, result, context)
        return self._append(stream, request, 'committed', result, descriptor)

    def acknowledge(self, stream, request, descriptor, context):
        return self.effects.acknowledge(stream, request, descriptor, context)

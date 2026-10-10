"""Explicit ordinary protected ACK sessions; metadata establishes no authority."""
from dataclasses import dataclass, field
from typing import Callable, Mapping, Protocol
from types import MappingProxyType
from ashlar.schema import decode_retained_json
from .ack import AckError, AckScope, ProtectedOutboxAck, receipt_bytes
from .postgres import Session
from .source_sessions import OutboxSourceRegistration


class AckSessionPolicy(Protocol):
    def admit_scope(self, scope: AckScope, session: Session, context: object) -> None: ...
    def admit_publication(self, scope: AckScope, request: Mapping[str, str],
                          resolved: object, session: Session, context: object) -> None: ...


@dataclass(frozen=True)
class OutboxAckRegistration:
    source: OutboxSourceRegistration
    connection_factory: Callable[[object], object] = field(repr=False, compare=False)
    policy: AckSessionPolicy = field(repr=False, compare=False)

    def __post_init__(self):
        if (type(self.source) is not OutboxSourceRegistration or not callable(self.connection_factory)
                or any(not callable(getattr(self.policy, name, None))
                       for name in ('admit_scope', 'admit_publication'))):
            raise AckError('Explicit original registration, ordinary ACK factory and current policy required')
        item = self.source
        object.__setattr__(self, 'source', OutboxSourceRegistration(item.scope,
            item.source_schema, item.source_signature_sha256, item.connection_factory))

    def metadata(self):
        return self.source.metadata()


@dataclass(frozen=True)
class RegisteredOutboxAcks:
    registrations: tuple[OutboxAckRegistration, ...]

    def __post_init__(self):
        if (type(self.registrations) is not tuple or not 1 <= len(self.registrations) <= 32
                or any(type(item) is not OutboxAckRegistration for item in self.registrations)
                or len({item.source.scope.feed for item in self.registrations}) != len(self.registrations)):
            raise AckError('Distinct complete original ACK registrations required')
        object.__setattr__(self, 'registrations', tuple(OutboxAckRegistration(
            item.source, item.connection_factory, item.policy) for item in self.registrations))

    def metadata(self):
        return {'profile': 'ashlar-registered-outbox-ack-sessions/0.1',
                'registrations': [item.metadata() for item in self.registrations]}

    def _perform(self, request_bytes, manifest_bytes, vector, *, context, pins,
                 publication_backend, supported_profiles, supported_revisions, reconcile):
        if any(type(raw) is not bytes or not 0 < len(raw) <= 4194304
               for raw in (request_bytes, manifest_bytes)):
            raise AckError('Bounded exact original ACK bytes required')
        # Decode only for source selection; owning ACK validates all exact original bytes.
        request = decode_retained_json(request_bytes)
        checkpoint = decode_retained_json(request['source_checkpoint_json'].encode())
        selected = [item for item in self.registrations if
            (item.source.scope.feed, item.source.scope.epoch) == (checkpoint['feed'], checkpoint['epoch'])]
        if len(selected) != 1:
            raise AckError('Original registered ACK feed/epoch required')
        item = selected[0]
        receipt_bytes(item.source.scope, request_bytes, manifest_bytes)
        original = self.metadata()
        owner = self
        class Policy:
            def admit_scope(self, scope, session, context):
                rows = session.query('SELECT "' + scope.service_schema +
                    '".scope_binding(CAST(:scope AS uuid)) AS binding', {'scope': scope.scope_id}).rows
                if (len(rows) != 1 or type(rows[0].get('binding')) is not dict
                        or rows[0]['binding'].get('source_schema') != item.source.source_schema
                        or rows[0]['binding'].get('source_signature_sha256') != item.source.source_signature_sha256):
                    raise AckError('Original registered ACK namespace/signature differs')
                if owner.metadata() != original:
                    raise AckError('Original ACK registrations changed')
                return item.policy.admit_scope(scope, session, context)
            def admit_publication(self, scope, request, resolved, session, context):
                return item.policy.admit_publication(scope, MappingProxyType(dict(request)), resolved, session, context)
        port = ProtectedOutboxAck(item.connection_factory, pins, publication_backend, Policy(),
            item.source.scope, supported_profiles=supported_profiles, supported_revisions=supported_revisions)
        answer = (port.reconcile if reconcile else port.acknowledge)(
            request_bytes, manifest_bytes, vector, context=context)
        if self.metadata() != original:
            raise AckError('Closing original ACK registrations changed')
        return answer

    def acknowledge(self, request_bytes, manifest_bytes, vector, **ports):
        return self._perform(request_bytes, manifest_bytes, vector, reconcile=False, **ports)

    def reconcile(self, request_bytes, manifest_bytes, vector, **ports):
        return self._perform(request_bytes, manifest_bytes, vector, reconcile=True, **ports)

"""Explicit ordinary outbox sessions; registration metadata grants no authority."""
from dataclasses import asdict, dataclass, field
import json
import re
from types import MappingProxyType
from typing import Callable, Mapping, Protocol
from ashlar.attempt_store import verify_request_digest
from ashlar.outbox import PostgresOutbox
from ashlar.source_checkpoint import outbox_checkpoint
from ashlar.staging import batch_row
from .ack import AckScope
from .lifecycle import finish
from .postgres import Session


class SourceSessionError(ValueError):
    """Original source/session custody unavailable; no publication or ACK permission."""


@dataclass(frozen=True)
class OutboxSourceRegistration:
    scope: AckScope
    source_schema: str
    source_signature_sha256: str
    connection_factory: Callable[[object], object] = field(repr=False, compare=False)

    def __post_init__(self):
        if type(self.scope) is not AckScope:
            raise SourceSessionError('Explicit original source scope required')
        fields = ('service_schema', 'scope_id', 'installation_id', 'consumer', 'feed', 'epoch')
        values = {name: getattr(self.scope, name) for name in fields}
        if any(type(value) is not str for value in values.values()):
            raise SourceSessionError('Exact original scope strings required')
        # Validate primitive carriers before any copying or equality can execute.
        object.__setattr__(self, 'scope', AckScope(**values))
        if (type(self.source_schema) is not str or len(self.source_schema) > 63
                or not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', self.source_schema)
                or type(self.source_signature_sha256) is not str
                or not re.fullmatch('[0-9a-f]{64}', self.source_signature_sha256)
                or not callable(self.connection_factory)):
            raise SourceSessionError('Explicit original namespace/signature/session factory required')

    def metadata(self):
        return {'scope': asdict(self.scope), 'source_schema': self.source_schema,
                'source_signature_sha256': self.source_signature_sha256}


def request_snapshot(request):
    if (type(request) not in (dict, MappingProxyType)
            or len(request) != 8
            or any(type(key) is not str or len(key) > 64 or type(value) is not str for key, value in request.items())):
        raise SourceSessionError('Exact original request strings required')
    expected = {'stream', 'batch_id', 'predecessor', 'schema_revisions_json', 'source_batch_json',
                'source_batch_digest', 'source_checkpoint_json', 'request_digest'}
    if set(request) != expected:
        raise SourceSessionError('Complete original outbox request inventory required')
    frozen = dict(request)
    if any(len(value) > 4194304 for value in frozen.values()):
        raise SourceSessionError('Bounded original request required')
    try:
        raw = json.dumps(frozen, sort_keys=True, separators=(',', ':')).encode('utf-8')
    except (ValueError, UnicodeError):
        raise SourceSessionError('Exact original request UTF8 required') from None
    if len(raw) > 4194304:
        raise SourceSessionError('Bounded original request required')
    verify_request_digest(frozen)
    if 'source_checkpoint_json' not in frozen:
        raise SourceSessionError('Original outbox checkpoint required')
    checkpoint = json.loads(frozen['source_checkpoint_json'])
    if checkpoint['profile'] != 'ashlar-postgresql-outbox/0.1':
        raise SourceSessionError('Original outbox source profile required')
    return MappingProxyType(frozen), checkpoint


class SourceAdmissionPolicy(Protocol):
    def admit_source(self, registration: OutboxSourceRegistration, request: Mapping[str, str],
                     session: Session, context: object) -> None: ...


@dataclass(frozen=True)
class RegisteredOutboxSources:
    """Read exact original group using supplied sessions and mandatory native policy.

    policy.admit_source(registration, request, session, context) must establish
    current ordinary-role/source installation, namespace, immutable signature,
    feed/epoch and writer/fence authority, returning None or raising. The reader
    verifies complete checkpoint and original transaction bytes at both gates.
    All acquired connections roll back and close; this owner never commits,
    provisions, discovers credentials or acknowledges source progress.
    """
    registrations: tuple[OutboxSourceRegistration, ...]
    policy: SourceAdmissionPolicy = field(repr=False, compare=False)

    def __post_init__(self):
        if (type(self.registrations) is not tuple or not self.registrations
                or len(self.registrations) > 32
                or any(type(item) is not OutboxSourceRegistration for item in self.registrations)
                or len({item.scope.feed for item in self.registrations}) != len(self.registrations)
                or not callable(getattr(self.policy, 'admit_source', None))):
            raise SourceSessionError('Distinct original registrations and current native policy required')
        object.__setattr__(self, 'registrations', tuple(OutboxSourceRegistration(
            item.scope, item.source_schema, item.source_signature_sha256, item.connection_factory)
            for item in self.registrations))

    def metadata(self):
        return {'profile': 'ashlar-registered-outbox-sessions/0.1',
                'registrations': [item.metadata() for item in self.registrations]}

    def admit(self, request, *, context):
        request, checkpoint = request_snapshot(request)
        selected = [item for item in self.registrations if
                    (item.scope.feed, item.scope.epoch) == (checkpoint['feed'], checkpoint['epoch'])]
        if len(selected) != 1:
            raise SourceSessionError('Original registered feed/epoch required')
        registration = selected[0]
        original_metadata = self.metadata()
        connection = registration.connection_factory(context)
        session = None
        primary = None
        try:
            if not callable(getattr(connection, 'rollback', None)) or not callable(getattr(connection, 'close', None)):
                raise SourceSessionError('Owned ordinary transaction connection required')
            session = Session(connection)
            for gate in ('opening', 'closing'):
                if self.policy.admit_source(registration, request, session, context) is not None:
                    raise SourceSessionError('Current native source admission did not complete')
                transactions = PostgresOutbox(session, feed=registration.scope.feed,
                    epoch=registration.scope.epoch, schema=registration.source_schema).read(checkpoint['previous'], limit=1)
                if (len(transactions) != 1
                        or outbox_checkpoint(transactions[0]) != request['source_checkpoint_json']
                        or batch_row(transactions[0].batch)['batch_json'] != request['source_batch_json']):
                    raise SourceSessionError('Exact original native outbox group differs')
            if self.policy.admit_source(registration, request, session, context) is not None:
                raise SourceSessionError('Closing native source admission did not complete')
            if self.metadata() != original_metadata:
                raise SourceSessionError('Original source registration changed')
        except BaseException as error:
            primary = error
        finish(primary, [lambda: setattr(session, 'active', False) if session is not None else None,
                         lambda: connection.rollback(), lambda: connection.close()])

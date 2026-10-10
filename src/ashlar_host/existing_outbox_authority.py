"""Current ordinary-role admission for an existing registered local outbox.

The SQL service recomputes its retained installation signature. A separately
owned current writer/native reservation lease is mandatory; role facts or source
metadata alone confer no native publication authority. No discovery or DDL.
"""
from dataclasses import dataclass, field
import re
from typing import Callable, Protocol
from .source_sessions import OutboxSourceRegistration, request_snapshot
from .postgres import Session
from .lifecycle import finish
from .ack import AckScope


class OutboxAuthorityError(ValueError):
    pass


class ExistingWriterAuthority(Protocol):
    def admit_source_writer(self, registration: OutboxSourceRegistration, context: object) -> None:
        """Prove actual held original reservation and current native writer lease."""


@dataclass(frozen=True, repr=False)
class OutboxAuthorityRegistration:
    source: OutboxSourceRegistration
    source_role: str
    ack_role: str
    database: str
    metadata_factory: Callable[[object], object] = field(compare=False, repr=False)

    def __post_init__(self):
        if type(self.source) is not OutboxSourceRegistration:
            raise OutboxAuthorityError('Exact existing source registration required')
        object.__setattr__(self, 'source', OutboxSourceRegistration(self.source.scope,
            self.source.source_schema, self.source.source_signature_sha256, self.source.connection_factory))
        for value in (self.source_role, self.ack_role, self.database):
            if type(value) is not str or not re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,62}', value):
                raise OutboxAuthorityError('Explicit bounded existing ordinary role/database required')
        if self.source_role == self.ack_role or not callable(self.metadata_factory):
            raise OutboxAuthorityError('Separate ordinary reader and protected service roles required')


def one(session, statement, parameters, fields):
    rows = session.query(statement, parameters).rows
    if (type(rows) is not list or len(rows) != 1 or type(rows[0]) is not dict
            or set(rows[0]) != set(fields)):
        raise OutboxAuthorityError('Exact complete current native observation required')
    return rows[0]


def ordinary(session, role, database):
    if type(session) is not Session or session.active is not True or session.connection.autocommit is not False:
        raise OutboxAuthorityError('Owned active ordinary transaction session required')
    row = one(session, 'SELECT session_user,current_user,current_database() AS database,'
        "current_setting('transaction_isolation') AS isolation,"
        'rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls '
        'FROM pg_catalog.pg_roles WHERE rolname=current_user', {},
        ('session_user', 'current_user', 'database', 'isolation', 'rolsuper',
         'rolcreaterole', 'rolcreatedb', 'rolreplication', 'rolbypassrls'))
    expected = {'session_user': role, 'current_user': role, 'database': database,
        'isolation': 'read committed', 'rolsuper': False, 'rolcreaterole': False,
        'rolcreatedb': False, 'rolreplication': False, 'rolbypassrls': False}
    if any(type(row[key]) is not type(value) or row[key] != value for key, value in expected.items()):
        raise OutboxAuthorityError('Current ordinary role/database/isolation differs')


def privileges(session, selected, *, ack):
    source = selected.source.source_schema; service = selected.source.scope.service_schema
    if ack:
        row = one(session, "SELECT has_schema_privilege(current_user,:namespace,'USAGE') AS usage,"
            "has_schema_privilege(current_user,:namespace,'CREATE') AS create,"
            "has_function_privilege(current_user,:binding,'EXECUTE') AS binding,"
            "has_function_privilege(current_user,:observe,'EXECUTE') AS observe,"
            "has_function_privilege(current_user,:ack,'EXECUTE') AS ack,"
            "has_table_privilege(current_user,:installation,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') AS installation_write,"
            "has_table_privilege(current_user,:batch,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') AS batch_write,"
            "has_table_privilege(current_user,:head,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') AS head_write,"
            "has_function_privilege(current_user,:append,'EXECUTE') AS append,"
            "pg_has_role(current_user,(SELECT nspowner FROM pg_catalog.pg_namespace WHERE nspname=:source),'MEMBER') AS source_owner,"
            "has_table_privilege(current_user,:scope,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') AS scope_write,"
            "has_table_privilege(current_user,:receipt,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') AS receipt_write,"
            "pg_has_role(current_user,(SELECT nspowner FROM pg_catalog.pg_namespace WHERE nspname=:namespace),'MEMBER') AS owner",
            {'namespace': service, 'binding': service + '.scope_binding(uuid)',
             'observe': service + '.observe(uuid,bigint)',
             'ack': service + '.ack(uuid,bigint,bigint,text,bytea,bytea,bytea,bytea)',
             'scope': service + '.consumer_scope', 'receipt': service + '.receipt',
             'installation': service + '.installation', 'batch': source + '.batch',
             'head': source + '.head', 'append': source + '.append(text,text)', 'source': source},
            ('usage', 'create', 'binding', 'observe', 'ack', 'installation_write', 'batch_write',
             'head_write', 'append', 'source_owner', 'scope_write', 'receipt_write', 'owner'))
        expected = {'usage': True, 'create': False, 'binding': True, 'observe': True,
            'ack': True, 'installation_write': False, 'batch_write': False, 'head_write': False,
            'append': False, 'source_owner': False, 'scope_write': False, 'receipt_write': False, 'owner': False}
    else:
        row = one(session, "SELECT has_schema_privilege(current_user,:namespace,'USAGE') AS usage,"
            "has_schema_privilege(current_user,:namespace,'CREATE') AS create,"
            "has_table_privilege(current_user,:batch,'SELECT') AS batch_read,"
            "has_table_privilege(current_user,:head,'SELECT') AS head_read,"
            "has_table_privilege(current_user,:batch,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') AS batch_write,"
            "has_table_privilege(current_user,:head,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') AS head_write,"
            "has_function_privilege(current_user,:append,'EXECUTE') AS append,"
            "pg_has_role(current_user,(SELECT nspowner FROM pg_catalog.pg_namespace WHERE nspname=:namespace),'MEMBER') AS owner",
            {'namespace': source, 'batch': source + '.batch', 'head': source + '.head',
             'append': source + '.append(text,text)'},
            ('usage', 'create', 'batch_read', 'head_read', 'batch_write', 'head_write', 'append', 'owner'))
        expected = {'usage': True, 'create': False, 'batch_read': True, 'head_read': True,
            'batch_write': False, 'head_write': False, 'append': False, 'owner': False}
    if any(type(row[key]) is not bool or row[key] is not value for key, value in expected.items()):
        raise OutboxAuthorityError('Current ordinary source/service privileges differ')


@dataclass(frozen=True, repr=False)
class ExistingOutboxAuthority:
    registrations: tuple[OutboxAuthorityRegistration, ...]
    writer: ExistingWriterAuthority = field(compare=False, repr=False)

    def __post_init__(self):
        if (type(self.registrations) is not tuple or not 1 <= len(self.registrations) <= 32
                or any(type(item) is not OutboxAuthorityRegistration for item in self.registrations)
                or len({item.source.scope.feed for item in self.registrations}) != len(self.registrations)
                or not callable(getattr(self.writer, 'admit_source_writer', None))):
            raise OutboxAuthorityError('Complete distinct registered sources and actual writer owner required')
        object.__setattr__(self, 'registrations', tuple(OutboxAuthorityRegistration(
            item.source, item.source_role, item.ack_role, item.database, item.metadata_factory)
            for item in self.registrations))

    def _writer(self, selected, context):
        owned = OutboxSourceRegistration(selected.source.scope, selected.source.source_schema,
            selected.source.source_signature_sha256, selected.source.connection_factory)
        if self.writer.admit_source_writer(owned, context) is not None:
            raise OutboxAuthorityError('Actual original writer/native reservation admission incomplete')

    def _binding(self, selected, context):
        # Factory errors before acquisition own no connection. Once acquired,
        # rollback and close are attempted on every failure/cancellation path.
        connection = selected.metadata_factory(context)
        primary = None
        try:
            if connection.autocommit is not False:
                raise OutboxAuthorityError('Fresh ordinary metadata transaction required')
            session = Session(connection)
            self.admit_scope(selected.source.scope, session, context)
        except BaseException as error:
            primary = error
        finish(primary, [lambda: connection.rollback(), lambda: connection.close()])

    def admit_scope(self, scope: AckScope, session: Session, context: object) -> None:
        """Renew actual ordinary ACK privileges and original source installation.

        The supplied session remains owned by ProtectedOutboxAck. This gate
        grants no publication authority; its native publication gate is separate.
        """
        fields = ('service_schema', 'scope_id', 'installation_id', 'consumer', 'feed', 'epoch')
        if type(scope) is not AckScope or any(type(getattr(scope, name)) is not str for name in fields):
            raise OutboxAuthorityError('Exact original ACK scope required')
        scope = AckScope(**{name: getattr(scope, name) for name in fields})
        matches = [item for item in self.registrations if item.source.scope == scope]
        if len(matches) != 1:
            raise OutboxAuthorityError('Complete original ACK/source registration differs')
        selected = matches[0]
        self._writer(selected, context)
        ordinary(session, selected.ack_role, selected.database)
        privileges(session, selected, ack=True)
        row = one(session, 'SELECT "' + selected.source.scope.service_schema
                + '".scope_binding(CAST(:scope AS uuid)) AS binding',
                {'scope': selected.source.scope.scope_id}, ('binding',))
        expected = selected.source.metadata()['scope']; expected.pop('service_schema')
        expected.update(source_schema=selected.source.source_schema,
                source_signature_sha256=selected.source.source_signature_sha256)
        binding = row['binding']
        if (type(binding) is not dict or set(binding) != set(expected)
                    or any(type(value) is not str for value in binding.values())
                    or binding != expected):
            raise OutboxAuthorityError('Actual retained source/scope installation changed')
        ordinary(session, selected.ack_role, selected.database)
        privileges(session, selected, ack=True)
        self._writer(selected, context)

    def admit_source(self, registration, request, session, context):
        if type(registration) is not OutboxSourceRegistration:
            raise OutboxAuthorityError('Exact registered source owner required')
        registration = OutboxSourceRegistration(registration.scope, registration.source_schema,
            registration.source_signature_sha256, registration.connection_factory)
        _, checkpoint = request_snapshot(request)
        matches = [item for item in self.registrations
                   if item.source.scope.feed == checkpoint['feed']]
        if len(matches) != 1:
            raise OutboxAuthorityError('Exact original source inventory required')
        selected = matches[0]
        if (registration.metadata() != selected.source.metadata()
                or checkpoint['epoch'] != selected.source.scope.epoch):
            raise OutboxAuthorityError('Complete original source/checkpoint scope differs')
        self._writer(selected, context)
        ordinary(session, selected.source_role, selected.database)
        privileges(session, selected, ack=False)
        self._binding(selected, context)
        ordinary(session, selected.source_role, selected.database)
        privileges(session, selected, ack=False)
        self._writer(selected, context)

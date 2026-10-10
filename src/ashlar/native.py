"""Read-only SQL backend; authenticated execution and policy are injected."""
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence, Tuple
from .publication import Descriptor, ResolutionError, Snapshot, _name, validate_table_identifier

@dataclass(frozen=True)
class SQLResult:
    rows: Sequence[Mapping[str, Any]]
    columns: Tuple[Tuple[str, str], ...] = ()

class Executor(Protocol):
    def query(self, sql: str, parameters: Mapping[str, str]) -> SQLResult:
        """Execute through authenticated transport with complete, untruncated results."""

class Policy(Protocol):
    def authorize(self, context: Any, publication_id: str, tables: Sequence[str]) -> None: ...
    def validate_descriptor(self, descriptor: Descriptor, context: Any) -> None: ...
    def validate_snapshot(self, table: str, uuid: str, version: int, columns) -> None:
        """Verify protocol, retained data-file availability and active pin custody.

        LIMIT 0 alone proves none of these. Refuse when evidence is unavailable.
        """

def quote_table_identifier(table: str) -> str:
    """Quote an admitted three-part identifier; never accept an SQL fragment.

    This syntax operation grants no permission to access the named table.
    """
    return '.'.join('`' + part + '`' for part in validate_table_identifier(table).split('.'))

# Retained compatibility name for existing internal consumers.
_quoted = quote_table_identifier

class NativeBackend:
    def __init__(self, executor: Executor, policy: Policy, manifest_table: str,
                 manifest_uuid: str, schemas: Mapping[str, Sequence[Tuple[str, str]]]):
        self.executor = executor
        self.policy = policy
        self.manifest_table = _name(manifest_table)
        if not isinstance(manifest_uuid, str) or not manifest_uuid:
            raise ResolutionError('Missing trusted manifest UUID')
        self.manifest_uuid = manifest_uuid
        self.schemas = { _name(t): tuple(tuple(c) for c in cols) for t, cols in schemas.items() }

    def authorize(self, context, publication_id, tables):
        return self.policy.authorize(context, publication_id, tables)

    def _uuid(self, table):
        rows = self.executor.query('DESCRIBE DETAIL ' + _quoted(table), {}).rows
        if len(rows) != 1 or not isinstance(rows[0].get('id'), str):
            raise ResolutionError('Missing native table identity')
        return rows[0]['id']

    def descriptors(self, publication_id):
        if self._uuid(self.manifest_table) != self.manifest_uuid:
            raise ResolutionError('Manifest table replaced')
        rows = self.executor.query(
            'SELECT publication_id, profile_version, table_versions_json, source_progress_json, '
            'schema_revisions_json, validation_report_json, cast(unix_micros(recorded_at) AS STRING) AS recorded_at FROM '
            + _quoted(self.manifest_table) + ' WHERE publication_id = :publication_id',
            {'publication_id': publication_id}).rows
        if self._uuid(self.manifest_table) != self.manifest_uuid:
            raise ResolutionError('Manifest table replaced during resolution')
        return rows

    def validate_descriptor(self, descriptor, context):
        return self.policy.validate_descriptor(descriptor, context)

    def inspect_snapshot(self, table, version):
        if type(version) is not int or not 0 <= version <= 2**63 - 1:
            raise ResolutionError('Invalid Delta version')
        if table not in self.schemas or not self.schemas[table]:
            raise ResolutionError('Missing trusted native schema')
        before = self._uuid(table)
        result = self.executor.query('SELECT * FROM ' + _quoted(table)
                                     + ' VERSION AS OF ' + str(version) + ' LIMIT 0', {})
        if result.rows or tuple(result.columns) != self.schemas[table]:
            raise ResolutionError('Pinned schema mismatch')
        if self.policy.validate_snapshot(table, before, version, result.columns) is not None:
            raise ResolutionError('Snapshot policy did not complete its contract')
        if self._uuid(table) != before:
            raise ResolutionError('Native table replaced during inspection')
        return Snapshot(table, before, version)

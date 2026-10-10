"""Publication resolver core. Native custody and policy require a trusted backend."""
from dataclasses import dataclass
from decimal import Decimal
import json
import re
from types import MappingProxyType
from typing import Any, Mapping, Protocol, Sequence

_IDENTIFIER = re.compile(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z')

class ResolutionError(ValueError):
    """Invalid or unavailable publication; never a partial read admission."""

def validate_table_identifier(value: str) -> str:
    """Validate the existing closed three-part SQL identifier grammar.

    This syntax check grants no catalog, publication or execution authority.
    """
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ResolutionError('Invalid qualified table identifier')
    return value

# Retained compatibility name for existing internal consumers.
_name = validate_table_identifier

def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ResolutionError('Duplicate JSON member')
        result[key] = value
    return result

def _decode(value):
    if not isinstance(value, str):
        raise ResolutionError('Descriptor JSON must be retained text')
    try:
        return json.loads(value, object_pairs_hook=_pairs, parse_float=Decimal,
                          parse_constant=lambda _: (_ for _ in ()).throw(ResolutionError('Non-finite JSON')))
    except (ValueError, TypeError, RecursionError) as exc:
        raise ResolutionError('Invalid descriptor JSON') from exc

def _freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    return value

@dataclass(frozen=True)
class Descriptor:
    publication_id: str
    profile: str
    versions: Mapping[str, int]
    revisions: Mapping[str, str]
    source_progress: Any
    validation_report: Mapping[str, Any]
    raw: Mapping[str, Any]

@dataclass(frozen=True)
class Snapshot:
    """Backend observation of this exact retained version, never merely latest."""
    table: str
    uuid: str
    version: int

@dataclass(frozen=True)
class ResolvedPublication:
    descriptor: Descriptor
    snapshots: Mapping[str, Snapshot]

class Backend(Protocol):
    def authorize(self, context: Any, publication_id: str, tables: Sequence[str]) -> None:
        """Authenticate and enforce read policy; raise on refusal."""
    def descriptors(self, publication_id: str) -> Sequence[Mapping[str, Any]]:
        """Read authoritative immutable manifest rows using a bound ID."""
    def validate_descriptor(self, descriptor: Descriptor, context: Any) -> None:
        """Check custody, complete validation, source progress and effective policy."""
    def inspect_snapshot(self, table: str, version: int) -> Snapshot:
        """Verify native UUID, schema/protocol suitability and version readability."""

def resolve_publication(backend: Backend, publication_id: str,
                        required_tables: Mapping[str, str], *, context: Any,
                        supported_profiles: Sequence[str],
                        supported_revisions: Mapping[str, Sequence[str]]) -> ResolvedPublication:
    """Resolve all consumed tables or raise; no writes, latest fallback or SQL execution."""
    if not isinstance(publication_id, str) or not publication_id:
        raise ResolutionError('Missing publication identity')
    required = dict(required_tables)
    if not required:
        raise ResolutionError('Empty read-plan inventory')
    for table, uuid in required.items():
        _name(table)
        if not isinstance(uuid, str) or not uuid:
            raise ResolutionError('Missing trusted table UUID')
    profiles = tuple(supported_profiles)
    revisions_allowed = {k: tuple(v) for k, v in supported_revisions.items()}
    if backend.authorize(context, publication_id, tuple(required)) is not None:
        raise ResolutionError('Authorization provider did not complete its contract')
    rows = backend.descriptors(publication_id)
    if len(rows) != 1:
        raise ResolutionError('Missing or ambiguous publication descriptor')
    raw = dict(rows[0])
    if raw.get('publication_id') != publication_id or raw.get('profile_version') not in profiles:
        raise ResolutionError('Publication identity/profile mismatch')
    versions = _decode(raw.get('table_versions_json'))
    revisions = _decode(raw.get('schema_revisions_json'))
    progress = _decode(raw.get('source_progress_json'))
    report = _decode(raw.get('validation_report_json'))
    if not isinstance(versions, dict) or not isinstance(revisions, dict) or not revisions or not isinstance(report, dict):
        raise ResolutionError('Invalid descriptor structure')
    for table, version in versions.items():
        _name(table)
        if type(version) is not int or not 0 <= version <= 2**63 - 1:
            raise ResolutionError('Invalid Delta version')
    for source, revision in revisions.items():
        if not isinstance(revision, str) or revision not in revisions_allowed.get(source, ()):
            raise ResolutionError('Unsupported schema revision')
    if not set(required).issubset(versions):
        raise ResolutionError('Consumed table absent from publication')
    descriptor = Descriptor(publication_id, raw['profile_version'], _freeze(versions),
                            _freeze(revisions), _freeze(progress), _freeze(report), _freeze(raw))
    if backend.validate_descriptor(descriptor, context) is not None:
        raise ResolutionError('Descriptor policy did not complete its contract')
    snapshots = {}
    for table, expected_uuid in required.items():
        version = versions[table]
        snapshot = backend.inspect_snapshot(table, version)
        if (not isinstance(snapshot, Snapshot) or snapshot.table != table
                or snapshot.uuid != expected_uuid or type(snapshot.version) is not int
                or snapshot.version != version):
            raise ResolutionError('Native snapshot identity/version mismatch')
        snapshots[table] = snapshot
    return ResolvedPublication(descriptor, MappingProxyType(snapshots))

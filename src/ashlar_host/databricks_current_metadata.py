# SPDX-License-Identifier: MIT OR Apache-2.0
"""Read-only current UC metadata facts, never SELECT/ABAC/Delta-version authority.

UC table_id is distinct from the native Delta table UUID. This component does
not inspect Delta identity, historical schemas, privileges or policy revisions.
"""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
import json
from urllib.parse import urlsplit


@dataclass(frozen=True)
class CurrentColumnMetadata:
    position: int
    name: str
    type_name: str
    type_json: str
    nullable: bool
    collation: str | None


@dataclass(frozen=True)
class DatabricksCurrentMetadata:
    workspace_host: str
    principal_id: str
    relation: tuple[str, str, str]
    uc_table_id: str
    table_type: str
    data_source_format: str
    columns: tuple[CurrentColumnMetadata, ...]
    original_selected_metadata_json: str
    column_metadata_sha256: str
    # Deliberately no Delta table identity, version, readiness or authority fields.


def _text(value, maximum=4096):
    if type(value) is not str or not value or len(value.encode()) > maximum or '\0' in value:
        raise ValueError('Bounded original metadata string required')
    return value


def inspect_databricks_current_metadata(workspace, *, relation, expected_uc_table_id):
    """Use explicitly supplied SDK current_user.me and tables.get only.

    The column digest covers complete original SDK column metadata serialization,
    not a Weft/native Delta schema fingerprint or authorization proof.
    Native SDK failures propagate without a discovery or SQL fallback.
    """
    if type(relation) is not tuple or len(relation) != 3:
        raise ValueError('Exact three-part registered relation tuple required')
    for part in relation:
        _text(part, 255)
        if '.' in part: raise ValueError('Unambiguous three-part relation required')
    expected_uc_table_id = _text(expected_uc_table_id, 255)
    host = _text(workspace.config.host)
    parsed = urlsplit(host)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.query or parsed.fragment
            or parsed.path not in ('', '/')):
        raise ValueError('Explicit HTTPS workspace host required')
    principal = workspace.current_user.me()
    principal_id = _text(principal.id, 255)
    metadata = workspace.tables.get(full_name='.'.join(relation), include_browse=False).as_dict()
    if type(metadata) is not dict:
        raise ValueError('Original SDK metadata object required')
    if (metadata.get('browse_only') is not False
            or metadata.get('full_name') != '.'.join(relation)
            or metadata.get('table_id') != expected_uc_table_id
            or metadata.get('table_type') != 'MANAGED'
            or metadata.get('data_source_format') != 'DELTA'):
        raise ValueError('Registered current managed Delta UC table differs')
    original = metadata.get('columns')
    if type(original) is not list or not 1 <= len(original) <= 4096:
        raise ValueError('Bounded complete current column inventory required')
    columns = []; names = set()
    for position, column in enumerate(original):
        if type(column) is not dict or type(column.get('position')) is not int or column['position'] != position:
            raise ValueError('Exact original ordered column positions required')
        name = _text(column.get('name'), 255)
        if name.casefold() in names: raise ValueError('Ambiguous current column name')
        names.add(name.casefold())
        typename = _text(column.get('type_name'), 255)
        native_json = _text(column.get('type_json'), 65536)
        nullable = column.get('nullable')
        if type(nullable) is not bool: raise ValueError('Exact nullable metadata required')
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result: raise ValueError('Duplicate native type JSON member')
                result[key] = value
            return result
        def reject_number(_): raise ValueError('Exact finite native type metadata required')
        native_type = json.loads(native_json, object_pairs_hook=pairs,
            parse_constant=reject_number, parse_float=reject_number)
        if (type(native_type) is not dict or native_type.get('name') != name
                or type(native_type.get('type')) not in (str, dict) or not native_type['type']
                or type(native_type.get('nullable')) is not bool
                or native_type['nullable'] != nullable):
            raise ValueError('Native type JSON field correspondence differs')

        collation = column.get('collation')
        if collation is not None: _text(collation, 255)
        columns.append(CurrentColumnMetadata(position, name, typename, native_json, nullable, collation))
    column_json = json.dumps(original, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
    if len(column_json.encode()) > 1_048_576:
        raise ValueError('Current metadata capacity exceeded')
    raw = json.dumps({key: metadata[key] for key in
        ('full_name', 'table_id', 'table_type', 'data_source_format', 'browse_only')}, separators=(',', ':'))
    # Retain exact string values/type_json and optional member presence in a
    # detached selected table subset and complete original column metadata; SDK as_dict is not the original HTTP wire body.
    raw = raw[:-1] + ',"columns":' + column_json + '}'
    closing_principal = workspace.current_user.me()
    if workspace.config.host != host or _text(closing_principal.id, 255) != principal_id:
        raise ValueError('Authenticated workspace/principal changed during observation')
    return DatabricksCurrentMetadata(host, principal_id, relation, expected_uc_table_id,
        metadata['table_type'], metadata['data_source_format'], tuple(columns), raw,
        hashlib.sha256(column_json.encode()).hexdigest())

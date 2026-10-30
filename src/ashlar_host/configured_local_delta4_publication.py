# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure core for configured-local Delta 0.4 transaction publication.

This module prepares and checks a single admitted source transaction. It never
starts Spark or writes native tables. Native xxhash64 values and observed Delta
identities enter as explicit inputs from the future runtime composition.
"""
from __future__ import annotations

from dataclasses import dataclass
import datetime
import hashlib
import json
import re
from uuid import UUID

from ashlar.source import jsonl_batches
from ashlar.staging import batch_row
from ashlar.source_checkpoint import jsonl_checkpoint
from ashlar.schema import decode_retained_json
from .managed_current_carriers import load_current_managed_carriers
from .managed_source_oracle import _original_inventory

PROFILE = "ashlar-configured-local-delta4-publication-core/0.1"
CARRIER_PROFILE = "ashlar-delta/0.4-xxhash64"
LOOKUP_HASH_PROFILE = "spark-xxhash64-typed-key/0.1"
DATA_ROLES = ("edge_current", "object_current", "tombstone", "whole_source_history")
INSTALLATION_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history",
                      "publication_attempt_phase", "publication_manifest")
_MINT = object()


def _require(value, message="Configured-local Delta 0.4 publication refused"):
    if not value:
        raise ValueError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _uuid(value):
    try:
        canonical = str(UUID(value))
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Canonical Delta UUID required") from None
    _require(value == canonical, "Canonical Delta UUID required")
    return canonical


def _digest(value):
    _require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None,
             "Exact SHA-256 required")
    return value


def _original_string_keys(configuration, batch):
    """Derive declared simple STRING primary keys without reading destinations."""
    model = decode_retained_json(configuration.model_source)
    elements = {(module["id"], element["id"]): element
                for module in model["modules"] for element in module["elements"]}
    result = {}
    for record in batch.records:
        event = decode_retained_json(record.raw)
        if event["entity_kind"] != "object" or event["operation"] == "delete":
            continue
        type_id = str(event["type_id"])
        binding = configuration.bindings[type_id]
        record_module, record_id = binding["record"]
        declaration = elements[(record_module, record_id)]
        primary = [key for key in declaration.get("keys", []) if key.get("primary") is True]
        _require(len(primary) == 1, "Exactly one original primary key required for `.4` fixture projection")
        property_by_field = {(field[0], field[1]): property_id
                             for property_id, field in binding["fields"].items()}
        properties = decode_retained_json(event["props_json"].encode("utf-8"))
        values = []
        for field in primary[0]["fields"]:
            identity = (field["module"], field["element"])
            property_id = property_by_field.get(identity)
            _require(property_id is not None and property_id in properties,
                     "Original primary key field must be present in the source event")
            value = properties[property_id]
            _require(type(value) is str,
                     "This transaction-1 projection admits only STRING primary-key values")
            values.append(value)
        entity_key = ("object", event["source_system"], int(event["type_id"]), int(event["id"]))
        _require(entity_key not in result, "Duplicate source-qualified object identity in transaction 1")
        result[entity_key] = _canonical(values)
    return result


@dataclass(frozen=True, init=False)
class ConfiguredLocalDelta4BatchPlan:
    """Detached source plan for exactly the first transaction, with no native authority."""
    stream: str
    batch_id: str
    publication_id: str
    request_json: str
    source_sha256: str
    configuration_sha256: str
    model_sha256: str
    binding_sha256: str
    batch_sha256: str
    materialized_at_us: str
    expected_rows_json: str
    pending_lookup_keys_json: str
    _identity: tuple

    def __init__(self, stream, batch_id, publication_id, request_json, source_sha256,
                 configuration_sha256, model_sha256, binding_sha256, batch_sha256,
                 materialized_at_us, expected_rows_json, pending_lookup_keys_json, *, _mint=None):
        _require(_mint is _MINT, "Factory-owned `.4` source plan required")
        for key, value in locals().copy().items():
            if key not in ("self", "_mint"):
                object.__setattr__(self, key, value)
        object.__setattr__(self, "_identity", tuple(getattr(self, name) for name in (
            "stream", "batch_id", "publication_id", "request_json", "source_sha256",
            "configuration_sha256", "model_sha256", "binding_sha256", "batch_sha256",
            "materialized_at_us", "expected_rows_json", "pending_lookup_keys_json")))

    def require_current(self):
        _require(self._identity == tuple(getattr(self, name) for name in (
            "stream", "batch_id", "publication_id", "request_json", "source_sha256",
            "configuration_sha256", "model_sha256", "binding_sha256", "batch_sha256",
            "materialized_at_us", "expected_rows_json", "pending_lookup_keys_json")),
            "Frozen `.4` source plan changed")

    @property
    def request(self):
        self.require_current()
        return decode_retained_json(self.request_json.encode("utf-8"))

    @property
    def expected_rows(self):
        self.require_current()
        return decode_retained_json(self.expected_rows_json.encode("utf-8"))

    @property
    def pending_lookup_keys(self):
        self.require_current()
        return decode_retained_json(self.pending_lookup_keys_json.encode("utf-8"))

    @property
    def source_oracle_sha256(self):
        self.require_current()
        return _sha(_canonical(self.expected_rows).encode("utf-8"))


def plan_configured_local_delta4_transaction1(configuration, *, stream: str,
                                                materialized_at_us: str):
    """Plan transaction 1 from exact typed-graph input bytes and pinned `.4` carriers.

    The typed graph configuration must already have passed its public producer
    admission before native use. This pure projection does not itself confer
    producer, publication or ACK authority.
    """
    from ashlar.typed_graph_source_config import TypedGraphJsonlConfiguration
    _require(type(configuration) is TypedGraphJsonlConfiguration,
             "Original typed graph JSONL configuration required")
    configuration.verify()
    _require(type(stream) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:_-]{0,127}", stream),
             "Closed configured stream identity required")
    _require(type(materialized_at_us) is str
             and re.fullmatch(r"0|[1-9][0-9]{0,18}", materialized_at_us)
             and int(materialized_at_us) < 2**63,
             "Canonical signed 64-bit UTC materialization clock required")
    batches = tuple(jsonl_batches(configuration.source_bytes.splitlines(keepends=True),
                                  feed=configuration.feed, epoch=configuration.epoch))
    _require(len(batches) >= 1 and batches[0].batch_id == "typed-graph-1"
             and len(batches[0].records) == 8,
             "Exact first typed-graph transaction required")
    batch = batches[0]
    row = batch_row(batch)
    request = {
        "stream": stream,
        "batch_id": batch.batch_id,
        "predecessor": "empty-local-configured-jsonl-stream",
        "schema_revisions_json": _canonical({configuration.schema_alias: configuration.document_revision}),
        "source_batch_json": row["batch_json"],
        "source_batch_digest": row["batch_digest"],
        "source_checkpoint_json": jsonl_checkpoint(batch),
    }
    request["request_digest"] = _sha(_canonical(request).encode("utf-8"))
    publication_id = "local-configured-jsonl-" + request["request_digest"]

    carriers = load_current_managed_carriers(CARRIER_PROFILE)
    columns = {role: tuple((name, kind) for name, kind, _nullable in
                           carriers.ordered_fields(role)) for role in DATA_ROLES}
    instant = (datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
               + datetime.timedelta(microseconds=int(materialized_at_us))).isoformat()
    logical_keys = _original_string_keys(configuration, batch)
    templates = _original_inventory((batch,), columns, materialized_at=instant,
                                    carrier_profile=CARRIER_PROFILE, logical_keys=logical_keys)
    pending = {"object_current": [], "edge_current": []}
    for role, identity_column in (("object_current", "type_id"), ("edge_current", "rel_type_id")):
        for item in templates[role]:
            source_key = (item["source_system"], int(item[identity_column]), int(item["id"]))
            pending[role].append({"role": role, "source_system": source_key[0], identity_column: source_key[1], "id": source_key[2]})
            # The shared source projection emits a pending hash slot for `.4`;
            # native Spark SQL supplies its value in the materialization step.
    for role in DATA_ROLES:
        expected_fields = {name for name, _kind in columns[role]}
        if role in ("object_current", "edge_current"):
            expected_fields.remove("lookup_hash")
        _require(all(set(item) == expected_fields for item in templates[role]),
                 "Exact generated `.4` carrier field set required")
    return ConfiguredLocalDelta4BatchPlan(
        stream, batch.batch_id, publication_id, _canonical(request),
        _sha(configuration.source_bytes), _sha(configuration.original),
        configuration.model_sha256, _sha(configuration.binding_source), _sha(batch.begin +
        b"".join(record.raw for record in batch.records) + batch.commit),
        materialized_at_us, _canonical(templates), _canonical(pending), _mint=_MINT)


def materialize_delta4_expected_rows(plan: ConfiguredLocalDelta4BatchPlan, lookup_hashes: dict):
    """Attach exact native Spark xxhash64 observations to `.4` source templates."""
    _require(type(plan) is ConfiguredLocalDelta4BatchPlan)
    plan.require_current()
    _require(type(lookup_hashes) is dict, "Exact native xxhash64 observation map required")
    expected_keys = set()
    for role in ("object_current", "edge_current"):
        identity_column = "type_id" if role == "object_current" else "rel_type_id"
        for key in plan.pending_lookup_keys[role]:
            expected_keys.add((role, key["source_system"], key[identity_column], key["id"]))
    _require(set(lookup_hashes) == expected_keys,
             "Native xxhash64 keys must exactly match the complete source identity set")
    for key, value in lookup_hashes.items():
        _require(type(key) is tuple and len(key) == 4 and key[0] in ("object_current", "edge_current")
                 and type(key[1]) is str and type(key[2]) is int and type(key[3]) is int
                 and type(value) is int and -(1 << 63) <= value < (1 << 63),
                 "Role-qualified native signed BIGINT xxhash64 observations required")
    rows = plan.expected_rows
    for role in ("object_current", "edge_current"):
        identity_column = "type_id" if role == "object_current" else "rel_type_id"
        for row in rows[role]:
            key = (role, row["source_system"], int(row[identity_column]), int(row["id"]))
            row["lookup_hash"] = lookup_hashes[key]
    carriers = load_current_managed_carriers(CARRIER_PROFILE)
    columns = {role: tuple((name, kind) for name, kind, _nullable in
                           carriers.ordered_fields(role)) for role in DATA_ROLES}
    _require(all(set(item) == {name for name, _kind in columns[role]}
                 for role in DATA_ROLES for item in rows[role]),
             "Materialized `.4` carrier rows differ from original generated schema")
    return rows


def build_delta4_realized_vector(manifest_row: dict, installation: dict):
    """Bind six-role observations to a separately retained manifest row.

    This checks row shape/profile and observed identities only. It does not
    authenticate the manifest, prove native comparisons, or mint a manifest.
    Callers must independently obtain the row from the retained Delta manifest.
    """
    from ashlar.manifest import validate_manifest_row
    validate_manifest_row(manifest_row)
    _require(manifest_row["profile_version"] == CARRIER_PROFILE,
             "Original `.4` publication manifest required")
    versions = decode_retained_json(manifest_row["table_versions_json"].encode("utf-8"))
    expected_names = {"local.configured." + role for role in DATA_ROLES}
    _require(set(versions) == expected_names, "Complete original four-role data vector required")
    _require(type(installation) is dict and set(installation) == set(INSTALLATION_ROLES),
             "Complete six-role current installation observations required")
    uuids = set()
    inventory = {}
    for role in INSTALLATION_ROLES:
        observed = installation[role]
        _require(type(observed) is dict and set(observed) == {
            "name", "uuid", "version", "schema_sha256", "protocol_sha256"},
            "Closed six-role native identity observation required")
        name = "local.configured." + role
        _require(observed["name"] == name and type(observed["version"]) is int
                 and 0 <= observed["version"] < 2**63,
                 "Exact named Delta head observation required")
        uuid = _uuid(observed["uuid"])
        _require(uuid not in uuids, "Six native role UUIDs must be unique")
        uuids.add(uuid)
        _digest(observed["schema_sha256"]); _digest(observed["protocol_sha256"])
        inventory[role] = {"name": name, "uuid": uuid, "version": observed["version"],
                           "schemaSha256": observed["schema_sha256"],
                           "protocolSha256": observed["protocol_sha256"]}
        if role in DATA_ROLES:
            _require(versions[name] == observed["version"],
                     "Manifest version differs from the observed four-role head")
    manifest_uuid = installation["publication_manifest"]["uuid"]
    body = {
        "profile": "ashlar-configured-local-delta4-realized-vector/0.1",
        "carrierProfile": CARRIER_PROFILE,
        "publicationId": manifest_row["publication_id"],
        "manifestUuid": manifest_uuid,
        "manifestSha256": _sha(_canonical(manifest_row).encode("utf-8")),
        "dataRoles": {role: {"name": "local.configured." + role,
                              "uuid": installation[role]["uuid"],
                              "version": versions["local.configured." + role]}
                      for role in DATA_ROLES},
        "installationInventory": inventory,
    }
    body["canonicalSha256"] = _sha(_canonical(body).encode("utf-8"))
    return body

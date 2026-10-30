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
EFFECT_PLAN_PROFILE = "ashlar-configured-local-delta4-effect-plan/0.1"
CARRIER_PROFILE = "ashlar-delta/0.4-xxhash64"
LOOKUP_HASH_PROFILE = "spark-xxhash64-typed-key/0.1"
DATA_ROLES = ("edge_current", "object_current", "tombstone", "whole_source_history")
INSTALLATION_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history",
                      "publication_attempt_phase", "publication_manifest")
_MINT = object()
_EFFECT_MINT = object()


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


def observe_delta4_lookup_hashes(plan: ConfiguredLocalDelta4BatchPlan, transport,
                                installation_profile: dict):
    """Observe transaction-1 `.4` fingerprints through its owned Spark transport.

    This is a bounded read-only native operation. It does not admit or write a
    publication, and it cannot mint a manifest. The separate installation
    profile must be the retained six-role `.4` profile from initialization.
    """
    from .delta_custody import LocalDeltaTransport
    _require(type(plan) is ConfiguredLocalDelta4BatchPlan)
    plan.require_current()
    _require(type(transport) is LocalDeltaTransport,
             "Caller-owned local Delta transport required")
    _require(type(installation_profile) is dict
             and installation_profile.get("profile") == "ashlar-local-current-installation-profile/0.1"
             and installation_profile.get("physical_profile") == CARRIER_PROFILE
             and installation_profile.get("lookup_hash_profile") == LOOKUP_HASH_PROFILE
             and type(installation_profile.get("roles")) is dict
             and set(installation_profile["roles"]) == set(INSTALLATION_ROLES),
             "Retained six-role `.4` installation profile required")
    _require(getattr(transport, "installation_profile", None) == installation_profile,
             "`.4` installation profile must be pinned by the native operation journal")
    _require(set(transport.targets) == {"local.configured." + role for role in INSTALLATION_ROLES},
             "Exact six-role initialized local Delta registry required")
    for role in INSTALLATION_ROLES:
        observed = installation_profile["roles"][role]
        target = transport.targets["local.configured." + role]
        _require(type(observed) is dict and observed.get("table") == target.table
                 and observed.get("path") == str(target.path) and observed.get("uuid") == target.uuid,
                 "Initialized `.4` role identity differs from its retained profile")
    keys = []
    expected = set()
    for role in ("object_current", "edge_current"):
        identity_column = "type_id" if role == "object_current" else "rel_type_id"
        for key in plan.pending_lookup_keys[role]:
            values = (role, key["source_system"], key[identity_column], key["id"])
            _require(type(values[0]) is str and type(values[1]) is str
                     and type(values[2]) is int and type(values[3]) is int,
                     "Exact role-qualified typed hash key required")
            token = (role, values[1], values[2], values[3])
            _require(token not in expected, "Unique role-qualified source identity required")
            expected.add(token)
            keys.append({"role": role, "source_system": values[1],
                         "type_id": values[2], "id": values[3]})
    _require(0 < len(keys) <= 1000, "Bounded transaction-1 typed hash inventory required")
    sql = ("SELECT k.role AS role,k.source_system AS source_system,k.type_id AS type_id,k.id AS id,"
           "xxhash64(CAST(k.source_system AS STRING),CAST(k.type_id AS BIGINT),CAST(k.id AS BIGINT)) AS lookup_hash "
           "FROM (SELECT explode(from_json(:keys,'ARRAY<STRUCT<role:STRING,source_system:STRING,type_id:BIGINT,id:BIGINT>>')) AS k)")
    native_result = transport.query(sql, {"keys": _canonical(keys)})
    _require(type(native_result.columns) is tuple and native_result.columns == (
        ("role", "STRING"), ("source_system", "STRING"), ("type_id", "BIGINT"),
        ("id", "BIGINT"), ("lookup_hash", "BIGINT")),
        "Exact ordered native `.4` Spark hash result schema required")
    result = native_result.rows
    _require(type(result) is list and len(result) == len(expected),
             "Complete bounded native `.4` Spark hash observations required")
    hashes = {}
    for row in result:
        _require(type(row) is dict and set(row) == {
            "role", "source_system", "type_id", "id", "lookup_hash"},
            "Exact native `.4` Spark hash result shape required")
        key = (row["role"], row["source_system"], row["type_id"], row["id"])
        _require(key in expected and key not in hashes
                 and type(row["type_id"]) is int and type(row["id"]) is int
                 and type(row["lookup_hash"]) is int
                 and -(1 << 63) <= row["lookup_hash"] < (1 << 63),
                 "Exact unique signed BIGINT Spark hash result required")
        hashes[key] = row["lookup_hash"]
    _require(set(hashes) == expected, "Complete role-qualified Spark hash vector required")
    return hashes


@dataclass(frozen=True, init=False)
class ConfiguredLocalDelta4EffectPlan:
    """Immutable parameterized transaction-1 SQL intent; never executed here."""
    profile: str
    request_digest: str
    installation_profile_sha256: str
    intent_digest: str
    effect_plan_sha256: str
    steps_json: str
    _identity: tuple

    def __init__(self, request_digest, installation_profile_sha256, intent_digest,
                 effect_plan_sha256, steps_json, *, _mint=None):
        _require(_mint is _EFFECT_MINT, "Factory-owned `.4` native effect plan required")
        for key, value in locals().copy().items():
            if key not in ("self", "_mint"):
                object.__setattr__(self, key, value)
        object.__setattr__(self, "profile", EFFECT_PLAN_PROFILE)
        object.__setattr__(self, "_identity", (self.profile, self.request_digest,
            self.installation_profile_sha256, self.intent_digest,
            self.effect_plan_sha256, self.steps_json))

    @property
    def steps(self):
        _require(self._identity == (self.profile, self.request_digest,
                                     self.installation_profile_sha256, self.intent_digest,
                                     self.effect_plan_sha256, self.steps_json),
                 "Frozen `.4` native effect plan changed")
        steps = decode_retained_json(self.steps_json.encode("utf-8"))
        _require(_sha(_canonical(steps).encode("utf-8")) == self.effect_plan_sha256,
                 "Exact `.4` native effect plan digest required")
        _require(self.intent_digest == _sha(_canonical({
            "request_digest": self.request_digest,
            "installation_profile_sha256": self.installation_profile_sha256,
            "effect_plan_sha256": self.effect_plan_sha256,
        }).encode("utf-8")), "Exact bound `.4` native effect intent digest required")
        return steps


def build_delta4_transaction1_effect_plan(plan: ConfiguredLocalDelta4BatchPlan, *, tables: dict,
                                           installation_profile: dict):
    """Build three exact parameterized INSERTs for a fresh empty `.4` installation.

    Fresh-empty native heads remain a caller admission precondition. This
    builder only binds the plan to the retained six-role profile and emits
    immutable SQL intent; it performs no native reads or mutations.
    """
    from ashlar.native import quote_table_identifier
    _require(type(plan) is ConfiguredLocalDelta4BatchPlan)
    plan.require_current()
    _require(type(tables) is dict and set(tables) == set(INSTALLATION_ROLES),
             "Exact six-role configured local table map required")
    _require(type(installation_profile) is dict
             and installation_profile.get("profile") == "ashlar-local-current-installation-profile/0.1"
             and installation_profile.get("physical_profile") == CARRIER_PROFILE
             and installation_profile.get("lookup_hash_profile") == LOOKUP_HASH_PROFILE
             and type(installation_profile.get("roles")) is dict
             and set(installation_profile["roles"]) == set(INSTALLATION_ROLES),
             "Exact retained six-role `.4` installation profile required")
    for role in INSTALLATION_ROLES:
        table = tables[role]
        quote_table_identifier(table)
        role_profile = installation_profile["roles"][role]
        _require(type(role_profile) is dict and table == "local.configured." + role
                 and role_profile.get("table") == table,
                 "`.4` native effect target differs from retained installation role")
        _uuid(role_profile.get("uuid"))
    source_rows = plan.expected_rows
    _require(source_rows["tombstone"] == [] and bool(source_rows["object_current"])
             and bool(source_rows["edge_current"]) and bool(source_rows["whole_source_history"])
             and len(source_rows["object_current"]) + len(source_rows["edge_current"]) == 8
             and len(source_rows["whole_source_history"]) == 8,
             "Exact eight-event transaction-1 fresh-install projection required")
    carriers = load_current_managed_carriers(CARRIER_PROFILE)
    steps = []
    for role in ("object_current", "edge_current", "whole_source_history"):
        rows = source_rows[role]
        fields = carriers.ordered_fields(role)
        native_names = [name for name, _kind, _nullable in fields]
        staging_fields = [(name, "STRING" if name == "published_at" else kind)
                          for name, kind, _nullable in fields if name != "lookup_hash"]
        if role == "whole_source_history":
            staging_fields = [(name, kind) for name, kind, _nullable in fields]
        _require(all(set(row) == {name for name, _kind in staging_fields} for row in rows),
                 "Exact `.4` source staging field set required")
        for row in rows:
            _require(all(type(value) is not float for value in row.values()),
                     "Floating-point source staging values are forbidden")
            for name, kind in staging_fields:
                value = row[name]
                if kind == "BIGINT":
                    _require(value is None or type(value) is int and -(1 << 63) <= value < (1 << 63),
                             "Exact signed BIGINT source staging value required")
                else:
                    _require(value is None or type(value) is str,
                             "Exact STRING source staging value required")
        staging_schema = "ARRAY<STRUCT<" + ",".join(name + ":" + kind for name, kind in staging_fields) + ">>"
        relation = "(SELECT explode(from_json(:rows,'" + staging_schema + "')) AS s)"
        output = []
        for name in native_names:
            if name == "lookup_hash":
                type_column = "type_id" if role == "object_current" else "rel_type_id"
                output.append("xxhash64(CAST(s.source_system AS STRING),CAST(s." + type_column
                              + " AS BIGINT),CAST(s.id AS BIGINT)) AS lookup_hash")
            elif name == "published_at":
                output.append("timestamp_micros(CAST(s.published_at AS BIGINT)) AS published_at")
            else:
                output.append("s." + name)
        sql = ("INSERT INTO " + quote_table_identifier(tables[role]) + " ("
               + ",".join(native_names) + ") SELECT " + ",".join(output) + " FROM " + relation)
        steps.append({"statement": sql, "parameters": {"rows": _canonical(rows)}})
    effect_plan_sha256 = _sha(_canonical(steps).encode("utf-8"))
    request_digest = plan.request["request_digest"]
    installation_profile_sha256 = _sha(_canonical(installation_profile).encode("utf-8"))
    intent_digest = _sha(_canonical({"request_digest": request_digest,
        "installation_profile_sha256": installation_profile_sha256,
        "effect_plan_sha256": effect_plan_sha256}).encode("utf-8"))
    return ConfiguredLocalDelta4EffectPlan(request_digest, installation_profile_sha256,
                                           intent_digest, effect_plan_sha256,
                                           _canonical(steps), _mint=_EFFECT_MINT)


_FRESH_HEAD_MINT = object()


@dataclass(frozen=True, init=False)
class ConfiguredLocalDelta4FreshHeadAdmission:
    """Read-only proof that one journal-pinned six-role installation is fresh and empty."""
    installation_profile_sha256: str
    heads_json: str
    inventory_json: str
    _identity: tuple

    def __init__(self, installation_profile_sha256, heads_json, inventory_json, *, _mint=None):
        _require(_mint is _FRESH_HEAD_MINT, "Native fresh `.4` head admission required")
        for key, value in locals().copy().items():
            if key not in ("self", "_mint"):
                object.__setattr__(self, key, value)
        object.__setattr__(self, "_identity", (installation_profile_sha256, heads_json, inventory_json))

    def require_current(self):
        _require(self._identity == (self.installation_profile_sha256, self.heads_json,
                                     self.inventory_json),
                 "Frozen native fresh `.4` head admission changed")

    @property
    def heads(self):
        self.require_current()
        return decode_retained_json(self.heads_json.encode("utf-8"))


def _require_delta4_transport(transport, installation_profile):
    from .delta_custody import LocalDeltaTransport
    from ashlar.native import quote_table_identifier
    _require(type(transport) is LocalDeltaTransport
             and type(installation_profile) is dict
             and transport.installation_profile == installation_profile
             and installation_profile.get("profile") == "ashlar-local-current-installation-profile/0.1"
             and installation_profile.get("physical_profile") == CARRIER_PROFILE
             and installation_profile.get("lookup_hash_profile") == LOOKUP_HASH_PROFILE
             and type(installation_profile.get("roles")) is dict
             and set(installation_profile["roles"]) == set(INSTALLATION_ROLES)
             and set(transport.targets) == {"local.configured." + role for role in INSTALLATION_ROLES},
             "Journal-pinned exact six-role `.4` local installation required")
    seen_uuids = set()
    for role in INSTALLATION_ROLES:
        target = transport.targets["local.configured." + role]
        profile_role = installation_profile["roles"][role]
        _require(type(profile_role) is dict and profile_role.get("table") == target.table
                 and profile_role.get("path") == str(target.path)
                 and profile_role.get("uuid") == target.uuid,
                 "Native target UUID/path differs from exact retained `.4` role profile")
        _uuid(profile_role["uuid"])
        _require(target.uuid not in seen_uuids, "Unique six-role Delta UUID inventory required")
        seen_uuids.add(target.uuid)
        quote_table_identifier(target.table)
    return _sha(_canonical(installation_profile).encode("utf-8"))


def _observe_delta4_role(transport, role, installation_profile):
    from ashlar.native import quote_table_identifier
    from ashlar.protocol import validate_protocol_detail
    from .configured_local_runtime import PROFILE as READER_PROTOCOL
    target = transport.targets["local.configured." + role]
    selected = installation_profile["roles"][role]
    # The path-based proof and table-name proof must resolve to the same Delta
    # object.  A catalog alias can otherwise point somewhere else while the
    # path reader still sees the initialized installation.
    path_detail = transport._detail(target, full=True)
    _require(type(path_detail) is dict and path_detail.get("id") == target.uuid,
             "Path-based Delta UUID must match the retained table UUID")
    detail_result = transport.query("DESCRIBE DETAIL " + quote_table_identifier(target.table), {})
    _require(type(detail_result.rows) is list and len(detail_result.rows) == 1,
             "Unique native Delta detail required")
    detail = detail_result.rows[0]
    _require(type(detail) is dict and detail.get("id") == target.uuid
             and detail.get("format") == "delta",
             "Exact native Delta UUID and format required")
    normalized = dict(detail)
    normalized["minReaderVersion"] = str(normalized.get("minReaderVersion"))
    normalized["minWriterVersion"] = str(normalized.get("minWriterVersion"))
    features = normalized.get("tableFeatures")
    _require(type(features) in (list, tuple), "Complete native Delta feature inventory required")
    normalized["tableFeatures"] = json.dumps(list(features), separators=(",", ":"))
    protocol = validate_protocol_detail(normalized, uuid=target.uuid, profile=READER_PROTOCOL)
    _require(protocol == selected.get("protocol"),
             "Native Delta protocol differs from retained `.4` role profile")

    history = transport.original_history(target)
    _require(type(history) is list and 0 < len(history) <= 1000
             and all(type(item) is dict and type(item.get("version")) is int
                     and 0 <= item["version"] < 2**63 for item in history),
             "Complete bounded native Delta history required")
    versions = [item["version"] for item in history]
    head = max(versions)
    _require(len(set(versions)) == len(versions)
             and head < len(versions)
             and sorted(versions) == list(range(len(versions)))
             and versions.count(head) == 1,
             "Unique contiguous native Delta history from version zero through current head required")

    native = (transport.spark.read.format("delta").option("versionAsOf", head)
              .load(str(target.path)).schema)
    kind_names = {"string": "STRING", "long": "BIGINT", "timestamp": "TIMESTAMP"}
    actual_fields = tuple((field.name, kind_names.get(field.dataType.typeName()), field.nullable)
                          for field in native.fields)
    expected_fields = tuple((name, kind, nullable) for name, kind, nullable in selected["fields"])
    _require(actual_fields == expected_fields,
             "Pinned native schema differs from retained `.4` UMF role fields")
    return target, head, protocol, actual_fields


def admit_delta4_fresh_empty_installation(transport, installation_profile):
    """Read-only fresh-empty gate for all six initialized `.4` local roles."""
    from ashlar.native import quote_table_identifier
    profile_sha = _require_delta4_transport(transport, installation_profile)
    heads = {}
    inventory = {}
    for role in INSTALLATION_ROLES:
        target, version, protocol, fields = _observe_delta4_role(transport, role, installation_profile)
        table = quote_table_identifier(target.table)
        count_result = transport.query("SELECT count(*) AS row_count FROM " + table
                                       + " VERSION AS OF " + str(version), {})
        _require(count_result.columns == (("row_count", "BIGINT"),)
                 and type(count_result.rows) is list and len(count_result.rows) == 1
                 and type(count_result.rows[0].get("row_count")) is int
                 and count_result.rows[0]["row_count"] == 0,
                 "All six fresh `.4` native heads must be empty")
        versions = [entry["version"] for entry in transport.original_history(target)]
        _require(sorted(versions) == [0, 1] and version == 1,
                 "Fresh initialized `.4` role must have only CREATE and installation registration commits")
        heads[role] = {"uuid": target.uuid, "version": version}
        inventory[role] = {"name": target.table, "uuid": target.uuid, "version": version,
                           "schemaSha256": _sha(_canonical(fields).encode("utf-8")),
                           "protocolSha256": _sha(_canonical(protocol).encode("utf-8"))}
    for table_name in ("local_operation", "local_plan", "local_publication_artifact",
                       "local_source_plan"):
        row = transport.db.execute("SELECT count(*) FROM " + table_name).fetchone()
        _require(row == (0,), "Fresh `.4` operation journal must contain no prior publication intent")
    return ConfiguredLocalDelta4FreshHeadAdmission(profile_sha, _canonical(heads),
                                                    _canonical(inventory), _mint=_FRESH_HEAD_MINT)


def validate_delta4_transaction1_snapshots(plan: ConfiguredLocalDelta4BatchPlan, transport,
                                           installation_profile: dict,
                                           fresh: ConfiguredLocalDelta4FreshHeadAdmission, *,
                                           lookup_hashes: dict, effect_versions: dict):
    """Compare four pinned `.4` role bags after the three retained INSERT effects.

    Successful historical reads prove those Delta snapshots are readable now;
    this result does not establish a future retention window or ACK authority.
    """
    from ashlar.effect_validation import validate_effect_snapshot
    from ashlar.native import quote_table_identifier
    profile_sha = _require_delta4_transport(transport, installation_profile)
    _require(type(plan) is ConfiguredLocalDelta4BatchPlan
             and type(fresh) is ConfiguredLocalDelta4FreshHeadAdmission)
    plan.require_current(); fresh.require_current()
    _require(fresh.installation_profile_sha256 == profile_sha
             and type(effect_versions) is dict and set(effect_versions) == set(DATA_ROLES)
             and all(type(value) is int and 0 <= value < 2**63 for value in effect_versions.values()),
             "Exact four-role post-effect version vector and matching fresh admission required")
    baseline = fresh.heads
    for role in DATA_ROLES:
        delta = 0 if role == "tombstone" else 1
        _require(effect_versions[role] == baseline[role]["version"] + delta,
                 "One-shot `.4` role version must follow its exact fresh head by the fixed INSERT count")
    expected_rows = materialize_delta4_expected_rows(plan, lookup_hashes)
    carriers = load_current_managed_carriers(CARRIER_PROFILE)
    observed = {}
    inventory = {}
    for role in INSTALLATION_ROLES:
        target, version, protocol, fields = _observe_delta4_role(transport, role, installation_profile)
        if role in ("publication_attempt_phase", "publication_manifest"):
            _require(version == baseline[role]["version"],
                     "`.4` metadata roles must remain at their fresh versions after transaction-1")
        if role in DATA_ROLES:
            expected_version = effect_versions[role]
            _require(version == expected_version,
                     "Native `.4` current head differs from exact expected role version")
            columns = tuple((name, kind) for name, kind, _nullable in carriers.ordered_fields(role))
            string_rows = [{name: (str(value) if type(value) is int else value)
                            for name, value in row.items()} for row in expected_rows[role]]
            validate_effect_snapshot(transport, target.table, target.uuid, version, columns,
                                     string_rows)
            if role in ("object_current", "edge_current"):
                from .local_current_carrier_profile import verify_local_xxhash64_snapshot
                hash_role_count = verify_local_xxhash64_snapshot(transport.spark, target.path,
                                                                  version, role)
                _require(hash_role_count == len(expected_rows[role]),
                         "Complete native `.4` xxhash64 full-key snapshot required")
            observed[role] = {"uuid": target.uuid, "version": version,
                              "rows": len(expected_rows[role])}
        inventory[role] = {"name": target.table, "uuid": target.uuid, "version": version,
                           "schemaSha256": _sha(_canonical(fields).encode("utf-8")),
                           "protocolSha256": _sha(_canonical(protocol).encode("utf-8"))}
    return {"profile": "ashlar-configured-local-delta4-snapshot-admission/0.1",
            "carrierProfile": CARRIER_PROFILE,
            "requestDigest": plan.request["request_digest"],
            "installationProfileSha256": profile_sha,
            "freshHeadsSha256": _sha(fresh.heads_json.encode("utf-8")),
            "dataRoles": observed,
            "installationInventory": inventory,
            "snapshotReadability": "pinned snapshots readable at observation time; future retention not established",
            "protectedAck": False}


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

# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure core for configured-local Delta 0.4 transaction publication.

This module prepares and checks a single admitted source transaction. It never
starts Spark or writes native tables. Native xxhash64 values and observed Delta
identities enter as explicit inputs from the future runtime composition.
"""
from __future__ import annotations

from dataclasses import dataclass
import datetime
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
import re
import stat
from uuid import UUID

from ashlar.source import jsonl_batches
from ashlar.staging import batch_row
from ashlar.source_checkpoint import jsonl_checkpoint
from ashlar.schema import decode_retained_json
from .managed_current_carriers import load_current_managed_carriers
from .managed_source_oracle import _original_inventory

PROFILE = "ashlar-configured-local-delta4-publication-core/0.1"
EFFECT_PLAN_PROFILE = "ashlar-configured-local-delta4-effect-plan/0.1"
SOURCE_SCOPE_PROFILE = "ashlar-configured-local-delta4-source-scope/0.1"
SOURCE_SCOPE_JOURNAL_VERSION = 4001
MAX_SOURCE_SCOPE_BYTES = 4 * 1024 * 1024
CARRIER_PROFILE = "ashlar-delta/0.4-xxhash64"
LOOKUP_HASH_PROFILE = "spark-xxhash64-typed-key/0.1"
DATA_ROLES = ("edge_current", "object_current", "tombstone", "whole_source_history")
INSTALLATION_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history",
                      "publication_attempt_phase", "publication_manifest")
_MINT = object()
_EFFECT_MINT = object()
_AUTHORITY_MINT = object()


def _require(value, message="Configured-local Delta 0.4 publication refused"):
    if not value:
        raise ValueError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _validate_delta4_source_scope_row(request_digest, original):
    """Validate the separately versioned pre-effect custody row envelope."""
    _require(type(original) is str, "Retained `.4` source scope must be exact text")
    try:
        value = json.loads(original)
    except (TypeError, ValueError):
        raise ValueError("Retained `.4` source scope JSON required") from None
    _require(type(value) is dict and set(value) == {
        "profile", "request_digest", "scope", "scope_sha256"}
        and value["profile"] == SOURCE_SCOPE_PROFILE
        and value["request_digest"] == request_digest
        and type(value["scope"]) is dict
        and _canonical(value) == original
        and value["scope_sha256"] == _sha(_canonical(value["scope"]).encode("utf-8")),
        "Exact canonical `.4` source scope record required")
    scope = value["scope"]
    _require(type(scope.get("request")) is dict
             and scope["request"].get("request_digest") == request_digest
             and type(scope["request"].get("json")) is str
             and scope["request"].get("sha256") == _sha(
                 scope["request"]["json"].encode("utf-8")),
             "Exact retained `.4` transaction request required")
    for field, digest_field in (("fresh_heads_json", "fresh_heads_sha256"),
                                ("fresh_inventory_json", "fresh_inventory_sha256")):
        _require(type(scope.get(field)) is str
                 and scope.get(digest_field) == _sha(scope[field].encode("utf-8")),
                 "Exact retained `.4` fresh-head and inventory vector required")
    return value


def _read_delta4_source_scope_rows(db):
    """Bound retained scope text before SQLite returns its contents."""
    count = db.execute("SELECT count(*) FROM local_source_plan").fetchone()[0]
    _require(type(count) is int and count <= 1,
             "At most one durable `.4` source-scope record is allowed")
    if count == 0:
        return []
    key, size = db.execute(
        "SELECT request_digest,length(CAST(original AS BLOB)) FROM local_source_plan").fetchone()
    _require(type(key) is str and type(size) is int and 0 <= size <= MAX_SOURCE_SCOPE_BYTES,
             "Durable `.4` source-scope record exceeds its byte bound")
    return db.execute("SELECT request_digest,original FROM local_source_plan").fetchall()


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


class ConfiguredLocalDelta4WriterAuthority:
    """Owner-held `.4` source/head scope; native effects remain unauthorized."""
    def __init__(self, plan, effect_plan, transport, installation_profile, fresh,
                 source, current_admission, context, policy, *, _mint=None):
        _require(_mint is _AUTHORITY_MINT, "Factory-owned `.4` writer authority required")
        self.plan = plan
        self.effect_plan = effect_plan
        self.transport = transport
        self.installation_profile = installation_profile
        self.fresh = fresh
        self.source = source
        self.current_admission = current_admission
        self.context = context
        self.policy = policy
        self.held = False
        self._active = False
        self._fd = None
        self._path = None
        self._parent_identity = None
        self._file_identity = None
        self.source_scope_sha256 = None
        self._witness = (plan, effect_plan, transport, installation_profile, fresh,
                         source, current_admission, context, policy)

    def _require_current(self, *, held=False):
        from .delta_custody import LocalDeltaTransport
        _require((self.plan, self.effect_plan, self.transport, self.installation_profile,
                  self.fresh, self.source, self.current_admission, self.context,
                  self.policy) == self._witness,
                 "Original `.4` authority inputs changed")
        self.plan.require_current()
        steps = self.effect_plan.steps
        _require(type(self.transport) is LocalDeltaTransport
                 and self.transport.policy is self.policy
                 and self.policy.context is self.context
                 and {target.table: target for target in self.policy.targets}
                 == dict(self.transport.targets)
                 and not self.policy.initializing
                 and self.source.context is self.context
                 and self.source.graph is True
                 and self.current_admission.source is self.source
                 and self.current_admission.context is self.context
                 and callable(self.current_admission)
                 and self.policy.active is None,
                 "Original configured-local source and private writer owner required")
        _require(self.plan.batch_id == "typed-graph-1"
                 and self.source.spec(1, self.context).request() == self.plan.request,
                 "Exact original source transaction-1 request required")
        _require(self.effect_plan.request_digest == self.plan.request["request_digest"]
                 and self.effect_plan.installation_profile_sha256
                 == _sha(_canonical(self.installation_profile).encode("utf-8"))
                 and len(steps) == 3,
                 "Exact frozen `.4` effect plan/profile binding required")
        self.fresh.require_current()
        _require(self.fresh.installation_profile_sha256
                 == self.effect_plan.installation_profile_sha256,
                 "Fresh native heads bind the exact effect installation profile")
        _require(getattr(self.transport, "installation_profile", None)
                 == self.installation_profile,
                 "Journal-pinned `.4` installation profile required")
        if held:
            _require(self.held and self._fd is not None,
                     "Active original `.4` writer authority required")
            self._check_writer_lock()

    def _check_writer_lock(self):
        path = self._path
        parent = path.parent.lstat()
        named = path.lstat()
        opened = os.fstat(self._fd)
        _require(stat.S_ISDIR(parent.st_mode) and parent.st_uid == os.getuid()
                 and not parent.st_mode & 0o022
                 and (parent.st_dev, parent.st_ino) == self._parent_identity
                 and stat.S_ISREG(named.st_mode) and named.st_uid == os.getuid()
                 and not named.st_mode & 0o022
                 and (named.st_dev, named.st_ino) == (opened.st_dev, opened.st_ino)
                 == self._file_identity,
                 "Original private configured-local writer lock changed")

    def _check_fresh_heads(self):
        self._require_current(held=True)
        baseline = self.fresh.heads
        for role in INSTALLATION_ROLES:
            target, version, _protocol, _fields = _observe_delta4_role(
                self.transport, role, self.installation_profile)
            _require(version == baseline[role]["version"]
                     and target.uuid == baseline[role]["uuid"],
                     "Native `.4` heads changed since fresh-empty admission")
        self._require_current(held=True)

    def _retain_source_scope(self):
        """Create once or byte-verify the durable pre-effect `.4` source scope."""
        self._require_current(held=True)
        spec = self.source.spec(1, self.context)
        _require(type(spec.request_json) is str
                 and spec.request_json.encode("utf-8") == self.plan.request_json.encode("utf-8")
                 and spec.request() == self.plan.request,
                 "Exact original transaction-1 request bytes required")
        request_raw = spec.request_json.encode("utf-8")
        metadata = self.source.metadata(self.context)
        _require(type(metadata) is dict
                 and metadata.get("configuration_sha256") == self.plan.configuration_sha256
                 and metadata.get("source_sha256") == self.plan.source_sha256
                 and metadata.get("model_sha256") == self.plan.model_sha256,
                 "Current source custody digests must match the frozen `.4` source plan")
        metadata_json = _canonical(metadata)
        source_plan = {
            "stream": self.plan.stream, "batch_id": self.plan.batch_id,
            "publication_id": self.plan.publication_id,
            "source_sha256": self.plan.source_sha256,
            "configuration_sha256": self.plan.configuration_sha256,
            "model_sha256": self.plan.model_sha256,
            "binding_sha256": self.plan.binding_sha256,
            "batch_sha256": self.plan.batch_sha256,
            "materialized_at_us": self.plan.materialized_at_us,
            "source_oracle_sha256": self.plan.source_oracle_sha256,
        }
        profile_raw = _canonical(self.installation_profile).encode("utf-8")
        heads_raw = self.fresh.heads_json.encode("utf-8")
        inventory_raw = self.fresh.inventory_json.encode("utf-8")
        journal_path_object = self.transport.journal_path
        journal_stat = journal_path_object.lstat()
        _require(stat.S_ISREG(journal_stat.st_mode)
                 and journal_stat.st_uid == os.getuid()
                 and not journal_stat.st_mode & 0o022,
                 "Private original local journal file identity required")
        journal_path = str(journal_path_object.resolve())
        scope = {
            "request": {"json": spec.request_json, "sha256": _sha(request_raw),
                        "request_digest": self.plan.request["request_digest"]},
            "source_metadata": {"json": metadata_json,
                                "sha256": _sha(metadata_json.encode("utf-8"))},
            "source_plan": source_plan,
            "effect_plan": {"profile": self.effect_plan.profile,
                            "request_digest": self.effect_plan.request_digest,
                            "installation_profile_sha256": self.effect_plan.installation_profile_sha256,
                            "intent_digest": self.effect_plan.intent_digest,
                            "effect_plan_sha256": self.effect_plan.effect_plan_sha256,
                            "steps_json": self.effect_plan.steps_json},
            "installation_profile_sha256": _sha(profile_raw),
            "fresh_heads_json": self.fresh.heads_json,
            "fresh_inventory_json": self.fresh.inventory_json,
            "fresh_heads_sha256": _sha(heads_raw),
            "fresh_inventory_sha256": _sha(inventory_raw),
            "installation_id": self.transport.installation_id,
            "journal": {"path": journal_path,
                        "registry_sha256": self.transport.registry_sha,
                        "journal_identity_sha256": self.transport.journal_sha,
                        "file_device": journal_stat.st_dev,
                        "file_inode": journal_stat.st_ino},
        }
        scope_raw = _canonical(scope)
        original = _canonical({"profile": SOURCE_SCOPE_PROFILE,
                               "request_digest": self.plan.request["request_digest"],
                               "scope": scope,
                               "scope_sha256": _sha(scope_raw.encode("utf-8"))})
        original_raw = original.encode("utf-8")
        _require(len(original_raw) <= MAX_SOURCE_SCOPE_BYTES,
                 "Durable `.4` source-scope record exceeds its byte bound")
        db = self.transport.db
        definition = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='local_source_plan'").fetchall()
        _require(len(definition) == 1 and type(definition[0][0]) is str
                 and re.sub(r"\s+", "", definition[0][0]).lower()
                 == "createtablelocal_source_plan(request_digesttextprimarykey,originaltextnotnull)",
                 "Original durable local source-plan journal schema required")
        rows = _read_delta4_source_scope_rows(db)
        digest = self.plan.request["request_digest"]
        journal_version = db.execute("PRAGMA user_version").fetchone()[0]
        if journal_version == 0 and not rows:
            # This is the sole first-write boundary. A previously used journal
            # cannot be repurposed as the initial `.4` transaction scope.
            for table in ("local_operation", "local_plan", "local_publication_artifact"):
                _require(db.execute("SELECT count(*) FROM " + table).fetchone() == (0,),
                         "Fresh `.4` journal required to create its first source scope")
            with db:
                db.execute("INSERT INTO local_source_plan(request_digest,original) VALUES (?,?)",
                           (digest, original))
                db.execute("PRAGMA user_version=" + str(SOURCE_SCOPE_JOURNAL_VERSION))
            rows = _read_delta4_source_scope_rows(db)
            journal_version = db.execute("PRAGMA user_version").fetchone()[0]
        _require(journal_version == SOURCE_SCOPE_JOURNAL_VERSION
                 and len(rows) == 1 and rows == [(digest, original)],
                 "Exact unique durable `.4` source-scope replay required")
        self.source_scope_sha256 = _sha(original.encode("utf-8"))

    def _observe_effect_prefix(self):
        """Read-only exact journal/native reconciliation; never grants dispatch."""
        from .delta_custody import (LocalDeltaEffects, original_operation_intent,
                                    sha as custody_sha)
        from .local_receipt_codec import local_receipt_equals
        from ashlar.effect_validation import validate_effect_snapshot
        self._require_current(held=True)
        self._retain_source_scope()
        rows = _read_delta4_source_scope_rows(self.transport.db)
        _require(len(rows) == 1, "Original durable `.4` source scope required")
        envelope = _validate_delta4_source_scope_row(*rows[0])
        scope = envelope["scope"]
        baseline = json.loads(scope["fresh_heads_json"])
        _require(set(baseline) == set(INSTALLATION_ROLES)
                 and scope["fresh_heads_json"] == self.fresh.heads_json
                 and scope["fresh_inventory_json"] == self.fresh.inventory_json
                 and all(type(baseline[role]) is dict
                         and set(baseline[role]) == {"uuid", "version"}
                         and baseline[role]["uuid"] == self.installation_profile["roles"][role]["uuid"]
                         and type(baseline[role]["version"]) is int
                         and 0 <= baseline[role]["version"] < 2**63
                         for role in INSTALLATION_ROLES),
                 "Original source scope must retain the complete six-role fresh baseline")

        operation = "effects:" + self.effect_plan.intent_digest
        steps = self.effect_plan.steps
        original, plan_digest, operation_ids = LocalDeltaEffects(self.transport).original_plan(
            operation, self.effect_plan.intent_digest, steps)
        _require(len(steps) == 3 and len(operation_ids) == 3,
                 "Exact three-step `.4` transaction-one plan required")
        targets = [self.transport._target(step["statement"]) for step in steps]
        step_roles = [target.table.rsplit(".", 1)[-1] for target in targets]
        _require(step_roles == ["object_current", "edge_current", "whole_source_history"],
                 "Exact ordered `.4` transaction-one target roles required")
        plan_count = self.transport.db.execute("SELECT count(*) FROM local_plan").fetchone()[0]
        _require(type(plan_count) is int and plan_count == 1,
                 "One durable original `.4` effect plan required")
        plan_key, plan_bytes, plan_digest_record = self.transport.db.execute(
            "SELECT operation,length(CAST(original AS BLOB)),digest FROM local_plan").fetchone()
        _require(plan_key == operation and plan_digest_record == plan_digest
                 and type(plan_bytes) is int and 0 < plan_bytes <= 4 * 1024 * 1024,
                 "Original `.4` effect plan exceeds its byte bound")
        plan_rows = self.transport.db.execute(
            "SELECT operation,original,digest FROM local_plan").fetchall()
        _require(plan_rows == [(operation, original, plan_digest)],
                 "Exact durable original `.4` effect plan required")

        operation_count = self.transport.db.execute("SELECT count(*) FROM local_operation").fetchone()[0]
        _require(type(operation_count) is int and operation_count <= len(operation_ids),
                 "Bounded exact `.4` operation row inventory required")
        small_rows = self.transport.db.execute(
            "SELECT operation,length(CAST(intent AS BLOB)),length(CAST(receipt AS BLOB)),"
            "intent_sha,before_version,state FROM local_operation ORDER BY operation").fetchall()
        _require(len(small_rows) == operation_count
                 and all(row[0] in operation_ids
                         and type(row[1]) is int and 0 < row[1] <= 4 * 1024 * 1024
                         and (row[2] is None or (type(row[2]) is int
                             and 0 <= row[2] <= 4 * 1024 * 1024))
                         for row in small_rows),
                 "Exact bounded original `.4` operation intent/receipt inventory required")
        small_by_operation = {row[0]: row for row in small_rows}
        operation_rows = []
        for key in operation_ids:
            small = small_by_operation.get(key)
            if small is None:
                operation_rows.append(None)
                continue
            _op, _intent_bytes, _receipt_bytes, _intent_sha, _before, state = small
            _require(state == "committed",
                     "Prepared/submitted `.4` operation state is ambiguous and must recover")
            _require(type(_receipt_bytes) is int and 0 < _receipt_bytes <= 4 * 1024 * 1024,
                     "Exact bounded committed `.4` receipt required")
            operation_rows.append(self.transport.db.execute(
                "SELECT intent,intent_sha,before_version,state,receipt FROM local_operation WHERE operation=?",
                (key,)).fetchone())
        states = ["missing" if row is None else row[3] for row in operation_rows]
        prefix = 0
        while prefix < len(states) and states[prefix] == "committed":
            prefix += 1
        _require(states == ["committed"] * prefix + ["missing"] * (len(states) - prefix),
                 "Only an exact contiguous committed operation prefix is admissible")
        _require(self.transport.db.execute("SELECT count(*) FROM local_operation").fetchone()
                 == (prefix,), "Extra original local operation journal rows refuse reconciliation")

        expected_native_metadata = []
        for index, (key, step, target, row) in enumerate(
                zip(operation_ids, steps, targets, operation_rows)):
            intent = original_operation_intent(
                self.transport.installation_id, key, self.effect_plan.intent_digest,
                step["statement"], step["parameters"], target,
                self.transport.operation_capacity)
            body_sha = custody_sha(intent)
            metadata_text = _canonical({
                "profile": "ashlar-local-delta-commit/0.1",
                "installation_id": self.transport.installation_id,
                "operation": key, "operation_sha256": body_sha,
                "request_digest": self.effect_plan.intent_digest,
            })
            expected_native_metadata.append(metadata_text)
            if index >= prefix:
                continue
            baseline_version = baseline[target.table.rsplit(".", 1)[-1]]["version"]
            _require(type(row[2]) is int and row[2] == baseline_version
                     and row[0] == intent and row[1] == body_sha
                     and row[3] == "committed" and type(row[4]) is str,
                     "Exact committed `.4` operation custody required")

        lookup_hashes = observe_delta4_lookup_hashes(
            self.plan, self.transport, self.installation_profile)
        expected_rows = materialize_delta4_expected_rows(self.plan, lookup_hashes)
        carriers = load_current_managed_carriers(CARRIER_PROFILE)
        all_history = {}
        observed_heads = {}
        for role in INSTALLATION_ROLES:
            target, version, _protocol, _fields = _observe_delta4_role(
                self.transport, role, self.installation_profile)
            expected_version = baseline[role]["version"] + int(
                role in step_roles[:prefix])
            _require(version == expected_version,
                     "Native `.4` role head includes a changed baseline or extra commit")
            history = self.transport.original_history(target)
            all_history[role] = history
            observed_heads[role] = {"uuid": target.uuid, "version": version}
        for index, key in enumerate(operation_ids):
            named = []
            for role, history in all_history.items():
                for record in history:
                    try:
                        metadata = json.loads(record.get("userMetadata") or "{}")
                    except (TypeError, ValueError):
                        metadata = {}
                    if type(metadata) is dict and metadata.get("operation") == key:
                        named.append((role, record, _canonical(metadata)))
            expected_role = step_roles[index]
            if index < prefix:
                _require(len(named) == 1 and named[0][0] == expected_role
                         and named[0][2] == expected_native_metadata[index],
                         "Exact unique original named native commit required")
                target = targets[index]
                record = named[0][1]
                before = baseline[expected_role]["version"]
                version = before + 1
                _require(type(record.get("version")) is int
                         and record["version"] == version
                         and type(record.get("operation")) is str,
                         "Exact committed native version/operation required")
                snapshot = self.transport._snapshot(target, version)
                proof = {
                    "profile": "ashlar-local-delta-receipt/0.1",
                    "operation": key,
                    "operation_sha256": custody_sha(
                        original_operation_intent(self.transport.installation_id, key,
                            self.effect_plan.intent_digest, steps[index]["statement"],
                            steps[index]["parameters"], target,
                            self.transport.operation_capacity)),
                    "request_digest": self.effect_plan.intent_digest,
                    "table": target.table, "uuid": target.uuid,
                    "before_version": before, "version": version,
                    "commit_metadata": expected_native_metadata[index],
                    "native_operation": record["operation"], "snapshot": snapshot,
                }
                _require(local_receipt_equals(operation_rows[index][4], _canonical(proof)),
                         "Exact retained original native receipt required")
            else:
                _require(not named,
                         "Named native commit exists without committed journal custody")

        for role in INSTALLATION_ROLES:
            target = self.transport.targets["local.configured." + role]
            if role in DATA_ROLES:
                step_index = step_roles.index(role) if role in step_roles else len(step_roles)
                rows_for_role = expected_rows[role] if step_index < prefix else []
                columns = tuple((name, kind) for name, kind, _nullable
                                in carriers.ordered_fields(role))
                rows_for_role = [{name: (str(value) if type(value) is int else value)
                                  for name, value in row.items()} for row in rows_for_role]
            else:
                rows_for_role = []
                columns = tuple((name, kind) for name, kind, _nullable
                                in carriers.ordered_fields(role))
            validate_effect_snapshot(self.transport, target.table, target.uuid,
                                     observed_heads[role]["version"], columns,
                                     rows_for_role)
        return {"profile": "ashlar-configured-local-delta4-prefix-observation/0.1",
                "source_scope_sha256": self.source_scope_sha256,
                "effect_plan_sha256": plan_digest,
                "operation_ids": operation_ids, "states": states,
                "committed_prefix": prefix, "heads": observed_heads,
                "lookup_hashes": [{"role": key[0], "source_system": key[1],
                                    "type_id": key[2], "id": key[3], "lookup_hash": value}
                                   for key, value in sorted(lookup_hashes.items())]}

    def reconcile_effect_prefix(self):
        """Hold the original writer lock and read-only verify a committed prefix.

        This is a separate reconciliation scope from ``hold``: it admits no
        ``PrivatePolicy`` intents and never dispatches SQL. It exists for
        exact observation of already retained `.4` effect custody.
        """
        from .lifecycle import finish
        _require(not self.held and self._fd is None and not self._active
                 and self.policy.active is None,
                 "Inactive original `.4` owner required for read-only prefix reconciliation")
        self._require_current()
        parent = self.transport.journal_path.parent.lstat()
        _require(stat.S_ISDIR(parent.st_mode) and parent.st_uid == os.getuid()
                 and not parent.st_mode & 0o022,
                 "Private original local journal directory required")
        path = self.transport.journal_path.parent / ".configured-source-writer.lock"
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        locked = False
        primary = None
        result = None
        self._fd, self._path = fd, path
        self._parent_identity = (parent.st_dev, parent.st_ino)
        try:
            opened = os.fstat(fd)
            _require(stat.S_ISREG(opened.st_mode) and opened.st_uid == os.getuid()
                     and not opened.st_mode & 0o022,
                     "Private original local writer lock required")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
            self._file_identity = (opened.st_dev, opened.st_ino)
            self.held = True
            self._check_writer_lock()
            self.current_admission(self.source, self.plan.request, None, self.context)
            self._retain_source_scope()
            result = self._observe_effect_prefix()
        except BaseException as error:
            primary = error

        def close_lock():
            self._deactivate()
            self.held = False
            if locked:
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            self._fd = None

        closing = []
        if locked and result is not None:
            def repeat_observation():
                current = self._observe_effect_prefix()
                _require(_canonical(current) == _canonical(result),
                         "Closing original `.4` journal/native prefix changed")
            closing = [self._check_writer_lock,
                       lambda: self.current_admission.closing(self.source, None,
                                                              self.context),
                       self._retain_source_scope, repeat_observation]
        finish(primary, [*closing, close_lock])
        return result

    def _deactivate(self):
        self._active = False

    @contextmanager
    def hold(self):
        from .lifecycle import finish
        _require(not self.held and self._fd is None,
                 "Reentrant or concurrent `.4` writer authority refused")
        self._require_current()
        from .configured_local_runtime import ConfiguredLocalAdmission
        from .configured_source_local import ConfiguredLocalSource
        from .delta_custody import LocalDeltaTransport
        _require(type(self.transport) is LocalDeltaTransport
                 and type(self.source) is ConfiguredLocalSource
                 and type(self.current_admission) is ConfiguredLocalAdmission,
                 "Nominal current configured-local owners required")
        parent = self.transport.journal_path.parent.lstat()
        _require(stat.S_ISDIR(parent.st_mode) and parent.st_uid == os.getuid()
                 and not parent.st_mode & 0o022,
                 "Private original local journal directory required")
        path = self.transport.journal_path.parent / ".configured-source-writer.lock"
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        primary = None
        locked = False
        self._fd, self._path = fd, path
        self._parent_identity = (parent.st_dev, parent.st_ino)
        try:
            opened = os.fstat(fd)
            _require(stat.S_ISREG(opened.st_mode) and opened.st_uid == os.getuid()
                     and not opened.st_mode & 0o022,
                     "Private original local writer lock required")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
            self._file_identity = (opened.st_dev, opened.st_ino)
            self.held = True
            self._check_writer_lock()
            request = self.plan.request
            self.current_admission(self.source, request, None, self.context)
            self._check_fresh_heads()
            self._retain_source_scope()
            self._active = True
            yield self
        except BaseException as error:
            primary = error
        def close_lock():
            self._deactivate()
            self.held = False
            if locked:
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            self._fd = None
        closing = []
        if locked and self._active:
            closing = [self._check_writer_lock, self._check_fresh_heads,
                       lambda: self.current_admission.closing(self.source, None,
                                                              self.context),
                       self._retain_source_scope]
        finish(primary, [*closing, close_lock])


def open_configured_local_delta4_authority(plan, effect_plan, transport,
                                            installation_profile, fresh, source,
                                            current_admission, context, policy):
    """Create a source/head scope without admitting native effect intents."""
    from .configured_local_runtime import ConfiguredLocalAdmission
    from .configured_source_local import ConfiguredLocalSource
    from .delta_custody import LocalDeltaTransport
    from .driver import PrivatePolicy
    _require(type(plan) is ConfiguredLocalDelta4BatchPlan
             and type(effect_plan) is ConfiguredLocalDelta4EffectPlan
             and type(transport) is LocalDeltaTransport
             and type(source) is ConfiguredLocalSource
             and type(current_admission) is ConfiguredLocalAdmission
             and type(policy) is PrivatePolicy and policy is transport.policy
             and source.context is context and policy.context is context
             and not policy.initializing,
             "Exact configured-local `.4` runtime owners required")
    profile_sha = _require_delta4_transport(transport, installation_profile)
    plan.require_current(); effect_plan.steps; fresh.require_current()
    _require(effect_plan.request_digest == plan.request["request_digest"]
             and effect_plan.installation_profile_sha256 == profile_sha
             and fresh.installation_profile_sha256 == profile_sha
             and source.spec(1, context).request() == plan.request,
             "Original source request, effect plan and fresh installation must bind")
    return ConfiguredLocalDelta4WriterAuthority(
        plan, effect_plan, transport, installation_profile, fresh, source,
        current_admission, context, policy, _mint=_AUTHORITY_MINT)


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
             and detail.get("format") == "delta"
             and type(detail.get("location")) is str
             and detail["location"].removeprefix("file:").rstrip("/")
             == str(target.path).rstrip("/"),
             "Exact native Delta UUID, format and retained table location required")
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
    for table_name in ("local_operation", "local_plan", "local_publication_artifact"):
        row = transport.db.execute("SELECT count(*) FROM " + table_name).fetchone()
        _require(row == (0,), "Fresh `.4` operation journal must contain no prior publication intent")
    source_rows = _read_delta4_source_scope_rows(transport.db)
    scope_version = transport.db.execute("PRAGMA user_version").fetchone()[0]
    _require(scope_version in (0, SOURCE_SCOPE_JOURNAL_VERSION)
             and (scope_version != 0 or not source_rows)
             and (scope_version != SOURCE_SCOPE_JOURNAL_VERSION or len(source_rows) == 1),
             "Durable `.4` source-scope journal marker and record must agree")
    _require(len(source_rows) <= 1,
             "Fresh `.4` journal may retain only its single original source-scope record")
    if source_rows:
        _validate_delta4_source_scope_row(*source_rows[0])
    return ConfiguredLocalDelta4FreshHeadAdmission(profile_sha, _canonical(heads),
                                                    _canonical(inventory), _mint=_FRESH_HEAD_MINT)


def rehydrate_delta4_fresh_head_admission(plan, effect_plan, transport,
                                           installation_profile, source, context):
    """Rebuild only the pinned pre-effect baseline token from durable custody.

    The function never treats current heads as the baseline. It authenticates
    the retained scope and re-reads every original historical baseline version,
    requiring its complete table snapshot to remain empty.
    """
    from .configured_source_local import ConfiguredLocalSource
    from .delta_custody import LocalDeltaTransport
    from ashlar.effect_validation import validate_effect_snapshot
    _require(type(plan) is ConfiguredLocalDelta4BatchPlan
             and type(effect_plan) is ConfiguredLocalDelta4EffectPlan
             and type(transport) is LocalDeltaTransport
             and type(source) is ConfiguredLocalSource
             and source.context is context and source.graph is True,
             "Exact `.4` recovery source and local journal owners required")
    profile_sha = _require_delta4_transport(transport, installation_profile)
    plan.require_current(); effect_plan.steps
    _require(effect_plan.request_digest == plan.request["request_digest"]
             and effect_plan.installation_profile_sha256 == profile_sha,
             "Frozen `.4` source/effect/profile binding required")
    spec = source.spec(1, context)
    _require(spec.request_json == plan.request_json
             and spec.request() == plan.request,
             "Exact original transaction-1 request bytes required for recovery")
    metadata = source.metadata(context)
    _require(type(metadata) is dict
             and metadata.get("configuration_sha256") == plan.configuration_sha256
             and metadata.get("source_sha256") == plan.source_sha256
             and metadata.get("model_sha256") == plan.model_sha256,
             "Current recovery source custody differs from frozen `.4` plan")

    db = transport.db
    schema = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='local_source_plan'").fetchall()
    _require(len(schema) == 1 and type(schema[0][0]) is str
             and re.sub(r"\s+", "", schema[0][0]).lower()
             == "createtablelocal_source_plan(request_digesttextprimarykey,originaltextnotnull)",
             "Original durable `.4` source-scope journal schema required")
    _require(db.execute("PRAGMA user_version").fetchone()[0] == SOURCE_SCOPE_JOURNAL_VERSION,
             "Original durable `.4` source-scope journal marker required")
    retained = _read_delta4_source_scope_rows(db)
    _require(len(retained) == 1
             and retained[0][0] == plan.request["request_digest"],
             "Unique original transaction-1 source scope required")
    envelope = _validate_delta4_source_scope_row(*retained[0])
    scope = envelope["scope"]
    source_plan = {
        "stream": plan.stream, "batch_id": plan.batch_id,
        "publication_id": plan.publication_id,
        "source_sha256": plan.source_sha256,
        "configuration_sha256": plan.configuration_sha256,
        "model_sha256": plan.model_sha256,
        "binding_sha256": plan.binding_sha256,
        "batch_sha256": plan.batch_sha256,
        "materialized_at_us": plan.materialized_at_us,
        "source_oracle_sha256": plan.source_oracle_sha256,
    }
    metadata_json = _canonical(metadata)
    expected_effect = {"profile": effect_plan.profile,
                       "request_digest": effect_plan.request_digest,
                       "installation_profile_sha256": effect_plan.installation_profile_sha256,
                       "intent_digest": effect_plan.intent_digest,
                       "effect_plan_sha256": effect_plan.effect_plan_sha256,
                       "steps_json": effect_plan.steps_json}
    journal_path_object = transport.journal_path
    journal_stat = journal_path_object.lstat()
    _require(stat.S_ISREG(journal_stat.st_mode) and journal_stat.st_uid == os.getuid()
             and not journal_stat.st_mode & 0o022,
             "Private original local journal file required for recovery")
    _require(scope["request"] == {"json": spec.request_json,
                                  "sha256": _sha(spec.request_json.encode("utf-8")),
                                  "request_digest": plan.request["request_digest"]}
             and scope["source_metadata"] == {
                 "json": metadata_json,
                 "sha256": _sha(metadata_json.encode("utf-8"))}
             and scope["source_plan"] == source_plan
             and scope["effect_plan"] == expected_effect
             and scope["installation_profile_sha256"]
             == _sha(_canonical(installation_profile).encode("utf-8"))
             and scope["installation_id"] == transport.installation_id
             and scope["journal"] == {
                 "path": str(journal_path_object.resolve()),
                 "registry_sha256": transport.registry_sha,
                 "journal_identity_sha256": transport.journal_sha,
                 "file_device": journal_stat.st_dev,
                 "file_inode": journal_stat.st_ino},
             "Retained `.4` source/effect/install/journal binding changed")

    heads_json = scope["fresh_heads_json"]
    inventory_json = scope["fresh_inventory_json"]
    heads = decode_retained_json(heads_json.encode("utf-8"))
    inventory = decode_retained_json(inventory_json.encode("utf-8"))
    _require(type(heads) is dict and set(heads) == set(INSTALLATION_ROLES)
             and _canonical(heads) == heads_json
             and type(inventory) is dict and set(inventory) == set(INSTALLATION_ROLES),
             "Complete retained six-role `.4` baseline required")
    carriers = load_current_managed_carriers(CARRIER_PROFILE)
    observed_inventory = {}
    for role in INSTALLATION_ROLES:
        target, current_version, protocol, fields = _observe_delta4_role(
            transport, role, installation_profile)
        baseline = heads[role]
        expected_fields = tuple((name, kind, nullable) for name, kind, nullable
                                in installation_profile["roles"][role]["fields"])
        _require(type(baseline) is dict and set(baseline) == {"uuid", "version"}
                 and baseline["uuid"] == target.uuid
                 and type(baseline["version"]) is int
                 and baseline["version"] == 1
                 and baseline["version"] <= current_version < 2**63
                 and fields == expected_fields
                 and protocol == installation_profile["roles"][role]["protocol"],
                 "Retained `.4` version-1 baseline identity/schema/protocol differs from its role")
        history = transport.original_history(target)
        history_by_version = {item["version"]: item for item in history}
        registration_metadata = _canonical({
            "profile": "ashlar-local-delta-installation-commit/0.1",
            "installation_id": transport.installation_id,
            "registry_sha256": transport.registry_sha,
            "journal_sha256": transport.journal_sha,
            "uuid": target.uuid,
        })
        _require(set(history_by_version) >= {0, 1}
                 and history_by_version[0].get("operation") == "CREATE TABLE"
                 and history_by_version[0].get("userMetadata") is None
                 and history_by_version[1].get("operation") == "SET TBLPROPERTIES"
                 and history_by_version[1].get("userMetadata") == registration_metadata,
                 "Native version 1 must retain exact CREATE and installation-registration history")
        expected_inventory = {
            "name": target.table, "uuid": target.uuid,
            "version": baseline["version"],
            "schemaSha256": _sha(_canonical(fields).encode("utf-8")),
            "protocolSha256": _sha(_canonical(protocol).encode("utf-8")),
        }
        _require(inventory[role] == expected_inventory,
                 "Retained `.4` baseline inventory differs from its six-role profile")
        columns = tuple((name, kind) for name, kind, _nullable
                        in carriers.ordered_fields(role))
        validate_effect_snapshot(transport, target.table, target.uuid,
                                 baseline["version"], columns, [])
        observed_inventory[role] = expected_inventory
    _require(_canonical(observed_inventory) == inventory_json,
             "Rehydrated `.4` baseline inventory is not canonical")
    return ConfiguredLocalDelta4FreshHeadAdmission(
        profile_sha, heads_json, inventory_json, _mint=_FRESH_HEAD_MINT)


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

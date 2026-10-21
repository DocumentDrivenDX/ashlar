# SPDX-License-Identifier: MIT OR Apache-2.0
"""Affected-key managed current publication candidate (0.1).

This module contains the bounded read and portable reduction boundary for a
single already-admitted Truss page. It intentionally does not qualify a
predecessor or grant writer authority; those remain responsibilities of the
held publication owner.
"""
from dataclasses import dataclass
import json
from ashlar.apply import ApplyState, EntityKey, empty_state, plan_apply
from ashlar.native import quote_table_identifier

PROFILE = "ashlar-managed-current-incremental/0.1"
DATA_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history")
MAX_PAGE_BYTES = 1024 * 1024
MAX_PAGE_EVENTS = 1000


def _require(value, reason="Incremental managed publication refused"):
    if not value:
        raise ValueError(reason)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def request_anchor(request):
    """Compact correspondence anchor; original request bytes stay in attempt custody."""
    keys = ("request_digest", "stream", "batch_id", "predecessor", "source_batch_digest",
            "source_checkpoint_json", "schema_revisions_json")
    _require(type(request) is dict and all(type(request.get(key)) is str for key in keys),
             "Complete original request anchor required")
    return {key: request[key] for key in keys}


def _stable_native(value):
    from datetime import datetime, timezone
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if type(value) is dict:
        return {key: _stable_native(item) for key, item in value.items()}
    if type(value) in (list, tuple):
        return [_stable_native(item) for item in value]
    return value


def affected_keys(changes):
    """Return source-qualified event identities and endpoint object identities."""
    result = set()
    for change in changes:
        result.add(change.state.key)
        if change.state.endpoints:
            result.update(change.state.endpoints)
    return frozenset(result)


@dataclass(frozen=True)
class AffectedRead:
    """Only predecessor rows needed to reduce one admitted page."""
    current: dict
    tombstones: dict
    delivery_markers: dict
    endpoint_objects: dict
    surviving_incident_edge_count: int
    witness_sha256: str


class ManagedCurrentAffectedReader:
    """Issue key-filtered native reads and bounded scalar delete witnesses."""
    def __init__(self, transport, tables):
        _require(type(tables) is dict and set(tables) == set(DATA_ROLES))
        self.transport = transport
        self.tables = dict(tables)

    @staticmethod
    def _in_predicate(alias, fields, keys, prefix, parameters):
        bigint_fields = {"type_id", "rel_type_id", "id", "source_type", "source_id", "target_type", "target_id"}
        terms = []
        for index, key in enumerate(keys):
            atoms = []
            for field, value in zip(fields, key):
                name = f"{prefix}_{index}_{field}"
                parameters[name] = str(value)
                parameter = f"CAST(:{name} AS BIGINT)" if field in bigint_fields else f":{name}"
                atoms.append(f"{alias}.{field}={parameter}")
            terms.append("(" + " AND ".join(atoms) + ")")
        return "(" + " OR ".join(terms) + ")" if terms else "(FALSE)"

    def _rows_for(self, role, identity_column, keys, *, prefix):
        if not keys:
            return ()
        parameters = {}
        predicate = self._in_predicate("t", ("source_system", identity_column, "id"), keys,
                                       prefix, parameters)
        sql = "SELECT t.* FROM " + quote_table_identifier(self.tables[role]) + " t WHERE " + predicate
        return self.transport.query(sql, parameters).rows

    def read(self, changes):
        import hashlib
        _require(type(changes) in (list, tuple) and len(changes) <= MAX_PAGE_EVENTS,
                 "Incremental page exceeds explicit event bound; refusing without truncation")
        touched = affected_keys(changes)
        object_keys = sorted((k.source, k.type_id, k.id) for k in touched if k.kind == "object")
        edge_keys = sorted((k.source, k.type_id, k.id) for k in touched if k.kind == "edge")
        object_rows = self._rows_for("object_current", "type_id", object_keys, prefix="obj")
        edge_rows = self._rows_for("edge_current", "rel_type_id", edge_keys, prefix="edge")
        for role, rows, keys, idfield in (("object_current", object_rows, object_keys, "type_id"),
                                           ("edge_current", edge_rows, edge_keys, "rel_type_id")):
            expected = set(keys)
            actual = {(row["source_system"], int(row[idfield]), int(row["id"])) for row in rows}
            _require(len(actual) == len(rows) and actual <= expected,
                     "Affected current lookup returned duplicate or unrequested identities")
        tomb_keys = [(k.source, k.kind, k.type_id, k.id) for k in sorted(touched)]
        tombstones = ()
        if tomb_keys:
            parameters = {}
            predicate = self._in_predicate("t", ("source_system", "entity_kind", "type_id", "id"),
                                           tomb_keys, "tomb", parameters)
            tombstones = self.transport.query("SELECT t.* FROM " + quote_table_identifier(
                self.tables["tombstone"]) + " t WHERE " + predicate, parameters).rows
            expected = set(tomb_keys)
            actual = {(row["source_system"], row["entity_kind"], int(row["type_id"]), int(row["id"]))
                      for row in tombstones}
            _require(len(actual) == len(tombstones) and actual <= expected,
                     "Affected tombstone lookup returned duplicate or unrequested identities")
        markers = sorted({(c.feed, c.epoch, c.delivery_id) for c in changes}
                         | {(r["source_feed"], r["source_epoch"], r["source_delivery_id"])
                            for r in tombstones})
        marker_rows = ()
        if markers:
            parameters = {}
            predicate = self._in_predicate("h", ("feed", "epoch", "delivery_id"), markers,
                                           "delivery", parameters)
            marker_rows = self.transport.query("SELECT h.feed,h.epoch,h.delivery_id,h.digest,h.change_json,h.raw_base64 FROM "
                + quote_table_identifier(self.tables["whole_source_history"]) + " h WHERE " + predicate,
                parameters).rows
            actual = {(row["feed"], row["epoch"], row["delivery_id"]) for row in marker_rows}
            _require(len(actual) == len(marker_rows) and actual <= set(markers),
                     "Affected delivery lookup returned duplicate or unrequested markers")
        deleted_objects = sorted({(c.state.key.source, c.state.key.type_id, c.state.key.id)
                                  for c in changes if c.operation == "delete" and c.state.key.kind == "object"})
        remaining = 0
        if deleted_objects:
            parameters = {}
            deleted_predicate = self._in_predicate("e", ("source_system", "source_type", "source_id"),
                                                    deleted_objects, "node", parameters)
            touched_edge_predicate = self._in_predicate("e", ("source_system", "rel_type_id", "id"),
                edge_keys, "touched_edge", parameters) if edge_keys else "(FALSE)"
            sql = ("SELECT COUNT(1) AS remaining FROM " + quote_table_identifier(self.tables["edge_current"])
                   + " e WHERE (" + deleted_predicate + " OR "
                   + self._in_predicate("e", ("source_system", "target_type", "target_id"),
                       deleted_objects, "target_node", parameters) + ") AND NOT "
                   + touched_edge_predicate)
            scalar = self.transport.query(sql, parameters).rows
            _require(len(scalar) == 1 and type(scalar[0].get("remaining")) is int,
                     "Native scalar incident-edge witness required")
            remaining = scalar[0]["remaining"]
        value = {"objects": _stable_native(list(object_rows)), "edges": _stable_native(list(edge_rows)),
                 "tombstones": _stable_native(list(tombstones)),
                 "markers": list(marker_rows), "remainingIncidentEdges": remaining}
        digest = hashlib.sha256(_canonical(value).encode()).hexdigest()
        return AffectedRead({"object_current": tuple(object_rows), "edge_current": tuple(edge_rows)},
                            tuple(tombstones), tuple(marker_rows), {}, remaining, digest)


def reduce_affected_page(changes, read: AffectedRead, *, schema_policy,
                         schema_transition_policy=None, incoming_raw=None):
    """Apply the exact portable per-event semantics to bounded predecessor state.

    Existing current/tombstone rows and exact delivery matches seed the reducer.
    Endpoint object afterimages are included by the reader key set. The caller
    must enforce the scalar node-delete witness and qualify that all affected
    keys were returned before persisting intent.
    """
    import base64
    import hashlib
    from ashlar.apply import Change, EntityState
    predecessor = empty_state()
    current = {}; tombstones = {}; deliveries = {}
    for role, kind, idfield in (("object_current", "object", "type_id"),
                                ("edge_current", "edge", "rel_type_id")):
        for raw in read.current[role]:
            row = dict(raw)
            key = EntityKey(row["source_system"], kind, int(row[idfield]), int(row["id"]))
            _require(key not in current, "Duplicate affected current identity")
            endpoints = None
            if kind == "edge":
                endpoints = (EntityKey(key.source, "object", int(row["source_type"]), int(row["source_id"])),
                             EntityKey(key.source, "object", int(row["target_type"]), int(row["target_id"])))
            current[key] = EntityState(key, int(row["entity_version"]), row["schema_revision"],
                row["props_json"], row["retained_json"], endpoints)
    for raw in read.tombstones:
        row = dict(raw)
        key = EntityKey(row["source_system"], row["entity_kind"], int(row["type_id"]), int(row["id"]))
        _require(key not in tombstones, "Duplicate affected tombstone identity")
        # Tombstone fencing needs its exact version and delivery marker. The
        # original event is also present in retained history for that marker.
        marker = (row["source_feed"], row["source_epoch"], row["source_delivery_id"])
        native = next((dict(item) for item in read.delivery_markers
                       if (item["feed"], item["epoch"], item["delivery_id"]) == marker), None)
        _require(native is not None, "Exact predecessor tombstone delivery must be retained")
        original = base64.b64decode(native["raw_base64"], validate=True)
        _require(hashlib.sha256(original).hexdigest() == native["digest"],
                 "Predecessor tombstone raw delivery digest mismatch")
        value = json.loads(native["change_json"])
        state = value["state"]
        ekey = state["key"]
        _require(value["operation"] == "delete" and value["raw_digest"] == native["digest"]
                 and (ekey["source"], ekey["kind"], int(ekey["type_id"]), int(ekey["id"]))
                     == (key.source, key.kind, key.type_id, key.id)
                 and int(state["version"]) == int(row["entity_version"]),
                 "Affected tombstone differs from its exact retained delete event")
        entity = EntityState(key, int(state["version"]), state["schema_revision"], state["props_json"],
            state["retained_json"], None if state.get("endpoints") is None else tuple(
                EntityKey(e["source"], e["kind"], int(e["type_id"]), int(e["id"])) for e in state["endpoints"]))
        tombstones[key] = Change(value["feed"], value["epoch"], value["delivery_id"],
                                 value["raw_digest"], value["operation"], entity)
    _require(not (set(current) & set(tombstones)),
             "Affected predecessor identity cannot be live and tombstoned simultaneously")
    marker_by_key = {(r["feed"], r["epoch"], r["delivery_id"]): r for r in read.delivery_markers}
    _require(len(marker_by_key) == len(read.delivery_markers), "Duplicate affected delivery marker")
    for change in changes:
        marker = (change.feed, change.epoch, change.delivery_id)
        row = marker_by_key.get(marker)
        if row is not None:
            raw = base64.b64decode(row["raw_base64"], validate=True)
            _require(hashlib.sha256(raw).hexdigest() == row["digest"] == change.raw_digest
                     and row["change_json"] == _change_json(change),
                     "Incoming delivery marker conflicts with exact original event")
            if incoming_raw is not None:
                _require(incoming_raw.get(change.delivery_id) == raw,
                         "Incoming delivery marker raw bytes differ from the exact accepted page")
            deliveries[marker] = change
    predecessor = ApplyState(current, {}, tombstones, deliveries)
    after = plan_apply(predecessor, changes, schema_policy=schema_policy,
                       schema_transition_policy=schema_transition_policy)
    _require(not any(c.operation == "delete" and c.state.key.kind == "object" for c in changes)
             or read.surviving_incident_edge_count == 0,
             "Native incident-edge witness found a surviving predecessor edge")
    return predecessor, after


def _change_json(change):
    from dataclasses import asdict
    return json.dumps(asdict(change), separators=(",", ":"))


def prepare_incremental_page(*, admission, reader, tables, request, predecessor_manifest,
                             four_role_heads, six_role_inventory, schema_policy,
                             schema_transition_policy, source_progress, materialized_at):
    """Prepare one canonical page intent from bounded reads and public admission.

    The function reparses through the existing graph SQL planner, which keeps
    the established native Truss event semantics and UMF admission callbacks.
    It never reads or embeds complete role snapshots.
    """
    from .graph_sql import graph_sql_plan
    read = reader.read(admission.changes)
    _require(len(admission.batch.begin) <= MAX_PAGE_BYTES
             and len(admission.changes) <= MAX_PAGE_EVENTS,
             "Incremental source page exceeds explicit byte/event bound; refusing without truncation")
    incoming_raw = {}
    for raw in admission.parsed.event_bytes:
        event = json.loads(raw)
        delivery_id = str(event["xid"]) + ":" + str(event["seq"])
        _require(delivery_id not in incoming_raw, "Duplicate event delivery identity in exact accepted page")
        incoming_raw[delivery_id] = raw
    prior, after = reduce_affected_page(admission.changes, read,
        schema_policy=schema_policy, schema_transition_policy=schema_transition_policy,
        incoming_raw=incoming_raw)
    _require(type(materialized_at) is str and materialized_at, "Exact admitted page materialization time required")
    planned_after, generated = graph_sql_plan(prior, admission.batch, tables,
        materialized_at=materialized_at, schema_policy=schema_policy,
        schema_transition_policy=schema_transition_policy, native_scalar_profile="typed")
    _require(dict(planned_after.current) == dict(after.current)
             and dict(planned_after.tombstones) == dict(after.tombstones),
             "Portable affected reducer differs from original graph planner")
    prior_rows = {"object_current": list(read.current["object_current"]),
                  "edge_current": list(read.current["edge_current"])}
    # Elide only generated deletes whose complete page-key set has no match in
    # the exact affected-key read. Evidence names this bounded set explicitly;
    # it does not claim to hash a complete role snapshot.
    steps, elisions = _affected_effect_plan(generated, prior_rows, tables)
    fresh = [c for c in admission.changes
             if (c.feed, c.epoch, c.delivery_id) not in prior.deliveries]
    afterimages = {role: [] for role in DATA_ROLES}
    for step in generated:
        statement = step["statement"]
        role = next((name for name, table in tables.items()
                     if statement.startswith("INSERT INTO " + quote_table_identifier(table) + " ")), None)
        if role is not None:
            afterimages[role].extend(json.loads(step["parameters"]["rows"]))
    event_keys = {change.state.key for change in admission.changes}
    for role, kind in (("object_current", "object"), ("edge_current", "edge")):
        idfield = "type_id" if kind == "object" else "rel_type_id"
        touched = {(key.source, key.type_id, key.id) for key in event_keys if key.kind == kind}
        prior = [dict(row) for row in read.current[role]
                 if (row["source_system"], int(row[idfield]), int(row["id"])) in touched]
        replaced = set()
        for step in generated:
            if step["statement"].startswith("MERGE INTO " + quote_table_identifier(tables[role]) + " "):
                replaced.update((key["source_system"], int(key["type_id"]), int(key["id"]))
                                for key in json.loads(step["parameters"]["keys"]))
        afterimages[role] = [row for row in prior
            if (row["source_system"], int(row[idfield]), int(row["id"])) not in replaced] + afterimages[role]
    event_identities = {(key.source, key.kind, key.type_id, key.id) for key in event_keys}
    afterimages["tombstone"] = ([dict(row) for row in read.tombstones
        if (row["source_system"], row["entity_kind"], int(row["type_id"]), int(row["id"])) in event_identities]
        + afterimages["tombstone"])
    incoming_markers = {(change.feed, change.epoch, change.delivery_id) for change in admission.changes}
    afterimages["whole_source_history"] = [dict(row) for row in read.delivery_markers
        if (row["feed"], row["epoch"], row["delivery_id"]) in incoming_markers] + afterimages["whole_source_history"]
    afterimages = _stable_native(afterimages)
    touched = sorted((k.source, k.kind, k.type_id, k.id)
                     for k in {c.state.key for c in fresh} | {
                         endpoint for c in fresh if c.state.endpoints for endpoint in c.state.endpoints})
    body, encoded = canonical_intent(request=request_anchor(request), predecessor_manifest=predecessor_manifest,
        four_role_heads=four_role_heads, six_role_inventory=six_role_inventory,
        touched_keys=touched, afterimages={"rows": afterimages,
            "currentDeleteKeys": [json.loads(s["parameters"]["keys"])
                for s in generated if s["statement"].startswith("MERGE INTO ")],
            "nativeWitnessSha256": read.witness_sha256},
        raw_delivery_digests=[c.raw_digest for c in fresh], effects=steps,
        source_progress=source_progress)
    body["zeroMatchElisions"] = elisions
    body["pageBound"] = {"maximumBytes": MAX_PAGE_BYTES, "observedBytes": len(admission.batch.begin),
                         "maximumEvents": MAX_PAGE_EVENTS, "observedEvents": len(admission.changes)}
    encoded = _canonical(body)
    _require(len(encoded.encode("utf-8")) <= 4 * 1024 * 1024,
             "Affected-key intent exceeds explicit durable attempt bound")
    return body, encoded, read, after


def _affected_effect_plan(generated, prior_rows, tables):
    import hashlib
    selected = []; elisions = []
    for ordinal, step in enumerate(generated):
        matched_role = None; identity_field = None
        for role, typed in (("object_current", "type_id"), ("edge_current", "rel_type_id")):
            table = quote_table_identifier(tables[role])
            expected_prefix = "MERGE INTO " + table + " t USING (SELECT k.* FROM (SELECT explode(from_json(:keys,'ARRAY<STRUCT<source_system:STRING,type_id:STRING,id:STRING>>')) k)) s ON t.source_system=s.source_system AND t." + typed + "=cast(s.type_id AS BIGINT) AND t.id=cast(s.id AS BIGINT) WHEN MATCHED THEN DELETE"
            if step.get("statement") == expected_prefix:
                matched_role, identity_field = role, typed
                break
        if matched_role is None:
            selected.append(step)
            continue
        _require(type(step.get("parameters")) is dict and set(step["parameters"]) == {"keys"},
                 "Exact generated affected-key delete required")
        keys = json.loads(step["parameters"]["keys"])
        _require(type(keys) is list and bool(keys) and all(type(key) is dict
            and set(key) == {"source_system", "type_id", "id"}
            and all(type(value) is str for value in key.values()) for key in keys),
            "Exact source-qualified delete key list required")
        match_set = {(row["source_system"], str(row[identity_field]), str(row["id"]))
                     for row in prior_rows[matched_role]}
        matches = [key for key in keys
                   if (key["source_system"], key["type_id"], key["id"]) in match_set]
        if matches:
            selected.append(step)
        else:
            digest = hashlib.sha256(_canonical(sorted(
                (_stable_native(row) for row in prior_rows[matched_role]), key=_canonical)).encode()).hexdigest()
            elisions.append({"ordinal": ordinal, "profile": PROFILE + "/affected-key-zero-match",
                "table": tables[matched_role], "sourceKeys": keys,
                "observedAffectedRowsSha256": digest, "matchedRows": []})
    return selected, elisions


def canonical_intent(*, request, predecessor_manifest, four_role_heads, six_role_inventory,
                     touched_keys, afterimages, raw_delivery_digests, effects, source_progress):
    """Build compact durable intent; complete baseline/graph bags are forbidden."""
    _require(type(request) is dict and type(predecessor_manifest) is dict
             and type(four_role_heads) is dict and len(four_role_heads) == 4
             and type(six_role_inventory) is dict and len(six_role_inventory) == 6)
    _require({name.rsplit(".", 1)[-1] for name in four_role_heads} == set(DATA_ROLES)
             and set(six_role_inventory) == set(DATA_ROLES) | {
                 "publication_manifest", "publication_attempt_phase"}
             and all(type(item) is dict and set(item) == {"name", "uuid", "version", "schema", "protocol"}
                     for item in six_role_inventory.values()),
             "Exact four data heads and six installation identities required")
    body = {"profile": PROFILE, "request": request, "predecessorManifest": predecessor_manifest,
        "fourRoleHeads": four_role_heads, "sixRoleInventory": six_role_inventory,
        "touchedKeys": sorted(touched_keys), "expectedAffectedAfterimages": afterimages,
        "rawDeliveryDigests": raw_delivery_digests, "effects": effects,
        "sourceProgressTransition": source_progress}
    text = _canonical(body)
    _require(len(text.encode("utf-8")) <= 4 * 1024 * 1024,
             "Affected-key intent exceeds explicit durable attempt bound")
    _require("baselineDataRows" not in body and "expectedRows" not in body)
    return body, text

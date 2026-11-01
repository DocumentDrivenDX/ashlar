# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit typed source-genesis progress and create-once managed manifest.

This is an initialization boundary only. Source admission, schedule/model
authority, and the native manifest writer remain caller-owned capabilities.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
import re

from ashlar.manifest import validate_manifest_row
from ashlar.native import quote_table_identifier
from .managed_current_core import CARRIER_PROFILE_XXHASH64, ROLES
from .managed_current_truss_cursor import validate_selected_truss_cursor
from .r04_schedule import R04Schedule

PROGRESS_PROFILE = "ashlar-truss-source-genesis-progress/0.1"
PAGE_ABSENT = {"profile": "ashlar-source-page-absent/0.1"}
MANIFEST_PROFILE = "ashlar-managed-current-genesis-manifest/0.1"
DATA_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history")
_SHA = re.compile(r"[0-9a-f]{64}\Z")


def _require(value, message="Managed current genesis initialization refused"):
    if not value:
        raise ValueError(message)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class TrussSourceGenesisProgress:
    """Typed source position before any page has been accepted."""
    feed: str
    epoch: str
    position: str
    position_sha256: str
    catalog_registration_id: str
    catalog_registration_sha256: str
    consumer_id: str
    consumer_registration_id: str
    installation_id: str
    catalog_registration_bytes: bytes
    cursor_bytes: bytes

    def document(self):
        return {
            "profile": PROGRESS_PROFILE,
            "feed": self.feed,
            "epoch": self.epoch,
            "position": self.position,
            "position_sha256": self.position_sha256,
            "catalog_registration_id": self.catalog_registration_id,
            "catalog_registration_sha256": self.catalog_registration_sha256,
            "consumer_id": self.consumer_id,
            "consumer_registration_id": self.consumer_registration_id,
            "installation_id": self.installation_id,
            "page": dict(PAGE_ABSENT),
        }


def admit_truss_source_genesis_progress(*, feed, epoch, cursor_bytes,
        cursor_sha256, catalog_registration_id, catalog_registration_bytes,
        consumer_id, consumer_registration_id, installation_id):
    """Build the marker from separate selected identities and exact source bytes.

    The selected registration bytes are retained by the caller. This function
    checks their selected digest and cursor correspondence, but does not claim
    to authenticate the external Truss registration.
    """
    strings = (feed, epoch, catalog_registration_id, consumer_id,
               consumer_registration_id, installation_id)
    _require(all(type(value) is str and value and "\0" not in value for value in strings),
             "Separately admitted typed source identities required")
    _require(type(catalog_registration_bytes) is bytes and catalog_registration_bytes,
             "Exact original catalog-registration bytes required")
    registration_sha = hashlib.sha256(catalog_registration_bytes).hexdigest()
    _require(type(cursor_sha256) is str and _SHA.fullmatch(cursor_sha256),
             "Selected genesis cursor SHA-256 required")
    selected = {"installation_id": installation_id, "epoch": epoch,
                "consumer_id": consumer_id,
                "registration_id": consumer_registration_id}
    cursor = validate_selected_truss_cursor(cursor_bytes,
        cursor_sha256=cursor_sha256, selected_identity=selected)
    _require(cursor.next_xid == "0", "Selected Truss cursor must remain at genesis")
    return TrussSourceGenesisProgress(feed, epoch,
        base64.b64encode(cursor.original).decode("ascii"), cursor.sha256,
        catalog_registration_id, registration_sha, consumer_id,
        consumer_registration_id, installation_id, catalog_registration_bytes,
        cursor.original)


class ManagedCurrentGenesisManifestOwner:
    """Create or exactly reopen one empty-target `.4` genesis manifest.

    `manifest_store` is the caller's already admitted immutable manifest port;
    it must provide `executor`, `sql_table`, and `commit(row, context=...)`.
    This class never creates a Spark session, accesses Truss, or ACKs a source.
    """
    def __init__(self, targets, manifest_store):
        self.targets = targets
        self.store = manifest_store

    def initialize(self, *, schedule_bytes, source_progress,
                   selected_model_evidence, schema_revisions, recorded_at,
                   context):
        created_fresh = getattr(self.targets, "created_fresh", False)
        _require(type(created_fresh) is bool
                 and self.targets.carriers.physical_profile == CARRIER_PROFILE_XXHASH64,
                 "Explicitly selected six-role `.4` installation required")
        schedule = R04Schedule(schedule_bytes)
        _require(type(source_progress) is dict and set(source_progress) == {"A", "B"}
                 and all(isinstance(item, TrussSourceGenesisProgress)
                         for item in source_progress.values()),
                 "Two typed, independently admitted A/B genesis progress entries required")
        _require(type(schema_revisions) is dict and bool(schema_revisions)
                 and all(type(key) is str and key and type(value) is str and value
                         for key, value in schema_revisions.items()),
                 "Selected model schema revisions required")
        _require(type(recorded_at) is str and re.fullmatch(r"0|[1-9][0-9]{0,18}", recorded_at)
                 and int(recorded_at) < 2**63,
                 "Canonical recorded UTC microsecond clock required")

        schedule_doc = schedule.document()
        _require(type(selected_model_evidence) is dict
                 and set(selected_model_evidence) == {"A", "B"},
                 "Selected A/B model evidence bound to the frozen schedule required")
        model_evidence_digests = {}
        model_revision_keys = set()
        for source in ("A", "B"):
            evidence = selected_model_evidence[source]
            _require(type(evidence) is dict and set(evidence) == {"pin", "revision_key", "bytes"}
                     and evidence["pin"] == schedule_doc["accepted_model_pins"][source]
                     and type(evidence["pin"]) is dict
                     and type(evidence["pin"].get("documentRevision")) is str
                     and evidence["pin"]["documentRevision"] == evidence["revision_key"]
                     and type(evidence["pin"].get("modelSha256")) is str
                     and _SHA.fullmatch(evidence["pin"]["modelSha256"]) is not None
                     and type(evidence["revision_key"]) is str and evidence["revision_key"]
                     and type(evidence["bytes"]) is bytes and evidence["bytes"],
                     "Exact scheduled model pin, revision and original evidence bytes required")
            digest = hashlib.sha256(evidence["bytes"]).hexdigest()
            _require(evidence["pin"]["modelSha256"] == digest
                     and schema_revisions.get(evidence["revision_key"]) == digest,
                     "Scheduled model pin and schema revision must bind the exact evidence bytes")
            model_revision_keys.add(evidence["revision_key"])
            model_evidence_digests[source] = digest
        _require(model_revision_keys == set(schema_revisions),
                 "Schema revision inventory must exactly cover selected A/B model evidence")
        progress_docs = {}
        for source in ("A", "B"):
            item = source_progress[source]
            revalidated = admit_truss_source_genesis_progress(
                feed=item.feed, epoch=item.epoch, cursor_bytes=item.cursor_bytes,
                cursor_sha256=item.position_sha256,
                catalog_registration_id=item.catalog_registration_id,
                catalog_registration_bytes=item.catalog_registration_bytes,
                consumer_id=item.consumer_id,
                consumer_registration_id=item.consumer_registration_id,
                installation_id=item.installation_id)
            _require(revalidated.document() == item.document(),
                     "Typed genesis marker differs from original registration/cursor bytes")
            binding = schedule_doc["source_bindings"][source]
            _require(binding.get("feed") == item.feed
                     and binding.get("epoch") == item.epoch
                     and binding.get("consumer_id") == item.consumer_id
                     and binding.get("registration_id") == item.consumer_registration_id
                     and binding.get("installation_id") == item.installation_id,
                     "Typed genesis source identity differs from frozen R04 binding")
            progress_docs[item.feed] = item.document()
        _require(len(progress_docs) == 2, "Distinct selected A/B feeds required")

        _require(set(self.targets.opening) == set(ROLES),
                 "Complete six-role target opening inventory required")
        inventory = self.targets.observe()
        _require(set(inventory) == set(ROLES), "Complete six-role native inventory required")
        for role in ROLES:
            before, current = self.targets.opening[role], inventory[role]
            _require((before.name, before.uuid, before.schema, before.protocol)
                     == (current.name, current.uuid, current.schema, current.protocol),
                     "Fresh unchanged six-role native installation required")
            if role != "publication_manifest":
                _require(before.version == 0 and current.version == 0,
                         "All six-role non-manifest tables must remain at version zero")
            elif created_fresh:
                _require(before.version in (0, 1) and current.version == before.version,
                         "Fresh owner may observe only an empty or single-manifest Delta head")
            else:
                _require(before.version == 1 and current.version == 1,
                         "Existing target must have exactly one genesis-manifest commit")
        empty_witnesses = {}
        for role in DATA_ROLES:
            rows = self.targets.transport.query(
                "SELECT * FROM " + quote_table_identifier(self.targets.tables[role]), {}).rows
            _require(not rows, "All four graph-data roles must be empty before genesis")
            empty_witnesses[role] = {"row_count": 0,
                "rows_sha256": hashlib.sha256(b"[]").hexdigest()}

        versions = {self.targets.tables[role]: inventory[role].version for role in DATA_ROLES}
        inventory_doc = {role: {"name": inventory[role].name, "uuid": inventory[role].uuid,
            # The frozen installation inventory is the pre-genesis state. The
            # one immutable manifest commit advances only its own Delta log.
            "version": 0, "schema": list(inventory[role].schema),
            "protocol": inventory[role].protocol} for role in ROLES}
        sources = {source: {
            "feed": source_progress[source].feed,
            "epoch": source_progress[source].epoch,
            "catalog_registration_id": source_progress[source].catalog_registration_id,
            "catalog_registration_sha256": source_progress[source].catalog_registration_sha256,
            "consumer_id": source_progress[source].consumer_id,
            "consumer_registration_id": source_progress[source].consumer_registration_id,
            "installation_id": source_progress[source].installation_id,
            "genesis_cursor_sha256": source_progress[source].position_sha256}
            for source in ("A", "B")}
        report = {"complete": True, "publication_plan_profile": MANIFEST_PROFILE,
            "carrier_profile": CARRIER_PROFILE_XXHASH64,
            "schedule_sha256": hashlib.sha256(schedule_bytes).hexdigest(),
            "sources": sources,
            "accepted_model_pins": schedule_doc["accepted_model_pins"],
            "source_progress_sha256": hashlib.sha256(_canonical(progress_docs).encode()).hexdigest(),
            "model_evidence_sha256": model_evidence_digests,
            "six_role_inventory": inventory_doc,
            "fresh_target_creation": {"profile": "ashlar-managed-current-fresh-target/0.1",
                                      "created_by_owner": True,
                                      "initial_role_heads": {role: 0 for role in ROLES}},
            "empty_data_role_witnesses": empty_witnesses,
            "recorded_at": recorded_at,
            "source_page_accepted": False,
            "source_acknowledged": False}
        identity = {"profile": MANIFEST_PROFILE, "table_versions": versions,
            "source_progress": progress_docs, "schema_revisions": schema_revisions,
            "validation_report": report}
        publication_id = "managed-genesis-" + hashlib.sha256(_canonical(identity).encode()).hexdigest()
        row = {"publication_id": publication_id,
            "profile_version": CARRIER_PROFILE_XXHASH64,
            "table_versions_json": _canonical(versions),
            "source_progress_json": _canonical(progress_docs),
            "schema_revisions_json": _canonical(schema_revisions),
            "validation_report_json": _canonical(report),
            "recorded_at": recorded_at}
        validate_manifest_row(row)

        query = ("SELECT publication_id,profile_version,table_versions_json,source_progress_json,"
                 "schema_revisions_json,validation_report_json,"
                 "cast(unix_micros(recorded_at) AS STRING) AS recorded_at FROM "
                 + self.store.sql_table + " ORDER BY recorded_at DESC LIMIT 2")
        prior = self.store.executor.query(query, {}).rows
        if created_fresh:
            if not prior:
                _require(inventory["publication_manifest"].version == 0,
                         "Manifest Delta head without its genesis row refuses")
                self.store.commit(row, context=context)
            else:
                _require(len(prior) == 1 and dict(prior[0]) == row
                         and inventory["publication_manifest"].version == 1,
                         "Fresh owner can only reopen its exact single genesis manifest")
        else:
            _require(len(prior) == 1 and dict(prior[0]) == row,
                     "Existing target can only reopen its exact genesis manifest")
        actual = self.store.executor.query(
            "SELECT publication_id,profile_version,table_versions_json,source_progress_json,"
            "schema_revisions_json,validation_report_json,"
            "cast(unix_micros(recorded_at) AS STRING) AS recorded_at FROM "
            + self.store.sql_table + " WHERE publication_id=:id", {"id": publication_id}).rows
        _require(len(actual) == 1 and dict(actual[0]) == row,
                 "Exact create-once genesis manifest readback required")
        return row

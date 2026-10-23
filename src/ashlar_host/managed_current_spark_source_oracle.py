# SPDX-License-Identifier: MIT OR Apache-2.0
"""Frozen source-prefix projection for the offline managed-current Spark reader."""
from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re

from ashlar.apply import empty_state, plan_apply
from ashlar.schema import decode_retained_json
from .managed_current_core import (CARRIER_PROFILE_LEGACY, CARRIER_PROFILE_XXHASH64,
                                   ManagedCurrentCoreConfig, restore_managed_current_admission)
from .managed_current_carriers import load_current_managed_carriers
from .managed_current_query import _managed_source_identity
from .truss_note_publication import graph_expected_rows

PROFILE = "ashlar-managed-current-source-prefix/0.1"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_CLOCK = re.compile(r"0|-?[1-9][0-9]{0,18}\Z")
_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history")


def _require(value, message="Frozen managed-current source prefix refused"):
    if not value:
        raise ValueError(message)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _clock_iso(token):
    _require(type(token) is str and _CLOCK.fullmatch(token),
             "Canonical signed64 source materialization clock required")
    micros = int(token)
    _require(-(1 << 63) <= micros < (1 << 63), "Signed64 source clock required")
    try:
        value = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=micros)
    except OverflowError as exc:
        raise ValueError("Source materialization clock outside exact UTC range") from exc
    return value.isoformat(timespec="microseconds")


def _progress_union(progress, checkpoint):
    feed, epoch, previous = checkpoint.get("feed"), checkpoint.get("epoch"), checkpoint.get("previous")
    _require(type(feed) is str and feed and type(epoch) is str and epoch
             and type(previous) is str and previous)
    old = progress.get(feed)
    if old is not None:
        _require(type(old) is dict and old.get("epoch") == epoch
                 and old.get("position") == previous,
                 "Contiguous source epoch and checkpoint required")
    else:
        try:
            raw = base64.b64decode(previous, validate=True)
            cursor = decode_retained_json(raw)
        except (ValueError, TypeError) as exc:
            raise ValueError("Exact original Truss genesis cursor required") from exc
        _require(base64.b64encode(raw).decode("ascii") == previous
                 and type(cursor) is dict
                 and cursor.get("profile") == "truss-caller-asserted-feed/0.1"
                 and cursor.get("sourceEpoch") == epoch and cursor.get("nextXid") == "0",
                 "Original Truss source must begin at its exact genesis cursor")
    result = dict(progress)
    result[feed] = dict(checkpoint)
    return result


def compare_complete_managed_current_bags(expected, actual, *, carrier_profile):
    """Compare bounded, closed four-role bags as multisets, retaining duplicates."""
    _require(type(expected) is dict and set(expected) == set(_ROLES)
             and type(actual) is dict and set(actual) == set(_ROLES)
             and carrier_profile in (CARRIER_PROFILE_LEGACY, CARRIER_PROFILE_XXHASH64),
             "Complete four-role source and native bags required")
    carriers = load_current_managed_carriers(carrier_profile)
    total_rows = total_bytes = 0
    for role in _ROLES:
        _require(type(expected[role]) is list and type(actual[role]) is list
                 and all(type(row) is dict for row in expected[role] + actual[role]),
                 "Complete role row arrays required")
        names = {name for name, _kind, _nullable in carriers.ordered_fields(role)}
        _require(all(set(row) == names for row in expected[role] + actual[role]),
                 "Closed complete role row shape required")
        total_rows += len(expected[role]) + len(actual[role])
        total_bytes += len(_canonical(expected[role]).encode("utf-8"))
        total_bytes += len(_canonical(actual[role]).encode("utf-8"))
        _require(total_rows <= 2048 and total_bytes <= 8 * 1024 * 1024,
                 "Source/native four-role comparison exceeds bounded inputs")
        source = sorted(_canonical(row) for row in expected[role])
        observed = sorted(_canonical(row) for row in actual[role])
        _require(source == observed, "Complete native role bag differs from frozen source prefix")
    return hashlib.sha256(_canonical(actual).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ManagedCurrentSourcePrefixOracle:
    """Immutable projection output, not schedule or publication-chain proof.

    The supplied entries are restored and folded from genesis, but schedule
    assignment, prior-publication identity, complete feed registration and
    initial multi-feed checkpoints remain external unproved gates. For .4,
    supplied hash values are caller correspondence only; no native probe is
    performed here. This value carries no live source, native-bag, or ACK
    authority.
    """
    profile: str
    carrier_profile: str
    prefix_sha256: str
    schedule_sha256: str
    schedule_step: int
    destination_json: str
    progress_json: str
    final_request_json: str
    bundle_identity_json: str
    expected_rows_json: str

    @property
    def progress(self):
        return decode_retained_json(self.progress_json.encode("utf-8"))

    @property
    def final_request(self):
        return decode_retained_json(self.final_request_json.encode("utf-8"))

    @property
    def destination(self):
        return decode_retained_json(self.destination_json.encode("utf-8"))

    @property
    def bundle_identities(self):
        return decode_retained_json(self.bundle_identity_json.encode("utf-8"))

    @property
    def expected_rows(self):
        return decode_retained_json(self.expected_rows_json.encode("utf-8"))


def build_managed_current_prefix_oracle(prefix_bytes, *, prefix_sha256, config,
                                        lookup_hashes=None):
    """Restore and fold supplied prefix bytes without proving schedule custody.

    Per-entry ordinals and predecessor request anchors are checked locally;
    they do not prove the external schedule assignment, prior publication
    chain, or all registered feed checkpoints. The result is an offline
    source projection only. Supplied .4 hash values are not native-verified.
    """
    from ashlar.attempt_store import verify_request_digest
    from .publication_planning import truss_request_for
    from ashlar.source_checkpoint import truss_checkpoint

    _require(type(prefix_bytes) is bytes and 0 < len(prefix_bytes) <= 4 * 1024 * 1024
             and type(prefix_sha256) is str and _SHA.fullmatch(prefix_sha256)
             and hashlib.sha256(prefix_bytes).hexdigest() == prefix_sha256,
             "Separately pinned bounded source-prefix bytes required")
    _require(type(config) is ManagedCurrentCoreConfig)
    prefix = decode_retained_json(prefix_bytes)
    _require(type(prefix) is dict and set(prefix) == {
        "profile", "carrierProfile", "destination", "scheduleSha256", "scheduleStep",
        "entries", "progress"} and prefix.get("profile") == PROFILE,
        "Exact versioned source-prefix carrier required")
    destination = prefix["destination"]
    _require(type(destination) is dict and set(destination) == {"catalog", "schema"}
             and destination == {"catalog": config.catalog, "schema": config.schema}
             and prefix["carrierProfile"] == config.carrier_profile
             and config.carrier_profile in (CARRIER_PROFILE_LEGACY, CARRIER_PROFILE_XXHASH64)
             and type(prefix["scheduleSha256"]) is str and _SHA.fullmatch(prefix["scheduleSha256"])
             and type(prefix["scheduleStep"]) is int
             and 1 <= prefix["scheduleStep"] <= 8
             and type(prefix["entries"]) is list and 1 <= len(prefix["entries"]) <= 8
             and prefix["scheduleStep"] == len(prefix["entries"])
             and type(prefix["progress"]) is dict,
             "Exact destination, schedule, carrier, bounded entries and progress required")
    carriers = load_current_managed_carriers(config.carrier_profile)
    columns = {role: tuple((name, kind) for name, kind, _ in carriers.ordered_fields(role))
               for role in _ROLES}
    expected = {role: [] for role in _ROLES}
    state = empty_state()
    progress = {}
    event_total = 0
    bundle_identities = []
    final_request = None
    lookup_tokens = set()
    page_states = []
    final_request_raw = None
    for ordinal, entry in enumerate(prefix["entries"], start=1):
        _require(type(entry) is dict and set(entry) == {
            "bundleJson", "requestJson", "materializationClockUs"}
            and all(type(entry[key]) is str and entry[key] for key in entry),
            "Exact retained source bundle, request and materialization clock required")
        bundle_bytes = entry["bundleJson"].encode("utf-8")
        request_bytes = entry["requestJson"].encode("utf-8")
        bundle = decode_retained_json(bundle_bytes)
        request = decode_retained_json(request_bytes)
        _require(type(bundle) is dict and type(request) is dict)
        admission = restore_managed_current_admission(bundle, config)
        identity = _managed_source_identity(bundle, schedule_sha256=prefix["scheduleSha256"],
                                            schedule_step=ordinal)
        verify_request_digest(request)
        revisions = {revision: hashlib.sha256(selected["report"]).hexdigest()
                     for revision, selected in admission.revisions.items()}
        _require(request["schema_revisions_json"] == _canonical(revisions),
                 "Request schema revisions must bind every restored admission report")
        rebuilt_request = truss_request_for(request["stream"], admission.batch,
                                            request["predecessor"], revisions)
        _require(rebuilt_request == request
                 and request["source_checkpoint_json"] == truss_checkpoint(admission.batch),
                 "Frozen request differs from its reconstructed original Truss page")
        checkpoint = decode_retained_json(request["source_checkpoint_json"].encode("utf-8"))
        progress = _progress_union(progress, checkpoint)
        next_state = plan_apply(state, admission.changes, schema_policy=admission.admit,
                                schema_transition_policy=admission.admit_transition)
        event_total += len(admission.changes)
        _require(event_total <= 1000, "Source-prefix admitted-event limit exceeded")
        for key in next_state.current:
            if key.kind in ("object", "edge"):
                lookup_tokens.add((key.kind, key.source, key.type_id, key.id))
        for change in admission.changes:
            if change.state.key.kind in ("object", "edge") and change.operation != "delete":
                lookup_tokens.add((change.state.key.kind, change.state.key.source,
                                   change.state.key.type_id, change.state.key.id))
        page_states.append((state, next_state, admission, _clock_iso(entry["materializationClockUs"])))
        bundle_identities.append(identity)
        final_request = request
        final_request_raw = entry["requestJson"]
        state = next_state
    _require(progress == prefix["progress"],
             "Frozen encountered-feed progress differs from source fold")
    if config.carrier_profile == CARRIER_PROFILE_XXHASH64:
        _require(type(lookup_hashes) is dict and set(lookup_hashes) == lookup_tokens
                 and len(lookup_tokens) <= 32,
                 "Exact bounded native typed-key hash mapping required for .4")
        _require(all(type(value) is int and -(1 << 63) <= value < (1 << 63)
                     for value in lookup_hashes.values()),
                 "Native non-Boolean signed64 hash values required")
    else:
        _require(lookup_hashes is None, "Legacy source-prefix profile cannot inherit .4 hashes")
    for prior_state, following_state, admission, clock in page_states:
        expected = graph_expected_rows(expected, following_state, admission.changes,
            admission.batch, columns, clock, admission.parsed.event_bytes,
            admission.parsed.edge_order_keys,
            lookup_hashes=lookup_hashes if config.carrier_profile == CARRIER_PROFILE_XXHASH64 else None)
        total_rows = sum(len(expected[role]) for role in _ROLES)
        _require(total_rows <= 1024, "Source-prefix expected-row limit exceeded")
        _require(len(_canonical(expected).encode("utf-8")) <= 4 * 1024 * 1024,
                 "Source-prefix expected-row byte limit exceeded")
    return ManagedCurrentSourcePrefixOracle(PROFILE, config.carrier_profile, prefix_sha256,
        prefix["scheduleSha256"], prefix["scheduleStep"], _canonical(destination),
        _canonical(progress), final_request_raw, _canonical(bundle_identities),
        _canonical(expected))

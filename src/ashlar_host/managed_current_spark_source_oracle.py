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
from .managed_current_query import managed_source_identity
from .truss_note_publication import graph_expected_rows

PROFILE = "ashlar-managed-current-source-prefix/0.1"
A1_PROFILE = "ashlar-managed-current-a1-source-oracle/0.1"
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


def _source_prefix_lookup_tokens(page_states):
    """Return every typed key needed by any intermediate page projection."""
    tokens = set()
    for _prior, following, admission, _clock in page_states:
        for key in following.current:
            if key.kind in ("object", "edge"):
                tokens.add((key.kind, key.source, key.type_id, key.id))
        for change in admission.changes:
            key = change.state.key
            if key.kind in ("object", "edge") and change.operation != "delete":
                tokens.add((key.kind, key.source, key.type_id, key.id))
    return tokens


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


def _build_managed_current_prefix_oracle(prefix_bytes, *, prefix_sha256, config,
                                         lookup_hashes=None, lookup_keys_only=False,
                                         expected_source_truss_version=None):
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
        if expected_source_truss_version is not None:
            _require(type(expected_source_truss_version) is str
                     and type(bundle.get("source")) is dict
                     and bundle["source"].get("trussVersion") == expected_source_truss_version,
                     "Every frozen prefix page must use the selected Truss source version")
        admission = restore_managed_current_admission(bundle, config)
        identity = managed_source_identity(bundle, schedule_sha256=prefix["scheduleSha256"],
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
        page_states.append((state, next_state, admission, _clock_iso(entry["materializationClockUs"])))
        bundle_identities.append(identity)
        final_request = request
        final_request_raw = entry["requestJson"]
        state = next_state
    _require(progress == prefix["progress"],
             "Frozen encountered-feed progress differs from source fold")
    lookup_tokens = _source_prefix_lookup_tokens(page_states)
    if lookup_keys_only:
        _require(config.carrier_profile == CARRIER_PROFILE_XXHASH64,
                 "Native lookup-key probes are reserved for the .4 carrier")
        _require(len(lookup_tokens) <= 32,
                 "Source-derived native lookup-key probe limit exceeded")
        # Project every page with the widest signed64 placeholder so the
        # bounded native owner can reject row/byte overflow before probing.
        # These placeholders are capacity-only and are never returned as bags.
        lookup_hashes = {key: -(1 << 63) for key in lookup_tokens}
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
    if lookup_keys_only:
        return tuple(sorted(lookup_tokens))
    return ManagedCurrentSourcePrefixOracle(PROFILE, config.carrier_profile, prefix_sha256,
        prefix["scheduleSha256"], prefix["scheduleStep"], _canonical(destination),
        _canonical(progress), final_request_raw, _canonical(bundle_identities),
        _canonical(expected))


def derive_managed_current_prefix_lookup_keys(prefix_bytes, *, prefix_sha256, config,
                                               expected_source_truss_version):
    """Validate/fold the prefix and expose every intermediate-page .4 probe key."""
    return _build_managed_current_prefix_oracle(prefix_bytes,
        prefix_sha256=prefix_sha256, config=config, lookup_keys_only=True,
        expected_source_truss_version=expected_source_truss_version)


def build_managed_current_prefix_oracle(prefix_bytes, *, prefix_sha256, config,
                                        lookup_hashes=None,
                                        expected_source_truss_version=None):
    """Restore and fold supplied prefix bytes without proving schedule custody.

    For .4, ``lookup_hashes`` must come from the injected Spark owner's bounded
    native probes; this pure helper does not perform or authenticate those probes.
    """
    return _build_managed_current_prefix_oracle(prefix_bytes,
        prefix_sha256=prefix_sha256, config=config, lookup_hashes=lookup_hashes,
        expected_source_truss_version=expected_source_truss_version)


def managed_current_a1_lookup_keys(plan, *, config):
    """Derive the complete bounded `.4` key probe set from the selected A1 owner output."""
    from .managed_current_a1_trial_plan import (
        ManagedCurrentA1TrialPlan, ManagedCurrentA1ContinuationPredecessor,
    )
    from .managed_current_genesis import (
        ManagedCurrentGenesisPredecessor, transition_managed_current_first_a_page,
    )
    from .managed_current_truss052_genesis_owner import (
        ManagedCurrentTruss052GenesisSelection,
    )
    from .managed_current_truss053_genesis_owner import (
        ManagedCurrentTruss053GenesisSelection,
    )
    from .truss_graph_admission import TrussGraphAdmission

    _require(type(plan) is ManagedCurrentA1TrialPlan
             and type(plan.selection) in (ManagedCurrentTruss052GenesisSelection,
                                          ManagedCurrentTruss053GenesisSelection)
             and type(plan.predecessor) in (ManagedCurrentGenesisPredecessor,
                                             ManagedCurrentA1ContinuationPredecessor)
             and type(config) is ManagedCurrentCoreConfig
             and config.carrier_profile == CARRIER_PROFILE_XXHASH64,
             "Exact selected A1 plan and `.4` source configuration required")
    selection = plan.selection
    admission = selection.admission
    batch = selection.source_owner_admitted_batch
    inputs = plan.driver_inputs
    _require(type(admission) is TrussGraphAdmission
             and admission.batch is batch
             and inputs.get("admission") is admission
             and ((type(plan.predecessor) is ManagedCurrentGenesisPredecessor
                   and inputs.get("source_owner_admitted_batch") is batch
                   and inputs.get("managed_genesis_predecessor") is plan.predecessor)
                  or (type(plan.predecessor) is ManagedCurrentA1ContinuationPredecessor
                      and inputs.get("source_owner_admitted_batch") is None
                      and inputs.get("managed_genesis_predecessor") is None))
             and type(inputs.get("stream")) is str and inputs["stream"]
             and type(inputs.get("materialized_at")) is str
             and config.catalog and config.schema,
             "A1 plan must retain its exact source-owner batch and verified predecessor")
    if plan.phase == "a0_metadata":
        _require(not admission.changes,
                 "A0 catalog-only publication must have zero graph effects")
        return ()
    from .publication_planning import truss_request_for
    expected_request = truss_request_for(inputs["stream"], batch,
        plan.predecessor.manifest["publication_id"], selection.schema_revisions)
    _require(plan.request == expected_request,
             "A1 request differs from its original source-owner batch and genesis ID")
    from .r04_schedule import R04Schedule
    schedule = R04Schedule(selection.schedule_bytes)
    _require(schedule.sha256 == selection.schedule_sha256
             and schedule.document()["slots"][0]["source"] == "A",
             "A1 plan differs from the original first R04 source slot")
    if type(plan.predecessor) is ManagedCurrentGenesisPredecessor:
        transition_managed_current_first_a_page(
            plan.predecessor, source_owner_admitted_batch=batch)
    else:
        _validate_a1_continuation_progress(plan, batch)

    before = empty_state()
    after = plan_apply(before, admission.changes, schema_policy=admission.admit,
                       schema_transition_policy=admission.admit_transition)
    clock = _clock_iso(inputs["materialized_at"])
    keys = _source_prefix_lookup_tokens(((before, after, admission, clock),))
    _require(len(keys) <= 32, "A1 source-derived `.4` hash probe bound exceeded")
    return tuple(sorted(keys))


def build_managed_current_a1_source_oracle(plan, *, config, prefix_bytes,
        prefix_sha256, selected, manifest, lookup_hashes):
    """Build bounded first-page bags from the selected `.52`, `.53` or `.54` owner result.

    The complete `/0.2` prefix is independently selected and checked for exact
    schedule/page/request/publication correspondence. This helper adds no live
    source, native-publication, retention, or ACK authority.
    """
    from .managed_current_a1_trial_plan import (
        ManagedCurrentA1TrialPlan, ManagedCurrentA1ContinuationPredecessor,
    )
    from .managed_current_genesis import transition_managed_current_first_a_page
    from .managed_current_truss052_genesis_owner import (
        ManagedCurrentTruss052GenesisSelection,
    )
    from .managed_current_truss053_genesis_owner import (
        ManagedCurrentTruss053GenesisSelection,
    )
    from .truss_graph_admission import TrussGraphAdmission

    _require(type(plan) is ManagedCurrentA1TrialPlan
             and type(plan.selection) in (ManagedCurrentTruss052GenesisSelection,
                                          ManagedCurrentTruss053GenesisSelection)
             and type(config) is ManagedCurrentCoreConfig
             and config.carrier_profile == CARRIER_PROFILE_XXHASH64
             and type(manifest) is dict,
             "Exact selected A1 plan, `.4` configuration and manifest required")
    correspondence = plan.verify_delivery(prefix_bytes, prefix_sha256=prefix_sha256,
        selected=selected, schedule_bytes=plan.selection.schedule_bytes,
        pages=(plan.delivery_page,))
    from ashlar_host.managed_current_prefix_delivery import decode_prefix_payload
    package = decode_prefix_payload(prefix_bytes)
    if plan.phase == "a0_metadata":
        selection = plan.selection
        admission = selection.admission
        batch = selection.source_owner_admitted_batch
        _require(not admission.changes and type(admission) is TrussGraphAdmission
                 and admission.batch is batch
                 and config.catalog == selected["destination"]["catalog"]
                 and config.schema == selected["destination"]["schema"]
                 and manifest.get("publication_id") != plan.predecessor.manifest.get("publication_id")
                 and manifest.get("profile_version") == config.carrier_profile,
                 "A0 must publish a distinct metadata-only manifest in the selected destination")
        from .managed_current_genesis import transition_managed_current_first_a_page
        progress = transition_managed_current_first_a_page(
            plan.predecessor, source_owner_admitted_batch=batch)
        _require(manifest.get("source_progress_json") == _canonical(progress)
                 and manifest.get("schema_revisions_json") == plan.request[
                     "schema_revisions_json"],
                 "A0 manifest must retain the exact A0 checkpoint and unchanged B vector")
        _require(package.get("phase") == "a0_metadata" and package.get("scheduleStep") == 0
                 and package.get("entries") == []
                 and package.get("progress") == json.loads(package[
                     "initialProgressJson"]),
                 "A0 verification must bypass R04 semantic slots")
        empty = {role: [] for role in _ROLES}
        return ManagedCurrentSourcePrefixOracle(
            A1_PROFILE, config.carrier_profile, prefix_sha256,
            plan.selection.schedule_sha256, 0,
            _canonical({"catalog": config.catalog, "schema": config.schema}),
            _canonical(progress), _canonical(plan.request),
            _canonical({"source": "A", "phase": "a0_metadata"}),
            _canonical(empty))
    entry = package["entries"][0]
    _require(type(entry) is dict
             and entry.get("requestJson") == _canonical(plan.request)
             and type(entry.get("publicationManifestJson")) is str
             and _decode_retained(entry["publicationManifestJson"]) == manifest
             and entry.get("materializationClockUs") == plan.driver_inputs["materialized_at"],
             "Selected `/0.2` A1 entry must retain the exact task request, clock and manifest")
    selection = plan.selection
    admission = selection.admission
    batch = selection.source_owner_admitted_batch
    _require(type(admission) is TrussGraphAdmission and admission.batch is batch
             and config.catalog == selected["destination"]["catalog"]
             and config.schema == selected["destination"]["schema"],
             "A1 source admission and destination differ from separate `.2` selection")
    from .publication_planning import truss_request_for
    request = truss_request_for(plan.driver_inputs["stream"], batch,
        plan.predecessor.manifest["publication_id"], selection.schema_revisions)
    from ashlar.attempt_store import verify_request_digest
    verify_request_digest(request)
    _require(request == plan.request,
             "Exact A1 request digest and source batch required")
    from ashlar.manifest import validate_manifest_row
    validate_manifest_row(manifest)
    if type(plan.predecessor) is ManagedCurrentA1ContinuationPredecessor:
        progress = _validate_a1_continuation_progress(plan, batch)
    else:
        progress = transition_managed_current_first_a_page(
            plan.predecessor, source_owner_admitted_batch=batch)
    _require(manifest.get("publication_id") != plan.predecessor.manifest.get("publication_id")
             and manifest.get("profile_version") == config.carrier_profile
             and manifest.get("source_progress_json") == _canonical(progress)
             and manifest.get("schema_revisions_json") == request["schema_revisions_json"],
             "Published A1 manifest must retain the exact first-page progress and schema vector")
    keys = managed_current_a1_lookup_keys(plan, config=config)
    _require(type(lookup_hashes) is dict and set(lookup_hashes) == set(keys)
             and all(type(value) is int and -(1 << 63) <= value < (1 << 63)
                     for value in lookup_hashes.values()),
             "Exact native A1 `.4` lookup hash results required")
    before = empty_state()
    after = plan_apply(before, admission.changes, schema_policy=admission.admit,
                       schema_transition_policy=admission.admit_transition)
    carriers = load_current_managed_carriers(config.carrier_profile)
    columns = {role: tuple((name, kind) for name, kind, _ in carriers.ordered_fields(role))
               for role in _ROLES}
    expected = graph_expected_rows({role: [] for role in _ROLES}, after,
        admission.changes, batch, columns, _clock_iso(plan.driver_inputs["materialized_at"]),
        admission.parsed.event_bytes, admission.parsed.edge_order_keys,
        lookup_hashes=lookup_hashes)
    row_count = sum(len(expected[role]) for role in _ROLES)
    byte_count = len(_canonical(expected).encode("utf-8"))
    _require(row_count <= 1024 and byte_count <= 4 * 1024 * 1024,
             "A1 source-derived four-role bags exceed the operational bounds")
    return ManagedCurrentSourcePrefixOracle(
        A1_PROFILE, config.carrier_profile, correspondence.prefix_sha256,
        correspondence.schedule_sha256, 1,
        _canonical({"catalog": config.catalog, "schema": config.schema}),
        _canonical(progress), _canonical(request), _canonical({"source": "A", "ordinal": 1}),
        _canonical(expected))


def _validate_a1_continuation_progress(plan, batch):
    """Advance A0's durable A/B progress by precisely the held A1 page."""
    import base64
    from ashlar.source_checkpoint import truss_checkpoint
    try:
        progress = json.loads(plan.predecessor.manifest["source_progress_json"])
        source_a = plan.selection.source_progress["A"]
        source_b = plan.selection.source_progress["B"]
        old_a = progress[source_a.feed]
        checkpoint = json.loads(truss_checkpoint(batch))
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("Exact A0 source progress and A1 page checkpoint required") from error
    _require(type(progress) is dict and set(progress) == {source_a.feed, source_b.feed}
             and type(old_a) is dict and old_a.get("epoch") == source_a.epoch
             and old_a.get("profile") == "truss-native-feed/0.1"
             and old_a.get("position") == batch.cursor_before
             and old_a.get("previous")
             and progress[source_b.feed] == source_b.document(),
             "A1 prior cursor must equal committed A0 A progress and preserve B genesis")
    progress[source_a.feed] = checkpoint
    return progress


def _decode_retained(text):
    from ashlar.schema import decode_retained_json
    return decode_retained_json(text.encode("utf-8"))

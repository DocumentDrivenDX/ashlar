# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure correspondence checks for the proposed complete source-prefix carrier.

The selected external anchors are inputs, not authority established here. This
module has no Truss, Spark, Delta, publication, source, or ACK ports. Cursor
and page checks establish exact correspondence only; original registration
parsing, acceptance, freshness and ACK authority remain with their owners.
"""
from __future__ import annotations

from dataclasses import dataclass
import base64
import hashlib
import json
import re

from ashlar_host.r04_schedule import R04Schedule, R04CustodyError
from .managed_current_truss_cursor import validate_selected_truss_cursor

PROFILE = "ashlar-managed-current-source-prefix/0.2"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_CLOCK = re.compile(r"0|-?[1-9][0-9]{0,18}\Z")
_MAX_BYTES = 4 * 1024 * 1024


def _require(value, message="Complete source-prefix custody correspondence required"):
    if not value:
        raise ValueError(message)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, "Duplicate source-prefix custody member")
            result[key] = value
        return result
    return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(
                          ValueError("Non-finite source-prefix custody value")))


@dataclass(frozen=True)
class ManagedCurrentPrefixCustodyCorrespondence:
    """Bounded correspondence summary; never source/ACK acceptance authority."""
    prefix_sha256: str
    schedule_sha256: str
    schedule_step: int
    initial_publication_id: str
    publication_ids: tuple[str, ...]
    final_progress_json: str


def validate_selected_managed_current_prefix_v2(prefix_bytes, *, prefix_sha256,
                                                 selected):
    """Check package correspondence against separately selected custody anchors.

    ``selected`` must come from independent schedule, source-registration and
    publication owners. Hash agreement alone cannot authenticate that selection.
    """
    _require(type(prefix_bytes) is bytes and 0 < len(prefix_bytes) <= _MAX_BYTES
             and type(prefix_sha256) is str and _SHA.fullmatch(prefix_sha256)
             and hashlib.sha256(prefix_bytes).hexdigest() == prefix_sha256,
             "Exact bounded versioned prefix bytes and package digest required")
    try:
        package = _decode(prefix_bytes)
    except (UnicodeError, ValueError, RecursionError) as error:
        raise ValueError("Valid duplicate-free source-prefix package JSON required") from error
    base_fields = {"profile", "carrierProfile", "destination", "scheduleJson",
              "initialSources", "initialProgressJson", "initialPublicationJson",
              "scheduleStep", "entries", "progress"}
    a0_metadata = type(package) is dict and package.get("phase") == "a0_metadata"
    fields = base_fields | ({"phase"} if a0_metadata else set())
    _require(type(package) is dict and set(package) == fields
             and package.get("profile") == PROFILE,
             "Closed complete source-prefix /0.2 package required")
    _require(_canonical(package).encode("utf-8") == prefix_bytes,
             "Canonical exact source-prefix package bytes required")
    _require(type(selected) is dict and set(selected) == {
        "scheduleSha256", "sources", "initialProgressSha256",
        "initialPublicationSha256", "initialPublicationId", "carrierProfile",
        "destination"},
        "Separately selected external custody anchors required")
    _require(selected["carrierProfile"] in ("ashlar-delta/0.3", "ashlar-delta/0.4-xxhash64")
             and type(selected["destination"]) is dict
             and set(selected["destination"]) == {"catalog", "schema"}
             and all(type(value) is str and value for value in selected["destination"].values())
             and package["carrierProfile"] == selected["carrierProfile"]
             and package["destination"] == selected["destination"],
             "Carrier profile and destination must match separate external selection")
    schedule_raw = package["scheduleJson"].encode("utf-8") if type(package["scheduleJson"]) is str else b""
    schedule_sha = hashlib.sha256(schedule_raw).hexdigest()
    _require(type(selected["scheduleSha256"]) is str
             and _SHA.fullmatch(selected["scheduleSha256"])
             and schedule_sha == selected["scheduleSha256"],
             "Original schedule differs from separately selected schedule digest")
    try:
        schedule = R04Schedule(schedule_raw)
    except (R04CustodyError, TypeError) as error:
        raise ValueError("Canonical original R04 schedule required") from error
    _require(type(package["destination"]) is dict
             and set(package["destination"]) == {"catalog", "schema"}
             and all(type(value) is str and value for value in package["destination"].values())
             and package["carrierProfile"] in ("ashlar-delta/0.3", "ashlar-delta/0.4-xxhash64")
             and type(package["scheduleStep"]) is int
             and (package["scheduleStep"] == 0 if a0_metadata
                  else 1 <= package["scheduleStep"] <= 8)
             and type(package["entries"]) is list
             and len(package["entries"]) == package["scheduleStep"],
             "Bounded complete global source-prefix dimensions required")

    source_ids = selected["sources"]
    _require(type(source_ids) is dict and set(source_ids) == {"A", "B"}
             and type(package["initialSources"]) is dict
             and set(package["initialSources"]) == {"A", "B"},
             "Both independently selected source registrations required")
    schedule_doc = schedule.document()
    identity_fields = ("feed", "consumer_id", "registration_id", "ack_scope",
                       "ack_registration_id", "installation_id", "epoch")
    selected_identities = {}
    for source in ("A", "B"):
        expected = source_ids[source]
        actual = package["initialSources"][source]
        _require(type(expected) is dict and set(expected) == {
            "registrationSha256", "genesisCursorSha256", "identity"}
            and all(type(expected[key]) is str and _SHA.fullmatch(expected[key])
                    for key in ("registrationSha256", "genesisCursorSha256"))
            and type(actual) is dict and set(actual) == {
                "registrationJson", "genesisCursorBase64"}
            and type(actual["registrationJson"]) is str and actual["registrationJson"]
            and type(actual["genesisCursorBase64"]) is str,
            "Closed original source registration/cursor and external selection required")
        registration_sha = hashlib.sha256(actual["registrationJson"].encode("utf-8")).hexdigest()
        try:
            cursor_raw = base64.b64decode(actual["genesisCursorBase64"], validate=True)
        except (ValueError, TypeError) as error:
            raise ValueError("Original base64 genesis cursor required") from error
        identity = expected.get("identity")
        _require(type(identity) is dict and set(identity).issuperset(identity_fields)
                 and all(type(identity[key]) is str and identity[key] and "\0" not in identity[key]
                         for key in identity_fields)
                 and all(type(key) is str for key in identity)
                 and schedule_doc["source_bindings"][source] == identity,
                 "Selected typed source/consumer/ACK identity must match the frozen schedule")
        _require(bool(cursor_raw)
                 and base64.b64encode(cursor_raw).decode("ascii") == actual["genesisCursorBase64"]
                 and registration_sha == expected["registrationSha256"]
                 and hashlib.sha256(cursor_raw).hexdigest() == expected["genesisCursorSha256"],
                 "Registration, genesis cursor or selected schedule source identity differs")
        try:
            selected_cursor = validate_selected_truss_cursor(
                cursor_raw, cursor_sha256=expected["genesisCursorSha256"],
                selected_identity={
                    "installation_id": identity["installation_id"],
                    "epoch": identity["epoch"],
                    "consumer_id": identity["consumer_id"],
                    "registration_id": identity["registration_id"],
                })
        except ValueError as error:
            raise ValueError("Original genesis cursor differs from selected typed identity") from error
        _require(selected_cursor.next_xid == "0",
                 "Original selected cursor must remain at Truss genesis")
        selected_identities[source] = identity
    for key in identity_fields:
        _require(selected_identities["A"][key] != selected_identities["B"][key],
                 "Selected A/B source, consumer and ACK identities must be distinct")

    initial_progress_raw = package["initialProgressJson"].encode("utf-8") \
        if type(package["initialProgressJson"]) is str else b""
    _require(type(selected["initialProgressSha256"]) is str
             and _SHA.fullmatch(selected["initialProgressSha256"])
             and hashlib.sha256(initial_progress_raw).hexdigest()
                 == selected["initialProgressSha256"],
             "Initial complete two-source progress differs from external selection")
    try:
        progress = _decode(initial_progress_raw)
    except (UnicodeError, ValueError, RecursionError) as error:
        raise ValueError("Original complete initial progress JSON required") from error
    _require(_canonical(progress).encode("utf-8") == initial_progress_raw,
             "Canonical exact complete initial progress JSON required")
    _require(type(progress) is dict and set(progress) == {
        selected_identities[source]["feed"] for source in ("A", "B")},
        "Complete selected A/B initial progress map required")
    for source in ("A", "B"):
        identity = selected_identities[source]
        checkpoint = progress[identity["feed"]]
        original_source = package["initialSources"][source]
        selected_source = source_ids[source]
        cursor_sha = selected_source["genesisCursorSha256"]
        registration_sha = selected_source["registrationSha256"]
        marker_fields = {"profile", "feed", "epoch", "position", "position_sha256",
                         "catalog_registration_id", "catalog_registration_sha256",
                         "consumer_id", "consumer_registration_id", "installation_id", "page"}
        _require(type(checkpoint) is dict and set(checkpoint) == marker_fields
                 and checkpoint.get("profile") == "ashlar-truss-source-genesis-progress/0.1"
                 and checkpoint.get("feed") == identity["feed"]
                 and checkpoint.get("epoch") == identity["epoch"]
                 and checkpoint.get("position") == original_source["genesisCursorBase64"]
                 and checkpoint.get("position_sha256") == cursor_sha
                 and checkpoint.get("catalog_registration_sha256") == registration_sha
                 and type(checkpoint.get("catalog_registration_id")) is str
                 and bool(checkpoint["catalog_registration_id"])
                 and checkpoint.get("consumer_id") == identity["consumer_id"]
                 and checkpoint.get("consumer_registration_id") == identity["registration_id"]
                 and checkpoint.get("installation_id") == identity["installation_id"]
                 and checkpoint.get("page") == {"profile": "ashlar-source-page-absent/0.1"},
                 "Initial progress must retain the selected typed genesis marker")

    initial_manifest_raw = package["initialPublicationJson"].encode("utf-8") \
        if type(package["initialPublicationJson"]) is str else b""
    _require(type(selected["initialPublicationSha256"]) is str
             and _SHA.fullmatch(selected["initialPublicationSha256"])
             and hashlib.sha256(initial_manifest_raw).hexdigest()
                 == selected["initialPublicationSha256"],
             "Initial publication differs from external selection")
    try:
        initial_manifest = _decode(initial_manifest_raw)
    except (UnicodeError, ValueError, RecursionError) as error:
        raise ValueError("Original initial publication manifest JSON required") from error
    initial_id = selected["initialPublicationId"]
    _require(type(initial_id) is str and initial_id
             and type(selected["initialPublicationSha256"]) is str
             and _SHA.fullmatch(selected["initialPublicationSha256"])
             and type(initial_manifest) is dict
             and initial_manifest.get("publication_id") == initial_id,
             "Initial manifest ID differs from selected original publication")
    _require(initial_manifest.get("source_progress_json") == initial_progress_raw.decode("utf-8"),
             "Initial manifest must retain the exact selected complete A/B progress")

    if a0_metadata:
        _require(package.get("phase") == "a0_metadata"
                 and package["entries"] == [] and package["progress"] == progress,
                 "A0 metadata prefix carries only the unchanged empty R04-semantic state")
        return ManagedCurrentPrefixCustodyCorrespondence(
            prefix_sha256, schedule.sha256, 0, initial_id, (), _canonical(progress))

    publication_ids = []
    predecessor = initial_id
    progress = {key: dict(value) for key, value in progress.items()}
    slots = schedule_doc["slots"]
    for ordinal, entry in enumerate(package["entries"]):
        _require(type(entry) is dict and set(entry) == {
            "bundleJson", "requestJson", "materializationClockUs", "publicationManifestJson"}
            and all(type(value) is str and value for value in entry.values()),
            "Closed original page/request/clock/publication entry required")
        _require(_CLOCK.fullmatch(entry["materializationClockUs"]) is not None
                 and -(1 << 63) <= int(entry["materializationClockUs"]) < (1 << 63),
                 "Canonical signed64 materialization clock required")
        slot = slots[ordinal]
        _require(entry["requestJson"], "Original source-bound request required")
        try:
            request = _decode(entry["requestJson"].encode("utf-8"))
            manifest = _decode(entry["publicationManifestJson"].encode("utf-8"))
        except (UnicodeError, ValueError, RecursionError) as error:
            raise ValueError("Original request and publication manifest JSON required") from error
        _require(type(request) is dict and request.get("predecessor") == predecessor
                 and type(manifest) is dict and type(manifest.get("publication_id")) is str
                 and manifest["publication_id"] and manifest["publication_id"] != predecessor
                 and manifest["publication_id"] not in {initial_id, *publication_ids},
                 "Original global publication predecessor chain differs")
        checkpoint_raw = request.get("source_checkpoint_json")
        _require(type(checkpoint_raw) is str and checkpoint_raw,
                 "Original source-qualified request checkpoint required")
        try:
            checkpoint = _decode(checkpoint_raw.encode("utf-8"))
        except (UnicodeError, ValueError, RecursionError) as error:
            raise ValueError("Original request checkpoint JSON required") from error
        source = slot["source"]
        identity = selected_identities[source]
        feed = identity["feed"]
        old = progress.get(feed)
        _require(type(old) is dict and type(checkpoint) is dict
                 and checkpoint.get("feed") == feed
                 and checkpoint.get("epoch") == identity.get("epoch")
                 and type(checkpoint.get("position")) is str and checkpoint["position"]
                 and checkpoint.get("previous") == old.get("position"),
                 "Source checkpoint must continue its separately selected A/B progress")
        from ashlar.attempt_store import verify_request_digest
        from ashlar.source_checkpoint import truss_checkpoint
        from ashlar.staging import batch_from_row
        try:
            verify_request_digest(request)
            batch_artifact = _decode(request["source_batch_json"].encode("utf-8"))
            _require(type(batch_artifact) is dict,
                     "Original Truss batch artifact object required")
            batch = batch_from_row({
                "source_profile": batch_artifact["profile"],
                "feed": batch_artifact["feed"],
                "epoch": batch_artifact["epoch"],
                "batch_id": batch_artifact["batch_id"],
                "cursor_before": batch_artifact["cursor_before"],
                "cursor_after": batch_artifact["cursor_after"],
                "records_digest": batch_artifact["records_sha256"],
                "batch_json": request["source_batch_json"],
                "batch_digest": request["source_batch_digest"],
            })
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("Original Truss page/checkpoint byte correspondence required") from error
        _require(batch.profile == "truss-native-feed/0.1"
                 and batch.feed == feed and batch.epoch == identity["epoch"]
                 and request["batch_id"] == batch.batch_id
                 and batch.cursor_before == checkpoint["previous"]
                 and batch.cursor_after == checkpoint["position"]
                 and request["source_checkpoint_json"] == truss_checkpoint(batch),
                 "Original page cursors must continue the selected source progress")
        cursor_identity = {"installation_id": identity["installation_id"],
                           "epoch": identity["epoch"],
                           "consumer_id": identity["consumer_id"],
                           "registration_id": identity["registration_id"]}
        try:
            prior_cursor = base64.b64decode(batch.cursor_before, validate=True)
            next_cursor = base64.b64decode(batch.cursor_after, validate=True)
            validate_selected_truss_cursor(prior_cursor,
                cursor_sha256=hashlib.sha256(prior_cursor).hexdigest(),
                selected_identity=cursor_identity)
            validate_selected_truss_cursor(next_cursor,
                cursor_sha256=hashlib.sha256(next_cursor).hexdigest(),
                selected_identity=cursor_identity)
        except (TypeError, ValueError) as error:
            raise ValueError("Original Truss page cursor identity differs from selected source") from error
        if old.get("profile") == "ashlar-truss-source-genesis-progress/0.1":
            original_source = package["initialSources"][source]
            _require(batch.cursor_before == original_source["genesisCursorBase64"]
                     and hashlib.sha256(prior_cursor).hexdigest()
                         == old["position_sha256"],
                     "First source page must begin at its exact selected genesis cursor")
        progress[feed] = checkpoint
        try:
            manifest_progress = _decode(manifest["source_progress_json"].encode("utf-8"))
        except (KeyError, AttributeError, UnicodeError, ValueError, RecursionError) as error:
            raise ValueError("Original publication complete progress JSON required") from error
        _require(manifest_progress == progress,
                 "Publication must retain the complete advanced A/B progress map")
        publication_ids.append(manifest["publication_id"])
        predecessor = manifest["publication_id"]
        # Bind the frozen request and retained source page to their global slot
        # assignment without claiming source-mutation semantic admission.
        try:
            bundle = _decode(entry["bundleJson"].encode("utf-8"))
        except (UnicodeError, ValueError, RecursionError) as error:
            raise ValueError("Original source bundle JSON required") from error
        _require(type(bundle) is dict and type(bundle.get("source")) is dict
                 and bundle["source"].get("feed")
                     == selected_identities[slot["source"]]["feed"],
                 "Entry source does not match original global schedule assignment")
    _require(package["progress"] == progress,
             "Package final progress differs from the complete source publication chain")

    return ManagedCurrentPrefixCustodyCorrespondence(
        prefix_sha256, schedule.sha256, package["scheduleStep"], initial_id,
        tuple(publication_ids), _canonical(progress))


def validate_selected_managed_current_prefix_a0_continuation(
        prefix_bytes, *, prefix_sha256, selected):
    """Validate the first semantic R04 page with the exact A0 manifest as base.

    A0 is a separately published metadata-only source transaction and is not
    an R04 semantic slot. This path preserves original A/B genesis anchors,
    verifies the A0 full vector, and admits exactly one A1 page after it.
    """
    from ashlar.attempt_store import verify_request_digest
    from ashlar.source_checkpoint import truss_checkpoint
    from ashlar.staging import batch_from_row
    _require(type(prefix_bytes) is bytes and 0 < len(prefix_bytes) <= _MAX_BYTES
             and type(prefix_sha256) is str and _SHA.fullmatch(prefix_sha256)
             and hashlib.sha256(prefix_bytes).hexdigest() == prefix_sha256,
             "Exact bounded A0-continuation prefix bytes required")
    package = _decode(prefix_bytes)
    required = {"profile", "carrierProfile", "destination", "scheduleJson",
        "initialSources", "initialProgressJson", "initialPublicationJson",
        "scheduleStep", "entries", "progress"}
    _require(type(package) is dict and set(package) == required
             and package.get("profile") == PROFILE
             and _canonical(package).encode("utf-8") == prefix_bytes,
             "Canonical closed `/0.2` A0-continuation package required")
    _require(type(selected) is dict and set(selected) == {
        "scheduleSha256", "sources", "initialProgressSha256",
        "initialPublicationSha256", "initialPublicationId", "carrierProfile",
        "destination"}, "Independent A/B genesis and A0 selection required")
    schedule_raw = package["scheduleJson"].encode("utf-8")
    schedule = R04Schedule(schedule_raw)
    schedule_doc = schedule.document()
    _require(hashlib.sha256(schedule_raw).hexdigest() == selected["scheduleSha256"]
             and package["scheduleStep"] == 1 and len(package["entries"]) == 1
             and schedule_doc["slots"][0]["source"] == "A"
             and package["destination"] == selected["destination"]
             and package["carrierProfile"] == selected["carrierProfile"],
             "A0 continuation must begin at the original first semantic A slot")
    identities = {}
    selected_genesis_progress = {}
    initial_sources = package["initialSources"]
    source_selections = selected["sources"]
    _require(type(initial_sources) is dict and set(initial_sources) == {"A", "B"}
             and type(source_selections) is dict and set(source_selections) == {"A", "B"},
             "Original A/B genesis registrations and cursors required")
    for source in ("A", "B"):
        selected_source = source_selections[source]
        _require(type(selected_source) is dict and set(selected_source) == {
            "registrationSha256", "genesisCursorSha256", "identity"},
            "Independent original source selection required")
        identity = selected_source["identity"]
        binding = schedule_doc["source_bindings"][source]
        identity_fields = {"feed": "feed", "epoch": "epoch",
            "installation_id": "installation_id", "consumer_id": "consumer_id",
            "registration_id": "registration_id"}
        _require(type(identity) is dict and identity == binding
            and all(identity.get(field) == binding.get(key)
            for field, key in identity_fields.items()),
            "Original source identity differs from the frozen schedule")
        actual = initial_sources[source]
        _require(type(actual) is dict and set(actual) == {
            "registrationJson", "genesisCursorBase64"},
            "Exact original registration and genesis cursor bytes required")
        registration = actual["registrationJson"].encode("utf-8")
        cursor = base64.b64decode(actual["genesisCursorBase64"], validate=True)
        _require(hashlib.sha256(registration).hexdigest()
                 == selected_source["registrationSha256"]
                 and hashlib.sha256(cursor).hexdigest()
                 == selected_source["genesisCursorSha256"],
                 "Original genesis registration/cursor digest differs")
        cursor_doc = _decode(cursor)
        _require(cursor_doc.get("nextXid") == "0"
                 and cursor_doc.get("installationId") == identity["installation_id"]
                 and cursor_doc.get("sourceEpoch") == identity["epoch"]
                 and cursor_doc.get("consumerId") == identity["consumer_id"]
                 and cursor_doc.get("registrationId") == identity["registration_id"],
                 "Original genesis cursor identity differs")
        identities[source] = identity
        try:
            registration_doc = _decode(registration)
            registration_id = registration_doc["registrationId"]
        except (ValueError, KeyError, TypeError) as error:
            raise ValueError("Exact original catalog registration identity required") from error
        _require(_canonical(registration_doc).encode("utf-8") == registration,
                 "Canonical exact original catalog registration bytes required")
        selected_genesis_progress[source] = {
            "profile": "ashlar-truss-source-genesis-progress/0.1",
            "feed": identity["feed"], "epoch": identity["epoch"],
            "position": actual["genesisCursorBase64"],
            "position_sha256": selected_source["genesisCursorSha256"],
            "catalog_registration_id": registration_id,
            "catalog_registration_sha256": selected_source["registrationSha256"],
            "consumer_id": identity["consumer_id"],
            "consumer_registration_id": identity["registration_id"],
            "installation_id": identity["installation_id"],
            "page": {"profile": "ashlar-source-page-absent/0.1"}}
    initial_progress_raw = package["initialProgressJson"].encode("utf-8")
    initial_manifest_raw = package["initialPublicationJson"].encode("utf-8")
    _require(hashlib.sha256(initial_progress_raw).hexdigest()
             == selected["initialProgressSha256"]
             and hashlib.sha256(initial_manifest_raw).hexdigest()
             == selected["initialPublicationSha256"],
             "A0 exact progress or manifest digest differs")
    progress = _decode(initial_progress_raw)
    a0 = _decode(initial_manifest_raw)
    _require(type(progress) is dict and set(progress) == {
        identities["A"]["feed"], identities["B"]["feed"]}
        and _canonical(progress).encode() == initial_progress_raw
        and a0.get("publication_id") == selected["initialPublicationId"]
        and a0.get("source_progress_json") == initial_progress_raw.decode("utf-8"),
        "A0 manifest must retain its exact full A/B progress vector")
    a_feed, b_feed = identities["A"]["feed"], identities["B"]["feed"]
    a0_a, a0_b = progress[a_feed], progress[b_feed]
    _require(type(a0_a) is dict and a0_a.get("epoch") == identities["A"]["epoch"]
             and a0_a.get("feed") == a_feed
             and type(a0_a.get("position")) is str
             and a0_a.get("profile") == "truss-native-feed/0.1"
             and type(a0_a.get("previous")) is str
             and a0_a.get("previous") == initial_sources["A"]["genesisCursorBase64"]
             and a0_b == selected_genesis_progress["B"],
             "A0 A checkpoint or untouched B genesis marker differs")
    entry = package["entries"][0]
    _require(type(entry) is dict and set(entry) == {"bundleJson", "requestJson",
        "materializationClockUs", "publicationManifestJson"}
        and _CLOCK.fullmatch(entry["materializationClockUs"]),
        "Exact first semantic page entry required")
    request = _decode(entry["requestJson"].encode("utf-8"))
    manifest = _decode(entry["publicationManifestJson"].encode("utf-8"))
    verify_request_digest(request)
    _require(request.get("predecessor") == a0["publication_id"]
             and request.get("schema_revisions_json") == a0.get("schema_revisions_json")
             and manifest.get("publication_id") != a0["publication_id"],
             "A1 request must descend from the exact A0 publication")
    batch_artifact = _decode(request["source_batch_json"].encode("utf-8"))
    batch = batch_from_row({"source_profile": batch_artifact["profile"],
        "feed": batch_artifact["feed"], "epoch": batch_artifact["epoch"],
        "batch_id": batch_artifact["batch_id"], "cursor_before": batch_artifact["cursor_before"],
        "cursor_after": batch_artifact["cursor_after"], "records_digest": batch_artifact["records_sha256"],
        "batch_json": request["source_batch_json"], "batch_digest": request["source_batch_digest"]})
    _require(batch.feed == a_feed and batch.epoch == identities["A"]["epoch"]
             and batch.cursor_before == a0_a["position"]
             and request["source_checkpoint_json"] == truss_checkpoint(batch),
             "A1 page must continue the exact A0 A checkpoint")
    final_progress = dict(progress)
    final_progress[a_feed] = _decode(request["source_checkpoint_json"].encode("utf-8"))
    _require(_decode(manifest["source_progress_json"].encode("utf-8")) == final_progress
             and package["progress"] == final_progress,
             "A1 manifest must advance A only and preserve the complete A/B vector")
    return ManagedCurrentPrefixCustodyCorrespondence(prefix_sha256, schedule.sha256,
        1, a0["publication_id"], (manifest["publication_id"],), _canonical(final_progress))

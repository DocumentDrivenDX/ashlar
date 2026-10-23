# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure correspondence checks for the proposed complete source-prefix carrier.

The selected external anchors are inputs, not authority established here. This
module has no Truss, Spark, Delta, publication, source, or ACK ports.
"""
from __future__ import annotations

from dataclasses import dataclass
import base64
import hashlib
import json
import re

from ashlar_host.r04_schedule import R04Schedule, R04CustodyError

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
    fields = {"profile", "carrierProfile", "destination", "scheduleJson",
              "initialSources", "initialProgressJson", "initialPublicationJson",
              "scheduleStep", "entries", "progress"}
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
             and 1 <= package["scheduleStep"] <= 8
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
    _require(type(progress) is dict and set(progress) == {
        selected_identities[source]["feed"] for source in ("A", "B")},
        "Complete selected A/B initial progress map required")
    for source in ("A", "B"):
        identity = selected_identities[source]
        checkpoint = progress[identity["feed"]]
        _require(type(checkpoint) is dict and checkpoint.get("feed") == identity["feed"]
                 and checkpoint.get("epoch") == identity["epoch"]
                 and type(checkpoint.get("position")) is str and checkpoint["position"],
                 "Selected initial checkpoint must retain both feed and epoch positions")

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

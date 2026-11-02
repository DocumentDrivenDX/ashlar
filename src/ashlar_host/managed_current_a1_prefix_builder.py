# SPDX-License-Identifier: MIT OR Apache-2.0
"""Build a one-step A1 `/0.2` prefix from detached original owner evidence."""
from __future__ import annotations

import hashlib
import json


class ManagedCurrentA1PrefixBuildError(ValueError):
    """A1 publication bytes do not bind to the detached original A/B inputs."""


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _decode(raw, label):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON member")
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise ManagedCurrentA1PrefixBuildError(f"Exact {label} JSON required") from error
    if type(value) is not dict:
        raise ManagedCurrentA1PrefixBuildError(f"Exact {label} object required")
    return value


def build_managed_current_a1_prefix_evidence(*, genesis_bytes: bytes,
        source_snapshot_bytes: bytes, initial_manifest_bytes: bytes,
        request_bytes: bytes, publication_manifest_bytes: bytes,
        destination: dict, carrier_profile: str, materialization_clock_us: str,
        phase: str = "a1"):
    """Return exact canonical prefix bytes and separately selected anchors.

    The package is created only after publication has returned its exact
    request and manifest. It carries no publication, native-read, or ACK
    authority; callers must still run the independent prefix validator and
    managed receipt reader before any source ACK.
    """
    try:
        from .truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot
        from .truss_detached_source_snapshot_codec import decode_detached_source_snapshot
        from .managed_current_prefix_custody_v2 import (PROFILE,
            validate_selected_managed_current_prefix_v2,
            validate_selected_managed_current_prefix_a0_continuation)
        from .managed_current_prefix_delivery import (
            ManagedCurrentPrefixPage, validate_managed_current_prefix_delivery,
        )
        from .r04_schedule import R04Schedule
        from ashlar.attempt_store import verify_request_digest
        from ashlar.source_checkpoint import truss_checkpoint
    except ImportError as error:
        raise ManagedCurrentA1PrefixBuildError("Pinned Ashlar prefix validators required") from error
    if (type(genesis_bytes) is not bytes or type(source_snapshot_bytes) is not bytes
            or type(initial_manifest_bytes) is not bytes
            or type(request_bytes) is not bytes or type(publication_manifest_bytes) is not bytes
            or type(destination) is not dict or set(destination) != {"catalog", "schema"}
            or any(type(value) is not str or not value for value in destination.values())
            or carrier_profile not in ("ashlar-delta/0.3", "ashlar-delta/0.4-xxhash64")
            or type(materialization_clock_us) is not str
            or phase not in ("a0", "a1")
            or not materialization_clock_us.isascii() or not materialization_clock_us.isdecimal()
            or str(int(materialization_clock_us)) != materialization_clock_us
            or not -(1 << 63) <= int(materialization_clock_us) < (1 << 63)):
        raise ManagedCurrentA1PrefixBuildError("Exact bounded A1 prefix inputs required")
    try:
        genesis = decode_detached_a1_genesis_snapshot(genesis_bytes)
        held = decode_detached_source_snapshot(source_snapshot_bytes)
        schedule = R04Schedule(genesis.schedule_bytes)
        schedule_doc = schedule.document()
        if schedule_doc["slots"][0]["slot"] != "A1":
            raise ValueError("first slot is not A1")
        initial_manifest = _decode(initial_manifest_bytes, "six-role genesis manifest")
        request = _decode(request_bytes, "frozen A1 request")
        publication_manifest = _decode(publication_manifest_bytes, "A1 publication manifest")
        if (_canonical(initial_manifest).encode("utf-8") != initial_manifest_bytes
                or _canonical(request).encode("utf-8") != request_bytes
                or _canonical(publication_manifest).encode("utf-8") != publication_manifest_bytes):
            raise ValueError("noncanonical publication chain bytes")
        verify_request_digest(request)
        identities = {source: schedule_doc["source_bindings"][source]
                      for source in ("A", "B")}
        initial_sources = {}
        selected_sources = {}
        progress = {}
        for source in ("A", "B"):
            evidence = genesis.sources[source]
            identity = evidence["identity"]
            source_progress = evidence["progress"]
            expected_identity = identities[source]
            for field, key in (("feed", "feed"), ("epoch", "epoch"),
                    ("installation_id", "installation_id"),
                    ("consumer_id", "consumer_id"),
                    ("consumer_registration_id", "registration_id")):
                if identity.get(field) != expected_identity.get(key):
                    raise ValueError(f"{source} genesis identity differs from schedule")
            if source == "A" and (
                    held.source_page.source != "A"
                    or held.source_page.identity.get("feed") != expected_identity["feed"]
                    or held.source_page.identity.get("epoch") != expected_identity["epoch"]
                    or held.source_page.identity.get("installation_id") != expected_identity["installation_id"]
                    or held.source_page.identity.get("consumer_id") != expected_identity["consumer_id"]
                    or held.source_page.identity.get("registration_id") != expected_identity["registration_id"]):
                raise ValueError("held A source snapshot differs from frozen schedule")
            registration = source_progress["catalog_registration_bytes"]
            cursor = source_progress["cursor_bytes"]
            if (type(registration) is not bytes or type(cursor) is not bytes
                    or cursor != evidence["page"]["prior_bytes"]
                    or evidence["page"]["outcome"] not in
                       (("transaction",) if source == "A" else ("transaction", "empty_interval"))):
                raise ValueError("original A/B catalog registration or genesis cursor differs")
            cursor_b64 = __import__("base64").b64encode(cursor).decode("ascii")
            registration_text = registration.decode("utf-8")
            registration_obj = _decode(registration, f"{source} catalog registration")
            if registration_obj.get("registrationId") != source_progress["catalog_registration_id"]:
                raise ValueError(f"{source} original catalog registration identity differs")
            initial_sources[source] = {"registrationJson": registration_text,
                                       "genesisCursorBase64": cursor_b64}
            selected_sources[source] = {
                "registrationSha256": hashlib.sha256(registration).hexdigest(),
                "genesisCursorSha256": hashlib.sha256(cursor).hexdigest(),
                "identity": expected_identity,
            }
            progress[expected_identity["feed"]] = {
                "profile": "ashlar-truss-source-genesis-progress/0.1",
                "feed": expected_identity["feed"], "epoch": expected_identity["epoch"],
                "position": cursor_b64,
                "position_sha256": hashlib.sha256(cursor).hexdigest(),
                "catalog_registration_id": source_progress["catalog_registration_id"],
                "catalog_registration_sha256": hashlib.sha256(registration).hexdigest(),
                "consumer_id": expected_identity["consumer_id"],
                "consumer_registration_id": expected_identity["registration_id"],
                "installation_id": expected_identity["installation_id"],
                "page": {"profile": "ashlar-source-page-absent/0.1"},
            }
        continuation = initial_manifest.get("profile_version") == carrier_profile and any(
            type(item) is dict and item.get("page", {}).get("profile") !=
                "ashlar-source-page-absent/0.1"
            for item in json.loads(initial_manifest.get("source_progress_json", "{}")).values())
        if continuation:
            initial_progress = initial_manifest.get("source_progress_json")
            if type(initial_progress) is not str or _canonical(_decode(
                    initial_progress.encode("utf-8"), "A0 full source progress")) != initial_progress:
                raise ValueError("Canonical A0 source progress required for A1 continuation")
            progress = _decode(initial_progress.encode("utf-8"), "A0 full source progress")
        else:
            initial_progress = _canonical(progress)
        initial_publication = initial_manifest_bytes.decode("utf-8")
        initial_id = initial_manifest.get("publication_id")
        if (not type(initial_id) is str or not initial_id
                or (not continuation and initial_manifest.get("source_progress_json") != initial_progress)
                or request.get("predecessor") != initial_id):
            raise ValueError("A1 request does not descend from exact selected predecessor manifest")
        source_a = held.source_page
        if (source_a.request_bytes != request_bytes
                or source_a.publication_id != publication_manifest.get("publication_id")):
            raise ValueError("held A anchor differs from exact request or publication identity")
        batch = __import__("ashlar.truss_native_feed", fromlist=["native_feed_batch"]).native_feed_batch(
            source_a.page_content, sha256=hashlib.sha256(source_a.page_content).hexdigest(),
            prior=source_a.prior_bytes, next=source_a.next_bytes,
            feed=identities["A"]["feed"])
        if (request.get("batch_id") != batch.batch_id
                or request.get("source_checkpoint_json") != truss_checkpoint(batch)):
            raise ValueError("A1 publication request differs from held original Truss page")
        checkpoint = _decode(request["source_checkpoint_json"].encode("utf-8"), "A1 checkpoint")
        progress[identities["A"]["feed"]] = checkpoint
        final_progress = _canonical(progress)
        if publication_manifest.get("source_progress_json") != final_progress:
            raise ValueError("A1 publication manifest does not retain the full A/B progress vector")
        entry = {
            "bundleJson": _canonical({"source": {"feed": identities["A"]["feed"]}}),
            "requestJson": request_bytes.decode("utf-8"),
            "materializationClockUs": materialization_clock_us,
            "publicationManifestJson": publication_manifest_bytes.decode("utf-8"),
        }
        package = {
            "profile": PROFILE, "carrierProfile": carrier_profile,
            "destination": destination,
            "scheduleJson": genesis.schedule_bytes.decode("utf-8"),
            "initialSources": initial_sources,
            "initialProgressJson": initial_progress,
            "initialPublicationJson": initial_publication,
            "scheduleStep": 1, "entries": [entry], "progress": progress,
        }
        if phase == "a0":
            if continuation:
                raise ValueError("A0 metadata phase cannot use an A0 continuation predecessor")
            package["phase"] = "a0_metadata"
            package["scheduleStep"] = 0
            package["entries"] = []
            package["progress"] = _decode(initial_progress.encode("utf-8"),
                                            "initial A/B genesis progress")
        prefix_bytes = _canonical(package).encode("utf-8")
        prefix_sha256 = hashlib.sha256(prefix_bytes).hexdigest()
        selected = {
            "scheduleSha256": genesis.schedule_sha256,
            "sources": selected_sources,
            "initialProgressSha256": hashlib.sha256(initial_progress.encode()).hexdigest(),
            "initialPublicationSha256": hashlib.sha256(initial_manifest_bytes).hexdigest(),
            "initialPublicationId": initial_id,
            "carrierProfile": carrier_profile, "destination": dict(destination),
        }
        pages = (ManagedCurrentPrefixPage(1, "A", source_a.page_content,
                    source_a.prior_bytes, source_a.next_bytes),)
        if continuation:
            validate_selected_managed_current_prefix_a0_continuation(
                prefix_bytes, prefix_sha256=prefix_sha256, selected=selected)
        else:
            validate_selected_managed_current_prefix_v2(
                prefix_bytes, prefix_sha256=prefix_sha256, selected=selected)
            if phase == "a1":
                validate_managed_current_prefix_delivery(
                    prefix_bytes, prefix_sha256=prefix_sha256, selected=selected,
                    schedule_bytes=genesis.schedule_bytes, pages=pages)
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise ManagedCurrentA1PrefixBuildError(
            "Original detached A/B, A page, genesis and publication outputs do not compose") from error
    return prefix_bytes, prefix_sha256, selected

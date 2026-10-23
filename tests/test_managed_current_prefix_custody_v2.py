# SPDX-License-Identifier: MIT OR Apache-2.0
"""Authored structural fixtures; no Truss acceptance or native evidence."""
import base64
import hashlib
import json
import unittest

from ashlar_host.managed_current_prefix_custody_v2 import (
    PROFILE, validate_selected_managed_current_prefix_v2,
)
from test_r04_schedule import schedule


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def make_fixture(step=2):
    selected_schedule = schedule()
    schedule_raw = selected_schedule.raw.decode("utf-8")
    schedule_doc = selected_schedule.document()
    sources = {}
    selected_sources = {}
    progress = {}
    for source in ("A", "B"):
        registration = canonical({"fixtureOnly": source})
        cursor = canonical({"fixtureOnlyGenesis": source}).encode()
        cursor_b64 = base64.b64encode(cursor).decode("ascii")
        identity = schedule_doc["source_bindings"][source]
        sources[source] = {"registrationJson": registration,
                            "genesisCursorBase64": cursor_b64}
        selected_sources[source] = {
            "registrationSha256": hashlib.sha256(registration.encode()).hexdigest(),
            "genesisCursorSha256": hashlib.sha256(cursor).hexdigest(),
            "identity": identity,
        }
        progress[identity["feed"]] = {"feed": identity["feed"],
            "epoch": identity["epoch"], "position": "genesis",
            "fixtureOnlyGenesis": source}
    initial_progress = canonical(progress)
    initial_manifest = canonical({"publication_id": "genesis-fixture",
                                  "source_progress_json": initial_progress})
    entries = []
    predecessor = "genesis-fixture"
    ids = []
    for ordinal, slot in enumerate(schedule_doc["slots"][:step]):
        publication_id = f"publication-fixture-{ordinal}"
        source = slot["source"]
        identity = selected_sources[source]["identity"]
        feed = identity["feed"]
        checkpoint = {"feed": feed, "epoch": identity["epoch"],
                      "previous": progress[feed].get("position", "genesis"),
                      "position": f"cursor-{ordinal}"}
        progress[feed] = checkpoint
        entries.append({
            "bundleJson": canonical({"source": {"feed": selected_sources[source]["identity"]["feed"]}}),
            "requestJson": canonical({"predecessor": predecessor,
                                       "source_checkpoint_json": canonical(checkpoint),
                                       "fixtureSlot": slot["slot"]}),
            "materializationClockUs": str(1792756800000000 + ordinal),
            "publicationManifestJson": canonical({"publication_id": publication_id,
                "source_progress_json": canonical(progress)}),
        })
        predecessor = publication_id
        ids.append(publication_id)
    package = {
        "profile": PROFILE, "carrierProfile": "ashlar-delta/0.4-xxhash64",
        "destination": {"catalog": "client_dev", "schema": "fixture"},
        "scheduleJson": schedule_raw, "initialSources": sources,
        "initialProgressJson": initial_progress,
        "initialPublicationJson": initial_manifest,
        "scheduleStep": step, "entries": entries,
        "progress": progress,
    }
    raw = canonical(package).encode()
    selected = {
        "scheduleSha256": hashlib.sha256(schedule_raw.encode()).hexdigest(),
        "sources": selected_sources,
        "initialProgressSha256": hashlib.sha256(initial_progress.encode()).hexdigest(),
        "initialPublicationSha256": hashlib.sha256(initial_manifest.encode()).hexdigest(),
        "initialPublicationId": "genesis-fixture",
        "carrierProfile": "ashlar-delta/0.4-xxhash64",
        "destination": {"catalog": "client_dev", "schema": "fixture"},
    }
    return package, raw, selected, ids


class ManagedCurrentPrefixCustodyV2Tests(unittest.TestCase):
    def test_authored_package_checks_two_source_anchors_schedule_and_predecessor_chain(self):
        _package, raw, selected, ids = make_fixture()
        result = validate_selected_managed_current_prefix_v2(
            raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)
        self.assertEqual(result.publication_ids, tuple(ids))
        self.assertEqual(result.initial_publication_id, "genesis-fixture")
        self.assertEqual(result.schedule_step, 2)

    def test_package_hash_cannot_replace_separately_selected_schedule_anchor(self):
        package, _raw, selected, _ids = make_fixture()
        package["scheduleJson"] = package["scheduleJson"].replace("A1", "Z1", 1)
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "separately selected schedule digest"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)

    def test_registration_genesis_and_initial_progress_are_separately_selected(self):
        package, _raw, selected, _ids = make_fixture()
        package["initialSources"]["B"]["registrationJson"] += " "
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "Registration, genesis cursor"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)

    def test_global_publication_predecessor_chain_must_be_contiguous(self):
        package, _raw, selected, _ids = make_fixture()
        package["entries"][1]["requestJson"] = canonical({"predecessor": "wrong"})
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "global publication predecessor chain"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)

    def test_publication_must_preserve_the_other_feed_progress_entry(self):
        package, _raw, selected, _ids = make_fixture(step=1)
        manifest = json.loads(package["entries"][0]["publicationManifestJson"])
        manifest["source_progress_json"] = canonical({
            "feed-A": json.loads(manifest["source_progress_json"])["feed-A"]})
        package["entries"][0]["publicationManifestJson"] = canonical(manifest)
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "complete advanced A/B progress"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)

    def test_schedule_assignment_binds_entry_source_but_not_mutation_semantics(self):
        package, _raw, selected, _ids = make_fixture()
        package["entries"][1]["bundleJson"] = canonical({"source": {"feed": "feed-A"}})
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "global schedule assignment"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)

    def test_global_publication_ids_are_unique_and_never_reuse_genesis(self):
        package, _raw, selected, _ids = make_fixture(step=3)
        package["entries"][2]["publicationManifestJson"] = canonical({
            "publication_id": "genesis-fixture",
            "source_progress_json": package["entries"][1]["publicationManifestJson"]
        })
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "global publication predecessor chain"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)

    def test_profile_destination_and_clock_must_match_selected_anchors(self):
        package, _raw, selected, _ids = make_fixture()
        package["carrierProfile"] = "unknown"
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "Carrier profile and destination"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)
        package["carrierProfile"] = selected["carrierProfile"]
        package["entries"][0]["materializationClockUs"] = "not-a-clock"
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "Canonical signed64 materialization clock"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)

    def test_source_identity_requires_typed_consumer_installation_and_ack_fields(self):
        package, _raw, selected, _ids = make_fixture()
        del selected["sources"]["A"]["identity"]["consumer_id"]
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "typed source/consumer/ACK identity"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)

    def test_external_carrier_destination_and_distinct_source_bindings_are_required(self):
        package, _raw, selected, _ids = make_fixture()
        selected["destination"] = {"catalog": "other", "schema": "fixture"}
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "destination must match"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)
        package, _raw, selected, _ids = make_fixture()
        schedule_doc = json.loads(package["scheduleJson"])
        schedule_doc["source_bindings"]["B"]["feed"] = \
            schedule_doc["source_bindings"]["A"]["feed"]
        package["scheduleJson"] = canonical(schedule_doc)
        selected["scheduleSha256"] = hashlib.sha256(
            package["scheduleJson"].encode()).hexdigest()
        selected["sources"]["B"]["identity"]["feed"] = \
            selected["sources"]["A"]["identity"]["feed"]
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "must be distinct"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)


if __name__ == "__main__":
    unittest.main()

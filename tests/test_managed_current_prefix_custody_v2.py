# SPDX-License-Identifier: MIT OR Apache-2.0
"""Authored structural fixtures; no Truss acceptance or native evidence."""
import base64
import hashlib
import json
import unittest

from ashlar_host.managed_current_prefix_custody_v2 import (
    PROFILE, validate_selected_managed_current_prefix_v2,
)
from ashlar_host.managed_current_prefix_delivery import (
    ManagedCurrentPrefixPage, validate_managed_current_prefix_delivery,
)
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.publication_planning import truss_request_for
from ashlar.staging import batch_from_row
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
        identity = schedule_doc["source_bindings"][source]
        cursor = canonical({"profile": "truss-caller-asserted-feed/0.1",
            "nextXid": "0", "installationId": identity["installation_id"],
            "sourceEpoch": identity["epoch"], "consumerId": identity["consumer_id"],
            "registrationId": identity["registration_id"],
            "fixtureExtension": source}).encode()
        cursor_b64 = base64.b64encode(cursor).decode("ascii")
        sources[source] = {"registrationJson": registration,
                            "genesisCursorBase64": cursor_b64}
        selected_sources[source] = {
            "registrationSha256": hashlib.sha256(registration.encode()).hexdigest(),
            "genesisCursorSha256": hashlib.sha256(cursor).hexdigest(),
            "identity": identity,
        }
        progress[identity["feed"]] = {
            "profile": "ashlar-truss-source-genesis-progress/0.1",
            "feed": identity["feed"], "epoch": identity["epoch"],
            "position": cursor_b64,
            "position_sha256": hashlib.sha256(cursor).hexdigest(),
            "catalog_registration_id": f"catalog-registration-{source}",
            "catalog_registration_sha256": hashlib.sha256(registration.encode()).hexdigest(),
            "consumer_id": identity["consumer_id"],
            "consumer_registration_id": identity["registration_id"],
            "installation_id": identity["installation_id"],
            "page": {"profile": "ashlar-source-page-absent/0.1"},
        }
    initial_progress = canonical(progress)
    initial_manifest = canonical({"publication_id": "genesis-fixture",
                                  "source_progress_json": initial_progress})
    entries = []
    predecessor = "genesis-fixture"
    ids = []
    cursors = {source: base64.b64decode(sources[source]["genesisCursorBase64"])
               for source in ("A", "B")}
    for ordinal, slot in enumerate(schedule_doc["slots"][:step]):
        publication_id = f"publication-fixture-{ordinal}"
        source = slot["source"]
        identity = selected_sources[source]["identity"]
        feed = identity["feed"]
        prior = cursors[source]
        next_doc = json.loads(prior)
        next_doc["nextXid"] = str(int(next_doc["nextXid"]) + 1)
        following = canonical(next_doc).encode()
        page = canonical({"interfaceVersion": "truss-native-feed/0.1",
            "prior": json.loads(prior), "next": json.loads(following),
            "catalogPrerequisites": [], "revisions": [], "changes": [],
            "sources": [], "reservations": [], "configuration": {}, "writerXid": None}).encode()
        batch = native_feed_batch(page, sha256=hashlib.sha256(page).hexdigest(),
                                  prior=prior, next=following, feed=feed)
        request = truss_request_for(feed, batch, predecessor, {})
        checkpoint = json.loads(request["source_checkpoint_json"])
        progress[feed] = checkpoint
        cursors[source] = following
        entries.append({
            "bundleJson": canonical({"source": {"feed": selected_sources[source]["identity"]["feed"]}}),
            "requestJson": canonical(request),
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

    def _pages(self, package):
        schedule_doc = json.loads(package["scheduleJson"])
        pages = []
        for ordinal, (slot, entry) in enumerate(
                zip(schedule_doc["slots"], package["entries"]), start=1):
            request = json.loads(entry["requestJson"])
            artifact = json.loads(request["source_batch_json"])
            batch = batch_from_row({
                "source_profile": artifact["profile"],
                "feed": artifact["feed"],
                "epoch": artifact["epoch"],
                "batch_id": artifact["batch_id"],
                "cursor_before": artifact["cursor_before"],
                "cursor_after": artifact["cursor_after"],
                "records_digest": artifact["records_sha256"],
                "batch_json": request["source_batch_json"],
                "batch_digest": request["source_batch_digest"],
            })
            pages.append(ManagedCurrentPrefixPage(
                ordinal, slot["source"], batch.begin,
                base64.b64decode(batch.cursor_before),
                base64.b64decode(batch.cursor_after)))
        return tuple(pages)

    def test_delivery_binds_exact_ordered_pages_to_retained_requests(self):
        package, raw, selected, ids = make_fixture()
        delivery = validate_managed_current_prefix_delivery(
            raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected,
            schedule_bytes=package["scheduleJson"].encode(), pages=self._pages(package))
        self.assertEqual(delivery.correspondence.publication_ids, tuple(ids))
        self.assertEqual(len(delivery.page_sha256), 2)

    def test_delivery_refuses_page_bytes_that_differ_from_retained_request(self):
        package, raw, selected, _ids = make_fixture(step=1)
        pages = list(self._pages(package))
        page = json.loads(pages[0].page_bytes)
        page["configuration"] = {"changed": True}
        pages[0] = ManagedCurrentPrefixPage(
            pages[0].ordinal, pages[0].source, canonical(page).encode(),
            pages[0].prior_cursor_bytes, pages[0].next_cursor_bytes)
        with self.assertRaisesRegex(ValueError, "page/cursor bytes differ"):
            validate_managed_current_prefix_delivery(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected,
                schedule_bytes=package["scheduleJson"].encode(), pages=tuple(pages))

    def test_delivery_refuses_page_order_that_differs_from_frozen_schedule(self):
        package, raw, selected, _ids = make_fixture()
        pages = list(self._pages(package))
        pages[0] = ManagedCurrentPrefixPage(
            1, "B", pages[0].page_bytes, pages[0].prior_cursor_bytes,
            pages[0].next_cursor_bytes)
        with self.assertRaisesRegex(ValueError, "global schedule slot"):
            validate_managed_current_prefix_delivery(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected,
                schedule_bytes=package["scheduleJson"].encode(), pages=tuple(pages))

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

    def test_cursor_bytes_must_match_selected_typed_identity(self):
        package, _raw, selected, _ids = make_fixture(step=1)
        source = package["initialSources"]["A"]
        cursor = json.loads(base64.b64decode(source["genesisCursorBase64"]))
        cursor["installationId"] = "unselected-installation"
        changed = canonical(cursor).encode()
        source["genesisCursorBase64"] = base64.b64encode(changed).decode("ascii")
        selected["sources"]["A"]["genesisCursorSha256"] = hashlib.sha256(changed).hexdigest()
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "selected typed identity"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)

    def test_initial_progress_must_be_the_closed_genesis_marker(self):
        package, _raw, selected, _ids = make_fixture(step=1)
        initial_progress = json.loads(package["initialProgressJson"])
        marker = initial_progress["feed-A"]
        marker["previous"] = "fabricated-checkpoint"
        package["initialProgressJson"] = canonical(initial_progress)
        initial_manifest = json.loads(package["initialPublicationJson"])
        initial_manifest["source_progress_json"] = package["initialProgressJson"]
        package["initialPublicationJson"] = canonical(initial_manifest)
        selected["initialProgressSha256"] = hashlib.sha256(
            package["initialProgressJson"].encode()).hexdigest()
        selected["initialPublicationSha256"] = hashlib.sha256(
            package["initialPublicationJson"].encode()).hexdigest()
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "typed genesis marker"):
            validate_selected_managed_current_prefix_v2(
                raw, prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=selected)

    def test_request_checkpoint_must_match_retained_native_page_bytes(self):
        package, _raw, selected, _ids = make_fixture(step=1)
        entry = package["entries"][0]
        request = json.loads(entry["requestJson"])
        checkpoint = json.loads(request["source_checkpoint_json"])
        checkpoint["position"] = "Zm9v"
        request["source_checkpoint_json"] = canonical(checkpoint)
        body = {key: value for key, value in request.items() if key != "request_digest"}
        request["request_digest"] = hashlib.sha256(canonical(body).encode()).hexdigest()
        entry["requestJson"] = canonical(request)
        raw = canonical(package).encode()
        with self.assertRaisesRegex(ValueError, "page/checkpoint byte correspondence"):
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

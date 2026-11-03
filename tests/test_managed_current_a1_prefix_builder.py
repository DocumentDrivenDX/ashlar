# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure byte-composition checks; fixtures confer no source authority."""
import base64
import hashlib
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.managed_current_a1_prefix_builder import (
    build_managed_current_a1_prefix_evidence,
)
from ashlar_host.publication_planning import truss_request_for
from test_r04_schedule import schedule


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


class TestManagedCurrentA1PrefixBuilder(unittest.TestCase):
    def test_a1_continuation_prefix_uses_a0_manifest_without_consuming_r04_slot(self):
        from ashlar_host.managed_current_prefix_custody_v2 import (
            validate_selected_managed_current_prefix_a0_continuation,
        )
        from ashlar.source_checkpoint import truss_checkpoint
        selected = schedule()
        schedule_doc = selected.document()
        def cursor(source, xid):
            identity = schedule_doc["source_bindings"][source]
            return canonical({"profile": "truss-caller-asserted-feed/0.1",
                "nextXid": str(xid), "installationId": identity["installation_id"],
                "sourceEpoch": identity["epoch"], "consumerId": identity["consumer_id"],
                "registrationId": identity["registration_id"]}).encode()
        batch_values = []
        for before, after, xid in ((0, 2, 1), (2, 3, 2)):
            prior, following = cursor("A", before), cursor("A", after)
            page = canonical({"interfaceVersion": "truss-native-feed/0.1",
                "prior": json.loads(prior), "next": json.loads(following),
                "writerXid": str(xid), "catalogPrerequisites": [], "revisions": [],
                "changes": [], "sources": [], "reservations": [], "configuration": {}}).encode()
            batch_values.append(native_feed_batch(page, sha256=hashlib.sha256(page).hexdigest(),
                prior=prior, next=following, feed="feed-A"))
        a0_batch, a1_batch = batch_values
        b_cursor = cursor("B", 0)
        initial_sources = {}
        selected_sources = {}
        for source in ("A", "B"):
            identity = schedule_doc["source_bindings"][source]
            registration = canonical({"registrationId": f"catalog-{source}", "revision": "1"})
            genesis_cursor = cursor(source, 0)
            initial_sources[source] = {"registrationJson": registration,
                "genesisCursorBase64": base64.b64encode(genesis_cursor).decode()}
            selected_sources[source] = {"registrationSha256": hashlib.sha256(
                registration.encode()).hexdigest(), "genesisCursorSha256":
                hashlib.sha256(genesis_cursor).hexdigest(), "identity": identity}
        b_progress = {"profile": "ashlar-truss-source-genesis-progress/0.1",
            "feed": "feed-B", "epoch": schedule_doc["source_bindings"]["B"]["epoch"],
            "position": base64.b64encode(b_cursor).decode(),
            "position_sha256": hashlib.sha256(b_cursor).hexdigest(),
            "catalog_registration_id": "catalog-B",
            "catalog_registration_sha256": selected_sources["B"]["registrationSha256"],
            "consumer_id": schedule_doc["source_bindings"]["B"]["consumer_id"],
            "consumer_registration_id": schedule_doc["source_bindings"]["B"]["registration_id"],
            "installation_id": schedule_doc["source_bindings"]["B"]["installation_id"],
            "page": {"profile": "ashlar-source-page-absent/0.1"}}
        a0_progress = {"feed-A": json.loads(truss_checkpoint(a0_batch)), "feed-B": b_progress}
        a0_progress_raw = canonical(a0_progress)
        a0_manifest = {"publication_id": "a0-publication",
            "source_progress_json": a0_progress_raw,
            "schema_revisions_json": "{}"}
        request = truss_request_for("feed-A", a1_batch, "a0-publication", {})
        final_progress = dict(a0_progress)
        final_progress["feed-A"] = json.loads(truss_checkpoint(a1_batch))
        manifest = {"publication_id": "a1-publication",
            "source_progress_json": canonical(final_progress)}
        package = {"profile": "ashlar-managed-current-source-prefix/0.2",
            "carrierProfile": "ashlar-delta/0.4-xxhash64",
            "destination": {"catalog": "client_dev", "schema": "trial"},
            "scheduleJson": selected.raw.decode(), "initialSources": initial_sources,
            "initialProgressJson": a0_progress_raw,
            "initialPublicationJson": canonical(a0_manifest), "scheduleStep": 1,
            "entries": [{"bundleJson": canonical({"source": {"feed": "feed-A"}}),
                "requestJson": canonical(request), "materializationClockUs": "1",
                "publicationManifestJson": canonical(manifest)}],
            "progress": final_progress}
        raw = canonical(package).encode()
        anchors = {"scheduleSha256": selected.sha256, "sources": selected_sources,
            "initialProgressSha256": hashlib.sha256(a0_progress_raw.encode()).hexdigest(),
            "initialPublicationSha256": hashlib.sha256(
                canonical(a0_manifest).encode()).hexdigest(),
            "initialPublicationId": "a0-publication",
            "carrierProfile": "ashlar-delta/0.4-xxhash64",
            "destination": {"catalog": "client_dev", "schema": "trial"}}
        proof = validate_selected_managed_current_prefix_a0_continuation(raw,
            prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=anchors)
        self.assertEqual(proof.initial_publication_id, "a0-publication")
        self.assertEqual(proof.schedule_step, 1)
        from ashlar_host.managed_current_a1_trial_plan import ManagedCurrentA1TrialPlan
        from ashlar_host.managed_current_prefix_delivery import ManagedCurrentPrefixPage
        from ashlar_host.managed_current_genesis import admit_truss_source_genesis_progress
        progress_a = admit_truss_source_genesis_progress(feed="feed-A", epoch=
            schedule_doc["source_bindings"]["A"]["epoch"], cursor_bytes=cursor("A", 2),
            cursor_sha256=hashlib.sha256(cursor("A", 2)).hexdigest(),
            catalog_registration_id="catalog-A",
            catalog_registration_bytes=canonical({"registrationId": "catalog-A"}).encode(),
            consumer_id=schedule_doc["source_bindings"]["A"]["consumer_id"],
            consumer_registration_id=schedule_doc["source_bindings"]["A"]["registration_id"],
            installation_id=schedule_doc["source_bindings"]["A"]["installation_id"],
            expected_next_xid="2")
        plan_selection = SimpleNamespace(schedule_bytes=selected.raw,
            source_progress={"A": progress_a})
        plan = ManagedCurrentA1TrialPlan(plan_selection,
            SimpleNamespace(manifest=a0_manifest), request, {},
            ManagedCurrentPrefixPage(1, "A", a1_batch.begin,
                base64.b64decode(a1_batch.cursor_before),
                base64.b64decode(a1_batch.cursor_after)), phase="a1_continuation")
        self.assertEqual(plan.verify_delivery(raw,
            prefix_sha256=hashlib.sha256(raw).hexdigest(), selected=anchors,
            schedule_bytes=selected.raw, pages=(plan.delivery_page,)), proof)

    def test_builds_and_validates_exact_one_step_from_detached_a1_evidence(self):
        selected = schedule()
        schedule_doc = selected.document()

        def cursor(source, xid):
            identity = schedule_doc["source_bindings"][source]
            return canonical({"profile": "truss-caller-asserted-feed/0.1",
                "nextXid": str(xid), "installationId": identity["installation_id"],
                "sourceEpoch": identity["epoch"], "consumerId": identity["consumer_id"],
                "registrationId": identity["registration_id"]}).encode()

        prior, following = cursor("A", 0), cursor("A", 2)
        page = canonical({"interfaceVersion": "truss-native-feed/0.1",
            "prior": json.loads(prior), "next": json.loads(following),
            "writerXid": "1", "catalogPrerequisites": [], "revisions": [],
            "changes": [], "sources": [], "reservations": [],
            "configuration": {}}).encode()
        batch = native_feed_batch(page, sha256=hashlib.sha256(page).hexdigest(),
            prior=prior, next=following, feed="feed-A")
        sources = {}
        for source in ("A", "B"):
            identity = schedule_doc["source_bindings"][source]
            source_cursor = prior if source == "A" else cursor("B", 0)
            next_cursor = following if source == "A" else source_cursor
            registration = canonical({"registrationId": f"catalog-{source}",
                                      "revision": "1"}).encode()
            sources[source] = {
                "identity": {"feed": identity["feed"], "epoch": identity["epoch"],
                    "installation_id": identity["installation_id"],
                    "consumer_id": identity["consumer_id"],
                    "consumer_registration_id": identity["registration_id"],
                    "catalog_registration_id": f"catalog-{source}",
                    "catalog_revision": "1"},
                "progress": {"catalog_registration_bytes": registration,
                    "cursor_bytes": source_cursor,
                    "catalog_registration_id": f"catalog-{source}"},
                "page": {"prior_bytes": source_cursor, "next_bytes": next_cursor,
                    "outcome": "transaction" if source == "A" else "empty_interval"},
            }
        progress = {}
        for source in ("A", "B"):
            evidence = sources[source]
            identity = schedule_doc["source_bindings"][source]
            source_cursor = evidence["progress"]["cursor_bytes"]
            registration = evidence["progress"]["catalog_registration_bytes"]
            progress[identity["feed"]] = {
                "profile": "ashlar-truss-source-genesis-progress/0.1",
                "feed": identity["feed"], "epoch": identity["epoch"],
                "position": base64.b64encode(source_cursor).decode("ascii"),
                "position_sha256": hashlib.sha256(source_cursor).hexdigest(),
                "catalog_registration_id": evidence["progress"]["catalog_registration_id"],
                "catalog_registration_sha256": hashlib.sha256(registration).hexdigest(),
                "consumer_id": identity["consumer_id"],
                "consumer_registration_id": identity["registration_id"],
                "installation_id": identity["installation_id"],
                "page": {"profile": "ashlar-source-page-absent/0.1"},
            }
        genesis_id = "genesis-fixture"
        initial_progress = canonical(progress)
        initial_manifest = canonical({"publication_id": genesis_id,
            "source_progress_json": initial_progress}).encode()
        request = truss_request_for("feed-A", batch, genesis_id, {})
        progress["feed-A"] = json.loads(request["source_checkpoint_json"])
        publication_id = "a1-publication-fixture"
        publication_manifest = canonical({"publication_id": publication_id,
            "source_progress_json": canonical(progress)}).encode()
        request_bytes = canonical(request).encode()
        detached_page = SimpleNamespace(source="A",
            identity=schedule_doc["source_bindings"]["A"],
            request_bytes=request_bytes, publication_id=publication_id,
            page_content=page, prior_bytes=prior, next_bytes=following)
        genesis = SimpleNamespace(schedule_bytes=selected.raw,
            schedule_sha256=selected.sha256, sources=sources)
        holder_snapshot = SimpleNamespace(source_page=detached_page)

        with patch("ashlar_host.truss_detached_a1_genesis_codec.decode_detached_a1_genesis_snapshot",
                   return_value=genesis) as decode_genesis, patch(
                "ashlar_host.truss_detached_source_snapshot_codec.decode_detached_source_snapshot",
                return_value=holder_snapshot):
            prefix, digest, anchors = build_managed_current_a1_prefix_evidence(
                genesis_bytes=b"detached-genesis-fixture",
                source_snapshot_bytes=b"detached-held-page-fixture",
                initial_manifest_bytes=initial_manifest,
                request_bytes=request_bytes,
                publication_manifest_bytes=publication_manifest,
                destination={"catalog": "client_dev", "schema": "trial"},
                carrier_profile="ashlar-delta/0.4-xxhash64",
                materialization_clock_us="1762000000000000")
            a0_prefix, a0_digest, a0_anchors = build_managed_current_a1_prefix_evidence(
                genesis_bytes=b"detached-genesis-fixture",
                source_snapshot_bytes=b"detached-held-page-fixture",
                initial_manifest_bytes=initial_manifest,
                request_bytes=request_bytes,
                publication_manifest_bytes=publication_manifest,
                destination={"catalog": "client_dev", "schema": "trial"},
                carrier_profile="ashlar-delta/0.4-xxhash64",
                materialization_clock_us="1762000000000000", phase="a0")
        self.assertEqual([call.kwargs for call in decode_genesis.call_args_list],
                         [{"expected_a_prior_cursor": "0"},
                          {"expected_a_prior_cursor": "0"}])
        package = json.loads(prefix)
        self.assertEqual(hashlib.sha256(prefix).hexdigest(), digest)
        self.assertEqual(package["scheduleStep"], 1)
        self.assertEqual(json.loads(package["entries"][0]["requestJson"]), request)
        self.assertEqual(anchors["sources"]["A"]["identity"],
                         schedule_doc["source_bindings"]["A"])
        a0_package = json.loads(a0_prefix)
        self.assertEqual(a0_package["phase"], "a0_metadata")
        self.assertEqual(a0_package["scheduleStep"], 0)
        self.assertEqual(a0_package["entries"], [])
        self.assertEqual(a0_package["progress"], json.loads(initial_progress))
        self.assertEqual(hashlib.sha256(a0_prefix).hexdigest(), a0_digest)
        self.assertEqual(a0_anchors["initialPublicationId"], genesis_id)


if __name__ == "__main__":
    unittest.main()

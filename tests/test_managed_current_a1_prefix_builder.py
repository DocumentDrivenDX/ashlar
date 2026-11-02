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
                   return_value=genesis), patch(
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
        package = json.loads(prefix)
        self.assertEqual(hashlib.sha256(prefix).hexdigest(), digest)
        self.assertEqual(package["scheduleStep"], 1)
        self.assertEqual(json.loads(package["entries"][0]["requestJson"]), request)
        self.assertEqual(anchors["sources"]["A"]["identity"],
                         schedule_doc["source_bindings"]["A"])


if __name__ == "__main__":
    unittest.main()

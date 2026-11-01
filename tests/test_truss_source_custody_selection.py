# SPDX-License-Identifier: MIT OR Apache-2.0
"""Correspondence-only tests; these do not qualify Truss or enable ACK."""
import base64
import hashlib
import json
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ashlar.pins import PinVector
from ashlar.source_checkpoint import truss_checkpoint
from ashlar.staging import batch_row
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt
from ashlar_host.managed_current_spark_receipt_read import (
    PROFILE as MANAGED_READ_PROFILE, ManagedCurrentSparkReceiptReopenObservation,
)
from ashlar_host.truss_source_custody_selection import (
    _RELEASE_ID, _RELEASE_SHA256, TrussSourceSelectionError,
    select_truss_source_custody_anchor,
)
from test_r04_schedule import schedule


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


class TrussSourceCustodySelectionTests(unittest.TestCase):
    def setUp(self):
        self.schedule = schedule()
        self.identity = self.schedule.document()["source_bindings"]["A"]
        before = {"profile": "truss-caller-asserted-feed/0.1",
            "installationId": self.identity["installation_id"],
            "sourceEpoch": self.identity["epoch"],
            "consumerId": self.identity["consumer_id"],
            "registrationId": self.identity["registration_id"], "nextXid": "1"}
        after = {**before, "nextXid": "43"}
        content = canonical({"interfaceVersion": "truss-native-feed/0.1",
            "prior": before, "next": after, "writerXid": "42",
            "catalogPrerequisites": [], "revisions": [], "changes": [],
            "sources": [], "reservations": [], "configuration": {}}).encode()
        self.prior = canonical(before).encode()
        self.following = canonical(after).encode()
        self.page = SimpleNamespace(content=content,
            sha256=hashlib.sha256(content).hexdigest(),
            prior=SimpleNamespace(content=self.prior),
            next=SimpleNamespace(content=self.following),
            consumer_id=self.identity["consumer_id"],
            registration_id=self.identity["registration_id"],
            outcome="transaction")
        batch = native_feed_batch(content, sha256=self.page.sha256,
            prior=self.prior, next=self.following, feed=self.identity["feed"])
        row = batch_row(batch)
        request = {"stream": "r04", "batch_id": batch.batch_id,
            "predecessor": "publication-before-A1", "schema_revisions_json": "{}",
            "source_batch_json": row["batch_json"],
            "source_batch_digest": row["batch_digest"],
            "source_checkpoint_json": truss_checkpoint(batch)}
        request["request_digest"] = hashlib.sha256(canonical(request).encode()).hexdigest()
        self.request_bytes = canonical(request).encode()
        self.vector = PinVector("authority", "manifest", "publication-A1", "a" * 64,
            {f"local.graph.{role}": (f"uuid-{role}", 3) for role in
             ("object_current", "edge_current", "tombstone", "whole_source_history")})
        self.manifest_bytes = canonical({"publication_id": "publication-A1"}).encode()

    def observation(self, receipt):
        return ManagedCurrentSparkReceiptReopenObservation(
            profile=MANAGED_READ_PROFILE, publication_id="publication-A1",
            manifest=json.loads(self.manifest_bytes),
            manifest_sha256=hashlib.sha256(self.manifest_bytes).hexdigest(),
            source_receipt_bytes=receipt,
            source_receipt_sha256=hashlib.sha256(receipt).hexdigest(),
            vector=self.vector, installation_inventory={})

    def test_freezes_schedule_page_and_destination_before_publication(self):
        anchor = select_truss_source_custody_anchor(schedule=self.schedule,
            source="A", page=self.page, request_bytes=self.request_bytes,
            destination_id="selected-r04-destination")
        registration = json.loads(anchor.destination_registration_bytes)
        scope = json.loads(anchor.ack_scope_bytes)
        self.assertEqual(registration["ackRegistrationId"], self.identity["ack_registration_id"])
        self.assertEqual(registration["consumerRegistrationId"], self.page.registration_id)
        self.assertEqual(registration["destinationId"], "selected-r04-destination")
        self.assertEqual(scope["scopeId"], self.identity["ack_scope"])
        self.assertEqual(scope["priorCursorSha256"], hashlib.sha256(self.prior).hexdigest())

    def test_binds_exact_manifest_vector_and_receipt(self):
        anchor = select_truss_source_custody_anchor(schedule=self.schedule,
            source="A", page=self.page, request_bytes=self.request_bytes,
            destination_id="selected-r04-destination")
        receipt = encode_truss_destination_receipt(
            self.request_bytes, self.manifest_bytes, self.vector)
        bound = anchor.bind_publication(manifest_bytes=self.manifest_bytes,
            vector=self.vector, receipt_bytes=receipt,
            observation=self.observation(receipt))
        self.assertEqual(bound.publication_id, "publication-A1")
        self.assertEqual(bound.request_sha256, hashlib.sha256(self.request_bytes).hexdigest())
        with self.assertRaisesRegex(TrussSourceSelectionError, "receipt differs"):
            anchor.bind_publication(manifest_bytes=self.manifest_bytes,
                vector=self.vector, receipt_bytes=receipt + b" ",
                observation=self.observation(receipt + b" "))

    def test_managed_observation_drift_refuses(self):
        anchor = select_truss_source_custody_anchor(schedule=self.schedule,
            source="A", page=self.page, request_bytes=self.request_bytes,
            destination_id="selected-r04-destination")
        receipt = encode_truss_destination_receipt(
            self.request_bytes, self.manifest_bytes, self.vector)
        changed = ManagedCurrentSparkReceiptReopenObservation(
            profile=MANAGED_READ_PROFILE, publication_id="different-publication",
            manifest=json.loads(self.manifest_bytes),
            manifest_sha256=hashlib.sha256(self.manifest_bytes).hexdigest(),
            source_receipt_bytes=receipt,
            source_receipt_sha256=hashlib.sha256(receipt).hexdigest(),
            vector=self.vector, installation_inventory={})
        with self.assertRaisesRegex(TrussSourceSelectionError, "Managed reader observation"):
            anchor.bind_publication(manifest_bytes=self.manifest_bytes,
                vector=self.vector, receipt_bytes=receipt, observation=changed)

    def test_actual_page_identity_drift_refuses(self):
        changed = SimpleNamespace(**{**vars(self.page), "consumer_id": "another-consumer"})
        with self.assertRaisesRegex(TrussSourceSelectionError, "page/cursor differs"):
            select_truss_source_custody_anchor(schedule=self.schedule, source="A",
                page=changed, request_bytes=self.request_bytes,
                destination_id="selected-r04-destination")

    def test_non_source_schedule_slot_refuses(self):
        with self.assertRaisesRegex(TrussSourceSelectionError, "source slot"):
            select_truss_source_custody_anchor(schedule=self.schedule, source="C",
                page=self.page, request_bytes=self.request_bytes,
                destination_id="selected-r04-destination")

    def test_candidate_sdk_materialization_refuses_051_metadata(self):
        anchor = select_truss_source_custody_anchor(schedule=self.schedule,
            source="A", page=self.page, request_bytes=self.request_bytes,
            destination_id="selected-r04-destination")
        receipt = encode_truss_destination_receipt(
            self.request_bytes, self.manifest_bytes, self.vector)
        bound = anchor.bind_publication(manifest_bytes=self.manifest_bytes,
            vector=self.vector, receipt_bytes=receipt,
            observation=self.observation(receipt))
        package = types.ModuleType("truss")
        package.__path__ = []
        runtime = types.ModuleType("truss.catalog_runtime")
        runtime._release = lambda: ({"releaseId": _RELEASE_ID}, _RELEASE_SHA256, ())
        feed = types.ModuleType("truss.feed")
        class FeedSourceSelection:
            def __init__(self, **kwargs): self.__dict__.update(kwargs)
        feed.FeedSourceSelection = FeedSourceSelection
        with patch.dict(sys.modules, {"truss": package,
                "truss.catalog_runtime": runtime, "truss.feed": feed}), \
                patch("importlib.metadata.version", return_value="0.0.51"):
            with self.assertRaisesRegex(TrussSourceSelectionError, "gated"):
                bound.to_candidate_sdk()


if __name__ == "__main__":
    unittest.main()

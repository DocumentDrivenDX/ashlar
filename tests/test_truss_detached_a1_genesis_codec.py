# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed detached .53 A/B genesis transport tests."""
import base64
import hashlib
import unittest
from unittest.mock import patch

from ashlar_host.r04_schedule import canonical
from ashlar_host.managed_a1_detached_admission import _continuation_expected_a_cursor
from ashlar_host.truss_detached_a1_genesis_codec import (
    PROFILE, DetachedA1GenesisError, decode_detached_a1_genesis_snapshot,
    _require_current_fresh_catalog_evidence,
)
from ashlar_host.truss053_source_custody import _genesis_matches_with_monotonic_watermarks
from ashlar_host.truss_current_catalog import (
    CURRENT_TRUSS_054_FRESH_RUNTIME_MANIFEST_SHA256,
    CURRENT_TRUSS_054_FRESH_WHEEL_SHA256,
    CURRENT_TRUSS_055_FRESH_RUNTIME_MANIFEST_SHA256,
    CURRENT_TRUSS_055_FRESH_WHEEL_SHA256,
    CurrentCatalogEvidence,
)
from test_managed_current_a1_trial_plan import TestManagedCurrentA1TrialPlan


def _source(schedule, source):
    binding = schedule.document()["source_bindings"][source]
    cursor = canonical({"profile": "truss-caller-asserted-feed/0.1",
        "installationId": binding["installation_id"], "sourceEpoch": binding["epoch"],
        "consumerId": binding["consumer_id"], "registrationId": binding["registration_id"],
        "nextXid": "0"})
    document = f"model-{source}".encode()
    model_pin = schedule.document()["accepted_model_pins"][source]
    document_sha = hashlib.sha256(document).hexdigest()
    registration_id = f"catalog-registration-{source}"
    revision = "1"
    accepted = [{"documentId": f"model-{source}", "documentRevision": model_pin["documentRevision"],
        "artifact": {"identity": f"artifact:model-{source}", "sha256": model_pin["modelSha256"]}}]
    report = canonical({"registrationId": registration_id, "revision": 1,
        "acceptedInput": {"documents": accepted}})
    view = canonical({"catalogRevision": revision, "scope": {"kind": "complete"}})
    page_content = b"public-page-" + source.encode()
    next_cursor = cursor if source == "B" else canonical({
        "profile": "truss-caller-asserted-feed/0.1",
        "installationId": binding["installation_id"], "sourceEpoch": binding["epoch"],
        "consumerId": binding["consumer_id"], "registrationId": binding["registration_id"],
        "nextXid": "1"})
    progress = {"feed": binding["feed"], "epoch": binding["epoch"],
        "position": base64.b64encode(cursor).decode(),
        "position_sha256": hashlib.sha256(cursor).hexdigest(),
        "catalog_registration_sha256": hashlib.sha256(report).hexdigest(),
        "catalog_registration_id": registration_id,
        "catalog_registration_bytes_base64": base64.b64encode(report).decode(),
        "consumer_id": binding["consumer_id"],
        "consumer_registration_id": binding["registration_id"],
        "installation_id": binding["installation_id"],
        "cursor_bytes_base64": base64.b64encode(cursor).decode()}
    return {"source": source,
        "identity": {"feed": binding["feed"], "epoch": binding["epoch"],
            "installation_id": binding["installation_id"], "consumer_id": binding["consumer_id"],
            "consumer_registration_id": binding["registration_id"],
            "catalog_registration_id": registration_id, "catalog_revision": revision},
        "progress": progress, "catalog_revision": revision,
        "report_bytes_base64": base64.b64encode(report).decode(),
        "view_bytes_base64": base64.b64encode(view).decode(),
        "page": {"consumer_id": binding["consumer_id"], "registration_id": binding["registration_id"],
            "prior_bytes_base64": base64.b64encode(cursor).decode(),
            "next_bytes_base64": base64.b64encode(next_cursor).decode(),
            "content_base64": base64.b64encode(page_content).decode(),
            "sha256": hashlib.sha256(page_content).hexdigest(), "writer_xid": "1" if source == "A" else None,
            "safe_watermark_xid": "1", "outcome": "transaction" if source == "A" else "empty_interval"},
        "documents": [{"document_id": f"model-{source}",
            "document_revision": model_pin["documentRevision"],
            "artifact_identity": f"artifact:model-{source}",
            "registration_id": registration_id, "catalog_revision": revision,
            "sha256": document_sha, "content_base64": base64.b64encode(document).decode()}],
        "model": {"pin": model_pin, "content_base64": base64.b64encode(document).decode()}}


def _raw():
    schedule = TestManagedCurrentA1TrialPlan._schedule()
    body = {"profile": PROFILE,
        "schedule_bytes_base64": base64.b64encode(schedule.raw).decode(),
        "schedule_sha256": schedule.sha256, "schema_revisions": {},
        "sources": [_source(schedule, "A"), _source(schedule, "B")]}
    return canonical({**body, "binding_sha256": hashlib.sha256(canonical(body)).hexdigest()})


class DetachedA1GenesisCodecTests(unittest.TestCase):
    def test_live_genesis_allows_only_canonical_monotonic_safe_watermark_drift(self):
        import json
        original = json.loads(_raw())

        def encode(value):
            body = {key: item for key, item in value.items() if key != "binding_sha256"}
            return canonical({**body, "binding_sha256": hashlib.sha256(
                canonical(body)).hexdigest()})

        newer = json.loads(json.dumps(original))
        newer["sources"][0]["page"]["safe_watermark_xid"] = "2"
        self.assertTrue(_genesis_matches_with_monotonic_watermarks(
            encode(original), encode(newer)))

        older = json.loads(json.dumps(original))
        older["sources"][0]["page"]["safe_watermark_xid"] = "0"
        self.assertFalse(_genesis_matches_with_monotonic_watermarks(
            encode(original), encode(older)))

        noncanonical = json.loads(json.dumps(original))
        noncanonical["sources"][0]["page"]["safe_watermark_xid"] = "01"
        self.assertFalse(_genesis_matches_with_monotonic_watermarks(
            encode(original), encode(noncanonical)))

        changed_page = json.loads(json.dumps(original))
        changed_page["sources"][0]["page"]["content_base64"] = base64.b64encode(
            b"changed-page").decode()
        changed_page["sources"][0]["page"]["sha256"] = hashlib.sha256(
            b"changed-page").hexdigest()
        self.assertFalse(_genesis_matches_with_monotonic_watermarks(
            encode(original), encode(changed_page)))

        continuation = json.loads(_raw())
        source_a = continuation["sources"][0]
        prior = json.loads(base64.b64decode(
            source_a["page"]["prior_bytes_base64"]))
        following = json.loads(base64.b64decode(
            source_a["page"]["next_bytes_base64"]))
        prior["nextXid"] = "5"
        following["nextXid"] = "6"
        prior_bytes = canonical(prior)
        following_bytes = canonical(following)
        progress = source_a["progress"]
        progress["cursor_bytes_base64"] = base64.b64encode(prior_bytes).decode()
        progress["position"] = progress["cursor_bytes_base64"]
        progress["position_sha256"] = hashlib.sha256(prior_bytes).hexdigest()
        source_a["page"]["prior_bytes_base64"] = progress["cursor_bytes_base64"]
        source_a["page"]["next_bytes_base64"] = base64.b64encode(following_bytes).decode()
        source_a["page"]["safe_watermark_xid"] = "5"
        continuation_bytes = encode(continuation)
        continuation_newer = json.loads(continuation_bytes)
        continuation_newer["sources"][0]["page"]["safe_watermark_xid"] = "7"
        self.assertTrue(_genesis_matches_with_monotonic_watermarks(
            continuation_bytes, encode(continuation_newer),
            expected_a_prior_cursor="5"))

    def test_continuation_cursor_reader_accepts_closed_source_list_shape(self):
        import json
        value = json.loads(_raw())
        source_a = value["sources"][0]
        prior = json.loads(base64.b64decode(source_a["page"]["prior_bytes_base64"]))
        prior["nextXid"] = "759"
        prior_bytes = canonical(prior)
        source_a["page"]["prior_bytes_base64"] = base64.b64encode(prior_bytes).decode()
        body = {key: item for key, item in value.items() if key != "binding_sha256"}
        raw = canonical({**body, "binding_sha256": hashlib.sha256(canonical(body)).hexdigest()})
        self.assertEqual(_continuation_expected_a_cursor(raw), "759")

        duplicate = json.loads(raw)
        duplicate["sources"][1]["source"] = "A"
        body = {key: item for key, item in duplicate.items() if key != "binding_sha256"}
        duplicate_raw = canonical({**body,
            "binding_sha256": hashlib.sha256(canonical(body)).hexdigest()})
        with self.assertRaisesRegex(ValueError, "prior cursor"):
            _continuation_expected_a_cursor(duplicate_raw)

    def test_current_055_evidence_uses_actual_pins_and_refuses_stale_054(self):
        pins = {"version": "0.0.55",
            "wheel_sha256": CURRENT_TRUSS_055_FRESH_WHEEL_SHA256,
            "manifest_sha256": CURRENT_TRUSS_055_FRESH_RUNTIME_MANIFEST_SHA256,
            "source_custody_profile": "ashlar-truss-055-fresh-baseline-source-custody/0.1"}
        with patch("ashlar_host.truss_current_catalog.installed_fresh_truss_runtime_pins",
                   return_value=pins):
            current = CurrentCatalogEvidence("0.0.55", {}, {}, {},
                pins["wheel_sha256"], pins["manifest_sha256"])
            self.assertIs(_require_current_fresh_catalog_evidence(current), pins)
            stale = CurrentCatalogEvidence("0.0.54", {}, {}, {},
                CURRENT_TRUSS_054_FRESH_WHEEL_SHA256,
                CURRENT_TRUSS_054_FRESH_RUNTIME_MANIFEST_SHA256)
            with self.assertRaisesRegex(DetachedA1GenesisError, "installed fresh Truss"):
                _require_current_fresh_catalog_evidence(stale)

    def test_decodes_exact_a_and_b_public_pages_and_accepted_documents(self):
        snapshot = decode_detached_a1_genesis_snapshot(_raw())
        self.assertEqual(set(snapshot.sources), {"A", "B"})
        self.assertEqual(snapshot.source_evidence("A")["page"]["outcome"], "transaction")
        self.assertEqual(snapshot.source_evidence("B")["page"]["outcome"], "empty_interval")
        self.assertEqual(snapshot.source_evidence("A")["documents"][0]["content"], b"model-A")
        self.assertEqual(snapshot.source_evidence("B")["progress"]["position"],
                         base64.b64encode(snapshot.source_evidence("B")["progress"]["cursor_bytes"]).decode())

    def test_rejects_accepted_document_bytes_differing_from_artifact_pin(self):
        import json
        raw = _raw()
        value = json.loads(raw)
        entry = value["sources"][1]["documents"][0]
        entry["content_base64"] = base64.b64encode(b"tampered").decode()
        value.pop("binding_sha256")
        value["binding_sha256"] = hashlib.sha256(canonical(value)).hexdigest()
        with self.assertRaisesRegex(DetachedA1GenesisError, "content binding"):
            decode_detached_a1_genesis_snapshot(canonical(value))

    def test_rejects_a_page_outside_the_frozen_schedule_identity(self):
        import json
        raw = _raw()
        value = json.loads(raw)
        value["sources"][0]["identity"]["installation_id"] = "foreign"
        value.pop("binding_sha256")
        value["binding_sha256"] = hashlib.sha256(canonical(value)).hexdigest()
        with self.assertRaisesRegex(DetachedA1GenesisError, "schedule or progress"):
            decode_detached_a1_genesis_snapshot(canonical(value))


if __name__ == "__main__":
    unittest.main()

import base64
import hashlib
import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ashlar.pins import PinVector
from ashlar_host.truss_source_custody_selection import TrussSourceCustodyAnchor
from ashlar_host.truss_local_publication_recovery import (
    TrussPublicationRecoveryError,
    recover_committed_truss_publication_and_ack,
)
import ashlar_host.truss_local_publication_recovery as recovery_module


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode()


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        os.chmod(self.root, 0o700)
        self.events = []
        self.request_digest = "a" * 64
        prior, following = b"prior-cursor", b"next-cursor"
        page = SimpleNamespace(content=b"page-content", sha256=hashlib.sha256(b"page-content").hexdigest(),
            prior=SimpleNamespace(content=prior), next=SimpleNamespace(content=following),
            registration_id="registration", writer_xid="768", consumer_id="consumer")
        checkpoint = {"feed": "feed", "batch_id": "768", "payload_digest": page.sha256,
                      "position": base64.b64encode(following).decode(),
                      "previous": base64.b64encode(prior).decode()}
        self.request = {"request_digest": self.request_digest, "stream": "feed", "batch_id": "768",
                        "source_checkpoint_json": canonical(checkpoint).decode()}
        self.request_raw = canonical(self.request)
        self.manifest = {"publication_id": "truss-graph-" + self.request_digest,
                         "profile_version": "ashlar-delta/0.4-xxhash64",
                         "table_versions_json": canonical({
                             "local.trussgraph.object_current": 2,
                             "local.trussgraph.edge_current": 2,
                             "local.trussgraph.tombstone": 1,
                             "local.trussgraph.whole_source_history": 2}).decode()}
        self.manifest_raw = canonical(self.manifest)
        self.vector = PinVector("private-local-process", "manifest", self.manifest["publication_id"],
            hashlib.sha256(self.manifest_raw).hexdigest(), {
                "local.trussgraph.object_current": ("u1", 2),
                "local.trussgraph.edge_current": ("u2", 2),
                "local.trussgraph.tombstone": ("u3", 1),
                "local.trussgraph.whole_source_history": ("u4", 2)})
        from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt
        self.receipt_raw = encode_truss_destination_receipt(self.request_raw, self.manifest_raw, self.vector)
        self.pending = {"profile": "ashlar-truss-publication-readback-pending/0.1",
            "status": "publication_returned_native_readback_pending", "acknowledgement": "not_attempted",
            "request_digest": self.request_digest, "request_sha256": hashlib.sha256(self.request_raw).hexdigest(),
            "request_bytes_base64": base64.b64encode(self.request_raw).decode(),
            "manifest": self.manifest, "manifest_sha256": hashlib.sha256(self.manifest_raw).hexdigest(),
            "manifest_bytes_base64": base64.b64encode(self.manifest_raw).decode(),
            "publication_id": self.manifest["publication_id"], "destination_id": "destination", "feed": "feed",
            "consumer_id": "consumer", "receipt_sha256": hashlib.sha256(self.receipt_raw).hexdigest(),
            "receipt_bytes_base64": base64.b64encode(self.receipt_raw).decode(),
            "publication_vector": {"authority": self.vector.authority, "scope_kind": self.vector.scope_kind,
                "scope_id": self.vector.scope_id, "custody_digest": self.vector.custody_digest,
                "targets": {key: list(value) for key, value in self.vector.targets.items()}},
            "source_page": {"page_sha256": page.sha256, "content_sha256": page.sha256,
                "prior_sha256": hashlib.sha256(prior).hexdigest(), "next_sha256": hashlib.sha256(following).hexdigest(),
                "registration_id": "registration", "writer_xid": "768"}}
        archive = self.root / f"source-page-{self.request_digest}.json"
        archive_raw = b"original private page archive"
        archive.write_bytes(archive_raw); os.chmod(archive, 0o600)
        self.intent_path = self.root / f"publication-intent-{self.request_digest}.json"
        self.intent_raw = canonical({"feed": "feed", "publication_id": self.manifest["publication_id"],
            "request_digest": self.request_digest, "source_page_path": str(archive),
            "source_page_sha256": hashlib.sha256(archive_raw).hexdigest()})
        self.intent_path.write_bytes(self.intent_raw); os.chmod(self.intent_path, 0o600)
        self.pending["source_page"].update({"path": str(archive),
            "archive_sha256": hashlib.sha256(archive_raw).hexdigest()})
        self.pending_raw = canonical(self.pending) + b"\n"
        self.anchor = TrussSourceCustodyAnchor(
            schedule_sha256="b" * 64, source="local-development", identity={"installation_id": "install"},
            page=page, destination_id="destination", request_bytes=self.request_raw,
            request_sha256=hashlib.sha256(self.request_raw).hexdigest(), destination_registration_bytes=b"{}",
            ack_scope_bytes=b"{}", authority_profile="local-development",
            preselected_publication_id=self.manifest["publication_id"])
        @contextmanager
        def writer(*_args):
            self.events.append("restore-writer-open")
            yield
            self.events.append("restore-writer-close")
        self.driver = SimpleNamespace(context=object(),
            source_sessions=SimpleNamespace(batch=lambda: SimpleNamespace(feed="feed")),
            writer=writer, transport=SimpleNamespace(targets={
                "local.trussgraph.object_current": SimpleNamespace(uuid="u1"),
                "local.trussgraph.edge_current": SimpleNamespace(uuid="u2"),
                "local.trussgraph.tombstone": SimpleNamespace(uuid="u3"),
                "local.trussgraph.whole_source_history": SimpleNamespace(uuid="u4")} ),
            policy=SimpleNamespace(active=None))
        self.descriptor = SimpleNamespace(raw=self.manifest)
        self.expected = SimpleNamespace(receipt=lambda: {"profile": "test-expected", "roles": {}})
        self.observation = SimpleNamespace(publication_id=self.manifest["publication_id"],
            manifest_sha256=hashlib.sha256(self.manifest_raw).hexdigest(),
            receipt_sha256=hashlib.sha256(self.receipt_raw).hexdigest(), vector=self.vector,
            roles={"object_current": SimpleNamespace(table="local.trussgraph.object_current", uuid="u1", version=2,
                head_version=2, head_json_sha256="1"*64,row_bag_sha256="2"*64,row_count=4),
                "edge_current": SimpleNamespace(table="local.trussgraph.edge_current", uuid="u2", version=2,
                head_version=2, head_json_sha256="3"*64,row_bag_sha256="4"*64,row_count=3),
                "tombstone": SimpleNamespace(table="local.trussgraph.tombstone", uuid="u3", version=1,
                head_version=1, head_json_sha256="5"*64,row_bag_sha256="6"*64,row_count=0),
                "whole_source_history": SimpleNamespace(table="local.trussgraph.whole_source_history", uuid="u4", version=2,
                head_version=2, head_json_sha256="7"*64,row_bag_sha256="8"*64,row_count=7)})

    def _call(self, *, readback=None, custody=None):
        selection = recovery_module.TrussSourceCustodySelection(
            page=self.anchor.page, maximum_facts=7, maximum_bytes=1048576,
            request_bytes=self.request_raw, destination_registration_bytes=b"{}", ack_scope_bytes=b"{}",
            destination_id="destination", request_sha256=hashlib.sha256(self.request_raw).hexdigest(),
            publication_id=self.manifest["publication_id"], manifest_bytes=self.manifest_raw,
            vector=self.vector, receipt_bytes=self.receipt_raw, schedule_sha256="c"*64,
            source="local-development", identity={"installation_id": "install"},
            authority_profile="local-development")
        if custody is None:
            class Custody:
                runtime_status_provider = staticmethod(lambda: object())
                def open(inner): self.events.append("open")
                def observe_selected_page(inner): self.events.append("observe"); return self.anchor.page
                def acknowledge(inner):
                    self.events.append("ack")
                    self.assertTrue((self.root / "readback-complete.json").exists())
                    self.assertTrue((self.root / "ack-attempt.json").exists())
                    return object()
                def reconcile(inner):
                    self.events.append("reconcile")
                    return SimpleNamespace(outcome="matched", checkpoint=self.anchor.page.next)
                def close(inner): self.events.append("close")
                def dispose(inner): self.events.append("dispose")
            custody = Custody()
        def restore(driver, digest):
            self.events.append("restore")
            driver.policy.active = {"request": self.request, "manifest": self.manifest,
                                    "expected": {"exact": "following"}}
            return self.descriptor
        def mint(**_kwargs): self.events.append("mint-expected"); return self.expected
        def vector(*_args, **_kwargs): return self.vector
        def read(**_kwargs):
            self.events.append("native-readback")
            if readback: readback()
            return self.observation
        with patch("ashlar_host.truss_local_publication_recovery.verify_request_digest", return_value=self.request_digest), \
             patch("ashlar_host.truss_local_publication_recovery.restore_applied_truss_publication", side_effect=restore), \
             patch("ashlar_host.truss_local_publication_recovery.manifest_pin_vector", side_effect=vector), \
             patch("ashlar_host.truss_local_publication_recovery._mint_expected_native_post_state", side_effect=mint), \
             patch("ashlar_host.truss_local_publication_recovery.read_local_native_delta_publication_observation", side_effect=read), \
             patch("ashlar_host.truss_page_archive.decode_truss_page", return_value=self.anchor.page), \
             patch.object(TrussSourceCustodyAnchor, "bind_local_native_publication", return_value=selection), \
             patch("ashlar_host.truss_local_publication_recovery.Truss052SourceCustodySession", type(custody)):
            return recover_committed_truss_publication_and_ack(self.driver, self.request_digest,
                self.pending_raw, self.anchor, spark=object(), custody_session_factory=lambda _s: custody,
                completion_path=self.root / "readback-complete.json", ack_attempt_path=self.root / "ack-attempt.json",
                ack_reconciliation_path=self.root / "ack-reconciled.json",
                publication_intent_path=self.intent_path,
                publication_intent_sha256=hashlib.sha256(self.intent_raw).hexdigest())

    def test_readback_refusal_prevents_marker_and_source_ack(self):
        def refusal(): raise ValueError("native row bag mismatch")
        with self.assertRaisesRegex(ValueError, "row bag"):
            self._call(readback=refusal)
        self.assertNotIn("ack", self.events)
        self.assertFalse((self.root / "readback-complete.json").exists())
        self.assertFalse((self.root / "ack-attempt.json").exists())

    def test_native_readback_marker_and_ack_are_strictly_ordered(self):
        result = self._call()
        self.assertEqual(self.events, ["restore-writer-open", "restore", "restore-writer-close",
            "mint-expected", "native-readback", "open", "observe", "ack", "reconcile", "close", "dispose"])
        self.assertTrue((self.root / "readback-complete.json").is_file())
        completion = json.loads((self.root / "readback-complete.json").read_bytes())
        self.assertEqual(completion["status"], "native_readback_verified_ack_not_attempted")
        self.assertEqual(completion["acknowledgement"], "not_attempted_at_marker_creation")
        self.assertEqual(json.loads((self.root / "ack-reconciled.json").read_bytes())["outcome"], "matched")
        self.assertEqual(result["reconciliation"].outcome, "matched")


    def test_ack_exception_is_reconciled_once_without_resubmission(self):
        class UnknownCustody:
            runtime_status_provider = staticmethod(lambda: object())
            def open(inner): self.events.append("open")
            def observe_selected_page(inner): self.events.append("observe"); return self.anchor.page
            def acknowledge(inner):
                self.events.append("ack")
                raise ConnectionError("commit outcome unknown")
            def reconcile(inner):
                self.events.append("reconcile")
                return SimpleNamespace(outcome="matched", checkpoint=self.anchor.page.next)
            def close(inner): self.events.append("close")
            def dispose(inner): self.events.append("dispose")
        result = self._call(custody=UnknownCustody())
        self.assertEqual(self.events[-4:], ["ack", "reconcile", "close", "dispose"])
        receipt = json.loads((self.root / "ack-reconciled.json").read_bytes())
        self.assertEqual(receipt["ack_call_outcome"], "raised_then_reconciled")
        self.assertEqual(receipt["acknowledgement"], "matched")
        self.assertEqual(result["reconciliation"].outcome, "matched")

    def test_unmatched_ack_reconciliation_is_durable_non_success(self):
        class MissingCustody:
            runtime_status_provider = staticmethod(lambda: object())
            def open(inner): self.events.append("open")
            def observe_selected_page(inner): self.events.append("observe"); return self.anchor.page
            def acknowledge(inner):
                self.events.append("ack")
                raise ConnectionError("commit outcome unknown")
            def reconcile(inner):
                self.events.append("reconcile")
                return SimpleNamespace(outcome="missing", checkpoint=self.anchor.page.prior)
            def close(inner): self.events.append("close")
            def dispose(inner): self.events.append("dispose")
        with self.assertRaisesRegex(TrussPublicationRecoveryError, "did not match"):
            self._call(custody=MissingCustody())
        receipt = json.loads((self.root / "ack-reconciled.json").read_bytes())
        self.assertEqual(receipt["outcome"], "missing")
        self.assertEqual(receipt["acknowledgement"], "not_confirmed")
        self.assertNotIn("close", self.events)
        self.assertEqual(self.events[-2:], ["reconcile", "dispose"])

    def test_malformed_reconciliation_checkpoint_fails_closed(self):
        class MalformedCustody:
            runtime_status_provider = staticmethod(lambda: object())
            def open(inner): self.events.append("open")
            def observe_selected_page(inner): self.events.append("observe"); return self.anchor.page
            def acknowledge(inner): self.events.append("ack"); return object()
            def reconcile(inner):
                self.events.append("reconcile")
                return SimpleNamespace(outcome="missing", checkpoint=None)
            def close(inner): self.events.append("close")
            def dispose(inner): self.events.append("dispose")
        with self.assertRaisesRegex(TrussPublicationRecoveryError, "checkpoint"):
            self._call(custody=MalformedCustody())
        self.assertTrue((self.root / "ack-attempt.json").is_file())
        self.assertFalse((self.root / "ack-reconciled.json").exists())
        self.assertNotIn("close", self.events)

    def test_existing_completion_marker_refuses_before_source_custody(self):
        (self.root / "readback-complete.json").write_bytes(b"existing")
        with self.assertRaises(TrussPublicationRecoveryError):
            self._call()
        self.assertNotIn("open", self.events)
        self.assertNotIn("ack", self.events)

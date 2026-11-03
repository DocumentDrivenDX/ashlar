# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure exact-byte checks for detached Truss .53 source snapshots."""
import base64
import hashlib
import json
from dataclasses import replace
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from ashlar_host.managed_current_genesis import TrussSourceGenesisProgress
from ashlar_host.managed_current_truss053_genesis_owner import (
    ManagedCurrentTruss053SourceObservation, _issue_source_observation,
)
from ashlar_host.truss_current_catalog import (
    CURRENT_TRUSS_053_FRESH_VERSION, CURRENT_TRUSS_053_FRESH_WHEEL_SHA256,
    CURRENT_TRUSS_053_FRESH_RUNTIME_MANIFEST_SHA256,
    CURRENT_TRUSS_054_FRESH_VERSION, CURRENT_TRUSS_054_FRESH_WHEEL_SHA256,
    CURRENT_TRUSS_054_FRESH_RUNTIME_MANIFEST_SHA256,
    CURRENT_TRUSS_055_FRESH_VERSION, CURRENT_TRUSS_055_FRESH_WHEEL_SHA256,
    CURRENT_TRUSS_055_FRESH_RUNTIME_MANIFEST_SHA256, CurrentCatalogEvidence,
)
from ashlar_host.truss_detached_source_snapshot_codec import (
    DetachedSourceSnapshotError, DetachedTrussSourceSnapshot,
    decode_detached_source_snapshot, encode_detached_source_snapshot,
)
from ashlar_host.truss053_source_custody import detached_source_custody_digests
from ashlar_host.truss_source_custody_selection import (
    TrussSourceCustodyAnchor, _ANCHORS, _issue,
)
from ashlar_host.r04_schedule import canonical


class PublicAcceptedDocument:
    def __init__(self, **kwargs): self.__dict__.update(kwargs)


class DetachedTrussSourceSnapshotCodecTests(unittest.TestCase):
    def setUp(self):
        self.content = b"exact source document content"
        self.document_id = "model-a"
        self.document_revision = "source-revision-1"
        self.artifact_identity = "artifact:model-a"
        self.catalog_revision = "9"
        self.catalog_registration_id = "catalog-registration-a"
        self.consumer_id = "consumer-a"
        self.consumer_registration_id = "consumer-registration-a"
        self.prior = b'{"nextXid":"0","installationId":"install-a"}'
        self.next = b'{"nextXid":"3","installationId":"install-a"}'
        self.page = SimpleNamespace(
            content=b"exact native transaction page",
            sha256=hashlib.sha256(b"exact native transaction page").hexdigest(),
            prior=SimpleNamespace(content=self.prior),
            next=SimpleNamespace(content=self.next),
            consumer_id=self.consumer_id,
            registration_id=self.consumer_registration_id,
            writer_xid="2", safe_watermark_xid="2", outcome="transaction")
        self.identity = {
            "feed": "feed-a", "epoch": "epoch-a", "installation_id": "install-a",
            "consumer_id": self.consumer_id,
            "registration_id": self.consumer_registration_id,
            "catalog_registration_id": self.catalog_registration_id,
        }
        self.request = b'{"request_digest":"a"}'
        self.anchor = _issue(_ANCHORS, TrussSourceCustodyAnchor(
            schedule_sha256="a" * 64, source="A", identity=self.identity,
            page=self.page, destination_id="destination-a", request_bytes=self.request,
            request_sha256=hashlib.sha256(self.request).hexdigest(),
            destination_registration_bytes=b'{"registration":"exact"}',
            ack_scope_bytes=b'{"scope":"exact"}', maximum_facts=8,
            maximum_bytes=4096, preselected_publication_id="ashlar-current:" + "b" * 64))
        self.registration_bytes = b'{"catalogRegistration":"exact"}'
        self.progress = TrussSourceGenesisProgress(
            feed="feed-a", epoch="epoch-a",
            position=base64.b64encode(self.prior).decode("ascii"),
            position_sha256=hashlib.sha256(self.prior).hexdigest(),
            catalog_registration_id=self.catalog_registration_id,
            catalog_registration_sha256=hashlib.sha256(self.registration_bytes).hexdigest(),
            consumer_id=self.consumer_id,
            consumer_registration_id=self.consumer_registration_id,
            installation_id="install-a", catalog_registration_bytes=self.registration_bytes,
            cursor_bytes=self.prior)
        report = {
            "interfaceVersion": "truss-catalog-registration-report/0.6",
            "revision": self.catalog_revision,
            "registrationId": self.catalog_registration_id,
            "acceptedInput": {"documents": [{
                "documentId": self.document_id,
                "documentRevision": self.document_revision,
                "artifact": {"identity": self.artifact_identity,
                             "sha256": hashlib.sha256(self.content).hexdigest()},
            }]},
        }
        view = {"interfaceVersion": "truss-catalog-view/0.2.0",
                "catalogRevision": self.catalog_revision,
                "scope": {"kind": "complete"}}
        self.report_bytes = canonical(report)
        self.view_bytes = canonical(view)
        document = PublicAcceptedDocument(
            document_id=self.document_id,
            document_revision=self.document_revision,
            artifact_identity=self.artifact_identity,
            registration_id=self.catalog_registration_id,
            catalog_revision=int(self.catalog_revision),
            sha256=hashlib.sha256(self.content).hexdigest(), content=self.content)
        source = SimpleNamespace(**document.__dict__)
        self.evidence = CurrentCatalogEvidence(
            CURRENT_TRUSS_053_FRESH_VERSION,
            {self.catalog_revision: self.report_bytes},
            {self.catalog_revision: self.view_bytes},
            {(self.catalog_revision, self.document_id): source},
            CURRENT_TRUSS_053_FRESH_WHEEL_SHA256,
            CURRENT_TRUSS_053_FRESH_RUNTIME_MANIFEST_SHA256)
        self.observation = _issue_source_observation(
            ManagedCurrentTruss053SourceObservation(
                "A", self.progress, self.report_bytes, self.view_bytes,
                {self.document_id: document}, {}, self.evidence, self.page, object()))

    def test_snapshot_roundtrips_plain_page_report_view_progress_and_documents(self):
        public_truss = ModuleType("truss")
        public_truss.__path__ = []
        accepted = ModuleType("truss.reader_catalog")
        accepted.AcceptedDocument = PublicAcceptedDocument
        with patch.dict("sys.modules", {"truss": public_truss,
                                        "truss.reader_catalog": accepted}):
            raw = encode_detached_source_snapshot(self.anchor, self.observation)
            snapshot = decode_detached_source_snapshot(raw)
        self.assertIs(type(snapshot), DetachedTrussSourceSnapshot)
        self.assertEqual(snapshot.source_page.page_content, self.page.content)
        self.assertEqual(snapshot.report_bytes, self.report_bytes)
        self.assertEqual(snapshot.view_bytes, self.view_bytes)
        self.assertEqual(snapshot.progress.cursor_bytes, self.prior)
        self.assertEqual(snapshot.documents[0].content, self.content)
        self.assertNotIsInstance(snapshot, ManagedCurrentTruss053SourceObservation)
        self.assertEqual(snapshot.current_catalog_evidence().truss_version,
                         CURRENT_TRUSS_053_FRESH_VERSION)

    def test_custody_digest_accepts_the_decoded_immutable_page_fields(self):
        public_truss = ModuleType("truss")
        public_truss.__path__ = []
        accepted = ModuleType("truss.reader_catalog")
        accepted.AcceptedDocument = PublicAcceptedDocument
        with patch.dict("sys.modules", {"truss": public_truss,
                                        "truss.reader_catalog": accepted}):
            snapshot = decode_detached_source_snapshot(
                encode_detached_source_snapshot(self.anchor, self.observation))
        digests = detached_source_custody_digests(snapshot)
        self.assertEqual(set(digests), {"source_registration", "consumer_registration",
            "cursor", "destination_registration", "ack_scope", "checkpoint",
            "current_page", "retained_page"})

    def test_snapshot_roundtrips_exact_054_runtime_pin(self):
        public_truss = ModuleType("truss")
        public_truss.__path__ = []
        accepted = ModuleType("truss.reader_catalog")
        accepted.AcceptedDocument = PublicAcceptedDocument
        evidence = replace(self.evidence, truss_version=CURRENT_TRUSS_054_FRESH_VERSION,
            runtime_wheel_sha256=CURRENT_TRUSS_054_FRESH_WHEEL_SHA256,
            runtime_manifest_sha256=CURRENT_TRUSS_054_FRESH_RUNTIME_MANIFEST_SHA256)
        observation = self.observation
        original_evidence = observation.catalog_evidence
        object.__setattr__(observation, "catalog_evidence", evidence)
        with patch.dict("sys.modules", {"truss": public_truss,
                                        "truss.reader_catalog": accepted}):
            try:
                snapshot = decode_detached_source_snapshot(
                    encode_detached_source_snapshot(self.anchor, observation))
            finally:
                object.__setattr__(observation, "catalog_evidence", original_evidence)
        self.assertEqual(snapshot.truss_version, CURRENT_TRUSS_054_FRESH_VERSION)
        self.assertEqual(snapshot.current_catalog_evidence().runtime_wheel_sha256,
                         CURRENT_TRUSS_054_FRESH_WHEEL_SHA256)

    def test_snapshot_roundtrips_exact_055_runtime_pin(self):
        public_truss = ModuleType("truss")
        public_truss.__path__ = []
        accepted = ModuleType("truss.reader_catalog")
        accepted.AcceptedDocument = PublicAcceptedDocument
        evidence = replace(self.evidence, truss_version=CURRENT_TRUSS_055_FRESH_VERSION,
            runtime_wheel_sha256=CURRENT_TRUSS_055_FRESH_WHEEL_SHA256,
            runtime_manifest_sha256=CURRENT_TRUSS_055_FRESH_RUNTIME_MANIFEST_SHA256)
        observation = self.observation
        original_evidence = observation.catalog_evidence
        object.__setattr__(observation, "catalog_evidence", evidence)
        with patch.dict("sys.modules", {"truss": public_truss,
                                        "truss.reader_catalog": accepted}):
            try:
                snapshot = decode_detached_source_snapshot(
                    encode_detached_source_snapshot(self.anchor, observation))
            finally:
                object.__setattr__(observation, "catalog_evidence", original_evidence)
        self.assertEqual(snapshot.truss_version, CURRENT_TRUSS_055_FRESH_VERSION)
        self.assertEqual(snapshot.current_catalog_evidence().runtime_wheel_sha256,
                         CURRENT_TRUSS_055_FRESH_WHEEL_SHA256)

    def test_encoder_refuses_document_registration_or_revision_drift(self):
        public_truss = ModuleType("truss")
        public_truss.__path__ = []
        accepted = ModuleType("truss.reader_catalog")
        accepted.AcceptedDocument = PublicAcceptedDocument
        changed = PublicAcceptedDocument(**{
            **self.observation.accepted_documents[self.document_id].__dict__,
            "registration_id": "other-registration"})
        forged_observation = _issue_source_observation(
            ManagedCurrentTruss053SourceObservation(
                "A", self.progress, self.report_bytes, self.view_bytes,
                {self.document_id: changed}, {}, self.evidence, self.page, object()))
        with patch.dict("sys.modules", {"truss": public_truss,
                                        "truss.reader_catalog": accepted}):
            with self.assertRaisesRegex(DetachedSourceSnapshotError,
                                        "Public accepted-document registration"):
                encode_detached_source_snapshot(self.anchor, forged_observation)

    def test_encoder_refuses_copied_unissued_observation(self):
        public_truss = ModuleType("truss")
        public_truss.__path__ = []
        accepted = ModuleType("truss.reader_catalog")
        accepted.AcceptedDocument = PublicAcceptedDocument
        copied = replace(self.observation)
        with patch.dict("sys.modules", {"truss": public_truss,
                                        "truss.reader_catalog": accepted}):
            with self.assertRaisesRegex(DetachedSourceSnapshotError,
                                        "Issued public fresh Truss source observation"):
                encode_detached_source_snapshot(self.anchor, copied)

    def test_decoder_refuses_document_bytes_even_with_recomputed_outer_digest(self):
        public_truss = ModuleType("truss")
        public_truss.__path__ = []
        accepted = ModuleType("truss.reader_catalog")
        accepted.AcceptedDocument = PublicAcceptedDocument
        with patch.dict("sys.modules", {"truss": public_truss,
                                        "truss.reader_catalog": accepted}):
            value = json.loads(encode_detached_source_snapshot(self.anchor, self.observation))
        value["documents"][0]["content_base64"] = "Y2hhbmdlZA=="
        value.pop("binding_sha256")
        value["binding_sha256"] = hashlib.sha256(canonical(value)).hexdigest()
        with self.assertRaisesRegex(DetachedSourceSnapshotError,
                                    "registration/revision/digest differs"):
            decode_detached_source_snapshot(canonical(value))


if __name__ == "__main__":
    unittest.main()

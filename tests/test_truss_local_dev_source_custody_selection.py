# SPDX-License-Identifier: MIT OR Apache-2.0
"""Local-development selection tests; no Truss install or source ACK occurs."""
import base64
import hashlib
import json
import sys
import types
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from ashlar.staging import batch_row
from ashlar.source_checkpoint import truss_checkpoint
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.publication_planning import truss_request_for
from ashlar_host.truss_source_custody_selection import (
    TrussSourceSelectionError,
    select_truss_local_dev_source_custody_anchor,
)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


class LocalDevSourceCustodySelectionTests(unittest.TestCase):
    def setUp(self):
        self.feed = "ashlar-local-dev-typed-graph-r21-20261101"
        self.types = self._types()
        self.consumer_id = "ashlar-local-dev-typed-graph-consumer-r21-20261101"
        self.consumer_registration = "consumer-registration-r21"
        self.installation_id = "installation-r21"
        self.epoch = "epoch-r21"
        prior = {"profile": "truss-caller-asserted-feed/0.1",
            "installationId": self.installation_id, "sourceEpoch": self.epoch,
            "consumerId": self.consumer_id,
            "registrationId": self.consumer_registration, "nextXid": "1"}
        following = {**prior, "nextXid": "43"}
        self.prior = canonical(prior).encode()
        self.next = canonical(following).encode()
        self.content = canonical({"interfaceVersion": "truss-native-feed/0.1",
            "prior": prior, "next": following, "writerXid": "42",
            "catalogPrerequisites": [], "revisions": [], "changes": [],
            "sources": [], "reservations": [], "configuration": {}}).encode()
        # The actual SDK carrier is exact-type checked by the production binder.
        self.page = self.types.FeedPage(**{
            "content": self.content,
            "sha256": hashlib.sha256(self.content).hexdigest(),
            "prior": SimpleNamespace(content=self.prior),
            "next": SimpleNamespace(content=self.next),
            "consumer_id": self.consumer_id,
            "registration_id": self.consumer_registration,
            "writer_xid": "42", "safe_watermark_xid": "43",
            "outcome": "transaction"})
        batch = native_feed_batch(self.content, sha256=self.page.sha256,
            prior=self.prior, next=self.next, feed=self.feed)
        request = truss_request_for("r21-local-development", batch,
            "explicit-truss-genesis", {"source": "r21"})
        self.request_bytes = canonical(request).encode()
        report = {"registrationId": "acceptance-registration-r21", "revision": 3,
            "acceptedInput": {"documents": [{"documentId": "typed-graph-r21",
                "documentRevision": "1.0.0", "artifact": {
                    "identity": "model-graph-r21",
                    "sha256": hashlib.sha256(b"model bytes").hexdigest(),
                    "bytesBase64": base64.b64encode(b"model bytes").decode()}}]}}
        self.registration = self.types.CatalogRegistration(
            3, "acceptance-registration-r21", canonical(report).encode(), False)
        self.consumer = self.types.FeedConsumer(self.consumer_id,
            self.consumer_registration, SimpleNamespace(content=self.prior))
        self.status = self.types.CatalogRuntimeStatus()
        self.status.installed = True
        self.status.installation_id = self.installation_id
        self.status.release_id = "truss-catalog-runtime/0.75-candidate"
        self.status.release_sha256 = "5ecb599f65d64e6656bf6c83c30cea6e2aefd488cb3fe821b749458d5e9b3522"

    @staticmethod
    def _types():
        t = SimpleNamespace()
        t.CatalogRuntimeStatus = type("CatalogRuntimeStatus", (), {})
        t.CatalogRegistration = type("CatalogRegistration", (), {
            "__init__": lambda self, revision, registration_id, report_bytes, replayed:
                self.__dict__.update(revision=revision, registration_id=registration_id,
                                     report_bytes=report_bytes, replayed=replayed)})
        t.FeedConsumer = type("FeedConsumer", (), {
            "__init__": lambda self, consumer_id, registration_id, checkpoint:
                self.__dict__.update(consumer_id=consumer_id, registration_id=registration_id,
                                     checkpoint=checkpoint)})
        t.FeedPage = type("FeedPage", (), {
            "__init__": lambda self, **kwargs: self.__dict__.update(kwargs)})
        return t

    def _modules(self):
        root = types.ModuleType("truss")
        root.CatalogRuntimeStatus = self.types.CatalogRuntimeStatus
        catalog = types.ModuleType("truss.catalog_results")
        catalog.CatalogRegistration = self.types.CatalogRegistration
        feed = types.ModuleType("truss.feed")
        feed.FeedConsumer = self.types.FeedConsumer
        feed.FeedPage = self.types.FeedPage
        return {"truss": root, "truss.catalog_results": catalog, "truss.feed": feed}

    def anchor(self, *, page=None, consumer=None):
        return select_truss_local_dev_source_custody_anchor(
            registration=self.registration,
            consumer=self.consumer if consumer is None else consumer,
            runtime_status=self.status, page=self.page if page is None else page,
            feed=self.feed,
            source_holder_id="ashlar-local-dev-source-holder-r21-20261101",
            document_id="typed-graph-r21",
            destination_id="ashlar-local-dev-delta-r21-20261101",
            ack_registration_id="ashlar-local-dev-ack-registration-r21",
            ack_scope_id="ashlar-local-dev-ack-scope-r21",
            publication_id="publication-r21", request_bytes=self.request_bytes,
            predecessor_id="explicit-truss-genesis")

    def test_binds_public_registration_genesis_page_and_one_page_ack_scope(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        registration = json.loads(anchor.destination_registration_bytes)
        scope = json.loads(anchor.ack_scope_bytes)
        self.assertEqual(anchor.authority_profile, "local-development")
        self.assertEqual(registration["authority"], "local-development")
        self.assertEqual(registration["acceptanceRegistrationId"], self.registration.registration_id)
        self.assertEqual(registration["consumerRegistrationId"], self.page.registration_id)
        self.assertEqual(registration["destinationId"], "ashlar-local-dev-delta-r21-20261101")
        self.assertEqual(registration["ackRegistrationId"], "ashlar-local-dev-ack-registration-r21")
        self.assertEqual(registration["ackScopeId"], "ashlar-local-dev-ack-scope-r21")
        self.assertEqual(scope["scopeId"], "ashlar-local-dev-ack-scope-r21")
        self.assertEqual(scope["publicationId"], "publication-r21")
        self.assertEqual(scope["maximumPages"], 1)
        self.assertEqual(scope["priorCursorSha256"], hashlib.sha256(self.prior).hexdigest())

    def test_refuses_public_consumer_registration_drift(self):
        foreign = self.types.FeedConsumer(self.consumer_id, "foreign-registration",
            SimpleNamespace(content=self.prior))
        with patch.dict(sys.modules, self._modules()):
            with self.assertRaisesRegex(TrussSourceSelectionError, "correspondence"):
                self.anchor(consumer=foreign)

    def test_refuses_duplicate_members_in_public_acceptance_report(self):
        self.registration.report_bytes = (
            b'{"registrationId":"acceptance-registration-r21",'
            b'"registrationId":"forged-registration","revision":3,'
            b'"acceptedInput":{"documents":[]}}')
        with patch.dict(sys.modules, self._modules()):
            with self.assertRaisesRegex(TrussSourceSelectionError, "correspondence"):
                self.anchor()

    def test_refuses_forged_registration_or_ack_scope_bytes(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        with self.assertRaisesRegex(TrussSourceSelectionError, "validated source selector"):
            replace(anchor, destination_registration_bytes=b"{}").bind_publication(
                manifest_bytes=b"{}", vector=None, receipt_bytes=b"", observation=None)
        with self.assertRaisesRegex(TrussSourceSelectionError, "validated source selector"):
            replace(anchor, ack_scope_bytes=b"{}").bind_publication(
                manifest_bytes=b"{}", vector=None, receipt_bytes=b"", observation=None)


if __name__ == "__main__":
    unittest.main()

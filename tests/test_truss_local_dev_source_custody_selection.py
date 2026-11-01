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
from ashlar.pins import PinVector
from ashlar.source_checkpoint import truss_checkpoint
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.publication_planning import truss_request_for
from ashlar_host.truss_source_custody_selection import (
    LOCAL_NATIVE_OBSERVATION_PROFILE, LocalNativeDeltaPublicationObservation,
    LocalNativeDeltaRoleObservation, TrussSourceSelectionError,
    _mint_expected_native_post_state, _issue_local_native_delta_publication_observation,
    local_native_row_bag_summary,
    select_truss_local_dev_source_custody_anchor,
)
from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt


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
        self.publication_id = "truss-graph-" + request["request_digest"]
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

    def anchor(self, *, page=None, consumer=None, publication_id=None):
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
            publication_id=self.publication_id if publication_id is None else publication_id,
            request_bytes=self.request_bytes,
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
        self.assertEqual(scope["publicationId"], self.publication_id)
        self.assertEqual(scope["maximumPages"], 1)
        self.assertEqual(scope["priorCursorSha256"], hashlib.sha256(self.prior).hexdigest())

    def test_refuses_public_consumer_registration_drift(self):
        foreign = self.types.FeedConsumer(self.consumer_id, "foreign-registration",
            SimpleNamespace(content=self.prior))
        with patch.dict(sys.modules, self._modules()):
            with self.assertRaisesRegex(TrussSourceSelectionError, "correspondence"):
                self.anchor(consumer=foreign)

    def test_refuses_publication_id_not_derived_from_request(self):
        with patch.dict(sys.modules, self._modules()):
            with self.assertRaisesRegex(TrussSourceSelectionError, "local-development source registration"):
                self.anchor(publication_id="caller-selected-foreign-publication")

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

    def _local_publication_evidence(self):
        roles = ("object_current", "edge_current", "tombstone", "whole_source_history")
        table_versions = {f"local.trussgraph.{role}": 0 for role in roles}
        manifest = {"publication_id": self.publication_id,
                    "profile_version": "ashlar-delta/0.4-xxhash64",
                    "table_versions_json": canonical(table_versions)}
        manifest_bytes = canonical(manifest).encode()
        targets = {f"local.trussgraph.{role}": (f"uuid-{role}", 0) for role in roles}
        vector = PinVector("private-local-process", "manifest", self.publication_id,
            hashlib.sha256(manifest_bytes).hexdigest(), targets)
        receipt = encode_truss_destination_receipt(self.request_bytes, manifest_bytes, vector)
        following = {role: ([{"role": role, "value": "present"}] if role != "tombstone" else [])
                     for role in roles}
        expected = _mint_expected_native_post_state(
            publication_id=self.publication_id,
            request_digest=json.loads(self.request_bytes)["request_digest"],
            following=following, carrier_profile="ashlar-delta/0.4-xxhash64")
        native_roles = {}
        for role in roles:
            table = f"local.trussgraph.{role}"
            digest = hashlib.sha256(("delta-log-" + role).encode()).hexdigest()
            bag = local_native_row_bag_summary(following[role])
            native_roles[role] = LocalNativeDeltaRoleObservation(
                role=role, table=table, uuid=f"uuid-{role}", version=0,
                head_version=0, head_json_sha256=digest,
                log_history=((0, digest),), active_files=1,
                physical_parquet_files=1, row_bag_sha256=bag["row_bag_sha256"],
                row_count=bag["row_count"])
        observation = _issue_local_native_delta_publication_observation(
            publication_id=self.publication_id,
            manifest_bytes=manifest_bytes,
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            vector=vector, receipt_bytes=receipt,
            receipt_sha256=hashlib.sha256(receipt).hexdigest(), roles=native_roles)
        return manifest_bytes, vector, receipt, expected, observation, following

    def test_local_native_publication_binds_exact_manifest_vector_and_role_bags(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        manifest, vector, receipt, expected, observation, _ = self._local_publication_evidence()
        selection = anchor.bind_local_native_publication(manifest_bytes=manifest,
            vector=vector, receipt_bytes=receipt, expected_post_state=expected,
            observation=observation)
        self.assertEqual(selection.authority_profile, "local-development")
        self.assertEqual(selection.publication_id, self.publication_id)

    def test_local_native_publication_refuses_row_bag_or_log_drift(self):
        from dataclasses import replace
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        manifest, vector, receipt, expected, observation, _ = self._local_publication_evidence()
        changed = dict(observation.roles)
        changed["edge_current"] = replace(changed["edge_current"], row_bag_sha256="0" * 64)
        bad_bag = _issue_local_native_delta_publication_observation(
            publication_id=observation.publication_id,
            manifest_bytes=observation.manifest_bytes,
            manifest_sha256=observation.manifest_sha256, vector=observation.vector,
            receipt_bytes=observation.receipt_bytes,
            receipt_sha256=observation.receipt_sha256, roles=changed)
        with self.assertRaisesRegex(TrussSourceSelectionError, "row bag"):
            anchor.bind_local_native_publication(manifest_bytes=manifest, vector=vector,
                receipt_bytes=receipt, expected_post_state=expected, observation=bad_bag)
        changed = dict(observation.roles)
        changed["edge_current"] = replace(changed["edge_current"], log_history=((1, "a" * 64),))
        bad_log = _issue_local_native_delta_publication_observation(
            publication_id=observation.publication_id,
            manifest_bytes=observation.manifest_bytes,
            manifest_sha256=observation.manifest_sha256, vector=observation.vector,
            receipt_bytes=observation.receipt_bytes,
            receipt_sha256=observation.receipt_sha256, roles=changed)
        with self.assertRaisesRegex(TrussSourceSelectionError, "Delta log version"):
            anchor.bind_local_native_publication(manifest_bytes=manifest, vector=vector,
                receipt_bytes=receipt, expected_post_state=expected, observation=bad_log)

    def test_local_native_publication_refuses_unissued_observation(self):
        from dataclasses import replace
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        manifest, vector, receipt, expected, observation, _ = self._local_publication_evidence()
        forged = replace(observation)
        with self.assertRaisesRegex(TrussSourceSelectionError, "native readback"):
            anchor.bind_local_native_publication(manifest_bytes=manifest, vector=vector,
                receipt_bytes=receipt, expected_post_state=expected, observation=forged)

    def test_local_native_publication_refuses_unissued_expected_post_state(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        manifest, vector, receipt, expected, observation, _ = self._local_publication_evidence()
        forged = replace(expected)
        with self.assertRaisesRegex(TrussSourceSelectionError, "publisher post-state"):
            anchor.bind_local_native_publication(manifest_bytes=manifest, vector=vector,
                receipt_bytes=receipt, expected_post_state=forged, observation=observation)

    def test_local_native_anchor_cannot_use_managed_r04_binder(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        with self.assertRaisesRegex(TrussSourceSelectionError, "limited to the scheduled R04"):
            anchor.bind_publication(manifest_bytes=b"{}", vector=None,
                receipt_bytes=b"x", observation=None)


if __name__ == "__main__":
    unittest.main()

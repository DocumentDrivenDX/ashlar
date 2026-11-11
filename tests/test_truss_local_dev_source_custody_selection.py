# SPDX-License-Identifier: MIT OR Apache-2.0
"""Local-development selection tests; no Truss install or source ACK occurs."""
import hashlib
import json
import tempfile
import sys
import types
import unittest
from dataclasses import dataclass, replace
from types import SimpleNamespace
from pathlib import Path
from types import MappingProxyType
from unittest.mock import patch

from ashlar.staging import batch_row
from ashlar.pins import PinVector
from ashlar.source_checkpoint import truss_checkpoint
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.publication_planning import truss_request_for
from ashlar_host.truss_source_custody_selection import (
    TrussSourceSelectionError, _mint_expected_native_post_state,
    local_native_row_bag_summary,
    read_local_native_delta_publication_observation,
    select_truss_local_dev_source_custody_anchor,
)
from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt
from ashlar_host.delta_custody import DeltaTarget, LocalDeltaTransport


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


class _NativeFrame:
    def __init__(self, fields, rows):
        self.schema = SimpleNamespace(fields=fields)
        self._rows = rows
    def limit(self, _count):
        return self
    def collect(self):
        return [SimpleNamespace(asDict=lambda recursive=True, row=row: dict(row))
                for row in self._rows]


class _NativeReader:
    def __init__(self, owner):
        self.owner = owner
        self._version = None
    def format(self, _fmt):
        return self
    def option(self, key, value):
        if key == "versionAsOf":
            self._version = value
        return self
    def load(self, path):
        self.owner.read_versions.append((path, self._version))
        role = Path(path).name
        rows = self.owner.rows[role]
        names = tuple(rows[0]) if rows else ("role", "value")
        fields = [SimpleNamespace(name=name,
            dataType=SimpleNamespace(simpleString=lambda: "string")) for name in names]
        return _NativeFrame(fields, rows)


class _NativeSparkMock:
    def __init__(self, rows, paths):
        self.rows = rows
        self.paths = paths
        self.read_versions = []
        self.read = _NativeReader(self)
    def sql(self, statement):
        root = statement.split("delta.`", 1)[1].rsplit("`", 1)[0]
        role = Path(root).name
        row = {"id": f"uuid-{role}"}
        return _NativeFrame([SimpleNamespace(name="id",
            dataType=SimpleNamespace(simpleString=lambda: "string"))], [row])


class LocalDevSourceCustodySelectionTests(unittest.TestCase):
    def setUp(self):
        self.native_tmp = tempfile.TemporaryDirectory()
        targets = {}
        for role in ("object_current", "edge_current", "tombstone", "whole_source_history",
                     "attempts", "manifest"):
            table = f"local.trussgraph.{role}"
            path = Path(self.native_tmp.name) / role
            path.mkdir()
            targets[table] = DeltaTarget(table, path, f"uuid-{role}")
        self.transport = LocalDeltaTransport.__new__(LocalDeltaTransport)
        self.transport.targets = MappingProxyType(targets)
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
        # This compact fixture is projected from the exact public Truss 0.0.52
        # report retained in the R24/R33 reconciliation, with source hash kept.
        fixture_path = Path(__file__).parent / "fixtures/truss_052_r24_public_report_document.json"
        fixture = json.loads(fixture_path.read_bytes())
        self.document_id = fixture["document"]["documentId"]
        model_path = (Path(__file__).parents[1] / "tests/fixtures/golden/"
            "current-upstream-release-20261101/r21-truss052-fresh-source-custody/"
            "r28-page-sequence/r28-model.umf.json")
        model_bytes = model_path.read_bytes()
        report = {"registrationId": fixture["registrationId"], "revision": fixture["revision"],
            "acceptedInput": {"documents": [fixture["document"]]}}
        self.registration = self.types.CatalogRegistration(
            int(fixture["revision"]), fixture["registrationId"], canonical(report).encode(), False)
        self.accepted_document = self.types.AcceptedDocument(
            document_id=fixture["document"]["documentId"],
            document_revision=fixture["document"]["documentRevision"],
            artifact_identity=fixture["document"]["artifact"]["identity"], content=model_bytes,
            sha256=hashlib.sha256(model_bytes).hexdigest(),
            catalog_revision=int(fixture["revision"]), registration_id=fixture["registrationId"],
            source_catalog_revision=int(fixture["revision"]),
            validation_bytes=None, ddd_profile_bytes=None, ddd_validation_bytes=None)
        self.consumer = self.types.FeedConsumer(self.consumer_id,
            self.consumer_registration, SimpleNamespace(content=self.prior))
        self.status = self.types.CatalogRuntimeStatus()
        self.status.installed = True
        self.status.installation_id = self.installation_id
        self.status.release_id = "truss-catalog-runtime/0.75-candidate"
        self.status.release_sha256 = "5ecb599f65d64e6656bf6c83c30cea6e2aefd488cb3fe821b749458d5e9b3522"

    def tearDown(self):
        self.native_tmp.cleanup()

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
        @dataclass(frozen=True)
        class AcceptedDocument:
            document_id: str
            document_revision: str
            artifact_identity: str
            content: bytes
            sha256: str
            catalog_revision: int
            registration_id: str
            source_catalog_revision: int
            validation_bytes: bytes | None = None
            ddd_profile_bytes: bytes | None = None
            ddd_validation_bytes: bytes | None = None
        t.AcceptedDocument = AcceptedDocument
        return t

    def _modules(self):
        root = types.ModuleType("truss")
        root.CatalogRuntimeStatus = self.types.CatalogRuntimeStatus
        catalog = types.ModuleType("truss.catalog_results")
        catalog.CatalogRegistration = self.types.CatalogRegistration
        feed = types.ModuleType("truss.feed")
        feed.FeedConsumer = self.types.FeedConsumer
        feed.FeedPage = self.types.FeedPage
        reader = types.ModuleType("truss.reader_catalog")
        reader.AcceptedDocument = self.types.AcceptedDocument
        return {"truss": root, "truss.catalog_results": catalog,
                "truss.feed": feed, "truss.reader_catalog": reader}

    def anchor(self, *, page=None, consumer=None, publication_id=None):
        return select_truss_local_dev_source_custody_anchor(
            registration=self.registration,
            accepted_document=self.accepted_document,
            consumer=self.consumer if consumer is None else consumer,
            runtime_status=self.status, page=self.page if page is None else page,
            feed=self.feed,
            source_holder_id="ashlar-local-dev-source-holder-r21-20261101",
            document_id=self.document_id,
            destination_id="ashlar-local-dev-delta-r21-20261101",
            ack_registration_id="ashlar-local-dev-ack-registration-r21",
            ack_scope_id="ashlar-local-dev-ack-scope-r21",
            publication_id=self.publication_id if publication_id is None else publication_id,
            request_bytes=self.request_bytes,
            predecessor_id="explicit-truss-genesis",
            destination_transport=self.transport)

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

    def test_selector_requires_exact_six_role_installation_but_binds_four_data_roles(self):
        selected = self.transport.targets
        with patch.dict(sys.modules, self._modules()):
            self.transport.targets = MappingProxyType({
                table: target for table, target in selected.items()
                if table.rsplit(".", 1)[-1] in {
                    "object_current", "edge_current", "tombstone", "whole_source_history"}})
            with self.assertRaisesRegex(TrussSourceSelectionError, "six-role"):
                self.anchor()
            extra_path = Path(self.native_tmp.name) / "unexpected"
            extra_path.mkdir()
            extra = dict(selected)
            extra["local.trussgraph.unexpected"] = DeltaTarget(
                "local.trussgraph.unexpected", extra_path, "uuid-unexpected")
            self.transport.targets = MappingProxyType(extra)
            with self.assertRaisesRegex(TrussSourceSelectionError, "six-role"):
                self.anchor()
            self.transport.targets = selected
            anchor = self.anchor()
        self.assertEqual(set(anchor.destination_target_bindings), {
            "local.trussgraph.object_current", "local.trussgraph.edge_current",
            "local.trussgraph.tombstone", "local.trussgraph.whole_source_history"})

    def test_refuses_publication_id_not_derived_from_request(self):
        with patch.dict(sys.modules, self._modules()):
            with self.assertRaisesRegex(TrussSourceSelectionError, "local-development source registration"):
                self.anchor(publication_id="caller-selected-foreign-publication")

    def test_refuses_duplicate_members_in_public_acceptance_report(self):
        self.registration.report_bytes = (
            b'{"registrationId":"4356b0ee-2d21-4a39-9182-b82c66dffd97",'
            b'"registrationId":"forged-registration","revision":"3",'
            b'"acceptedInput":{"documents":[]}}')
        with patch.dict(sys.modules, self._modules()):
            with self.assertRaisesRegex(TrussSourceSelectionError, "correspondence"):
                self.anchor()

    def test_binds_report_to_typed_public_accepted_document_without_embedded_bytes(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        registration = json.loads(self.registration.report_bytes)
        selected = registration["acceptedInput"]["documents"][0]
        self.assertNotIn("bytesBase64", selected["artifact"])
        self.assertEqual(anchor.identity["artifact_identity"], self.accepted_document.artifact_identity)
        self.assertEqual(anchor.identity["model_sha256"], self.accepted_document.sha256)

    def test_refuses_mismatched_public_accepted_document_fields(self):
        mismatches = (
            replace(self.accepted_document, document_id="other"),
            replace(self.accepted_document, document_revision="2"),
            replace(self.accepted_document, artifact_identity="other"),
            replace(self.accepted_document, content=b"other bytes"),
            replace(self.accepted_document, sha256="0" * 64),
            replace(self.accepted_document, catalog_revision=2),
            replace(self.accepted_document, registration_id="other"),
            replace(self.accepted_document, source_catalog_revision=4),
        )
        for accepted_document in mismatches:
            with self.subTest(accepted_document=accepted_document):
                self.accepted_document = accepted_document
                with patch.dict(sys.modules, self._modules()):
                    with self.assertRaisesRegex(TrussSourceSelectionError,
                            "local-development source registration/page/request correspondence"):
                        self.anchor()
        self.accepted_document = replace(
            self.accepted_document, document_id=self.document_id,
            document_revision="1", artifact_identity=self.document_id,
            content=(Path(__file__).parents[1] / "tests/fixtures/golden/"
                "current-upstream-release-20261101/r21-truss052-fresh-source-custody/"
                "r28-page-sequence/r28-model.umf.json").read_bytes(),
            sha256="b908c0dd6a0da65c864c0debd0c793d8ab06a933546f34050480909c5c9349f2",
            catalog_revision=1, registration_id=self.registration.registration_id,
            source_catalog_revision=1)

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
        for role in roles:
            table = f"local.trussgraph.{role}"
            target = self.transport.targets[table].path
            (target / "_delta_log").mkdir(parents=True)
            (target / "data").mkdir()
            (target / "data" / f"{role}.parquet").write_bytes(b"mock parquet")
            lines = [json.dumps({"metaData": {"id": f"uuid-{role}"}}, separators=(",", ":"))]
            if following[role]:
                lines.append(json.dumps({"add": {"path": f"data/{role}.parquet"}}, separators=(",", ":")))
            (target / "_delta_log" / f"{0:020d}.json").write_text("\n".join(lines) + "\n")
        fake_spark = _NativeSparkMock(following, {})
        self.transport.spark = fake_spark
        return manifest_bytes, vector, receipt, expected, following, fake_spark

    def test_local_native_publication_binds_exact_manifest_vector_and_role_bags(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        manifest, vector, receipt, expected, _, spark = self._local_publication_evidence()
        observation = read_local_native_delta_publication_observation(anchor=anchor,
            spark=spark, destination_transport=self.transport,
            manifest_bytes=manifest, vector=vector,
            receipt_bytes=receipt, expected_post_state=expected)
        self.assertEqual(set(spark.read_versions), {
            (str(self.transport.targets[f"local.trussgraph.{role}"].path), 0)
            for role in ("object_current", "edge_current", "tombstone", "whole_source_history")})
        selection = anchor.bind_local_native_publication(manifest_bytes=manifest,
            vector=vector, receipt_bytes=receipt, expected_post_state=expected,
            observation=observation)
        self.assertEqual(selection.authority_profile, "local-development")
        self.assertEqual(selection.publication_id, self.publication_id)

    def test_local_native_factory_refuses_row_bag_or_log_drift(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        manifest, vector, receipt, expected, following, spark = self._local_publication_evidence()
        path = self.transport.targets["local.trussgraph.edge_current"].path / "_delta_log" / f"{0:020d}.json"
        path.unlink()
        with self.assertRaisesRegex(TrussSourceSelectionError, "current contiguous log head"):
            read_local_native_delta_publication_observation(anchor=anchor, spark=spark,
                destination_transport=self.transport, manifest_bytes=manifest, vector=vector,
                receipt_bytes=receipt, expected_post_state=expected)
        path.write_text(json.dumps({"metaData": {"id": "uuid-edge_current"}}) + "\n"
                        + json.dumps({"add": {"path": "data/edge_current.parquet"}}) + "\n")
        later = path.with_name(f"{1:020d}.json")
        later.write_text(json.dumps({"commitInfo": {"operation": "WRITE"}}) + "\n")
        with self.assertRaisesRegex(TrussSourceSelectionError, "current contiguous log head"):
            read_local_native_delta_publication_observation(anchor=anchor, spark=spark,
                destination_transport=self.transport, manifest_bytes=manifest, vector=vector,
                receipt_bytes=receipt, expected_post_state=expected)
        later.unlink()
        spark.rows["edge_current"] = [{"role": "edge_current", "value": "forged"}]
        with self.assertRaisesRegex(TrussSourceSelectionError, "row bag"):
            read_local_native_delta_publication_observation(anchor=anchor, spark=spark,
                destination_transport=self.transport, manifest_bytes=manifest, vector=vector,
                receipt_bytes=receipt, expected_post_state=expected)

    def test_local_native_publication_refuses_unissued_observation(self):
        from dataclasses import replace
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        manifest, vector, receipt, expected, _, spark = self._local_publication_evidence()
        observation = read_local_native_delta_publication_observation(anchor=anchor,
            spark=spark, destination_transport=self.transport,
            manifest_bytes=manifest, vector=vector,
            receipt_bytes=receipt, expected_post_state=expected)
        forged = replace(observation)
        with self.assertRaisesRegex(TrussSourceSelectionError, "native readback"):
            anchor.bind_local_native_publication(manifest_bytes=manifest, vector=vector,
                receipt_bytes=receipt, expected_post_state=expected, observation=forged)

    def test_local_native_factory_refuses_replaced_transport_target(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        manifest, vector, receipt, expected, _, spark = self._local_publication_evidence()
        old_targets = self.transport.targets
        changed = dict(old_targets)
        table = "local.trussgraph.edge_current"
        rogue_path = Path(self.native_tmp.name) / "rogue-edge"
        rogue_path.mkdir()
        changed[table] = DeltaTarget(table, rogue_path, old_targets[table].uuid)
        self.transport.targets = MappingProxyType(changed)
        try:
            with self.assertRaisesRegex(TrussSourceSelectionError, "registry differs"):
                read_local_native_delta_publication_observation(anchor=anchor, spark=spark,
                    destination_transport=self.transport, manifest_bytes=manifest,
                    vector=vector, receipt_bytes=receipt, expected_post_state=expected)
        finally:
            self.transport.targets = old_targets

    def test_local_native_factory_refuses_replaced_auxiliary_installation_target(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        manifest, vector, receipt, expected, _, spark = self._local_publication_evidence()
        old_targets = self.transport.targets
        changed = dict(old_targets)
        table = "local.trussgraph.manifest"
        changed[table] = DeltaTarget(table, old_targets[table].path, "replaced-manifest-uuid")
        self.transport.targets = MappingProxyType(changed)
        try:
            with self.assertRaisesRegex(TrussSourceSelectionError, "installation registry differs"):
                read_local_native_delta_publication_observation(anchor=anchor, spark=spark,
                    destination_transport=self.transport, manifest_bytes=manifest,
                    vector=vector, receipt_bytes=receipt, expected_post_state=expected)
        finally:
            self.transport.targets = old_targets

    def test_local_native_publication_refuses_unissued_expected_post_state(self):
        with patch.dict(sys.modules, self._modules()):
            anchor = self.anchor()
        manifest, vector, receipt, expected, _, spark = self._local_publication_evidence()
        observation = read_local_native_delta_publication_observation(anchor=anchor,
            spark=spark, destination_transport=self.transport,
            manifest_bytes=manifest, vector=vector,
            receipt_bytes=receipt, expected_post_state=expected)
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

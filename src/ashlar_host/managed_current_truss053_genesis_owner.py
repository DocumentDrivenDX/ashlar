# SPDX-License-Identifier: MIT OR Apache-2.0
"""Rollback-only Truss 0.0.53 source selection for managed-current A1 genesis.

The owner reads both independently selected source installations through the
public Truss API. The complete catalog view requires a row-lock-capable
transaction, so the owner adopts READ WRITE access and always rolls back rather
than committing. It returns original report, accepted-document, complete-view,
consumer-cursor and page bytes for the existing genesis and driver boundaries.
It does not register consumers, publish, hold ACK custody, or acknowledge a
page; this code establishes no committed source mutation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json


class ManagedCurrentTruss053GenesisError(RuntimeError):
    """A selected Truss genesis source could not be read exactly."""


def _need(value, message):
    if not value:
        raise ManagedCurrentTruss053GenesisError(message)


def _outcome(value, label):
    from truss import Error, Ok
    if isinstance(value, Ok):
        return value.value
    if isinstance(value, Error):
        error = value.error
        raise ManagedCurrentTruss053GenesisError(
            f"Public Truss {label} refused ({error.code}/{error.catalog_code})")
    raise ManagedCurrentTruss053GenesisError(f"Public Truss {label} returned an unknown result")


def _begin_public_catalog_transaction(connection):
    """Enable Truss catalog-view row locks in the first transaction statement."""
    connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE")


def _rollback_and_close(connection):
    """Attempt rollback and close independently; return every cleanup failure."""
    failures = []
    if not getattr(connection, "closed", False):
        try:
            connection.rollback()
        except BaseException as error:
            failures.append(error)
        try:
            connection.close()
        except BaseException as error:
            failures.append(error)
    return failures


@dataclass(frozen=True)
class ManagedCurrentTruss053SourceSelection:
    """External immutable selection paired with one R04 source binding."""
    source: str
    feed: str
    epoch: str
    installation_id: str
    consumer_id: str
    consumer_registration_id: str
    catalog_registration_id: str
    catalog_revision: str
    connection_factory: object = field(repr=False, compare=False)


@dataclass(frozen=True)
class ManagedCurrentTruss053SourceObservation:
    """Original public evidence read from one selected source installation."""
    source: str
    progress: object = field(repr=False, compare=False)
    report_bytes: bytes = field(repr=False)
    catalog_view_bytes: bytes = field(repr=False)
    accepted_documents: dict = field(repr=False, compare=False)
    model_evidence: dict = field(repr=False, compare=False)
    catalog_evidence: object = field(repr=False, compare=False)
    page: object = field(repr=False, compare=False)
    batch: object | None = field(repr=False, compare=False)


@dataclass(frozen=True)
class ManagedCurrentTruss053GenesisSelection:
    """Inputs ready for the existing manifest owner and first-A driver."""
    schedule_sha256: str
    schedule_bytes: bytes = field(repr=False)
    source_progress: dict
    selected_model_evidence: dict
    catalog_acceptance_revisions: dict
    schema_revisions: dict
    source_observations: dict = field(repr=False, compare=False)
    admission: object = field(repr=False, compare=False)
    source_owner_admitted_batch: object = field(repr=False, compare=False)


class ManagedCurrentTruss053GenesisSourceOwner:
    """Read selected A/B registrations, accepted catalogs, cursors and A1 page.

    The caller supplies independently selected connection factories and source
    identity tuples. Each source is read under one original rollback-only
    Truss connection. The R04 schedule binds feed/epoch/installation/consumer IDs;
    the separately selected catalog registration/revision and consumer
    registration IDs are checked against the public readbacks.
    """

    def __init__(self, schedule, selections):
        from .r04_schedule import R04Schedule
        _need(type(schedule) is R04Schedule, "Exact frozen original R04 schedule required")
        _need(type(selections) is dict and set(selections) == {"A", "B"}
              and all(type(item) is ManagedCurrentTruss053SourceSelection
                      for item in selections.values()),
              "Two separately selected Truss .52 A/B source owner inputs required")
        self.schedule = schedule
        self.selections = dict(selections)

    def _observe_source(self, source):
        from .managed_current_genesis import admit_truss_source_genesis_progress
        from .managed_current_truss_cursor import validate_selected_truss_cursor
        from .truss_current_catalog import (
            CURRENT_TRUSS_053_FRESH_VERSION,
            CURRENT_TRUSS_053_FRESH_WHEEL_SHA256,
            CURRENT_TRUSS_053_FRESH_RUNTIME_MANIFEST_SHA256,
            CurrentCatalogEvidence,
            installed_truss_053_fresh_source_custody_profile,
        )
        from ashlar.truss_native_feed import native_feed_batch

        selection = self.selections[source]
        _need(selection.source == source and all(
            type(value) is str and value and "\0" not in value for value in (
                selection.feed, selection.epoch, selection.installation_id,
                selection.consumer_id, selection.consumer_registration_id,
                selection.catalog_registration_id, selection.catalog_revision)),
            "Complete separately selected Truss source identities required")
        _need(type(selection.catalog_revision) is str and selection.catalog_revision.isdecimal()
              and str(int(selection.catalog_revision)) == selection.catalog_revision
              and int(selection.catalog_revision) > 0
              and callable(selection.connection_factory),
              "Canonical selected catalog revision and owner connection required")
        binding = self.schedule.document()["source_bindings"][source]
        _need(all(binding.get(key) == expected for key, expected in (
            ("feed", selection.feed), ("epoch", selection.epoch),
            ("installation_id", selection.installation_id),
            ("consumer_id", selection.consumer_id),
            ("registration_id", selection.consumer_registration_id))),
            "Selected source owner identities differ from frozen R04 binding")
        installed_truss_053_fresh_source_custody_profile()
        try:
            from truss import (CatalogReport, CatalogRuntimeConfiguration,
                CatalogRuntimeStatus, adopt_psycopg_host,
                create_reference_assembly)
            from truss.catalog_view import CatalogViewAvailable
            from truss.feed import FeedPage
            from truss.reader_catalog import AcceptedDocument
        except ImportError as error:
            raise ManagedCurrentTruss053GenesisError(
                "Pinned public Truss 0.0.53 source/catalog API required") from error

        connection = selection.connection_factory()
        if (getattr(connection, "autocommit", True)
                or getattr(getattr(connection, "info", None), "transaction_status", None) != 0):
            try:
                connection.close()
            finally:
                raise ManagedCurrentTruss053GenesisError(
                    "Fresh idle non-autocommit Truss owner connection required")
        host = assembly = None
        primary = None
        try:
            _begin_public_catalog_transaction(connection)
            connection.execute("SET LOCAL statement_timeout = '10000ms'")
            host = adopt_psycopg_host(connection, statement_timeout_ms=10000,
                                      operation_timeout_ms=30000)
            transaction = _outcome(host.adopt_transaction(access_mode="read_write"),
                                   "rollback-only source transaction")
            assembly = _outcome(create_reference_assembly(
                CatalogRuntimeConfiguration(), host), "reference assembly")
            status = _outcome(assembly.status_in_transaction(transaction), "runtime status")
            _need(type(status) is CatalogRuntimeStatus and status.installed is True
                  and status.installation_id == selection.installation_id,
                  "Selected installed Truss source identity required")

            revision_probe = _outcome(assembly.catalog_revision_in_transaction(transaction),
                                      "catalog revision")
            _need(getattr(revision_probe, "catalog_revision", None)
                  == selection.catalog_revision,
                  "Selected accepted catalog revision is not the current source revision")
            revision = int(selection.catalog_revision)
            report = _outcome(assembly.report_in_transaction(transaction, revision),
                              "accepted catalog report")
            _need(type(report) is CatalogReport and report.revision == revision
                  and type(report.original_bytes) is bytes and report.original_bytes,
                  "Exact original accepted Truss report bytes required")
            report_doc = json.loads(report.original_bytes)
            _need(type(report_doc) is dict
                  and report_doc.get("registrationId") == selection.catalog_registration_id
                  and report_doc.get("revision") == selection.catalog_revision,
                  "Public accepted report differs from selected catalog registration")

            view_request = assembly.catalog_view_request()
            view_result = _outcome(assembly.catalog_view_in_transaction(transaction,
                                                                          view_request),
                                   "complete accepted catalog view")
            _need(type(view_result) is CatalogViewAvailable
                  and view_result.view.catalog_revision == selection.catalog_revision,
                  "Exact selected complete accepted Truss catalog view required")
            view_bytes = view_result.view.original_bytes
            _need(type(view_bytes) is bytes and view_bytes,
                  "Exact original complete catalog view bytes required")

            accepted = report_doc.get("acceptedInput", {}).get("documents")
            _need(type(accepted) is list and bool(accepted),
                  "Original accepted source-document inventory required")
            accepted_by_id = {}
            for item in accepted:
                artifact = item.get("artifact") if type(item) is dict else None
                _need(type(item) is dict and type(item.get("documentId")) is str
                      and item["documentId"] and item["documentId"] not in accepted_by_id
                      and type(item.get("documentRevision")) is str
                      and item["documentRevision"] and type(artifact) is dict
                      and type(artifact.get("identity")) is str and artifact["identity"]
                      and type(artifact.get("sha256")) is str
                      and len(artifact["sha256"]) == 64,
                      "Closed accepted source-document identity and artifact pin required")
                accepted_by_id[item["documentId"]] = item
            accepted_documents = {}
            sources = {}
            for document_id, item in accepted_by_id.items():
                artifact = item["artifact"]
                document = _outcome(assembly.accepted_document_in_transaction(
                    transaction, document_id=document_id, maximum_bytes=16777216),
                    "accepted source document")
                _need(type(document) is AcceptedDocument
                      and document.document_id == document_id
                      and document.document_revision == item["documentRevision"]
                      and document.artifact_identity == artifact["identity"]
                      and document.sha256 == artifact["sha256"]
                      and document.registration_id == selection.catalog_registration_id
                      and str(document.catalog_revision) == selection.catalog_revision
                      and type(document.content) is bytes
                      and hashlib.sha256(document.content).hexdigest() == document.sha256,
                      "Exact original accepted source-document bytes required")
                accepted_documents[document_id] = document
                sources[(selection.catalog_revision, document_id)] = type("PublicSource", (), {
                    "document_id": document.document_id,
                    "document_revision": document.document_revision,
                    "artifact_identity": document.artifact_identity,
                    "registration_id": document.registration_id,
                    "sha256": document.sha256,
                    "content": document.content,
                    "catalog_revision": document.catalog_revision,
                })()

            from .truss_current_catalog import CurrentCatalogEvidence
            catalog_evidence = CurrentCatalogEvidence(
                CURRENT_TRUSS_053_FRESH_VERSION,
                {selection.catalog_revision: report.original_bytes},
                {selection.catalog_revision: view_bytes}, sources,
                CURRENT_TRUSS_053_FRESH_WHEEL_SHA256,
                CURRENT_TRUSS_053_FRESH_RUNTIME_MANIFEST_SHA256)

            page = _outcome(assembly.read_feed_in_transaction(
                transaction, consumer_id=selection.consumer_id,
                maximum_facts=4096, maximum_bytes=8388608), "selected source page")
            _need(type(page) is FeedPage and page.consumer_id == selection.consumer_id
                  and page.registration_id == selection.consumer_registration_id
                  and type(page.prior.content) is bytes and page.prior.content,
                  "Public source page and existing consumer registration required")
        except BaseException as error:
            primary = error
            raise
        finally:
            failures = []
            if assembly is not None:
                try:
                    assembly.dispose()
                except BaseException as error:
                    failures.append(error)
            if host is not None:
                try:
                    if not host.detach():
                        raise ManagedCurrentTruss053GenesisError(
                            "Truss owner host detach was not confirmed")
                except BaseException as error:
                    failures.append(error)
            failures.extend(_rollback_and_close(connection))
            if failures and primary is None:
                raise ManagedCurrentTruss053GenesisError(
                    "Truss source owner cleanup failed") from failures[0]

        identity = {"installation_id": selection.installation_id,
                    "epoch": selection.epoch,
                    "consumer_id": selection.consumer_id,
                    "registration_id": selection.consumer_registration_id}
        try:
            cursor = validate_selected_truss_cursor(page.prior.content,
                cursor_sha256=hashlib.sha256(page.prior.content).hexdigest(),
                selected_identity=identity)
        except ValueError as error:
            raise ManagedCurrentTruss053GenesisError(
                "Public current source cursor differs from selected A/B genesis identity") from error
        _need(cursor.next_xid == "0", "Both selected source cursors must remain at genesis")
        progress = admit_truss_source_genesis_progress(
            feed=selection.feed, epoch=selection.epoch,
            cursor_bytes=page.prior.content,
            cursor_sha256=hashlib.sha256(page.prior.content).hexdigest(),
            catalog_registration_id=selection.catalog_registration_id,
            catalog_registration_bytes=report.original_bytes,
            consumer_id=selection.consumer_id,
            consumer_registration_id=selection.consumer_registration_id,
            installation_id=selection.installation_id)

        pin = self.schedule.document()["accepted_model_pins"][source]
        matching = [document for document in accepted_documents.values()
                    if document.document_revision == pin.get("documentRevision")
                    and document.sha256 == pin.get("modelSha256")]
        _need(len(matching) == 1,
              "Exact original source model bytes selected by the R04 pin required")
        model = matching[0]
        model_evidence = {"pin": pin, "bytes": model.content}

        batch = None
        if source == "A":
            _need(page.outcome == "transaction",
                  "The first A1 source page must be an actual committed Truss transaction")
            try:
                batch = native_feed_batch(page.content, sha256=page.sha256,
                    prior=page.prior.content, next=page.next.content,
                    feed=selection.feed)
            except (TypeError, ValueError) as error:
                raise ManagedCurrentTruss053GenesisError(
                    "Exact public first A native page required") from error
            _need(batch.profile == "truss-native-feed/0.1"
                  and batch.feed == selection.feed and batch.epoch == selection.epoch
                  and batch.cursor_before == progress.position,
                  "First A native batch differs from its selected genesis cursor")
        return ManagedCurrentTruss053SourceObservation(source, progress,
            report.original_bytes, view_bytes, accepted_documents, model_evidence,
            catalog_evidence, page, batch)

    def admit_a1(self):
        """Read A/B evidence and return exact arguments for existing owners.

        The A page is semantically admitted against its original report/view
        before a driver can construct effects. No native or ACK action occurs.
        """
        observations = {source: self._observe_source(source) for source in ("A", "B")}
        _need(len({item.progress.feed for item in observations.values()}) == 2
              and len({item.progress.installation_id for item in observations.values()}) == 2
              and len({item.progress.catalog_registration_id for item in observations.values()}) == 2
              and len({item.progress.consumer_registration_id for item in observations.values()}) == 2,
              "Selected A/B source, catalog and consumer registrations must be distinct")
        from .truss_graph_admission import TrussGraphAdmission
        admission = TrussGraphAdmission(observations["A"].batch,
                                        observations["A"].catalog_evidence)
        return ManagedCurrentTruss053GenesisSelection(
            self.schedule.sha256,
            self.schedule.raw,
            {source: item.progress for source, item in observations.items()},
            {source: item.model_evidence for source, item in observations.items()},
            {source: {"installation_id": item.progress.installation_id,
                      "revision": self.selections[source].catalog_revision,
                      "report_bytes": item.report_bytes,
                      "catalog_bytes": item.catalog_view_bytes}
             for source, item in observations.items()},
            {item.progress.installation_id: self.selections[source].catalog_revision
             for source, item in observations.items()},
            observations, admission, observations["A"].batch)

    @staticmethod
    def manifest_owner_arguments(selection, *, recorded_at, context):
        _need(type(selection) is ManagedCurrentTruss053GenesisSelection,
              "Exact admitted A1 genesis source selection required")
        return {"schedule_bytes": selection.schedule_bytes,
                "source_progress": selection.source_progress,
                "selected_model_evidence": selection.selected_model_evidence,
                "catalog_acceptance_revisions": selection.catalog_acceptance_revisions,
                "schema_revisions": selection.schema_revisions,
                "recorded_at": recorded_at, "context": context}

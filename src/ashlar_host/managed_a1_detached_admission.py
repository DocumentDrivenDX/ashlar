# SPDX-License-Identifier: MIT OR Apache-2.0
"""Transfer exact detached Truss A/B genesis evidence into one A1 plan.

This adapter revalidates public evidence bytes and creates only the ordinary
publisher plan.  It never calls a Truss issuer, opens source custody, or grants
ACK authority.  The original source-holder child remains the sole owner of
those capabilities.
"""
from __future__ import annotations

import base64
import hashlib
import json
from types import SimpleNamespace


def _need(value, message):
    if not value:
        raise ValueError(message)


def _member(value, *names):
    for name in names:
        if isinstance(value, dict) and name in value:
            return value[name]
        if hasattr(value, name):
            return getattr(value, name)
    raise ValueError("Detached A/B evidence omitted " + "/".join(names))


def continuation_expected_a_cursor(genesis_snapshot_bytes):
    """Read the A1 predecessor cursor from the closed detached source list.

    This is a parsing hint only; callers must still run the detached codec and
    bind the resulting snapshot to its A0 manifest and source identities.
    """
    _need(type(genesis_snapshot_bytes) is bytes
          and 0 < len(genesis_snapshot_bytes) <= 4 * 1024 * 1024,
          "Bounded detached A/B continuation package required")
    try:
        from ashlar.schema import decode_retained_json
        package = decode_retained_json(genesis_snapshot_bytes)
        sources = package["sources"]
        if type(sources) is not list or len(sources) != 2:
            raise ValueError("exact A/B source list required")
        a_sources = [item for item in sources
                     if type(item) is dict and item.get("source") == "A"]
        if len(a_sources) != 1:
            raise ValueError("one original A source required")
        page = a_sources[0]["page"]
        prior_b64 = page["prior_bytes_base64"]
        if type(prior_b64) is not str:
            raise ValueError("canonical prior cursor encoding required")
        prior_bytes = base64.b64decode(prior_b64, validate=True)
        if base64.b64encode(prior_bytes).decode("ascii") != prior_b64:
            raise ValueError("canonical prior cursor encoding required")
        cursor_doc = decode_retained_json(prior_bytes)
        if type(cursor_doc) is not dict:
            raise ValueError("prior cursor object required")
        next_xid = cursor_doc["nextXid"]
    except (UnicodeError, ValueError, KeyError, TypeError) as error:
        raise ValueError("Exact A1 continuation prior cursor is required") from error
    _need(type(next_xid) is str and next_xid.isascii() and next_xid.isdecimal()
          and str(int(next_xid)) == next_xid,
          "Canonical A1 continuation nextXid required")
    return next_xid


def admit_progress(source, evidence, *, expected_next_xid="0"):
    from .managed_current_genesis import (
        TrussSourceGenesisProgress, admit_truss_source_genesis_progress,
    )

    progress = _member(evidence, "progress")
    if type(progress) is TrussSourceGenesisProgress:
        cursor = progress.cursor_bytes
        registration = progress.catalog_registration_bytes
        args = {
            "feed": progress.feed, "epoch": progress.epoch,
            "cursor_bytes": cursor, "cursor_sha256": progress.position_sha256,
            "catalog_registration_id": progress.catalog_registration_id,
            "catalog_registration_bytes": registration,
            "consumer_id": progress.consumer_id,
            "consumer_registration_id": progress.consumer_registration_id,
            "installation_id": progress.installation_id,
        }
    else:
        # The closed A/B codec represents these original byte fields as base64
        # strings. Support its Python snake_case record without accepting any
        # caller-created normalized cursor or registration document.
        cursor = _member(progress, "cursor_bytes", "cursor_bytes_base64")
        registration = _member(progress, "catalog_registration_bytes",
                               "catalog_registration_bytes_base64")
        if type(cursor) is str:
            cursor = base64.b64decode(cursor, validate=True)
        if type(registration) is str:
            registration = base64.b64decode(registration, validate=True)
        args = {
            "feed": _member(progress, "feed"),
            "epoch": _member(progress, "epoch"),
            "cursor_bytes": cursor,
            "cursor_sha256": _member(progress, "position_sha256", "cursor_sha256"),
            "catalog_registration_id": _member(progress, "catalog_registration_id"),
            "catalog_registration_bytes": registration,
            "consumer_id": _member(progress, "consumer_id"),
            "consumer_registration_id": _member(progress, "consumer_registration_id"),
            "installation_id": _member(progress, "installation_id"),
        }
    _need(type(cursor) is bytes and type(registration) is bytes,
          f"Original {source} cursor and catalog registration bytes required")
    return admit_truss_source_genesis_progress(**args,
        expected_next_xid=expected_next_xid)


def _catalog_evidence(evidence):
    from .truss_current_catalog import CurrentCatalogEvidence, installed_fresh_truss_runtime_pins

    pins = installed_fresh_truss_runtime_pins()

    revision = _member(evidence, "catalog_revision")
    report = _member(evidence, "report_bytes", "report_bytes_base64")
    view = _member(evidence, "catalog_view_bytes", "view_bytes", "view_bytes_base64")
    if type(report) is str:
        report = base64.b64decode(report, validate=True)
    if type(view) is str:
        view = base64.b64decode(view, validate=True)
    documents = _member(evidence, "documents")
    _need(type(revision) is str and revision.isdecimal()
          and str(int(revision)) == revision and int(revision) > 0,
          "Canonical exact source catalog revision required")
    _need(type(report) is bytes and report and type(view) is bytes and view,
          "Exact original catalog report and complete-view bytes required")
    sources = {}
    for item in documents:
        doc_id = _member(item, "document_id", "documentId")
        doc_revision = _member(item, "document_revision", "documentRevision")
        artifact_id = _member(item, "artifact_identity", "artifactIdentity")
        registration = _member(item, "registration_id", "registrationId")
        doc_catalog_revision = str(_member(item, "catalog_revision", "catalogRevision"))
        digest = _member(item, "sha256")
        content = _member(item, "content", "content_bytes", "content_base64")
        if type(content) is str:
            content = base64.b64decode(content, validate=True)
        _need(type(content) is bytes and hashlib.sha256(content).hexdigest() == digest,
              "Exact selected accepted-document bytes and digest required")
        sources[(revision, doc_id)] = SimpleNamespace(
            document_id=doc_id, document_revision=doc_revision,
            artifact_identity=artifact_id, registration_id=registration,
            catalog_revision=int(doc_catalog_revision), sha256=digest,
            content=content)
    return CurrentCatalogEvidence(
        pins["version"], {revision: report}, {revision: view},
        sources, pins["wheel_sha256"], pins["manifest_sha256"])


def admit_detached_a1_plan(*, genesis_snapshot_bytes, genesis_manifest,
                           stream, materialized_at, context,
                           continuation=False):
    """Revalidate the exact A/B genesis package and prepare its first-A plan.

    The genesis manifest must be the exact result read back from the separate
    initializer task. A and B source progress, accepted catalogs and model pins
    are rebuilt from the frozen public bytes. The A page is re-admitted with
    the installed native-feed parser and graph-admission boundary.
    """
    from ashlar.truss_native_feed import native_feed_batch
    from .managed_current_truss053_genesis_owner import (
        ManagedCurrentTruss053GenesisSelection,
    )
    from .truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot
    from .truss_graph_admission import TrussGraphAdmission
    from .managed_current_a1_trial_plan import prepare_managed_current_a1_trial
    from .r04_schedule import R04Schedule

    _need(type(genesis_snapshot_bytes) is bytes
          and 0 < len(genesis_snapshot_bytes) <= 4 * 1024 * 1024,
          "Owner-private bounded exact A/B genesis source package required")
    expected_a_prior_cursor = "0"
    if continuation:
        # The detached codec defaults to the literal Truss genesis cursor. For
        # A1 continuation, derive only the numeric cursor bound from the closed
        # package, then bind its exact bytes to the supplied A0 manifest below.
        expected_a_prior_cursor = continuation_expected_a_cursor(genesis_snapshot_bytes)
    detached = decode_detached_a1_genesis_snapshot(
        genesis_snapshot_bytes, expected_a_prior_cursor=expected_a_prior_cursor)
    _need(detached.schedule_sha256 == hashlib.sha256(detached.schedule_bytes).hexdigest()
          and type(detached.schema_revisions) is dict and detached.schema_revisions,
          "Frozen original schedule and A/B schema revision pins required")
    schedule = R04Schedule(detached.schedule_bytes)
    schedule_doc = schedule.document()
    _need(type(detached.sources) is dict and set(detached.sources) == {"A", "B"},
          "Both independently selected public source snapshots required")

    source_progress = {}
    observations = {}
    selected_models = {}
    catalog_acceptance = {}
    batches = {}
    for source in ("A", "B"):
        evidence = detached.source_evidence(source) if callable(
            getattr(detached, "source_evidence", None)) else detached.sources[source]
        expected_next_xid = "0"
        if source == "A" and continuation:
            page_prior = _member(_member(evidence, "page", "source_page"),
                                 "prior_bytes", "prior_bytes_base64")
            if type(page_prior) is str:
                page_prior = base64.b64decode(page_prior, validate=True)
            try:
                cursor_doc = __import__("json").loads(page_prior)
                expected_next_xid = cursor_doc["nextXid"]
            except (TypeError, ValueError, KeyError) as error:
                raise ValueError("A1 continuation prior cursor must be exact canonical Truss bytes") from error
            _need(type(expected_next_xid) is str and expected_next_xid.isdecimal(),
                  "A1 continuation cursor nextXid required")
        progress = admit_progress(source, evidence, expected_next_xid=expected_next_xid)
        identity = _member(evidence, "identity")
        binding = schedule_doc["source_bindings"][source]
        _need(all(_member(identity, key) == progress_value for key, progress_value in (
            ("feed", progress.feed), ("epoch", progress.epoch),
            ("installation_id", progress.installation_id),
            ("consumer_id", progress.consumer_id),
            ("consumer_registration_id", progress.consumer_registration_id),
            ("catalog_registration_id", progress.catalog_registration_id)))
            and all(binding.get(key) == progress_value for key, progress_value in (
                ("feed", progress.feed), ("epoch", progress.epoch),
                ("installation_id", progress.installation_id),
                ("consumer_id", progress.consumer_id),
                ("registration_id", progress.consumer_registration_id))),
            "Frozen R04 source identity differs from detached original registration/cursor evidence")
        _need(_member(identity, "catalog_revision") == _member(evidence, "catalog_revision"),
              "Detached source catalog revision differs from frozen source selection")
        source_progress[source] = progress
        page = _member(evidence, "page", "source_page")
        report = _member(evidence, "report_bytes", "report_bytes_base64")
        view = _member(evidence, "catalog_view_bytes", "view_bytes", "view_bytes_base64")
        if type(report) is str:
            report = base64.b64decode(report, validate=True)
        if type(view) is str:
            view = base64.b64decode(view, validate=True)
        content = _member(page, "content", "page_bytes", "content_base64")
        page_digest = _member(page, "sha256", "page_sha256")
        prior = _member(page, "prior_bytes", "prior_cursor_bytes", "prior_bytes_base64")
        following = _member(page, "next_bytes", "next_cursor_bytes", "next_bytes_base64")
        if type(content) is str:
            content = base64.b64decode(content, validate=True)
        if type(prior) is str:
            prior = base64.b64decode(prior, validate=True)
        if type(following) is str:
            following = base64.b64decode(following, validate=True)
        _need(type(content) is bytes and content and type(prior) is bytes
              and type(following) is bytes and prior == progress.cursor_bytes
              and _member(page, "consumer_id") == progress.consumer_id
              and _member(page, "registration_id") == progress.consumer_registration_id,
              f"Exact original {source} page and genesis cursor bytes required")
        page_record = SimpleNamespace(content=content, sha256=page_digest,
            prior=SimpleNamespace(content=prior), next=SimpleNamespace(content=following),
            consumer_id=_member(page, "consumer_id"),
            registration_id=_member(page, "registration_id"),
            writer_xid=_member(page, "writer_xid"),
            safe_watermark_xid=_member(page, "safe_watermark_xid"),
            outcome=_member(page, "outcome"))
        if source == "A":
            batch = native_feed_batch(content, sha256=page_digest, prior=prior,
                                      next=following, feed=progress.feed)
            _need(batch.cursor_before == progress.position
                  and batch.epoch == progress.epoch,
                  "Admitted A page differs from its original genesis position")
        else:
            # B is evidence and schedule control only for this A1 publication.
            batch = None
        batches[source] = batch
        catalog = _catalog_evidence(evidence)
        catalog_revision = _member(evidence, "catalog_revision")
        documents = _member(evidence, "documents")
        if source == "B":
            # B has no A1 publication page, but its full accepted catalog and
            # original documents still require the same .54 interpretation.
            from .truss_current_catalog import interpret_current_catalog
            interpret_current_catalog(catalog_revision, report, view,
                catalog.sources, truss_version=catalog.truss_version)
        model_evidence = _member(evidence, "model_evidence", "model")
        if type(model_evidence) is dict and set(model_evidence) == {"pin", "content_base64"}:
            model_evidence = {"pin": model_evidence["pin"],
                              "bytes": base64.b64decode(
                                  model_evidence["content_base64"], validate=True)}
        elif type(model_evidence) is dict and set(model_evidence) in (
                {"pin", "bytes"}, {"pin", "content"}):
            model_bytes = _member(model_evidence, "bytes", "content")
            _need(type(model_bytes) is bytes and model_bytes,
                  "Original selected model content bytes required")
            model_evidence = {"pin": model_evidence["pin"], "bytes": model_bytes}
        else:
            raise ValueError("Exact selected A/B model pin and original content required")
        _need(model_evidence["pin"] == schedule_doc["accepted_model_pins"][source],
              f"Detached {source} model bytes differ from the frozen R04 model pin")
        observations[source] = SimpleNamespace(
            source=source, progress=progress, page=page_record, batch=batch,
            report_bytes=report,
            catalog_view_bytes=view,
            accepted_documents={_member(doc, "document_id", "documentId"): doc
                                for doc in documents},
            model_evidence=model_evidence,
            catalog_evidence=catalog)
        selected_models[source] = model_evidence
        catalog_acceptance[source] = {
            "installation_id": progress.installation_id,
            "revision": catalog_revision,
            "report_bytes": report,
            "catalog_bytes": view,
        }

    _need(len({p.feed for p in source_progress.values()}) == 2
          and len({p.installation_id for p in source_progress.values()}) == 2,
          "A/B source feed and installation identities must be distinct")
    admission = TrussGraphAdmission(batches["A"], observations["A"].catalog_evidence)
    selection = ManagedCurrentTruss053GenesisSelection(
        detached.schedule_sha256, detached.schedule_bytes, source_progress,
        selected_models, catalog_acceptance, detached.schema_revisions,
        observations, admission, batches["A"])
    if continuation:
        from .managed_current_a1_trial_plan import prepare_managed_current_a1_continuation
        return prepare_managed_current_a1_continuation(
            selection, genesis_manifest, stream=stream,
            materialized_at=materialized_at, context=context)
    plan = prepare_managed_current_a1_trial(selection, genesis_manifest,
        stream=stream, materialized_at=materialized_at, context=context)
    from dataclasses import replace
    return replace(plan, phase="a0_metadata",
        delivery_page=replace(plan.delivery_page, ordinal=0))

"""Bounded, forced-rollback Truss group invocation with exact public failure capture."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path

MAX_ARTIFACT_BYTES = 1024 * 1024


class RollbackOutcome(Exception):
    def __init__(self, result):
        super().__init__("R54 diagnostic probe forces transaction rollback")
        self.result = result


def _safe(value, limit=240):
    if value is None or type(value) in (int, bool):
        return value
    if type(value) is str:
        return value[:limit]
    try:
        return str(value)[:limit]
    except BaseException:
        return f"<{type(value).__name__}>"


def _profile(value):
    if value is None:
        return None
    return {name: _safe(getattr(value, name, None))
            for name in ("identity", "version", "sha256")}


def _artifact(value, field_name):
    if value is None:
        return None
    encoded = getattr(value, "bytes_base64", None)
    claimed_sha = getattr(value, "sha256", None)
    identity = getattr(value, "identity", None)
    if type(encoded) is not str or len(encoded) > 4 * ((MAX_ARTIFACT_BYTES + 2) // 3):
        raise ValueError("Public diagnostic artifact exceeds the 1 MiB bound")
    raw = base64.b64decode(encoded, validate=True)
    digest = hashlib.sha256(raw).hexdigest()
    if (len(raw) > MAX_ARTIFACT_BYTES or base64.b64encode(raw).decode("ascii") != encoded
            or type(claimed_sha) is not str or digest != claimed_sha
            or type(identity) is not str or digest != identity):
        raise ValueError("Public diagnostic artifact bytes or digest failed exact verification")
    return {"field": field_name, "identity": identity, "sha256": claimed_sha,
            "bytes_length": len(raw), "bytes_base64": encoded}


def failure_facts(wrapper, value=None):
    facts = {"public_result_wrapper": type(wrapper).__name__ if wrapper is not None else None,
        "group_variant": type(value).__name__ if value is not None else None,
        "response_variant": type(getattr(value, "response", None)).__name__ if value is not None and getattr(value, "response", None) is not None else None,
        "result_count": None}
    if value is None:
        error = getattr(wrapper, "error", None) if wrapper is not None else None
        facts["public_error"] = {"variant": type(error).__name__ if error is not None else None,
            **{name: _safe(getattr(error, name, None)) for name in
               ("code", "catalog_code", "sql_state", "retry_scope")}}
        return facts
    failure = getattr(value, "failure", None)
    if failure is None:
        response = getattr(value, "response", None)
        semantic = getattr(response, "semantic", None)
        results = getattr(semantic, "results", None)
        facts["result_count"] = len(results) if isinstance(results, (tuple, list)) else None
        return facts
    facts["failure_variant"] = type(failure).__name__
    facts["failure_code"] = _safe(getattr(failure, "code", None))
    facts["operation_index"] = _safe(getattr(failure, "operation_index", None))
    facts["operation_code"] = _safe(getattr(failure, "operation_code", None))
    facts["failure_phase"] = _safe(getattr(failure, "phase", None))
    facts["error_profile"] = _profile(getattr(failure, "error_profile", None))
    facts["diagnostic_profile"] = _profile(getattr(failure, "diagnostic_profile", None))
    artifact = getattr(failure, "inner_error", None)
    artifact_field = "inner_error"
    if artifact is None:
        artifact = getattr(failure, "diagnostic", None)
        artifact_field = "diagnostic"
    facts["diagnostic_artifact"] = _artifact(artifact, artifact_field) if artifact is not None else None
    failed_preconditions = getattr(failure, "failures", None)
    if isinstance(failed_preconditions, (tuple, list)):
        facts["precondition_failures"] = [
            {name: _safe(getattr(item, name, None)) for name in ("condition_id", "reason", "rule")}
            for item in failed_preconditions[:32]]
        facts["precondition_failure_count"] = len(failed_preconditions)
    return facts


def _write_exclusive(path: Path, value):
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False, allow_nan=False) + "\n").encode()
    if len(raw) > 2 * 1024 * 1024:
        raise ValueError("R54 result artifact exceeds 2 MiB bound")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        view = memoryview(raw)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short durable result write")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    dfd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def probe_group(base, connection_factory, *, model, document_id, document_revision,
                request_key, origin, operations, bun, receipt_path, expected_count=2):
    """Invoke Truss once inside a host-owned write transaction and always roll it back."""
    from truss import CatalogDocument, PreparedGroup
    from truss.contracts import MapEntry, MapValue, NullValue, TextValue
    from truss.groups import (AliasObjectReference, AuthoredValue, CreateEdge,
        CreateObject, CurrentCatalog, GroupSemanticInput, RootlessOwnership,
        TextOrderKey)
    import psycopg

    document = CatalogDocument(document_id, document_revision, model)
    params = connection_factory("writer")
    with psycopg.connect(**params, autocommit=True, connect_timeout=5) as conn:
        def invoke(assembly, tx):
            prepared = base._value(assembly.prepare_group_in_transaction(tx,
                records=((document_id, "m", "Person"), (document_id, "m", "Place")),
                relationships=((document_id, "m", "knows"), (document_id, "m", "located"))),
                "prepare typed group")
            if not isinstance(prepared, PreparedGroup):
                raise base.StopRun("Public Truss PreparedGroup required")
            type_pins = {item.authored_identity: item.definition_pin
                         for item in prepared.types if item is not None}
            relationship_pins = {item.authored_identity: item.definition_pin
                                 for item in prepared.relationships if item is not None}
            authored = []
            for item in operations:
                if item["op"] == "create_object":
                    values = tuple(AuthoredValue(name=name, value=(
                        NullValue() if value["kind"] == "null" else
                        TextValue(kind=value["kind"], text=value["text"])))
                        for name, value in item.get("values", {}).items())
                    authored.append(CreateObject(type_definition_pin=type_pins[item["type"]],
                        values=values, ownership=RootlessOwnership(), alias=item["alias"]))
                elif item["op"] == "create_edge":
                    authored.append(CreateEdge(relationship_definition_pin=relationship_pins[item["relationship"]],
                        source=AliasObjectReference(alias=item["source"]),
                        target=AliasObjectReference(alias=item["target"]), values=(),
                        order_key=TextOrderKey(text=item["order_key"]), alias=item["alias"]))
                else:
                    raise base.StopRun("Unsupported frozen R54 group operation")
            selection = assembly.group_selection
            asserted = MapValue(entries=tuple(MapEntry(key=key,
                value=TextValue(kind="string", text=value)) for key, value in sorted(origin.items())))
            semantic = GroupSemanticInput(layout_profile=selection.layout_profile,
                mutation_profile=selection.capability_profile, value_profile=selection.value_profile,
                catalog=CurrentCatalog(), operations=tuple(authored), asserted_origin=asserted)
            context = prepared.context
            request = context.request(request_key)
            if request.identity.authorized_scope_identity != context.authorized_scope_identity:
                raise base.StopRun("Prepared Truss request scope identity mismatch")
            wrapper = assembly.apply_in_transaction(tx, semantic, request,
                documents=(document,), bun_executable=str(bun))
            try:
                value = base._value(wrapper, "diagnostic atomic typed group")
                facts = failure_facts(wrapper, value)
            except BaseException as exc:
                facts = failure_facts(wrapper)
                facts.update({"decode_exception_type": type(exc).__name__,
                              "decode_exception": _safe(str(exc))})
            raise RollbackOutcome({"profile": "ashlar-r54-public-group-diagnostic/0.1",
                "request_key": request_key, "expected_operation_count": expected_count,
                "transaction_outcome": "forced_rollback", **facts})

        try:
            base._transaction(conn, invoke, read_only=False, timeout_ms=10000)
        except RollbackOutcome as result:
            record = {**result.result, "transaction_rolled_back": True,
                "rollback_trigger": "intentional probe sentinel raised inside callback",
                "source_ack_attempted": False, "spark_started": False,
                "delta_written": False, "cloud": False}
            _write_exclusive(receipt_path, record)
            return record
        except BaseException as exc:
            record = {"profile": "ashlar-r54-public-group-diagnostic/0.1",
                "request_key": request_key, "transaction_outcome": "failed_before_result_capture",
                "transaction_rolled_back": True, "rollback_trigger": "exception escaped transaction callback",
                "exception_type": type(exc).__name__, "exception": _safe(str(exc)),
                "source_ack_attempted": False, "spark_started": False,
                "delta_written": False, "cloud": False}
            _write_exclusive(receipt_path, record)
            raise
        raise base.StopRun("R54 callback unexpectedly returned; rollback sentinel was not raised")

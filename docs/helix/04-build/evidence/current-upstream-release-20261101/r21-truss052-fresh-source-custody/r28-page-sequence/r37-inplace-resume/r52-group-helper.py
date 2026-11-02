"""R52 continuation group apply with pre-commit result validation and durable failure facts."""
from __future__ import annotations

import json
import os
from pathlib import Path


class GroupResultMismatch(RuntimeError):
    def __init__(self, diagnostic):
        super().__init__("R52 public Truss group result failed pre-commit validation")
        self.diagnostic = diagnostic


def _write_exclusive(path: Path, value):
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False, allow_nan=False) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        view = memoryview(raw)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short durable diagnostic write")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    dfd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def _json_safe_field(value):
    if value is None or type(value) in (int, bool):
        return value
    if type(value) is str:
        return value[:160]
    try:
        rendered = str(value)
    except BaseException:
        rendered = f"<{type(value).__name__}>"
    return rendered[:160]


def _result_facts(wrapper, value=None, error=None):
    group = value
    response = getattr(group, "response", None) if group is not None else None
    semantic = getattr(response, "semantic", None) if response is not None else None
    results = getattr(semantic, "results", None) if semantic is not None else None
    count = len(results) if isinstance(results, (tuple, list)) else None
    facts = {
        "public_result_wrapper": type(wrapper).__name__ if wrapper is not None else None,
        "group_variant": type(group).__name__ if group is not None else None,
        "response_variant": type(response).__name__ if response is not None else None,
        "semantic_variant": type(semantic).__name__ if semantic is not None else None,
        "result_count": count,
    }
    if error is not None:
        err = getattr(error, "error", None)
        facts["error"] = {
            "variant": type(err).__name__ if err is not None else None,
            "code": _json_safe_field(getattr(err, "code", None)),
            "catalog_code": _json_safe_field(getattr(err, "catalog_code", None)),
            "sql_state": _json_safe_field(getattr(err, "sql_state", None)),
            "retry_scope": _json_safe_field(getattr(err, "retry_scope", None)),
        }
    return facts


def apply_group(base, connection_factory, *, model, document_id, document_revision,
                request_key, origin, operations, bun, receipt_path, expected_count):
    """Apply a frozen group; mismatched public results raise before transaction commit."""
    from truss import CatalogDocument, PreparedGroup
    from truss.contracts import MapEntry, MapValue, NullValue, TextValue
    from truss.groups import (AliasObjectReference, AuthoredValue, CreateEdge,
        CreateObject, CurrentCatalog, GroupSemanticInput, RootlessOwnership,
        TextOrderKey, GroupSuccess)
    import psycopg

    document = CatalogDocument(document_id, document_revision, model)
    params = connection_factory("writer")
    with psycopg.connect(**params, autocommit=True, connect_timeout=5) as conn:
        def apply(assembly, tx):
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
                    raise base.StopRun("Unsupported frozen R52 group operation")
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
                value = base._value(wrapper, "apply atomic typed group")
            except BaseException as exc:
                diagnostic = {"profile": "ashlar-r52-group-result-diagnostic/0.1",
                    "status": "result_refused_before_commit", "request_key": request_key,
                    **_result_facts(wrapper, error=wrapper),
                    "exception_type": type(exc).__name__, "exception": str(exc)[:600]}
                raise GroupResultMismatch(diagnostic) from exc
            facts = _result_facts(wrapper, value=value)
            if (not isinstance(value, GroupSuccess)
                    or type(getattr(value, "response", None)).__name__ != "AppliedPending"
                    or facts["result_count"] != expected_count):
                raise GroupResultMismatch({"profile": "ashlar-r52-group-result-diagnostic/0.1",
                    "status": "result_refused_before_commit", "request_key": request_key,
                    "expected_result_count": expected_count, **facts})
            return value, facts

        try:
            value, facts = base._transaction(conn, apply, read_only=False, timeout_ms=10000)
        except GroupResultMismatch as exc:
            # _transaction's context manager has rolled back and disposed its Truss host here.
            _write_exclusive(receipt_path, {**exc.diagnostic,
                "transaction_rolled_back": True, "diagnostic_written_after_rollback": True})
            raise base.StopRun("R52 Truss group result rejected and rolled back: " +
                json.dumps({k: exc.diagnostic.get(k) for k in
                    ("public_result_wrapper", "group_variant", "response_variant", "result_count")},
                    sort_keys=True, separators=(",", ":"))) from exc
        except BaseException as exc:
            _write_exclusive(receipt_path, {"profile": "ashlar-r52-group-result-diagnostic/0.1",
                "status": "transaction_failed_without_accepted_result", "request_key": request_key,
                "transaction_rolled_back": True, "diagnostic_written_after_rollback": True,
                "exception_type": type(exc).__name__, "exception": str(exc)[:600]})
            raise
        conn.commit()
        _write_exclusive(receipt_path, {"profile": "ashlar-r52-group-result-diagnostic/0.1",
            "status": "committed", "request_key": request_key, **facts,
            "transaction_rolled_back": False, "diagnostic_written_after_rollback": False,
            "commit_confirmed": True})
        return value

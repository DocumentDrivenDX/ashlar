#!/usr/bin/env python3
"""One bounded Truss 0.0.48 local development acceptance/feed trial."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import time

HERE = Path(__file__).resolve().parent
SCOPE_PATH = HERE / "fresh-local-trial-scope.json"
REQUEST_PATH = HERE / "fresh-local-seed-request.json"
REPO = HERE.parents[5]
MODEL_PATH = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r4-sdk010-initialize-only/inputs/typed-graph-model.umf.json"
MODEL_SOURCE_SHA = "19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b"
MODEL_SHA = "2c9b295a8232cae4770255650bf634941ab223e582b79ae2bb78f6acf0b8a880"
WHEEL_SHA = "1aedf614ff3d3ff7a61bd4d91887ede650a7135d1239e8564c1c297c0ddc37c3"
MAX_BYTES = 512 * 1024 * 1024
MAX_SECONDS = 900


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def write_once(path: Path, data: object) -> None:
    raw = (json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def write_bytes_once(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def tree_bytes(root: Path) -> int:
    total = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        dirs.sort()
        files.sort()
        for name in dirs + files:
            path = Path(base) / name
            if path.is_symlink() or not (path.is_file() or path.is_dir()):
                raise RuntimeError("symlink or special entry found in retained attempt tree")
        for name in files:
            total += (Path(base) / name).stat().st_size
            if total > MAX_BYTES:
                raise RuntimeError("retained attempt tree exceeds 512 MiB; data retained")
    return total


def finish(primary, callbacks):
    errors = []
    for callback in callbacks:
        try:
            callback()
        except BaseException as error:
            errors.append(error)
    if primary is not None:
        if errors:
            raise RuntimeError(f"primary failure {primary!r}; cleanup failure {errors!r}") from primary
        raise primary
    if errors:
        raise RuntimeError(f"cleanup failure {errors!r}") from errors[0]


def require_ok(result, label):
    from truss import Ok
    if not isinstance(result, Ok):
        detail = getattr(result, "error", None)
        raise RuntimeError(f"{label} refused: {type(result).__name__} {detail!r}"[:1200])
    return result.value


def main() -> None:
    scope = json.loads(SCOPE_PATH.read_bytes())
    request_raw = REQUEST_PATH.read_bytes()
    request = json.loads(request_raw)
    model_source = MODEL_PATH.read_bytes()
    before = b'"id": "typed-graph-example"'
    after = b'"id": "ashlar-local-dev-typed-graph-r6-20261031"'
    if sha(model_source) != MODEL_SOURCE_SHA or model_source.count(before) != 1:
        raise RuntimeError("frozen model source or exact identity token mismatch")
    model = model_source.replace(before, after)
    if importlib.metadata.version("truss-toolkit") != "0.0.48":
        raise RuntimeError("exact Truss 0.0.48 required")
    if sha(model) != MODEL_SHA or len(model) > 4 * 1024 * 1024:
        raise RuntimeError("derived G03 local model identity or size mismatch")
    if (len(request["operations"]) != 7 or len(request_raw) > 65_536
            or sha(Path("/private/tmp/ashlar-r16-truss048-readonly-wheelhouse/truss_toolkit-0.0.48-py3-none-any.whl").read_bytes()) != WHEEL_SHA):
        raise RuntimeError("exact bounded source/seed/Truss inputs required")
    root = Path(scope["paths"]["output_root"])
    data = Path(scope["paths"]["data_directory"])
    if not root.is_absolute() or root.resolve() != root or data.parent != root:
        raise RuntimeError("canonical isolated output and data paths required")
    if root.exists() or root.is_symlink():
        raise RuntimeError("fresh output path must be absent; no resume or cleanup")
    root.mkdir(mode=0o700, parents=False)
    data.mkdir(mode=0o700)
    os.chmod(root, 0o700)
    os.chmod(data, 0o700)
    def deadline(_signum, _frame):
        raise TimeoutError("single local trial exceeded 900 second deadline")
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(MAX_SECONDS)
    started = time.monotonic()
    (root / "inputs").mkdir(mode=0o700)
    write_bytes_once(root / "inputs/model.umf.json", model)
    write_once(root / "inputs/model-derivation.json", {"source_sha256": MODEL_SOURCE_SHA,
        "derived_sha256": MODEL_SHA, "replacement_count": 1,
        "document_id": scope["model"]["new_catalog_document_id"]})
    write_once(root / "inputs/seed-request.json", {"sha256": sha(request_raw), "bytes": len(request_raw), "operation_count": len(request["operations"])})
    write_once(root / "run-started.json", {"profile": scope["profile"], "one_attempt": True, "cloud": False, "migration": False, "acknowledgement": False})

    import psycopg
    from psycopg import sql
    from truss import (CatalogDocument, CatalogRuntimeConfiguration, LocalPostgres,
                      Ok, adopt_psycopg_host, create_reference_assembly,
                      prepare_acceptance)
    from truss.contracts import (MapEntry, MapValue, NullValue, ProfilePin,
                                 TextValue)
    from truss.groups import (AliasObjectReference, AuthoredValue, CreateEdge,
                              CreateObject, CurrentCatalog, GroupSemanticInput,
                              RequestIdentity, RequestPresent, RootlessOwnership,
                              TextOrderKey)

    document = CatalogDocument(
        document_id=scope["model"]["new_catalog_document_id"],
        document_revision=scope["model"]["new_document_revision"],
        content=model,
    )
    # Keep public pure preparation outside the local effect interval.
    prepared = prepare_acceptance((document,))
    runtime = LocalPostgres(data)
    primary = None
    receipt = {"profile": scope["profile"], "status": "running",
               "authority_class": "local_development_authority",
               "model_sha256": MODEL_SHA, "seed_request_sha256": sha(request_raw),
               "truss_version": "0.0.48", "postgresql_version_expected": "18.6",
               "truss_catalog_mutation": False, "postgres_cluster_initialized": False,
               "migration": False, "publication": False,
               "acknowledgement": False, "cloud": False}
    try:
        with runtime as pg:
            if pg.info.server_version != "18.6":
                raise RuntimeError("unexpected PostgreSQL version")
            receipt["postgresql_version"] = pg.info.server_version
            receipt["data_directory"] = str(pg.info.data_directory)
            receipt["socket_directory"] = str(pg.info.socket_directory)
            receipt["postgres_cluster_initialized"] = True
            receipt["attempt_tree_bytes_after_start"] = tree_bytes(root)
            with psycopg.connect(pg.info.connection_uri, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    host = adopt_psycopg_host(conn, statement_timeout_ms=5000, operation_timeout_ms=30000)
                    assembly = None
                    primary = None
                    try:
                        tx = require_ok(host.adopt_transaction(), "Truss host transaction")
                        assembly = require_ok(create_reference_assembly(CatalogRuntimeConfiguration(), host), "Truss public assembly")
                        pending = require_ok(assembly.install_in_transaction(tx), "fresh Truss 0.0.48 installation")
                        receipt["installation_id"] = pending.installation_id
                        receipt["release_id"] = pending.release_id
                        receipt["owner_role"] = pending.owner_role
                        receipt["registrar_role"] = pending.registrar_role
                        conn.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS").format(sql.Identifier(scope["source"]["ordinary_reader_role"])))
                        conn.execute(sql.SQL("CREATE ROLE {} LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS").format(sql.Identifier(scope["source"]["registrar_login_role"])))
                        conn.execute(sql.SQL("GRANT {} TO {} WITH INHERIT TRUE, SET FALSE").format(sql.Identifier(receipt["registrar_role"]), sql.Identifier(scope["source"]["registrar_login_role"])))
                    except BaseException as error:
                        primary = error
                    finish(primary, ([assembly.dispose] if assembly is not None else []) + [host.detach])

            # Acceptance is a separate committed transaction under Truss's registrar role.
            # First prove that the dedicated LOGIN principal is recognized by the
            # public registrar-status API in a read-only transaction.
            with psycopg.connect(pg.info.connection_uri, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    conn.execute("SET TRANSACTION READ ONLY")
                    conn.execute(sql.SQL("SET SESSION AUTHORIZATION {}").format(sql.Identifier(scope["source"]["registrar_login_role"])))
                    role_state = conn.execute("SELECT session_user,current_user,current_setting('role'),rolcanlogin FROM pg_catalog.pg_roles WHERE rolname=session_user").fetchone()
                    if role_state != (scope["source"]["registrar_login_role"], scope["source"]["registrar_login_role"], "none", True):
                        raise RuntimeError(f"registrar login principal identity mismatch: {role_state!r}")
                    host = adopt_psycopg_host(conn, statement_timeout_ms=5000, operation_timeout_ms=30000)
                    assembly = None
                    primary = None
                    try:
                        tx = require_ok(host.adopt_transaction(access_mode="read_only"), "registrar status preflight transaction")
                        assembly = require_ok(create_reference_assembly(CatalogRuntimeConfiguration(), host), "Truss public assembly")
                        status = require_ok(assembly.registrar_status_in_transaction(tx), "public registrar status preflight")
                        from truss.catalog_results import RegistrarStatus
                        if type(status) is not RegistrarStatus or status.can_register is not True:
                            raise RuntimeError(f"registrar principal not authorized by public status API: {status!r}")
                        receipt["registrar_principal_preflight"] = {"status": "passed", "session_user": role_state[0], "current_user": role_state[1], "role_setting": role_state[2], "rolcanlogin": role_state[3], "can_register": status.can_register, "transaction_read_only": True}
                    except BaseException as error:
                        primary = error
                    finish(primary, ([assembly.dispose] if assembly is not None else []) + [host.detach])

            with psycopg.connect(pg.info.connection_uri, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute(sql.SQL("SET SESSION AUTHORIZATION {}").format(sql.Identifier(scope["source"]["registrar_login_role"])))
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    host = adopt_psycopg_host(conn, statement_timeout_ms=5000, operation_timeout_ms=30000)
                    assembly = None
                    primary = None
                    try:
                        tx = require_ok(host.adopt_transaction(), "catalog acceptance transaction")
                        assembly = require_ok(create_reference_assembly(CatalogRuntimeConfiguration(), host), "Truss public assembly")
                        outcome = require_ok(assembly.accept_in_transaction(
                            tx, prepared, request_key=scope["source"]["request_key"],
                            origin_bytes=json.dumps({"profile": "local_truss_acceptance_trial", "authority_class": "local_development_authority"}, separators=(",", ":")).encode(),
                            expected_revision=0,
                        ), "Truss public catalog acceptance")
                        from truss.catalog_results import CatalogRegistration
                        if type(outcome) is not CatalogRegistration:
                            raise RuntimeError(f"expected a new CatalogRegistration, received {type(outcome).__name__}")
                        receipt["catalog_revision"] = getattr(outcome, "revision", None)
                        receipt["catalog_registration_id"] = getattr(outcome, "registration_id", None)
                        receipt["accepted_document_id"] = scope["model"]["new_catalog_document_id"]
                        receipt["accepted_document_revision"] = scope["model"]["new_document_revision"]
                        receipt["accepted_document_sha256"] = MODEL_SHA
                    except BaseException as error:
                        primary = error
                    finish(primary, ([assembly.dispose] if assembly is not None else []) + [host.detach])
            receipt["attempt_tree_bytes_after_acceptance"] = tree_bytes(root)

            # Enroll a separate ordinary reader at genesis before any seed write.
            with psycopg.connect(pg.info.connection_uri, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    host = adopt_psycopg_host(conn, statement_timeout_ms=5000, operation_timeout_ms=30000)
                    assembly = None
                    primary = None
                    try:
                        tx = require_ok(host.adopt_transaction(), "consumer registration transaction")
                        assembly = require_ok(create_reference_assembly(CatalogRuntimeConfiguration(), host), "Truss public assembly")
                        consumer = require_ok(assembly.register_feed_consumer_in_transaction(
                            tx, consumer_id=scope["source"]["consumer_id"],
                            reader_role=scope["source"]["ordinary_reader_role"]), "Truss public genesis consumer registration")
                        receipt["consumer_id"] = consumer.consumer_id
                        receipt["consumer_registration_id"] = consumer.registration_id
                        receipt["genesis_checkpoint_sha256"] = sha(consumer.checkpoint.content)
                    except BaseException as error:
                        primary = error
                    finish(primary, ([assembly.dispose] if assembly is not None else []) + [host.detach])

            # Resolve genuine catalog pins and submit one seven-operation public atomic group.
            with psycopg.connect(pg.info.connection_uri, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute(sql.SQL("SET SESSION AUTHORIZATION {}").format(sql.Identifier(scope["source"]["registrar_login_role"])))
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    host = adopt_psycopg_host(conn, statement_timeout_ms=5000, operation_timeout_ms=30000)
                    assembly = None
                    primary = None
                    try:
                        tx = require_ok(host.adopt_transaction(), "seed transaction")
                        assembly = require_ok(create_reference_assembly(CatalogRuntimeConfiguration(), host), "Truss public assembly")
                        group = require_ok(assembly.prepare_group_in_transaction(
                            tx,
                            records=((document.document_id, "m", "Person"), (document.document_id, "m", "Place")),
                            relationships=((document.document_id, "m", "knows"), (document.document_id, "m", "located")),
                        ), "Truss public group preparation")
                        type_pins = {ref.authored_identity: ref.definition_pin for ref in group.types if ref is not None}
                        relationship_pins = {ref.authored_identity: ref.definition_pin for ref in group.relationships if ref is not None}
                        operations = []
                        for item in request["operations"]:
                            if item["op"] == "create_object":
                                values = tuple(AuthoredValue(name=name, value=(NullValue() if val["kind"] == "null" else TextValue(kind=val["kind"], text=val["text"]))) for name, val in item["values"].items())
                                operations.append(CreateObject(type_definition_pin=type_pins[item["type"]], values=values, ownership=RootlessOwnership(), alias=item["alias"]))
                            elif item["op"] == "create_edge":
                                operations.append(CreateEdge(relationship_definition_pin=relationship_pins[item["relationship"]], source=AliasObjectReference(alias=item["source"]), target=AliasObjectReference(alias=item["target"]), values=(), order_key=TextOrderKey(text=item["order_key"]), alias=item["alias"]))
                            else:
                                raise RuntimeError("unsupported seed operation")
                        selection = assembly.group_selection
                        asserted = MapValue(entries=tuple(MapEntry(key=key, value=TextValue(kind="string", text=value)) for key, value in sorted(request["origin"].items())))
                        semantic = GroupSemanticInput(layout_profile=selection.layout_profile,
                            mutation_profile=selection.capability_profile, value_profile=selection.value_profile,
                            catalog=CurrentCatalog(), operations=tuple(operations), asserted_origin=asserted)
                        group_request = RequestPresent(identity=RequestIdentity(
                            authorized_scope_identity=scope["profile"], request_id=request["request_key"]))
                        applied = require_ok(assembly.apply_in_transaction(tx, semantic, group_request, documents=(document,)), "Truss public seven-operation seed")
                        from truss.groups import GroupSuccess
                        if type(applied) is not GroupSuccess or len(applied.response.semantic.results) != 7:
                            raise RuntimeError(f"expected successful seven-operation group, received {type(applied).__name__}")
                        receipt["seed_outcome_type"] = type(applied).__name__
                        receipt["seed_operation_count"] = 7
                        receipt["seed_request_key"] = request["request_key"]
                    except BaseException as error:
                        primary = error
                    finish(primary, ([assembly.dispose] if assembly is not None else []) + [host.detach])
            receipt["attempt_tree_bytes_after_seed"] = tree_bytes(root)

            # Read the next complete actual source transaction as the ordinary reader; do not ACK.
            with psycopg.connect(pg.info.connection_uri, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute(sql.SQL("SET SESSION AUTHORIZATION {}").format(sql.Identifier(scope["source"]["ordinary_reader_role"])))
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    conn.execute("SET TRANSACTION READ ONLY")
                    host = adopt_psycopg_host(conn, statement_timeout_ms=5000, operation_timeout_ms=30000)
                    assembly = None
                    primary = None
                    try:
                        tx = require_ok(host.adopt_transaction(access_mode="read_only"), "feed read transaction")
                        assembly = require_ok(create_reference_assembly(CatalogRuntimeConfiguration(), host), "Truss public assembly")
                        page = require_ok(assembly.read_feed_in_transaction(
                            tx, consumer_id=scope["source"]["consumer_id"], maximum_facts=4096, maximum_bytes=8 * 1024 * 1024), "Truss public feed read")
                        if getattr(page, "outcome", None) != "transaction":
                            raise RuntimeError("expected one complete committed source transaction")
                        receipt["feed_page_sha256"] = page.sha256
                        receipt["feed_page_bytes"] = len(page.content)
                        receipt["feed_page_writer_xid"] = page.writer_xid
                        receipt["feed_page_consumer_registration_id"] = page.registration_id
                        receipt["feed_page_read_as"] = scope["source"]["ordinary_reader_role"]
                        write_bytes_once(root / "feed-page.json", page.content)
                    except BaseException as error:
                        primary = error
                    finish(primary, ([assembly.dispose] if assembly is not None else []) + [host.detach])
        signal.alarm(0)
        receipt.update(status="completed", elapsed_seconds=round(time.monotonic() - started, 3),
                       retained_attempt_tree_bytes=tree_bytes(root), acknowledgement=False)
        write_once(root / "trial-result.json", receipt)
    except BaseException as error:
        signal.alarm(0)
        receipt.update(status="failed", elapsed_seconds=round(time.monotonic() - started, 3),
                       failure_type=type(error).__name__, failure=str(error)[:1000])
        try:
            receipt["retained_attempt_tree_bytes"] = tree_bytes(root)
        except BaseException as inventory_error:
            receipt["tree_inventory_failure"] = str(inventory_error)[:500]
        try:
            write_once(root / "trial-failure.json", receipt)
        except FileExistsError:
            pass
        raise


if __name__ == "__main__":
    main()

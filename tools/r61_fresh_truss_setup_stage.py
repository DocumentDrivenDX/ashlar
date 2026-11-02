#!/usr/bin/env python3
"""Callable first stage for a reviewed R61 run; never launched by preflight."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
MODEL = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261101/r21-truss052-fresh-source-custody/r28-page-sequence/r28-model.umf.json"
MODEL_SHA256 = "b908c0dd6a0da65c864c0debd0c793d8ab06a933546f34050480909c5c9349f2"
PROFILE = "ashlar-r61-fresh-truss-source-setup/0.1"


def _write_private(path: Path, value: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, "wb") as stream:
        if stream.write(value) != len(value):
            raise OSError("short private stage receipt write")
        stream.flush()
        os.fsync(stream.fileno())


def _connection_factory(psycopg, uri):
    def connect():
        return psycopg.connect(uri, autocommit=False, connect_timeout=5)
    return connect


def run_fresh_truss_setup_stage(*, root: Path, bun: Path, expected_adapter_profile: str) -> dict:
    """Create a clean local Truss installation, accept UMF 0.10 source, and read it.

    This function is intentionally absent from the CLI. It becomes callable only
    after exact candidate/runtime bindings and an independently reviewed native
    scope are supplied by the outer qualification runner.
    """
    root = Path(root)
    bun = Path(bun)
    if (type(expected_adapter_profile) is not str or not expected_adapter_profile
            or not root.is_absolute() or root.resolve() != root or root.exists()
            or root.is_symlink() or not root.parent.is_dir() or not bun.is_file()
            or not MODEL.is_file() or hashlib.sha256(MODEL.read_bytes()).hexdigest() != MODEL_SHA256):
        raise ValueError("Fresh output, selected Bun, and exact R28 UMF 0.10 source required")
    model = MODEL.read_bytes()
    parsed = json.loads(model)
    if parsed.get("umf") != "0.10.0" or parsed.get("id") != "ashlar-local-dev-typed-graph-r21-20261101":
        raise ValueError("Bound original declared UMF 0.10 source required")

    # Complete selected-runtime admission before creating the disposable root.
    from ashlar_host.truss_runtime import require_truss_psycopg_runtime
    from ashlar_host.truss_current_catalog import installed_current_truss_profile
    from ashlar_host.truss_local_setup import run_setup_truss_local_command
    from ashlar_host.truss_note_publication import read_page, register_note_consumer
    from truss import LocalPostgres, Ok, CatalogRuntimeConfiguration, create_reference_assembly, adopt_psycopg_host
    from importlib.metadata import version as distribution_version
    import psycopg

    require_truss_psycopg_runtime()
    if distribution_version("umf-truss") != "0.0.53":
        raise RuntimeError("Exact Truss 0.0.53 candidate required")
    candidate_profile = installed_current_truss_profile()
    if candidate_profile != expected_adapter_profile:
        raise RuntimeError("Exact reviewed Truss 0.0.53 adapter profile required")

    root.mkdir(mode=0o700)
    socket_tmp = root / "socket-tmp"
    socket_tmp.mkdir(mode=0o700)
    model_path = root / "accepted-model.umf.json"
    origin_path = root / "origin.json"
    setup_path = root / "setup.json"
    accepted_path = root / "accepted.json"
    _write_private(model_path, model)
    origin = b'{"operator":"ashlar-r61-local-fresh-baseline"}\n'
    _write_private(origin_path, origin)
    setup = {
        "data_directory": str(root / "pgdata"),
        "bun": str(bun),
        "document_path": str(model_path),
        "document_id": parsed["id"],
        "document_revision": "r1",
        "origin_path": str(origin_path),
        "request_key": "ashlar-r61-fresh-accept-r1",
    }
    _write_private(setup_path, (json.dumps(setup, sort_keys=True, separators=(",", ":")) + "\n").encode())

    # The reviewed launch environment sets TMPDIR to this fresh directory so
    # Truss LocalPostgres owns a distinct Unix socket alongside its fresh PGDATA.
    old_tmpdir = os.environ.get("TMPDIR")
    os.environ["TMPDIR"] = str(socket_tmp)
    primary = None
    result = None
    try:
        bootstrap = run_setup_truss_local_command(setup_path, accepted_path)
        consumer_id = "ashlar-r61-local-consumer"
        feed = "ashlar-r61-local-feed"
        with LocalPostgres(root / "pgdata") as runtime:
            admin_uri = runtime.info.connection_uri
            parameters = psycopg.conninfo.conninfo_to_dict(admin_uri)
            socket_host = Path(parameters.get("host", ""))
            if (not socket_host.is_absolute() or socket_host.resolve() != socket_host
                    or not socket_host.is_relative_to(socket_tmp)):
                raise RuntimeError("Truss LocalPostgres socket is outside the fresh owned socket directory")
            ordinary_parameters = dict(parameters, user=bootstrap["roles"]["consumer"])
            ordinary_uri = psycopg.conninfo.make_conninfo(**ordinary_parameters)
            admin_factory = _connection_factory(psycopg, admin_uri)
            ordinary_factory = _connection_factory(psycopg, ordinary_uri)
            registration = register_note_consumer(
                admin_factory, consumer_id, bootstrap["roles"]["reader"])
            page = read_page(ordinary_factory, consumer_id, maximum_bytes=1048576)
            if (page.outcome not in ("transaction", "empty_interval")
                    or page.consumer_id != consumer_id
                    or not page.prior.content or not page.next.content
                    or hashlib.sha256(page.content).hexdigest() != page.sha256):
                raise RuntimeError("Exact original initial public source page required")

            with ordinary_factory() as connection:
                if connection.info.transaction_status != 0:
                    raise RuntimeError("Fresh catalog connection required")
                connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE")
                connection.execute("SET LOCAL statement_timeout = '2000ms'")
                host = adopt_psycopg_host(connection)
                assembly = None
                failure = None
                report_raw = view_raw = accepted_raw = None
                try:
                    tx_result = host.adopt_transaction(access_mode="read_write")
                    if not isinstance(tx_result, Ok):
                        raise RuntimeError("Public Truss catalog transaction refused")
                    assembly_result = create_reference_assembly(CatalogRuntimeConfiguration(), host)
                    if not isinstance(assembly_result, Ok):
                        raise RuntimeError("Public Truss catalog assembly refused")
                    assembly = assembly_result.value
                    revision = str(bootstrap["revision"])
                    report_result = assembly.report_in_transaction(tx_result.value, revision)
                    if not isinstance(report_result, Ok):
                        raise RuntimeError("Public accepted report read refused")
                    report_raw = report_result.value.original_bytes
                    request = assembly.catalog_view_request(definition_count="65536", encoded_bytes="16777216")
                    view_result = assembly.catalog_view_in_transaction(tx_result.value, request)
                    if not isinstance(view_result, Ok) or not hasattr(view_result.value, "view"):
                        raise RuntimeError("Public complete catalog view unavailable")
                    view_raw = view_result.value.view.original_bytes
                    source_result = assembly.accepted_document_in_transaction(
                        tx_result.value, document_id=parsed["id"], maximum_bytes=1048576)
                    if not isinstance(source_result, Ok) or not hasattr(source_result.value, "content"):
                        raise RuntimeError("Public accepted source read refused")
                    accepted_raw = source_result.value.content
                    if (accepted_raw != model or type(report_raw) is not bytes
                            or type(view_raw) is not bytes):
                        raise RuntimeError("Public Truss report/catalog/source bytes differ")
                except BaseException as error:
                    failure = error
                def detach():
                    if not host.detach():
                        raise RuntimeError("Public Truss catalog host detach refused")
                cleanup = []
                if assembly is not None:
                    cleanup.append(assembly.dispose)
                cleanup.append(detach)
                for callback in cleanup:
                    try:
                        callback()
                    except BaseException as error:
                        if failure is None:
                            failure = error
                try:
                    connection.rollback()
                except BaseException as error:
                    if failure is None:
                        failure = error
                if failure is not None:
                    raise failure

            page_payload = {
                "outcome": page.outcome,
                "sha256": page.sha256,
                "content_base64": base64.b64encode(page.content).decode("ascii"),
                "prior_base64": base64.b64encode(page.prior.content).decode("ascii"),
                "next_base64": base64.b64encode(page.next.content).decode("ascii"),
                "consumer_id": page.consumer_id,
                "registration_id": page.registration_id,
                "writer_xid": page.writer_xid,
                "safe_watermark_xid": page.safe_watermark_xid,
                "qualification": "Exact original public initial page captured before later continuation; outcome is recorded without assumption.",
            }
            report = json.loads(report_raw)
            view = json.loads(view_raw)
            if (str(report.get("revision")) != str(bootstrap["revision"])
                    or str(view.get("catalogRevision")) != str(bootstrap["revision"])
                    or base64.b64decode(bootstrap["report_original_base64"], validate=True) != report_raw):
                raise RuntimeError("Accepted report and complete catalog view revision differ")
            result = {
                "profile": PROFILE,
                "truss_version": distribution_version("umf-truss"),
                "catalog_adapter_profile": candidate_profile,
                "model": {"id": parsed["id"], "revision": "r1", "umf": parsed["umf"],
                          "sha256": hashlib.sha256(model).hexdigest()},
                "installation": bootstrap["installation"],
                "postgres_socket_directory": str(socket_host),
                "registration": {"consumer_id": registration.consumer_id,
                                 "registration_id": registration.registration_id,
                                 "feed": feed},
                "report": {"sha256": hashlib.sha256(report_raw).hexdigest(),
                           "original_base64": base64.b64encode(report_raw).decode("ascii")},
                "catalog_view": {"sha256": hashlib.sha256(view_raw).hexdigest(),
                                 "original_base64": base64.b64encode(view_raw).decode("ascii")},
                "accepted_source": {"sha256": hashlib.sha256(accepted_raw).hexdigest(),
                                    "original_base64": base64.b64encode(accepted_raw).decode("ascii")},
                "initial_source_page": page_payload,
            }
    except BaseException as error:
        primary = error
    finally:
        if old_tmpdir is None:
            os.environ.pop("TMPDIR", None)
        else:
            os.environ["TMPDIR"] = old_tmpdir
    if primary is not None:
        raise primary
    result["postgres_owner_closed_before_receipt"] = True
    result["qualification"] = (
        "Fresh local public Truss 0.0.53 installation, declared UMF 0.10.0 acceptance, "
        "consumer registration and exact report/catalog/source/page readback after owner close. "
        "No Delta publication, continuation, ACK or graph export in this stage.")
    receipt = root / "setup-stage.json"
    _write_private(receipt, (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode())
    return result

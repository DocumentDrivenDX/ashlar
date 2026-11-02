#!/usr/bin/env python3
"""Callable first stage for a reviewed R61 run; never launched by preflight."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time


REPO = Path(__file__).resolve().parents[1]
MODEL = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261101/r21-truss052-fresh-source-custody/r28-page-sequence/r28-model.umf.json"
MODEL_SHA256 = "b908c0dd6a0da65c864c0debd0c793d8ab06a933546f34050480909c5c9349f2"
LOCAL_RUNTIME_SHA256 = "7a0fce2fd418a3c239e7f6e847ac952d152d1967638251a4ebba2f5f29b90728"
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


def _local_postgres_socket(data_directory: Path) -> tuple[Path, Path]:
    """Match the Truss 0.0.53 LocalPostgres deterministic socket policy."""
    base = Path("/private/tmp") if os.sys.platform == "darwin" else Path("/tmp")
    runtime_root = base / f"truss-pg18-{os.getuid()}"
    identity = hashlib.sha256(os.fsencode(data_directory)).hexdigest()[:24]
    return runtime_root, runtime_root / identity


def _require_owned_runtime_directory(path: Path) -> None:
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode)
            or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700):
        raise RuntimeError("Truss LocalPostgres directory ownership/mode differs")


def _preinitialize_postgres(pgdata: Path, root: Path) -> dict:
    """Initialize only the fresh cluster with exact PG18 initdb before Truss opens it."""
    scope = json.loads(Path(os.environ["R61_SCOPE"]).read_text())
    artifacts = scope["artifacts"]
    manifest_path = Path(artifacts["postgresql_18_runtime_manifest"])
    manifest_bytes = manifest_path.read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != artifacts["postgresql_18_runtime_manifest_sha256"]:
        raise RuntimeError("Selected PostgreSQL runtime manifest hash differs from scope")
    manifest = json.loads(manifest_bytes)
    initdb = Path(artifacts["postgresql_18_runtime"]) / "bin" / "initdb"
    if (not initdb.is_file() or not os.access(initdb, os.X_OK)
            or hashlib.sha256(initdb.read_bytes()).hexdigest() != manifest.get("initdb_sha256")):
        raise RuntimeError("Exact selected PostgreSQL 18 initdb binary required")
    if pgdata.exists() or pgdata.is_symlink():
        raise RuntimeError("Pre-bootstrap PGDATA must be absent")
    pgdata.mkdir(mode=0o700)
    stdout_path, stderr_path = root / "pg-initdb.stdout.log", root / "pg-initdb.stderr.log"
    receipt_path = root / "pg-initdb.json"
    argv = [str(initdb), "-D", str(pgdata), "--no-sync", "--auth=trust",
            "--auth-local=trust", "--encoding=UTF8", "-U", "postgres"]
    environment = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
    result = {"profile": "ashlar-r61-pg18-dev-no-sync-bootstrap/0.1",
              "status": "failed", "argv": argv, "initdb_sha256": manifest["initdb_sha256"],
              "server_fsync_after_start": "PostgreSQL default remains enabled"}
    started = time.monotonic()
    stdout_fd = os.open(stdout_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                        getattr(os, "O_NOFOLLOW", 0), 0o600)
    stderr_fd = os.open(stderr_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                        getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(stdout_fd, "wb") as stdout, os.fdopen(stderr_fd, "wb") as stderr:
            try:
                completed = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=stdout,
                    stderr=stderr, cwd=str(root), env=environment, timeout=180, check=False)
                result["returncode"] = completed.returncode
            except subprocess.TimeoutExpired:
                result["status"] = "timeout"
                raise
    finally:
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        result["stdout_sha256"] = hashlib.sha256(stdout_path.read_bytes()).hexdigest()
        result["stdout_bytes"] = stdout_path.stat().st_size
        result["stderr_sha256"] = hashlib.sha256(stderr_path.read_bytes()).hexdigest()
        result["stderr_bytes"] = stderr_path.stat().st_size
        if result.get("returncode") == 0 and (pgdata / "PG_VERSION").is_file() \
                and (pgdata / "PG_VERSION").read_text().strip() == "18":
            result["status"] = "completed"
        _write_private(receipt_path, (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode())
    if result["status"] != "completed":
        raise RuntimeError(f"Bounded PG18 initdb bootstrap failed; receipt={receipt_path}")
    return result


def run_fresh_truss_setup_stage(*, root: Path, bun: Path, expected_adapter_profile: str) -> dict:
    """Create a clean local Truss installation, accept UMF 0.10 source, and read it.

    This function is intentionally absent from the CLI. It becomes callable only
    after exact candidate/runtime bindings and an independently reviewed native
    scope are supplied by the outer qualification runner.
    """
    root = Path(root)
    bun = Path(bun)
    if (type(expected_adapter_profile) is not str or not expected_adapter_profile
            or not root.is_absolute() or root.resolve() != root
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
    from truss import Ok, CatalogRuntimeConfiguration, create_reference_assembly, adopt_psycopg_host
    import truss.local_runtime as local_runtime
    from importlib.metadata import version as distribution_version
    import psycopg

    require_truss_psycopg_runtime()
    if distribution_version("umf-truss") != "0.0.53":
        raise RuntimeError("Exact Truss 0.0.53 candidate required")
    candidate_profile = installed_current_truss_profile()
    if candidate_profile != expected_adapter_profile:
        raise RuntimeError("Exact reviewed Truss 0.0.53 adapter profile required")
    if hashlib.sha256(Path(local_runtime.__file__).read_bytes()).hexdigest() != LOCAL_RUNTIME_SHA256:
        raise RuntimeError("Truss .53 local runtime implementation hash differs")
    LocalPostgres = local_runtime.LocalPostgres

    if root.exists():
        root_info = root.lstat()
        if (not stat.S_ISDIR(root_info.st_mode) or root_info.st_uid != os.getuid()
                or stat.S_IMODE(root_info.st_mode) != 0o700):
            raise ValueError("Exact supervisor-created private run root required")
        for child in root.iterdir():
            info = child.lstat()
            if child.name in ("home", "tmp"):
                _require_owned_runtime_directory(child)
            elif (child.name not in ("worker.log", "supervisor-result.json")
                  or not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                  or info.st_mode & 0o077):
                raise ValueError("Unexpected supervisor-created run-root entry")
    else:
        root.mkdir(mode=0o700)
    pgdata = (root / "pgdata").resolve()
    socket_root, socket_directory = _local_postgres_socket(pgdata)
    lease_file = Path(str(pgdata) + ".truss-runtime.lock")
    if socket_root.exists() or socket_root.is_symlink():
        _require_owned_runtime_directory(socket_root)
    if (pgdata.exists() or pgdata.is_symlink() or lease_file.exists() or lease_file.is_symlink()
            or socket_directory.exists() or socket_directory.is_symlink()):
        raise ValueError("Fresh PGDATA, runtime lease and deterministic Truss .53 socket required")
    postgres_bootstrap = _preinitialize_postgres(pgdata, root)

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

    primary = None
    result = None
    try:
        bootstrap = run_setup_truss_local_command(setup_path, accepted_path)
        _require_owned_runtime_directory(socket_root)
        _require_owned_runtime_directory(socket_directory)
        if ((pgdata / "postmaster.pid").exists()
                or (socket_directory / ".s.PGSQL.5432").exists()):
            raise RuntimeError("Initial Truss PostgreSQL owner did not close cleanly")
        consumer_id = "ashlar-r61-local-consumer"
        feed = "ashlar-r61-local-feed"
        with LocalPostgres(pgdata) as runtime:
            if (runtime.info.data_directory.resolve() != pgdata
                    or runtime.info.server_version.split(".")[0] != "18"):
                raise RuntimeError("Truss local runtime requires PostgreSQL 18")
            admin_uri = runtime.info.connection_uri
            parameters = psycopg.conninfo.conninfo_to_dict(admin_uri)
            socket_host = Path(parameters.get("host", ""))
            if runtime.info.socket_directory != socket_directory or socket_host != socket_directory:
                raise RuntimeError("Truss LocalPostgres selected a different deterministic socket")
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
                "postgres_bootstrap": postgres_bootstrap,
                "truss_version": distribution_version("umf-truss"),
                "catalog_adapter_profile": candidate_profile,
                "model": {"id": parsed["id"], "revision": "r1", "umf": parsed["umf"],
                          "sha256": hashlib.sha256(model).hexdigest()},
                "installation": bootstrap["installation"],
                "roles": bootstrap["roles"],
                "postgres_server_version": runtime.info.server_version,
                "postgres_runtime_version": runtime.info.runtime_version,
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
    if primary is None:
        _require_owned_runtime_directory(socket_root)
        _require_owned_runtime_directory(socket_directory)
        if ((pgdata / "postmaster.pid").exists()
                or (socket_directory / ".s.PGSQL.5432").exists()):
            primary = RuntimeError("Truss PostgreSQL owner/socket was not fully closed")
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

#!/usr/bin/env python3
"""Callable first stage for a reviewed R61 run; never launched by preflight."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
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


def _copy_verified_tree(source: Path, destination: Path) -> tuple[str, int, int]:
    """Hash each source file while copying it once; bind the exact copied bytes."""
    copied_hashes = {}
    copied_bytes = 0
    def copy_file(source_name, destination_name):
        nonlocal copied_bytes
        source_path, destination_path = Path(source_name), Path(destination_name)
        before = source_path.lstat()
        if (not stat.S_ISREG(before.st_mode) or stat.S_ISLNK(before.st_mode)
                or before.st_uid != os.getuid()):
            raise RuntimeError("PG18 bootstrap seed contains a non-owned regular file")
        source_fd = os.open(source_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        destination_fd = os.open(destination_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                 getattr(os, "O_NOFOLLOW", 0), stat.S_IMODE(before.st_mode))
        file_hash = hashlib.sha256()
        count = 0
        try:
            with os.fdopen(source_fd, "rb") as incoming, os.fdopen(destination_fd, "wb") as outgoing:
                while True:
                    block = incoming.read(1024 * 1024)
                    if not block:
                        break
                    outgoing.write(block)
                    file_hash.update(block)
                    count += len(block)
                outgoing.flush()
                os.fsync(outgoing.fileno())
            after = source_path.lstat()
        except BaseException:
            raise
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != \
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) or count != before.st_size:
            raise RuntimeError("PG18 bootstrap seed changed during copy")
        os.chmod(destination_path, stat.S_IMODE(before.st_mode))
        os.utime(destination_path, ns=(before.st_atime_ns, before.st_mtime_ns))
        relative = source_path.relative_to(source).as_posix()
        copied_hashes[relative] = file_hash.hexdigest()
        copied_bytes += count
        return str(destination_path)
    shutil.copytree(source, destination, copy_function=copy_file, symlinks=True)
    digest = hashlib.sha256()
    for relative in sorted(copied_hashes):
        if (destination / relative).is_symlink():
            raise RuntimeError("Symlink in copied PostgreSQL bootstrap seed")
        encoded = relative.encode()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(copied_hashes[relative].encode())
    if any(path.is_symlink() for path in destination.rglob("*")):
        raise RuntimeError("Symlink in copied PostgreSQL bootstrap seed")
    return digest.hexdigest(), copied_bytes, len(copied_hashes)


def _preinitialize_postgres(pgdata: Path, root: Path) -> dict:
    """Copy only a stopped, exact, fresh initdb cluster before Truss opens it."""
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
    controldata = Path(artifacts["postgresql_18_runtime"]) / "bin" / "pg_controldata"
    if (not controldata.is_file() or not os.access(controldata, os.X_OK)
            or hashlib.sha256(controldata.read_bytes()).hexdigest() != manifest.get("pg_controldata_sha256")):
        raise RuntimeError("Exact selected PostgreSQL 18 pg_controldata binary required")
    if pgdata.exists() or pgdata.is_symlink():
        raise RuntimeError("Pre-bootstrap PGDATA must be absent")
    policy = scope["postgres_bootstrap_policy"]
    seed = Path(policy["seed_pgdata"])
    seed_receipt = Path(policy["seed_initdb_receipt"])
    seed_root = Path(policy["seed_root"])
    seed_worker_failure = Path(policy["seed_worker_failure_receipt"])
    seed_supervisor_receipt = Path(policy["seed_supervisor_receipt"])
    if (not seed.is_absolute() or seed.resolve() != seed or seed.is_symlink()
            or not seed_root.is_absolute() or seed_root.resolve() != seed_root
            or seed != seed_root / "pgdata" or not seed.is_dir()
            or seed_root.is_symlink() or not seed_root.is_dir()
            or seed_receipt != seed_root / "pg-initdb.json"
            or seed_worker_failure != seed_root / "worker-failure.json"
            or seed_supervisor_receipt != seed_root / "supervisor-result.json"
            or any(path.is_symlink() or not path.is_file() for path in
                   (seed_receipt, seed_worker_failure, seed_supervisor_receipt))
            or hashlib.sha256(seed_receipt.read_bytes()).hexdigest() != policy["seed_initdb_receipt_sha256"]
            or hashlib.sha256(seed_worker_failure.read_bytes()).hexdigest() != policy["seed_worker_failure_sha256"]
            or hashlib.sha256(seed_supervisor_receipt.read_bytes()).hexdigest() != policy["seed_supervisor_receipt_sha256"]):
        raise RuntimeError("Exact retained PG18 bootstrap source and failure receipts required")
    receipt = json.loads(seed_receipt.read_text())
    if (receipt.get("status") != "completed" or receipt.get("returncode") != 0
            or receipt.get("initdb_sha256") != manifest["initdb_sha256"]
            or "--no-sync" not in receipt.get("argv", [])):
        raise RuntimeError("Retained PG18 bootstrap provenance differs")
    worker_failure = json.loads(seed_worker_failure.read_text())
    supervisor_failure = json.loads(seed_supervisor_receipt.read_text())
    if (worker_failure.get("failure_type") != "ValueError"
            or "Data directory and report must be fresh" not in worker_failure.get("failure", "")
            or worker_failure.get("source_ack_attempted") is not False
            or worker_failure.get("delta_write_state") != "not_attempted"
            or worker_failure.get("spark_started") is not False
            or supervisor_failure.get("status") != "failed_worker"
            or supervisor_failure.get("postgres_cleanup", {}).get("postgres_closed") is not True):
        raise RuntimeError("Retained seed failure evidence does not prove pre-Truss/source closure")
    seed_pid = seed / "postmaster.pid"
    seed_lease = Path(str(seed) + ".truss-runtime.lock")
    _, seed_socket = _local_postgres_socket(seed)
    if (seed_pid.exists() or seed_pid.is_symlink()
            or seed_lease.exists() or seed_lease.is_symlink()
            or seed_socket.exists() or seed_socket.is_symlink()
            or (seed / "PG_VERSION").read_text().strip() != "18"):
        raise RuntimeError("Retained bootstrap seed must be stopped and uninstalled")
    control = subprocess.run([str(controldata), str(seed)], stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5, check=True)
    lines = control.stdout.splitlines()
    system_id = next((line.split(":", 1)[1].strip() for line in lines
                      if line.startswith("Database system identifier:")), None)
    state = next((line.split(":", 1)[1].strip() for line in lines
                  if line.startswith("Database cluster state:")), None)
    if state != "shut down" or system_id != policy["seed_system_identifier"]:
        raise RuntimeError("Retained PG18 seed is not the exact stopped fresh cluster")
    started = time.monotonic()
    copied_digest, copied_bytes, copied_files = _copy_verified_tree(seed, pgdata)
    if copied_digest != policy["seed_tree_sha256"]:
        raise RuntimeError("Copied PG18 bootstrap tree differs from reviewed digest")
    copied_control = subprocess.run([str(controldata), str(pgdata)], stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5, check=True)
    copied_lines = copied_control.stdout.splitlines()
    copied_system_id = next((line.split(":", 1)[1].strip() for line in copied_lines
                             if line.startswith("Database system identifier:")), None)
    copied_state = next((line.split(":", 1)[1].strip() for line in copied_lines
                         if line.startswith("Database cluster state:")), None)
    if copied_system_id != system_id or copied_state != "shut down":
        raise RuntimeError("Copied PG18 control state differs from the stopped seed")
    result = {"profile": "ashlar-r61-pg18-stopped-bootstrap-copy/0.1",
              "status": "completed", "source_pgdata": str(seed),
              "source_tree_sha256": policy["seed_tree_sha256"], "copied_tree_sha256": copied_digest,
              "copied_file_count": copied_files, "copied_bytes": copied_bytes,
              "system_identifier": system_id, "cluster_state_before_copy": state,
              "initdb_sha256": manifest["initdb_sha256"],
              "initdb_provenance_receipt_sha256": policy["seed_initdb_receipt_sha256"],
              "source_worker_failure_receipt_sha256": policy["seed_worker_failure_sha256"],
              "source_supervisor_receipt_sha256": policy["seed_supervisor_receipt_sha256"],
              "initialization": "prior fresh PG18 initdb with --no-sync; no Truss installation/source state",
              "server_fsync_after_start": "PostgreSQL default remains enabled",
              "copy_elapsed_seconds": round(time.monotonic() - started, 3)}
    _write_private(root / "pg-bootstrap-copy.json",
        (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode())
    return result


def _run_public_truss_setup(*, config: dict, paths: dict, source: bytes,
                            origin: bytes, module_ids: tuple[str, ...]) -> dict:
    """Use public Truss install, acceptance, access, and report operations."""
    from contextlib import contextmanager
    from dataclasses import asdict
    import psycopg
    from psycopg import sql
    from truss import (LocalPostgres, Ok, CatalogDocument, CatalogRuntimeConfiguration,
        create_reference_assembly, adopt_psycopg_host, CatalogRegistration,
        CatalogUnchanged, ModuleAccessGrant)
    from ashlar_host.lifecycle import finish

    consumer, reader, writer = "ashlar_note_consumer", "ashlar_note_reader", "ashlar_note_writer"
    def value(result):
        if not isinstance(result, Ok):
            detail = getattr(result, "error", None)
            raise RuntimeError(f"Public Truss setup operation refused: {detail!r}")
        return result.value
    @contextmanager
    def interval(connection, *, timeout_ms: int = 15000):
        connection.execute("BEGIN ISOLATION LEVEL READ COMMITTED, READ WRITE")
        connection.execute(f"SET LOCAL statement_timeout = '{timeout_ms}ms'")
        host = adopt_psycopg_host(
            connection, statement_timeout_ms=timeout_ms,
            operation_timeout_ms=timeout_ms)
        assembly = None
        primary = None
        try:
            tx = value(host.adopt_transaction())
            assembly = value(create_reference_assembly(CatalogRuntimeConfiguration(), host))
            yield assembly, tx
        except BaseException as error:
            primary = error
        callbacks = ([assembly.dispose] if assembly is not None else [])
        def detach():
            if not host.detach():
                raise RuntimeError("Host detach refused; recovery required")
        callbacks.append(detach)
        finish(primary, callbacks)

    runtime = LocalPostgres(paths["data_directory"])
    entered = False
    admin = ordinary = None
    primary = None
    try:
        runtime.__enter__()
        entered = True
        parameters = psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
        admin = psycopg.connect(**parameters, autocommit=True, connect_timeout=5,
                                options="-c statement_timeout=15000")
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(consumer)))
        for role in (reader, writer):
            admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        with interval(admin, timeout_ms=60000) as (assembly, tx):
            installed = value(assembly.install_in_transaction(tx))
        admin.commit()
        admin.execute(sql.SQL("GRANT {} TO {} WITH SET FALSE").format(
            sql.Identifier(installed.registrar_role), sql.Identifier(consumer)))
        ordinary_parameters = dict(parameters, user=consumer)
        ordinary = psycopg.connect(**ordinary_parameters, autocommit=True, connect_timeout=5,
                                   options="-c statement_timeout=15000")
        with interval(ordinary) as (assembly, tx):
            prepared = assembly.prepare_catalog(
                (CatalogDocument(config["document_id"], config["document_revision"], source),),
                bun_executable=str(paths["bun"]), source_version=json.loads(source)["umf"])
            accepted = value(assembly.accept_in_transaction(
                tx, prepared, request_key=config["request_key"], origin_bytes=origin))
            if not isinstance(accepted, (CatalogRegistration, CatalogUnchanged)):
                raise RuntimeError("Original document acceptance refused")
        ordinary.commit()
        with interval(admin) as (assembly, tx):
            value(assembly.configure_module_access_in_transaction(
                tx, tuple(ModuleAccessGrant(config["document_id"], module_id, reader, writer)
                          for module_id in module_ids)))
        admin.commit()
        for role in (reader, writer):
            admin.execute(sql.SQL("GRANT {} TO {} WITH SET FALSE").format(
                sql.Identifier(role), sql.Identifier(consumer)))
        with interval(ordinary) as (assembly, tx):
            status = value(assembly.status_in_transaction(tx))
            report = value(assembly.report_in_transaction(tx, accepted.revision))
            if not status.installed:
                raise RuntimeError("Current installed catalog required")
            result = {"data_directory": str(paths["data_directory"]),
                "installation": asdict(installed), "caller_commits_confirmed": True,
                "catalog_status": asdict(status), "registration_id": accepted.registration_id,
                "revision": accepted.revision, "roles": {"consumer": consumer, "reader": reader,
                "writer": writer, "registrar": installed.registrar_role},
                "report_original_base64": base64.b64encode(report.original_bytes).decode("ascii"),
                "scope": "Fresh local PG18 public Truss install and declared UMF 0.10 acceptance; no remote/feed/ACK/schema-evolution qualification."}
        ordinary.commit()
    except BaseException as error:
        primary = error
    callbacks = []
    for connection in (ordinary, admin):
        if connection is not None:
            callbacks.extend((connection.rollback, connection.close))
    if entered:
        callbacks.append(lambda: runtime.__exit__(None, None, None))
    finish(primary, callbacks)
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
    postgres_fsync = None
    try:
        bootstrap = _run_public_truss_setup(config=setup, paths={"data_directory": pgdata,
            "bun": bun}, source=model, origin=origin,
            module_ids=tuple(module["id"] for module in parsed["modules"]))
        _require_owned_runtime_directory(socket_root)
        _require_owned_runtime_directory(socket_directory)
        if ((pgdata / "postmaster.pid").exists()
                or (socket_directory / ".s.PGSQL.5432").exists()):
            raise RuntimeError("Initial Truss PostgreSQL owner did not close cleanly")
        consumer_id = "ashlar-r61-local-consumer"
        feed = "ashlar-r61-local-feed"
        with LocalPostgres(pgdata) as runtime:
            with psycopg.connect(runtime.info.connection_uri, autocommit=True, connect_timeout=5) as health:
                postgres_fsync = health.execute("SHOW fsync").fetchone()[0]
            if postgres_fsync != "on":
                raise RuntimeError("PostgreSQL server fsync must be enabled after bootstrap")
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
                "postgres_fsync": postgres_fsync,
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

#!/usr/bin/env python3
"""Review-gated r7 .48→.51 clone migration and local feed attempt.

This worker refuses the current design-only packet. It is not an approval and
must only be dispatched after Sol review and a new exact one-shot scope/approval.
"""
from __future__ import annotations

import hashlib
import importlib.metadata as metadata
import importlib.resources
import inspect
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import time

HERE = Path(__file__).resolve().parent
SCOPE_PATH = HERE / "r7-truss051-migration-scope.json"
SEED_PATH = HERE / "fresh-local-seed-request.json"
SUPERVISOR_PATH = HERE / "r7_truss051_supervisor.py"
SOURCE = Path("/private/tmp/ashlar-truss048-fresh-local-20261031-r6/pgdata")
EXPECTED_PATHS = {
    "output_root": "/private/tmp/ashlar-r7-truss051-continuation-20261031",
    "supervisor_root": "/private/tmp/ashlar-r7-truss051-continuation-20261031-supervisor",
    "cold_backup_pgdata": "/private/tmp/ashlar-r7-truss051-continuation-20261031/cold-backup-pgdata",
    "migration_clone_pgdata": "/private/tmp/ashlar-r7-truss051-continuation-20261031/migration-clone-pgdata",
    "recovery_registry": "/private/tmp/ashlar-r7-truss051-continuation-20261031/truss-recovery-registry.sqlite3",
    "receipt_root": "/private/tmp/ashlar-r7-truss051-continuation-20261031/receipts",
}
PG18_DIRS = (
    "global", "pg_wal/archive_status", "pg_wal/summaries", "pg_commit_ts",
    "pg_dynshmem", "pg_notify", "pg_serial", "pg_snapshots", "pg_subtrans",
    "pg_twophase", "pg_multixact", "pg_multixact/members",
    "pg_multixact/offsets", "base", "base/1", "pg_replslot", "pg_tblspc",
    "pg_stat", "pg_stat_tmp", "pg_xact", "pg_logical",
    "pg_logical/snapshots", "pg_logical/mappings",
)
WHEEL_SHA = "eb59141c48e94bb93ab7053e9e8725012eee1749e4432e9c71804d23abbdff57"
SOURCE_MANIFEST = "c4339ac7f09ab5a004c272ab00962107a970938004c1765ec4163e4d5ca9b6f0"
TARGET_MANIFEST = "288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4"
MAX_BYTES = 512 * 1024 * 1024
MAX_SECONDS = 900


class StopRun(RuntimeError):
    pass


class DuplicateKey(ValueError):
    pass


def pairs_unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKey(key)
        result[key] = value
    return result


def read_json(path: Path):
    return json.loads(path.read_text(), object_pairs_hook=pairs_unique)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def valid_role_name(value: str) -> bool:
    return (type(value) is str and bool(value) and "\0" not in value
            and not value.startswith("pg_") and len(value.encode("utf8")) <= 63)


def write_new(path: Path, value) -> str:
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    return sha(raw)


def write_bytes_new(path: Path, raw: bytes) -> str:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    return sha(raw)


def tree_digest(root: Path) -> tuple[int, str, int]:
    digest = hashlib.sha256(); total = count = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        dirs.sort(); files.sort()
        for name in dirs:
            path = Path(base) / name
            if path.is_symlink() or not path.is_dir():
                raise StopRun(f"non-directory/symlink in PGDATA: {path}")
            digest.update(b"D\0" + path.relative_to(root).as_posix().encode() + b"\0")
        for name in files:
            path = Path(base) / name
            if path.is_symlink() or not path.is_file():
                raise StopRun(f"special/symlink file in PGDATA: {path}")
            raw = path.read_bytes(); total += len(raw); count += 1
            digest.update(b"F\0" + path.relative_to(root).as_posix().encode() + b"\0"
                          + str(len(raw)).encode() + b"\0" + hashlib.sha256(raw).digest())
            if total > MAX_BYTES:
                raise StopRun("attempt or retained PostgreSQL tree exceeded 512 MiB")
    return total, digest.hexdigest(), count


def pg18_preflight(path: Path) -> tuple[int, str, int]:
    if not path.is_absolute() or path.resolve() != path or path.is_symlink() or not path.is_dir():
        raise StopRun("canonical existing PGDATA directory required")
    for base, dirs, files in os.walk(path, followlinks=False):
        for name in [".", *dirs, *files]:
            entry = Path(base) if name == "." else Path(base) / name
            info = entry.lstat()
            if (stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) & 0o077):
                raise StopRun(f"PGDATA ownership/private-mode check failed: {entry}")
    if (path / "PG_VERSION").is_symlink() or (path / "PG_VERSION").read_text().strip() != "18":
        raise StopRun("PostgreSQL 18 PG_VERSION required")
    if (path / "postmaster.pid").exists():
        raise StopRun("postmaster.pid present; refusing cold copy")
    missing = [item for item in PG18_DIRS if not (path / item).is_dir() or (path / item).is_symlink()]
    if missing:
        raise StopRun(f"incomplete PostgreSQL 18 directory layout: {missing!r}")
    return tree_digest(path)


def check_process_custody(path: Path) -> dict:
    """Conservative immediate pre-copy process check; inability to inspect stops."""
    result = subprocess.run(["/bin/ps", "-Ao", "pid=,command="], check=True,
                            text=True, capture_output=True, timeout=5)
    matches = []
    for line in result.stdout.splitlines():
        low = line.lower()
        if str(path) in line and ("postgres" in low or "pg_ctl" in low):
            matches.append(line.strip())
    if matches:
        raise StopRun(f"process command references r6 PGDATA: {matches[:4]!r}")
    return {"status": "passed", "inventory_command": "/bin/ps -Ao pid=,command=",
            "matching_processes": 0, "source_path": str(path)}


def require_ok(result, label):
    from truss import Ok
    if not isinstance(result, Ok):
        raise StopRun(f"{label} refused: {type(result).__name__} {getattr(result, 'error', None)!r}"[:1200])
    return result.value


def validate_supervisor_handshake(output_root: Path, supervisor_root: Path) -> None:
    """Accept the private supervisor-created root and its two open log files."""
    if output_root.exists() or output_root.is_symlink():
        raise StopRun("fresh worker output root must be absent")
    if (supervisor_root.is_symlink() or not supervisor_root.is_dir()
            or supervisor_root.resolve() != supervisor_root):
        raise StopRun("supervisor-created root must be a canonical directory")
    root_stat = supervisor_root.lstat()
    if (root_stat.st_uid != os.getuid()
            or stat.S_IMODE(root_stat.st_mode) & 0o077):
        raise StopRun("supervisor-created root must be user-owned and private")
    expected_logs = {"child.stdout.log", "child.stderr.log"}
    entries = {item.name for item in supervisor_root.iterdir()}
    if entries != expected_logs:
        raise StopRun(f"unexpected supervisor pre-child entries: {sorted(entries)!r}")
    for name in expected_logs:
        entry = supervisor_root / name
        info = entry.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise StopRun(f"supervisor log must be an owned regular file: {entry}")


def main() -> None:
    scope = read_json(SCOPE_PATH)
    # This one-shot candidate is hash-bound to the reviewed local authorization.
    if scope.get("status") != "approved_one_shot_native" or scope.get("authorization") != "one_run_local_native_lifecycle":
        raise StopRun("review gate closed: scope is design-only; no native dispatch")
    if scope.get("paths") != EXPECTED_PATHS or scope.get("source", {}).get("existing_pgdata") != str(SOURCE):
        raise StopRun("exact reviewed source and output paths required")
    if (scope.get("runtime", {}).get("python_path") != "/private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/bin/python"
            or scope["runtime"].get("pythonpath") != "/private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/lib/python3.12/site-packages"):
        raise StopRun("exact pinned Truss .51 runtime paths required")
    if scope.get("limits") != {
        "attempts":1,"automatic_retries":0,"wall_seconds":900,
        "process_group_rss_bytes":2147483648,"retained_plus_attempt_bytes":2147483648,
        "attempt_root_bytes":536870912,"migration_preservation_bytes":268435456,
        "combined_log_bytes":16777216,"parallelism":"sequential local only",
        "cloud":False,"cleanup":False}:
        raise StopRun("exact reviewed wall/RSS/storage limits required")
    approval_path = HERE / "r7-truss051-native-approval.json"
    approval = read_json(approval_path)
    scope_raw = SCOPE_PATH.read_bytes()
    if approval.get("scope_sha256") != sha(scope_raw):
        raise StopRun("approval scope digest mismatch")
    if approval.get("worker_sha256") != sha(Path(__file__).read_bytes()):
        raise StopRun("approval worker digest mismatch")
    if approval.get("supervisor_sha256") != sha(SUPERVISOR_PATH.read_bytes()):
        raise StopRun("approval supervisor digest mismatch")
    if approval.get("attempts") != 1 or approval.get("cloud") is not False:
        raise StopRun("exact one local attempt required")
    if os.getpgrp() != os.getpid():
        raise StopRun("worker must run as a supervised process-group leader")

    # Pure input/API validation runs before output creation or database effects.
    seed_raw = SEED_PATH.read_bytes(); seed = json.loads(seed_raw, object_pairs_hook=pairs_unique)
    expected = scope["source"]["catalog_accepted"]
    if (sha(seed_raw) != scope["consumer_and_model"]["seed_request_sha256"]
            or len(seed["operations"]) != 7 or seed["document_id"] != expected["document_id"]):
        raise StopRun("exact r6 seed bytes/identity required")
    if metadata.version("umf-truss") != "0.0.51":
        raise StopRun("pinned Truss .51 required")
    dist = metadata.distribution("umf-truss")
    direct = json.loads(dist.read_text("direct_url.json") or "{}")
    if direct.get("archive_info", {}).get("hashes", {}).get("sha256") != WHEEL_SHA:
        raise StopRun("pinned Truss .51 wheel digest mismatch")
    manifest = importlib.resources.files("truss").joinpath("_catalog_runtime/manifest.json").read_bytes()
    if sha(manifest) != TARGET_MANIFEST:
        raise StopRun("pinned .51 runtime manifest mismatch")
    from truss._migration_recipe import load_recipe, plan_route
    from truss.migrations import RuntimeMigrations
    from truss.recovery_registry import HostRecoveryRegistry
    from truss import LocalPostgres
    route_manifest, _, outer_sha = load_recipe()
    if outer_sha != scope["runtime"]["outer_migration_index_sha256"]:
        raise StopRun("pinned outer migration index mismatch")
    route = tuple({k: edge[k] for k in ("source", "target", "artifact")}
                   for edge in plan_route(route_manifest, SOURCE_MANIFEST))
    if route != tuple(scope["runtime"]["migration_edges"]):
        raise StopRun("pinned .48→.51 migration route mismatch")
    for name, keys in {"prepare": {"attempt_id"}, "apply": {"reference"},
                       "reconcile": {"reference"}, "receipt": {"reference"}}.items():
        if not keys <= set(inspect.signature(getattr(RuntimeMigrations, name)).parameters):
            raise StopRun(f"public migration API arguments changed: {name}")
    if not {"connect", "recovery_registry", "maximum_preservation_bytes",
            "operation_timeout_ms", "statement_timeout_ms"} <= set(inspect.signature(RuntimeMigrations).parameters):
        raise StopRun("public migration runtime constructor arguments changed")
    if "path" not in inspect.signature(HostRecoveryRegistry).parameters:
        raise StopRun("restart-durable migration registry constructor changed")
    if "data_directory" not in inspect.signature(LocalPostgres).parameters:
        raise StopRun("LocalPostgres constructor changed")
    from truss import ModuleAccessGrant
    grant_data = scope["consumer_and_model"]["module_grant"]
    grant = ModuleAccessGrant(grant_data["document_id"], grant_data["module_id"],
                              grant_data["reader_role"], grant_data["writer_role"])
    registrar_role = scope["consumer_and_model"]["registrar_login_role"]
    role_names = (scope["consumer_and_model"]["reader_login_role"], grant.reader_role,
                  grant.writer_role, registrar_role)
    for role in role_names:
        if not valid_role_name(role):
            raise StopRun(f"unbounded/unsafe role identity: {role!r}")
    if len(set(role_names)) != len(role_names):
        raise StopRun("registrar, reader and module role identifiers must be distinct")
    from truss import CatalogReferenceAssembly
    required = {
        "registrar_status_in_transaction": set(),
        "accepted_document_in_transaction": {"document_id", "maximum_bytes"},
        "configure_module_access_in_transaction": {"grants"},
        "register_feed_consumer_in_transaction": {"consumer_id", "reader_role"},
        "prepare_group_in_transaction": {"records", "relationships"},
        "apply_in_transaction": {"input", "request", "documents"},
        "read_feed_in_transaction": {"consumer_id", "maximum_facts", "maximum_bytes"},
    }
    for name, keys in required.items():
        if not keys <= set(inspect.signature(getattr(CatalogReferenceAssembly, name)).parameters):
            raise StopRun(f"public API arguments changed: {name}")
    from truss import CatalogDocument, prepare_acceptance
    model_path = HERE.parents[5] / "docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r4-sdk010-initialize-only/inputs/typed-graph-model.umf.json"
    model_source = model_path.read_bytes()
    old_token = b'"id": "typed-graph-example"'
    new_token = b'"id": "ashlar-local-dev-typed-graph-r6-20261031"'
    if model_source.count(old_token) != 1:
        raise StopRun("frozen model identity substitution is not unique")
    model = model_source.replace(old_token, new_token)
    if sha(model) != expected["document_sha256"]:
        raise StopRun("pure r6 model derivation differs from accepted source hash")
    prepared_acceptance = prepare_acceptance((CatalogDocument(
        expected["document_id"], expected["document_revision"], model),))
    if prepared_acceptance is None:
        raise StopRun("pure .51 accepted-document preparation failed")

    root = Path(scope["paths"]["output_root"])
    supervisor_root = Path(scope["paths"]["supervisor_root"])
    backup = Path(scope["paths"]["cold_backup_pgdata"])
    clone = Path(scope["paths"]["migration_clone_pgdata"])
    registry_path = Path(scope["paths"]["recovery_registry"])
    receipt_root = Path(scope["paths"]["receipt_root"])
    validate_supervisor_handshake(root, supervisor_root)
    if any(p.exists() or p.is_symlink() for p in (backup, clone, registry_path, receipt_root)):
        raise StopRun("all attempt destinations must be absent")
    if registry_path.exists():
        raise StopRun("new restart-durable migration registry must be absent")

    deadline = time.monotonic() + MAX_SECONDS
    def alarm(_sig, _frame):
        raise TimeoutError("900-second one-shot deadline")
    signal.signal(signal.SIGALRM, alarm); signal.alarm(MAX_SECONDS)
    root.mkdir(mode=0o700); os.chmod(root, 0o700)
    receipt_root.mkdir(mode=0o700)
    completed: list[str] = []
    effects_may_remain = False
    try:
        source_bytes, source_tree_sha, source_files = pg18_preflight(SOURCE)
        frozen = scope["source"]
        if (source_bytes != frozen["pgdata_bytes_at_packet_review"]
                or source_tree_sha != frozen["pgdata_tree_sha256_at_packet_review"]
                or source_files != frozen["pgdata_files_at_packet_review"]):
            raise StopRun("retained r6 PGDATA differs from reviewed tree")
        custody = check_process_custody(SOURCE)
        required_free = (2 * source_bytes
                         + scope["limits"]["attempt_root_bytes"])
        free_bytes = shutil.disk_usage(root.parent).free
        if free_bytes < required_free:
            raise StopRun(f"insufficient free disk before cold copies: {free_bytes} < {required_free}")
        write_new(receipt_root / "r6-source-preflight.json", {
            "path": str(SOURCE), "bytes": source_bytes, "files": source_files,
            "tree_sha256": source_tree_sha, "pg18_layout_directories": len(PG18_DIRS),
            "owner_uid": os.getuid(), "private_directory_mode": "no group/world permissions",
            "process_custody": custody, "free_bytes_before_two_copies": free_bytes,
            "required_free_bytes": required_free, "original_mutated": False})

        # Two cold copies: immutable rollback backup and a separate migration clone.
        shutil.copytree(SOURCE, backup, symlinks=False, copy_function=shutil.copy2)
        shutil.copytree(SOURCE, clone, symlinks=False, copy_function=shutil.copy2)
        for label, path in (("cold-backup", backup), ("migration-clone", clone)):
            actual = pg18_preflight(path)
            if actual != (source_bytes, source_tree_sha, source_files):
                raise StopRun(f"{label} differs from exact stopped r6 source")
        write_new(receipt_root / "cold-copy-verified.json", {
            "source_tree_sha256": source_tree_sha,
            "cold_backup_tree_sha256": tree_digest(backup)[1],
            "migration_clone_tree_sha256": tree_digest(clone)[1],
            "source_bytes_each": source_bytes, "source_files_each": source_files,
            "original_mutated": False})
        completed.append("verified_cold_backup_and_migration_clone")
        clone_custody = check_process_custody(clone)
        if pg18_preflight(SOURCE) != (source_bytes, source_tree_sha, source_files):
            raise StopRun("original r6 PGDATA changed before migration clone start")
        write_new(receipt_root / "migration-clone-prestart-custody.json", {
            "process_custody": clone_custody, "clone_layout_directories": len(PG18_DIRS),
            "source_tree_sha256": source_tree_sha, "original_source_unchanged": True})
        if time.monotonic() > deadline:
            raise TimeoutError("deadline before PostgreSQL start")

        import psycopg
        from truss import (CatalogRuntimeConfiguration, Ok,
                           adopt_psycopg_host, create_reference_assembly)
        from truss.recovery_registry import HostRecoveryRegistry
        runtime = LocalPostgres(clone)
        effects_may_remain = True
        write_new(receipt_root / "postgres-start-intent.json", {
            "data_directory": str(clone), "source_pgdata_untouched": str(SOURCE),
            "owned_start": True, "stop_on_context_exit": True})
        with runtime as pg:
            if pg.info.server_version != "18.6" or Path(pg.info.data_directory) != clone:
                raise StopRun("local runtime did not start exact PG18.6 migration clone")
            write_new(receipt_root / "migration-started.json", {
                "data_directory": str(pg.info.data_directory), "server_version": pg.info.server_version,
                "truss_version": "0.0.51", "original_source_path": str(SOURCE),
                "original_source_mutated": False})

            params = psycopg.conninfo.conninfo_to_dict(pg.info.connection_uri)
            connect = lambda: psycopg.connect(**params, autocommit=True, connect_timeout=5)
            if registry_path.exists():
                raise StopRun("migration registry path unexpectedly appeared before creation")
            write_new(receipt_root / "registry-pre-effect.json", {
                "path": str(registry_path), "present_before_creation": False,
                "durability": "restart_durable_host_recovery_registry"})
            registry = HostRecoveryRegistry(registry_path)
            migration = RuntimeMigrations(connect, registry,
                maximum_preservation_bytes=scope["migration"]["maximum_preservation_bytes"],
                operation_timeout_ms=scope["migration"]["operation_timeout_ms"],
                statement_timeout_ms=scope["migration"]["statement_timeout_ms"])
            status = migration.status(); backup_meta = migration.backup_metadata()
            write_new(receipt_root / "migration-status.json", {"status": status,
                "backup_metadata": backup_meta, "migration_invoked": False})
            if (status.get("state") != "upgrade_required"
                    or status.get("manifestSha256") != SOURCE_MANIFEST
                    or status.get("target") != TARGET_MANIFEST
                    or backup_meta.get("sourceManifestSha256") != SOURCE_MANIFEST
                    or backup_meta.get("context") != status.get("context")):
                raise StopRun("public migration status/backup metadata do not bind exact .48→.51 source")
            prepared = migration.prepare(attempt_id=scope["migration"]["attempt_id"])
            if (prepared.outcome != "prepared" or prepared.source != SOURCE_MANIFEST
                    or prepared.target != TARGET_MANIFEST
                    or prepared.attempt_id != scope["migration"]["attempt_id"]):
                raise StopRun("public migration prepare did not bind exact route and attempt")
            retained = registry.inspect(prepared.reference, status["context"])
            write_new(receipt_root / "migration-prepared.json", {
                "reference": prepared.reference, "attempt_id": prepared.attempt_id,
                "source": prepared.source, "target": prepared.target,
                "outcome": prepared.outcome,
                "original_obligation": retained["obligation"],
                "observations_before_apply": retained["observations"]})
            write_new(receipt_root / "migration-apply-intent.json", {
                "reference": prepared.reference, "attempt_id": prepared.attempt_id,
                "single_apply": True})
            apply_error = None
            try:
                applied = migration.apply(prepared.reference)
            except BaseException as error:
                apply_error = f"{type(error).__name__}: {error}"[:1200]
                applied = None
            if applied is not None:
                write_new(receipt_root / "migration-apply-result.json", {
                    "outcome": applied.outcome, "reference": applied.reference,
                    "attempt_id": applied.attempt_id, "source": applied.source,
                    "target": applied.target, "detail": applied.detail})
            final = applied
            if (applied is None or applied.outcome in
                    ("commit_unknown", "recovery_required", "committed_unverified")):
                write_new(receipt_root / "migration-reconcile-intent.json", {
                    "reference": prepared.reference, "attempt_id": prepared.attempt_id,
                    "single_reconcile": True, "apply_error": apply_error})
                resolved = migration.reconcile(prepared.reference)
                write_new(receipt_root / "migration-reconciled.json", {
                    "outcome": resolved.outcome, "reference": resolved.reference,
                    "detail": resolved.detail})
                final = resolved
            if (final.outcome not in ("migrated", "already_applied")
                    or not final.current_verification
                    or final.current_verification.get("state") != "current"):
                raise StopRun(f".51 migration not verified current: {final.outcome!r}")
            receipt = migration.receipt(prepared.reference)
            write_new(receipt_root / "migration-receipt.json", {
                "receipt": receipt, "current_verification": final.current_verification,
                "outcome": final.outcome, "reference": prepared.reference})
            completed.append("migration_route_applied_and_receipt_observed")

            # Public .51 identity proof gates every later mutation.
            rlogin = registrar_role
            read_params = dict(params); read_params["user"] = rlogin
            with psycopg.connect(**read_params, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    conn.execute("SET TRANSACTION READ ONLY")
                    host = require_ok(adopt_psycopg_host(conn, statement_timeout_ms=5000,
                        operation_timeout_ms=30000), "public readback host")
                    assembly = None
                    try:
                        tx = require_ok(host.adopt_transaction(access_mode="read_only"), "public readback transaction")
                        assembly = require_ok(create_reference_assembly(CatalogRuntimeConfiguration(), host), "public .51 assembly")
                        state = require_ok(assembly.status_in_transaction(tx), "public .51 status")
                        registrar = require_ok(assembly.registrar_status_in_transaction(tx), "public .51 registrar status")
                        doc = require_ok(assembly.accepted_document_in_transaction(tx,
                            document_id=expected["document_id"], maximum_bytes=1_048_576), "public .51 accepted document")
                        report = require_ok(assembly.report_in_transaction(tx, 1), "public .51 report")
                        view_request = assembly.catalog_view_request(definition_count="65536", encoded_bytes="16777216")
                        view_outcome = require_ok(assembly.catalog_view_in_transaction(tx, view_request), "public .51 catalog view")
                        from truss import CatalogViewAvailable
                        if not isinstance(view_outcome, CatalogViewAvailable):
                            raise StopRun("complete public catalog view unavailable")
                        view_wire = json.loads(view_outcome.view.original_bytes)
                        if (not state.installed or state.revision != 1
                                or registrar.can_register is not True
                                or report.revision != 1
                                or view_wire.get("scope") != {"kind":"complete"}
                                or str(view_wire.get("catalogRevision")) != "1"
                                or doc.document_id != expected["document_id"]
                                or doc.document_revision != expected["document_revision"]
                                or doc.registration_id != expected["registration_id"]
                                or doc.catalog_revision != expected["catalog_revision"]
                                or doc.sha256 != expected["document_sha256"] or sha(doc.content) != expected["document_sha256"]):
                            raise StopRun("post-migration accepted document/registration identity mismatch")
                        write_bytes_new(receipt_root / "accepted-r6-document-after-migration.umf.json", doc.content)
                        write_bytes_new(receipt_root / "catalog-report-after-migration.json", report.original_bytes)
                        write_bytes_new(receipt_root / "complete-catalog-view-after-migration.json", view_outcome.view.original_bytes)
                        write_new(receipt_root / "accepted-document-readback.json", {
                            "document_id": doc.document_id, "document_revision": doc.document_revision,
                            "registration_id": doc.registration_id, "catalog_revision": doc.catalog_revision,
                            "document_sha256": sha(doc.content), "report_sha256": sha(report.original_bytes),
                            "complete_view_sha256": sha(view_outcome.view.original_bytes),
                            "public_api": "Truss .51 accepted_document_in_transaction",
                            "status_revision": state.revision,
                            "registrar_can_register": registrar.can_register})
                    finally:
                        if assembly is not None: assembly.dispose()
                        if not host.detach():
                            raise StopRun("Truss host detach refused")
            completed.append("public_51_document_registration_readback")

            # Safe role creation, grant configuration and principal readback.
            reader = scope["consumer_and_model"]["reader_login_role"]
            module_reader = scope["consumer_and_model"]["module_reader_role"]
            module_writer = scope["consumer_and_model"]["module_writer_role"]
            admin_params = dict(params); admin_params["user"] = "postgres"
            write_new(receipt_root / "module-access-intent.json", {
                "document_id": expected["document_id"], "module_id": "m",
                "reader_role": module_reader, "writer_role": module_writer,
                "roles_absence_preflight_passed": True})
            with psycopg.connect(**admin_params, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    role_rows = conn.execute("SELECT rolname FROM pg_catalog.pg_roles WHERE rolname = ANY(%s)",
                                             ([reader, module_reader, module_writer],)).fetchall()
                    consumer_rows = conn.execute("SELECT consumer_id FROM truss.runtime_feed_consumer WHERE consumer_id=%s",
                                                 (scope["consumer_and_model"]["consumer_id"],)).fetchall()
                    module_rows = conn.execute("SELECT document_id,module FROM truss.module_access WHERE document_id=%s AND module=%s",
                                               (expected["document_id"], "m")).fetchall()
                    if role_rows or consumer_rows or module_rows:
                        raise StopRun("fresh r6 continuation role/consumer/module identities are not absent")
                    conn.execute(psycopg.sql.SQL("CREATE ROLE {} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS").format(psycopg.sql.Identifier(reader)))
                    for role in (module_reader, module_writer):
                        conn.execute(psycopg.sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS").format(psycopg.sql.Identifier(role)))
                    conn.execute(psycopg.sql.SQL("GRANT {} TO {} WITH INHERIT TRUE, SET FALSE").format(psycopg.sql.Identifier(module_reader), psycopg.sql.Identifier(reader)))
                    conn.execute(psycopg.sql.SQL("GRANT {} TO {} WITH INHERIT TRUE, SET FALSE").format(psycopg.sql.Identifier(module_writer), psycopg.sql.Identifier(rlogin)))
                    host = require_ok(adopt_psycopg_host(conn, statement_timeout_ms=5000,
                        operation_timeout_ms=30000), "module grant host")
                    assembly = None
                    try:
                        tx = require_ok(host.adopt_transaction(), "module grant transaction")
                        assembly = require_ok(create_reference_assembly(CatalogRuntimeConfiguration(), host), "public .51 assembly")
                        configured = require_ok(assembly.configure_module_access_in_transaction(tx, (grant,)), "public module access configuration")
                        if type(configured).__name__ != "PendingModuleAccessConfiguration":
                            raise StopRun(f"module grant refused: {configured!r}")
                    finally:
                        if assembly is not None: assembly.dispose()
                        if not host.detach():
                            raise StopRun("Truss host detach refused")
                    role_state = conn.execute("""SELECT rolname,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,
                      rolreplication,rolbypassrls FROM pg_catalog.pg_roles WHERE rolname=ANY(%s) ORDER BY rolname""",
                      ([reader, module_reader, module_writer],)).fetchall()
                    membership = conn.execute("""SELECT member_role.rolname,grant_role.rolname,m.inherit_option,m.set_option
                      FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles member_role ON member_role.oid=m.member
                      JOIN pg_catalog.pg_roles grant_role ON grant_role.oid=m.roleid
                      WHERE (member_role.rolname,grant_role.rolname) IN ((%s,%s),(%s,%s)) ORDER BY 1,2""",
                      (reader,module_reader,rlogin,module_writer)).fetchall()
                    expected_states = [(module_reader,False,False,False,False,False,False),
                                       (module_writer,False,False,False,False,False,False),
                                       (reader,True,False,False,False,False,False)]
                    if role_state != expected_states or membership != [(reader,module_reader,True,False),(rlogin,module_writer,True,False)]:
                        raise StopRun("fresh role attributes or INHERIT TRUE/SET FALSE membership readback mismatch")
                    policy = conn.execute("SELECT reader_role,writer_role FROM truss.module_access WHERE document_id=%s AND module=%s",
                        (expected["document_id"],"m")).fetchone()
                    if policy != (module_reader,module_writer):
                        raise StopRun("exact module access policy readback mismatch")
                    role_receipt = {
                        "roles": role_state, "memberships": membership,
                        "module_access": {"document_id": expected["document_id"],"module_id":"m",
                                          "reader_role":policy[0],"writer_role":policy[1]}}
            write_new(receipt_root / "module-roles-and-access.json", role_receipt)
            completed.append("safe_roles_and_module_access_configured")

            # Consumer enrollment is installation-administrator only and begins at genesis.
            write_new(receipt_root / "consumer-registration-intent.json", {
                "consumer_id": scope["consumer_and_model"]["consumer_id"],
                "reader_role": reader, "checkpoint_start": "genesis"})
            with psycopg.connect(**admin_params, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    host = require_ok(adopt_psycopg_host(conn, statement_timeout_ms=5000, operation_timeout_ms=30000), "consumer host")
                    assembly = None
                    try:
                        tx = require_ok(host.adopt_transaction(), "consumer transaction")
                        assembly = require_ok(create_reference_assembly(CatalogRuntimeConfiguration(), host), "public .51 assembly")
                        consumer = require_ok(assembly.register_feed_consumer_in_transaction(tx,
                            consumer_id=scope["consumer_and_model"]["consumer_id"], reader_role=reader),
                            "public genesis consumer registration")
                        consumer_receipt = {
                            "consumer_id": consumer.consumer_id, "registration_id": consumer.registration_id,
                            "checkpoint_sha256": sha(consumer.checkpoint.content),
                            "checkpoint_bytes": len(consumer.checkpoint.content), "start": "genesis"}
                    finally:
                        if assembly is not None: assembly.dispose()
                        if not host.detach():
                            raise StopRun("Truss host detach refused")
            write_new(receipt_root / "feed-consumer-genesis.json", consumer_receipt)
            completed.append("consumer_registered_at_genesis")

            # One seven-operation semantic group, through public Truss .51 API.
            from truss.contracts import MapEntry, MapValue, NullValue, TextValue
            from truss.groups import (AliasObjectReference, AuthoredValue, CreateEdge, CreateObject,
                CurrentCatalog, GroupSemanticInput, RequestIdentity, RequestPresent,
                RootlessOwnership, TextOrderKey)
            registrar_params = dict(params); registrar_params["user"] = rlogin
            write_new(receipt_root / "seed-apply-intent.json", {
                "request_key": seed["request_key"], "seed_request_sha256": sha(seed_raw),
                "operation_count": 7})
            # Use the accepted .51 bytes and preserve the exact one-token r6 derivation.
            from truss import CatalogDocument
            document = CatalogDocument(expected["document_id"], expected["document_revision"], model)
            with psycopg.connect(**registrar_params, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    identity = conn.execute("SELECT session_user,current_user,current_setting('role')").fetchone()
                    if identity != (rlogin,rlogin,"none"):
                        raise StopRun(f"unsafe registrar session identity: {identity!r}")
                    host = require_ok(adopt_psycopg_host(conn, statement_timeout_ms=5000, operation_timeout_ms=30000), "seed host")
                    assembly = None
                    try:
                        tx = require_ok(host.adopt_transaction(), "seed transaction")
                        assembly = require_ok(create_reference_assembly(CatalogRuntimeConfiguration(), host), "public .51 assembly")
                        group = require_ok(assembly.prepare_group_in_transaction(tx,
                            records=((expected["document_id"],"m","Person"),(expected["document_id"],"m","Place")),
                            relationships=((expected["document_id"],"m","knows"),(expected["document_id"],"m","located"))),
                            "public group preparation")
                        type_pins = {ref.authored_identity:ref.definition_pin for ref in group.types if ref is not None}
                        rel_pins = {ref.authored_identity:ref.definition_pin for ref in group.relationships if ref is not None}
                        operations = []
                        for item in seed["operations"]:
                            if item["op"] == "create_object":
                                values = tuple(AuthoredValue(name=name,value=(NullValue() if val["kind"]=="null"
                                    else TextValue(kind=val["kind"],text=val["text"]))) for name,val in item["values"].items())
                                operations.append(CreateObject(type_definition_pin=type_pins[item["type"]],values=values,
                                    ownership=RootlessOwnership(),alias=item["alias"]))
                            elif item["op"] == "create_edge":
                                operations.append(CreateEdge(relationship_definition_pin=rel_pins[item["relationship"]],
                                    source=AliasObjectReference(alias=item["source"]),target=AliasObjectReference(alias=item["target"]),
                                    values=(),order_key=TextOrderKey(text=item["order_key"]),alias=item["alias"]))
                            else: raise StopRun("unsupported seed op")
                        selection=assembly.group_selection
                        asserted=MapValue(entries=tuple(MapEntry(key=k,value=TextValue(kind="string",text=v))
                            for k,v in sorted(seed["origin"].items())))
                        semantic=GroupSemanticInput(layout_profile=selection.layout_profile,
                            mutation_profile=selection.capability_profile,value_profile=selection.value_profile,
                            catalog=CurrentCatalog(),operations=tuple(operations),asserted_origin=asserted)
                        request=RequestPresent(identity=RequestIdentity(
                            authorized_scope_identity=scope["profile"],request_id=seed["request_key"]))
                        applied=require_ok(assembly.apply_in_transaction(tx,semantic,request,documents=(document,)),
                            "public seven-operation seed")
                        if type(applied).__name__!="GroupSuccess" or len(applied.response.semantic.results)!=7:
                            raise StopRun("public Truss group did not confirm seven results")
                        seed_receipt = {
                            "result_type":type(applied).__name__,"operation_count":7,
                            "request_key":seed["request_key"],"seed_request_sha256":sha(seed_raw),
                            "source_system":seed["source_system"],"source_epoch":seed["source_epoch"]}
                    finally:
                        if assembly is not None: assembly.dispose()
                        if not host.detach():
                            raise StopRun("Truss host detach refused")
            write_new(receipt_root / "seed-committed.json", seed_receipt)
            completed.append("seven_operation_seed_committed")

            # Read one page as the dedicated ordinary LOGIN; intentionally no ACK call.
            write_new(receipt_root / "feed-page-read-intent.json", {
                "consumer_id": scope["consumer_and_model"]["consumer_id"],
                "maximum_facts": scope["consumer_and_model"]["feed_maximum_facts"],
                "maximum_bytes": scope["consumer_and_model"]["feed_maximum_bytes"],
                "acknowledgement": False})
            reader_params=dict(params); reader_params["user"]=reader
            with psycopg.connect(**reader_params, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    conn.execute("SET TRANSACTION READ ONLY")
                    host=require_ok(adopt_psycopg_host(conn,statement_timeout_ms=5000,operation_timeout_ms=30000),"feed reader host")
                    assembly=None
                    try:
                        tx=require_ok(host.adopt_transaction(access_mode="read_only"),"feed read-only transaction")
                        assembly=require_ok(create_reference_assembly(CatalogRuntimeConfiguration(),host),"public .51 assembly")
                        page=require_ok(assembly.read_feed_in_transaction(tx,
                            consumer_id=scope["consumer_and_model"]["consumer_id"],
                            maximum_facts=scope["consumer_and_model"]["feed_maximum_facts"],
                            maximum_bytes=scope["consumer_and_model"]["feed_maximum_bytes"]),"public feed page")
                        if page.outcome!="transaction":
                            raise StopRun(f"first feed page was not a complete transaction: {page.outcome!r}")
                        write_bytes_new(receipt_root / "first-feed-page.json",page.content)
                        write_new(receipt_root / "first-feed-page-receipt.json",{
                            "outcome":page.outcome,"sha256":page.sha256,"bytes":len(page.content),
                            "writer_xid":page.writer_xid,"consumer_registration_id":page.registration_id,
                            "read_as":reader,"acknowledgement":False,
                            "page_semantics":"first genesis page only; do not infer that seed page is visible"})
                    finally:
                        if assembly is not None: assembly.dispose()
                        if not host.detach():
                            raise StopRun("Truss host detach refused")
            completed.append("first_page_read_without_ack")

        signal.alarm(0)
        final_source_state = pg18_preflight(SOURCE)
        if final_source_state != (source_bytes, source_tree_sha, source_files):
            raise StopRun("original .48 PGDATA changed during clone migration")
        write_new(receipt_root / "original-source-preserved.json", {
            "path": str(SOURCE), "bytes": source_bytes, "files": source_files,
            "tree_sha256": source_tree_sha, "unchanged_after_attempt": True})
        attempt_bytes, attempt_tree_sha, attempt_files = tree_digest(root)
        write_new(receipt_root / "completed.json", {
            "status":"completed_local_development_only","completed_stages":completed,
            "source_pgdata_mutated":False,"cloud":False,"publication":False,
            "acknowledgement":False,"attempt_tree_bytes":attempt_bytes,
            "attempt_tree_sha256":attempt_tree_sha,"attempt_tree_files":attempt_files,
            "elapsed_seconds":round(time.monotonic()-(deadline-MAX_SECONDS),3)})
    except BaseException as error:
        signal.alarm(0)
        try:
            source_state = pg18_preflight(SOURCE)
            unchanged = source_state == (scope["source"]["pgdata_bytes_at_packet_review"],
                                         scope["source"]["pgdata_tree_sha256_at_packet_review"],
                                         scope["source"]["pgdata_files_at_packet_review"])
        except BaseException:
            unchanged = None
        try:
            write_new(receipt_root / "failure.json", {
                "status":"failed_or_interrupted","completed_stages":completed,
                "effects_may_remain":effects_may_remain,
                "failure_type":type(error).__name__,"failure":str(error)[:1500],
                "source_pgdata_unchanged":unchanged,"cloud":False,"publication":False,
                "acknowledgement":False})
        except BaseException:
            pass
        raise


if __name__ == "__main__":
    main()

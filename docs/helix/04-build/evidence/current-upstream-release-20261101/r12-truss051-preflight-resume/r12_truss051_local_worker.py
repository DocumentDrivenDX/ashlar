#!/usr/bin/env python3
"""Standing-authorized R12 resume of the already committed R10 .51 migration and local feed attempt.

Native execution remains disabled pending Sol review of this exact continuation packet.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata as metadata
import importlib.resources
from importlib import import_module
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
SCOPE_PATH = HERE / "r12-truss051-resume-scope.json"
SEED_PATH = HERE / "fresh-local-seed-request.json"
SUPERVISOR_PATH = HERE / "r12_truss051_supervisor.py"
SOURCE = Path("/private/tmp/ashlar-r10-truss051-current-cohort-20261101/migration-clone-pgdata")
EXPECTED_PATHS = {
    "output_root": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101",
    "supervisor_root": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101-supervisor",
    "cold_backup_pgdata": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101/r10-migrated-backup-pgdata",
    "migration_clone_pgdata": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101/resume-clone-pgdata",
    "recovery_registry": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101/truss-recovery-registry.sqlite3",
    "receipt_root": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101/receipts",
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
RUNTIME_PYTHONPATH = "/private/tmp/ashlar-r9-truss051-current-runtime-20261101/site-packages:/private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/lib/python3.12/site-packages"
LOCAL_EXTRA_VERSIONS = {
    "pgserver-pg18": ("18.6.1", "pgserver"),
    "fasteners": ("0.20", "fasteners"),
    "platformdirs": ("4.12.4", "platformdirs"),
    "psutil": ("7.2.2", "psutil"),
}


class StopRun(RuntimeError):
    pass


class DuplicateKey(ValueError):
    pass


def validate_local_runtime_extras(overlay: Path, version_lookup=None,
                                  module_import=None) -> dict[str, str]:
    """Fail before output creation unless exact Truss local extras resolve from the pinned current overlay."""
    version_lookup = version_lookup or metadata.version
    module_import = module_import or import_module
    overlay = overlay.resolve()
    versions = {}
    for distribution, (expected, module_name) in LOCAL_EXTRA_VERSIONS.items():
        try:
            actual = version_lookup(distribution)
        except metadata.PackageNotFoundError as error:
            raise StopRun(f"required Truss local extra missing: {distribution}=={expected}") from error
        if actual != expected:
            raise StopRun(f"wrong Truss local extra version: {distribution} {actual}, expected {expected}")
        try:
            module = module_import(module_name)
            module_path = Path(module.__file__).resolve()
            module_path.relative_to(overlay)
            distribution_path = Path(metadata.distribution(distribution).locate_file("")).resolve()
            if distribution_path != overlay:
                raise StopRun(f"{distribution} did not resolve from R9-built shared runtime overlay")
        except (ImportError, AttributeError, ValueError) as error:
            raise StopRun(f"required Truss local extra import/path check failed: {distribution}") from error
        versions[distribution] = actual
    from truss import LocalPostgres
    if "data_directory" not in inspect.signature(LocalPostgres).parameters:
        raise StopRun("LocalPostgres constructor changed")
    pgserver_binary_dir = importlib.resources.files("pgserver").joinpath("pginstall/bin")
    if not pgserver_binary_dir.is_dir():
        raise StopRun("pinned pgserver PostgreSQL binaries missing from the pinned current overlay")
    return versions


def pairs_unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKey(key)
        result[key] = value
    return result


def read_json(path: Path):
    return json.loads(path.read_text(), object_pairs_hook=pairs_unique)


def validate_runtime_paths(runtime: dict) -> None:
    """Require the exact overlay-first R12 runtime bound by the scope."""
    if (runtime.get("python_path") != "/private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/bin/python"
            or runtime.get("pythonpath") != RUNTIME_PYTHONPATH
            or runtime.get("local_extra_overlay") != "/private/tmp/ashlar-r9-truss051-current-runtime-20261101/site-packages"):
        raise StopRun("exact pinned Truss .51 shared current runtime overlay paths required")


def validate_umf_core0105_runtime(runtime: dict, *, version_lookup=None,
                                  module_import=None, distribution_root_lookup=None) -> str:
    """Require Core 0.10.5 to import from the exact first R12 runtime path."""
    version_lookup = version_lookup or metadata.version
    module_import = module_import or import_module
    distribution_root_lookup = distribution_root_lookup or (
        lambda name: Path(metadata.distribution(name).locate_file("")).resolve())
    expected = runtime.get("python_core_site_packages")
    if not isinstance(expected, str) or not expected.startswith("/"):
        raise StopRun("exact UMF Core 0.10.5 site-packages path required")
    site = Path(expected).resolve()
    pythonpath = runtime.get("pythonpath")
    if not isinstance(pythonpath, str) or pythonpath.split(os.pathsep, 1)[0] != str(site):
        raise StopRun("UMF Core site-packages must be first in isolated R12 PYTHONPATH")
    if not site.is_dir():
        raise StopRun("pinned UMF Core 0.10.5 site-packages directory missing")
    try:
        actual = version_lookup("umf-core")
    except metadata.PackageNotFoundError as error:
        raise StopRun("required UMF Core distribution missing: umf-core==0.10.5") from error
    if actual != "0.10.5":
        raise StopRun(f"wrong UMF Core Python distribution version: {actual}")
    try:
        module = module_import("umf")
        Path(module.__file__).resolve().relative_to(site)
        if distribution_root_lookup("umf-core") != site:
            raise StopRun("UMF Core distribution metadata resolved outside R10 site-packages")
    except (ImportError, AttributeError, ValueError) as error:
        raise StopRun("UMF Core module/import path is not from exact R10 site-packages") from error
    return actual


def validate_standing_authorization(scope: dict, scope_raw: bytes, *, receipt=None,
                                     worker_raw=None, supervisor_raw=None,
                                     require_dispatch=True) -> None:
    """Bind standing authority; require Sol dispatch enablement only for native mode."""
    if (scope.get("status") != "standing_local_native_authorization"
            or scope.get("authorization") != "standing_user_local_release_authorization"
            or scope.get("authorization_receipt") != "r12-standing-authorization-dispatch-receipt.json"):
        raise StopRun("standing local authorization scope identity mismatch")
    receipt = receipt if receipt is not None else read_json(HERE / scope["authorization_receipt"])
    worker_raw = worker_raw if worker_raw is not None else Path(__file__).read_bytes()
    supervisor_raw = supervisor_raw if supervisor_raw is not None else SUPERVISOR_PATH.read_bytes()
    expected = {
        "schema": "ashlar.standing-local-dispatch-binding/1",
        "authorization_basis": "standing_user_local_release_authorization",
        "user_authorization_statement": "User authorized fresh Truss source creation, a local seed, and a tested release; bounded local R12 resume execution is within that standing authorization and requires no per-attempt user approval.",
        "scope_sha256": sha(scope_raw),
        "worker_sha256": sha(worker_raw),
        "supervisor_sha256": sha(supervisor_raw),
        "attempts": 1,
        "cloud": False,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise StopRun(f"standing authorization binding mismatch: {key}")
    if set(receipt) != set(expected) | {"sol_review_status", "dispatch_enabled"}:
        raise StopRun("standing authorization receipt has unexpected fields")
    review_state = receipt.get("sol_review_status")
    enabled = receipt.get("dispatch_enabled")
    if require_dispatch:
        if review_state != "sol_reviewed" or enabled is not True:
            raise StopRun("Sol review receipt is pending; native dispatch remains disabled")
    elif review_state not in ("pending_sol_review", "sol_reviewed") or type(enabled) is not bool:
        raise StopRun("R12 preflight authorization status is malformed")


def validate_integration_binding(release: dict) -> None:
    """Bind reviewed SDK0101 commit plus the reviewed source-tree correction."""
    repo = HERE.parents[5]
    integration = "afeb78c41679454730a3bf0129db6f438191953a"
    integration_tree = "08862e9d2f3482f505cec40ed266f5e77cad0192"
    correction = "64f28c3b04803eadcac0f920cd2b8bb3f84a6f43"
    correction_tree = "28138503d679a480f7af9de8aac25b8ab0884f14"
    try:
        trees = [subprocess.run(["git", "-C", str(repo), "rev-parse", f"{commit}^{{tree}}"],
                                check=True, text=True, capture_output=True, timeout=10).stdout.strip()
                 for commit in (integration, correction)]
        ancestors = [subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", commit, "HEAD"],
                                    text=True, capture_output=True, timeout=10)
                    for commit in (integration, correction)]
        original_paths = subprocess.run(["git", "-C", str(repo), "show", "--format=", "--name-only", integration],
                                        check=True, text=True, capture_output=True, timeout=10).stdout.splitlines()
        corrected_paths = subprocess.run(["git", "-C", str(repo), "show", "--format=", "--name-only", correction],
                                         check=True, text=True, capture_output=True, timeout=10).stdout.splitlines()
    except (OSError, subprocess.SubprocessError) as error:
        raise StopRun("cannot prove reviewed SDK0101/source-custody commit ancestry") from error
    expected_paths = sorted(path for path in original_paths if path and not path.startswith("docs/"))
    corrected_paths = {path for path in corrected_paths if path and not path.startswith("docs/")}
    bound = release.get("integration_files")
    if (trees != [integration_tree, correction_tree] or any(item.returncode != 0 for item in ancestors)
            or not isinstance(bound, dict)):
        raise StopRun("reviewed SDK0101/source-custody commit/tree is not in current checkout ancestry")
    if not corrected_paths.issubset(expected_paths) or sorted(bound) != expected_paths:
        raise StopRun("SDK0101 integration file set differs from reviewed commit")
    for relative in expected_paths:
        path = (repo / relative).resolve()
        blob_commit = correction if relative in corrected_paths else integration
        try:
            path.relative_to(repo.resolve())
            actual = sha(path.read_bytes())
            committed_bytes = subprocess.run(
                ["git", "-C", str(repo), "show", f"{blob_commit}:{relative}"],
                check=True, capture_output=True, timeout=10).stdout
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            raise StopRun(f"cannot verify SDK0101 integration file: {relative}") from error
        if actual != bound[relative] or sha(committed_bytes) != bound[relative]:
            raise StopRun(f"SDK0101 integration file hash mismatch: {relative}")
    dirty = subprocess.run(["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=no", "--", *expected_paths],
                           check=True, text=True, capture_output=True, timeout=10).stdout
    if dirty.strip():
        raise StopRun("SDK0101 integration paths have local modifications")

def validate_release_cohort(release: dict, runtime: dict, **core_checks) -> str:
    expected = {
        "sdk_version": "0.10.1",
        "sdk_archive_sha256": "3d95fff822f4f44b3d1a8e56185c3697e94ee66f5315cda7389f5301f3d16d26",
        "sdk_public_entry_sha256": "7577c4efe5269d9f98187265b31be71f2be69b67cab357ce9bf07dabcd758f27",
        "source_commit_declared": "90cb61eb63aec3f5a77b5991a56e3dfc3d10bd27",
        "source_tree_object": "24d2531cbe9025fdc07fb1321a82a81ae31ff12a",
        "source_custody_correction_commit": "64f28c3b04803eadcac0f920cd2b8bb3f84a6f43",
        "source_custody_correction_tree": "28138503d679a480f7af9de8aac25b8ab0884f14",
        "ashlar_integration_commit": "afeb78c41679454730a3bf0129db6f438191953a",
        "ashlar_integration_tree": "08862e9d2f3482f505cec40ed266f5e77cad0192",
        "ashlar_runtime_profile": "ashlar-local-current-installation-profile/0.4",
        "semantic_core_version": "0.10.0",
        "python_core_distribution": "umf-core",
        "python_core_version": "0.10.5",
        "python_core_wheel_sha256": "9213ff4b87649f551abcfdff40cdb6b0d75d619260988fa8ea4fe26cac7f6788",
    }
    for key, value in expected.items():
        if release.get(key) != value:
            raise StopRun(f"current UMF release cohort mismatch: {key}")
    validate_integration_binding(release)
    if release.get("runtime_integration") != "Sol-reviewed SDK0101 runtime integration bound":
        raise StopRun("Sol-reviewed SDK0101 runtime integration binding mismatch")
    proof_name = release.get("runtime_proof")
    if proof_name != "r12-runtime-proof.json":
        raise StopRun("exact R12 current-runtime proof required")
    proof_path = (HERE / proof_name).resolve()
    if proof_path.parent != HERE or sha(proof_path.read_bytes()) != release.get("runtime_proof_sha256"):
        raise StopRun("R12 current-runtime proof digest mismatch")
    proof = read_json(proof_path)
    if (proof.get("result") != "passed"
            or proof.get("r9_overlay", {}).get("core_version") != "0.10.5"
            or proof.get("r9_overlay", {}).get("core_wheel_sha256") != expected["python_core_wheel_sha256"]
            or proof.get("truss", {}).get("version") != "0.0.51"
            or proof.get("ashlar_integration", {}).get("upstream_source_tree_object") != expected["source_tree_object"]
            or proof.get("ashlar_integration", {}).get("source_custody_correction_commit") != expected["source_custody_correction_commit"]
            or proof.get("ashlar_integration", {}).get("source_custody_correction_tree") != expected["source_custody_correction_tree"]
            or proof.get("effects", {}).get("native_postgres_started") is not False):
        raise StopRun("R12 current-runtime proof fields mismatch")
    return validate_umf_core0105_runtime(runtime, **core_checks)


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


def root_directory_signature(path: Path) -> tuple[int, int, int, int, int, int]:
    info = path.stat(follow_symlinks=False)
    return (info.st_dev, info.st_ino, info.st_uid, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


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
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight", action="store_true",
                        help="run all deterministic R12 checks without output creation or PostgreSQL startup")
    args = parser.parse_args()
    preflight_only = args.preflight
    scope_raw = SCOPE_PATH.read_bytes()
    scope = json.loads(scope_raw, object_pairs_hook=pairs_unique)
    validate_standing_authorization(scope, scope_raw, require_dispatch=not preflight_only)
    if scope.get("paths") != EXPECTED_PATHS or scope.get("source", {}).get("existing_pgdata") != str(SOURCE):
        raise StopRun("exact reviewed source and output paths required")
    validate_runtime_paths(scope.get("runtime", {}))
    validate_release_cohort(scope.get("release_cohort", {}), scope.get("runtime", {}))
    if scope.get("limits") != {
        "attempts":1,"automatic_retries":0,"wall_seconds":900,
        "process_group_rss_bytes":2147483648,"retained_plus_attempt_bytes":2147483648,
        "attempt_root_bytes":536870912,"migration_preservation_bytes":268435456,
        "combined_log_bytes":16777216,"storage_sample_seconds":15,"parallelism":"sequential local only",
        "cloud":False,"cleanup":False}:
        raise StopRun("exact reviewed wall/RSS/storage limits required")
    if not preflight_only and os.getpgrp() != os.getpid():
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
    from truss import LocalPostgres, PsycopgHost
    route_manifest, _, outer_sha = load_recipe()
    if outer_sha != scope["runtime"]["outer_migration_index_sha256"]:
        raise StopRun("pinned outer migration index mismatch")
    route = tuple({k: edge[k] for k in ("source", "target", "artifact")}
                   for edge in plan_route(route_manifest, SOURCE_MANIFEST))
    if route != tuple(scope["runtime"]["migration_edges"]):
        raise StopRun("pinned .48→.51 migration route mismatch")
    for name, keys in {"status": set(), "receipt": {"reference"}}.items():
        if not keys <= set(inspect.signature(getattr(RuntimeMigrations, name)).parameters):
            raise StopRun(f"public migration API arguments changed: {name}")
    if not {"connect", "recovery_registry", "maximum_preservation_bytes",
            "operation_timeout_ms", "statement_timeout_ms"} <= set(inspect.signature(RuntimeMigrations).parameters):
        raise StopRun("public migration runtime constructor arguments changed")
    if set(inspect.signature(HostRecoveryRegistry).parameters) != {"path"}:
        raise StopRun("restart-durable migration registry constructor changed")
    if set(inspect.signature(HostRecoveryRegistry.inspect).parameters) != {"self", "reference", "context"}:
        raise StopRun("restart-durable migration registry inspection API changed")
    if set(inspect.signature(PsycopgHost.adopt_transaction).parameters) != {"self", "isolation", "access_mode"}:
        raise StopRun("public PsycopgHost transaction adoption API changed")
    local_extra_versions = validate_local_runtime_extras(
        Path(scope["runtime"]["local_extra_overlay"]))
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
    from truss import CatalogReferenceAssembly, adopt_psycopg_host
    if not {"connection", "profile", "statement_timeout_ms", "operation_timeout_ms"} <= set(inspect.signature(adopt_psycopg_host).parameters):
        raise StopRun("public psycopg host adoption signature changed")
    if "connection" not in inspect.signature(PsycopgHost).parameters:
        raise StopRun("public PsycopgHost constructor contract changed")
    if "return Ok" in inspect.getsource(adopt_psycopg_host):
        raise StopRun("psycopg host adoption return contract changed; re-review required")
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
    if preflight_only:
        if any(p.exists() or p.is_symlink() for p in (root, supervisor_root)):
            raise StopRun("R12 preflight requires absent output and supervisor namespaces")
    else:
        validate_supervisor_handshake(root, supervisor_root)
    if any(p.exists() or p.is_symlink() for p in (backup, clone, registry_path, receipt_root)):
        raise StopRun("all fresh R12 continuation destinations must be absent")
    prior = Path("/private/tmp/ashlar-r10-truss051-current-cohort-20261101")
    prior_pgdata = prior / "migration-clone-pgdata"
    prior_registry = prior / "truss-recovery-registry.sqlite3"
    prior_receipts = prior / "receipts"
    expected_source = scope["source"]
    prior_migration = read_json(prior_receipts / "migration-receipt.json")
    prior_apply = read_json(prior_receipts / "migration-apply-result.json")
    prior_supervisor_path = Path("/private/tmp/ashlar-r10-truss051-current-cohort-20261101-supervisor/supervisor-receipt.json")
    prior_supervisor = read_json(prior_supervisor_path)
    if (sha((prior_receipts / "migration-receipt.json").read_bytes()) != expected_source["r10_migration_receipt_sha256"]
            or sha((prior_receipts / "migration-apply-result.json").read_bytes()) != expected_source["r10_apply_result_sha256"]
            or sha(prior_supervisor_path.read_bytes()) != expected_source["r10_supervisor_receipt_sha256"]
            or prior_apply.get("outcome") != "migrated"
            or prior_migration.get("outcome") != "migrated"
            or prior_migration.get("current_verification", {}).get("state") != "current"
            or prior_migration.get("current_verification", {}).get("manifestSha256") != TARGET_MANIFEST
            or prior_supervisor.get("custody_status") != "no_owned_processes_remain"
            or prior_supervisor.get("effects_may_remain") is not False):
        raise StopRun("R10 terminal migration/custody receipts do not support safe resume")
    if (prior_pgdata != SOURCE or not prior_registry.is_file() or prior_registry.stat().st_size <= 0
            or not prior_pgdata.is_dir() or (prior_pgdata / "postmaster.pid").exists()):
        raise StopRun("stopped R10 migrated clone and durable registry are required")
    source_bytes, source_tree_sha, source_files = pg18_preflight(prior_pgdata)
    source_signature_before = root_directory_signature(prior_pgdata)
    if (source_bytes != expected_source["pgdata_bytes_at_packet_review"]
            or source_tree_sha != expected_source["pgdata_tree_sha256_at_packet_review"]
            or source_files != expected_source["pgdata_files_at_packet_review"]):
        raise StopRun("R10 migrated clone differs from read-only reviewed tree")
    custody = check_process_custody(prior_pgdata)
    if sha(prior_registry.read_bytes()) != expected_source["r10_registry_sha256"]:
        raise StopRun("R10 restart-durable registry digest mismatch")
    projected_storage = (3 * source_bytes + scope["limits"]["attempt_root_bytes"]
                         + scope["limits"]["combined_log_bytes"])
    if projected_storage > scope["limits"]["retained_plus_attempt_bytes"]:
        raise StopRun("projected retained and R12 storage exceed aggregate cap")
    free_bytes = shutil.disk_usage(root.parent).free
    required_free = 2 * source_bytes + scope["limits"]["attempt_root_bytes"]
    if free_bytes < required_free:
        raise StopRun(f"insufficient free disk before R12 resume copies: {free_bytes} < {required_free}")
    if preflight_only:
        print(json.dumps({"status":"passed_effect_free_preflight","scope_sha256":sha(scope_raw),
            "source_tree_sha256":source_tree_sha,"source_bytes":source_bytes,"source_files":source_files,
            "process_custody":custody,"free_bytes":free_bytes,"required_free_bytes":required_free,
            "r10_migration_receipt_sha256":expected_source["r10_migration_receipt_sha256"],
            "r10_registry_sha256":expected_source["r10_registry_sha256"],
            "output_root_created":False,"postgres_started":False,"native_database_calls":False,
            "migration_replayed":False,"dispatch_enabled":False}, sort_keys=True))
        return

    deadline = time.monotonic() + MAX_SECONDS
    def alarm(_sig, _frame):
        raise TimeoutError("900-second one-shot deadline")
    signal.signal(signal.SIGALRM, alarm); signal.alarm(MAX_SECONDS)
    root.mkdir(mode=0o700); os.chmod(root, 0o700)
    receipt_root.mkdir(mode=0o700)
    completed: list[str] = []
    effects_may_remain = False
    try:
        write_new(receipt_root / "r10-migrated-clone-preflight.json", {
            "path":str(prior_pgdata),"bytes":source_bytes,"files":source_files,
            "tree_sha256":source_tree_sha,"owner_uid":os.getuid(),
            "process_custody":custody,"postmaster_pid_absent":True,
            "migration_receipt_sha256":expected_source["r10_migration_receipt_sha256"],
            "registry_sha256":expected_source["r10_registry_sha256"],
            "source_mutated":False,"free_bytes_before_copy":free_bytes})
        shutil.copytree(prior_pgdata, backup, symlinks=False, copy_function=shutil.copy2)
        shutil.copytree(prior_pgdata, clone, symlinks=False, copy_function=shutil.copy2)
        for name, path in (("backup",backup),("resume_clone",clone)):
            actual=pg18_preflight(path)
            if actual != (source_bytes,source_tree_sha,source_files):
                raise StopRun(f"R10-derived R12 {name} copy mismatch")
        shutil.copy2(prior_registry, registry_path)
        if sha(registry_path.read_bytes()) != expected_source["r10_registry_sha256"]:
            raise StopRun("R10 durable registry copy digest mismatch")
        write_new(receipt_root / "resume-copies-verified.json", {
            "source_tree_sha256":source_tree_sha,"bytes":source_bytes,"files":source_files,
            "backup_and_resume_clone_equal":True,"registry_sha256":expected_source["r10_registry_sha256"],
            "migration_replayed":False,"r10_namespace_mutated":False})
        if root_directory_signature(prior_pgdata) != source_signature_before:
            raise StopRun("R10 migrated source root signature changed during R12 copy")
        if time.monotonic() > deadline:
            raise TimeoutError("deadline before resumed PostgreSQL start")

        import psycopg
        from truss import (CatalogRuntimeConfiguration, Ok,
                           adopt_psycopg_host, create_reference_assembly)
        from truss.recovery_registry import HostRecoveryRegistry
        runtime = LocalPostgres(clone)
        effects_may_remain = True
        write_new(receipt_root / "postgres-start-intent.json", {
            "data_directory":str(clone),"prior_r10_pgdata_untouched":str(prior_pgdata),
            "owned_start":True,"stop_on_context_exit":True,"migration_replayed":False})
        with runtime as pg:
            if pg.info.server_version != "18.6" or Path(pg.info.data_directory) != clone:
                raise StopRun("LocalPostgres did not start exact R12 PG18.6 resume clone")
            write_new(receipt_root / "postgres-started.json", {
                "data_directory":str(pg.info.data_directory),"server_version":pg.info.server_version,
                "truss_version":"0.0.51","R10_migration_clone_untouched":True})
            params = psycopg.conninfo.conninfo_to_dict(pg.info.connection_uri)
            connect = lambda: psycopg.connect(**params, autocommit=True, connect_timeout=5)
            registry = HostRecoveryRegistry(registry_path)
            migration = RuntimeMigrations(connect, registry,
                maximum_preservation_bytes=scope["migration"]["maximum_preservation_bytes"],
                operation_timeout_ms=scope["migration"]["operation_timeout_ms"],
                statement_timeout_ms=scope["migration"]["statement_timeout_ms"])
            status = migration.status()
            if (status.get("state") != "current" or status.get("manifestSha256") != TARGET_MANIFEST
                    or status.get("target") != TARGET_MANIFEST):
                raise StopRun("public R12 migration status is not exact .51 current")
            reference=prior_migration["reference"]
            retained=registry.inspect(reference,status["context"])
            public_receipt=migration.receipt(reference)
            observations=retained.get("observations",[])
            if (public_receipt != prior_migration.get("receipt")
                    or public_receipt.get("attemptId") != scope["migration"]["attempt_id"]
                    or len(observations) != 2
                    or observations[-1].get("applicationDurability") != "committed"
                    or observations[-1].get("nativeTermination") != "confirmed"
                    or observations[-1].get("resourceRelease") != "confirmed"
                    or retained.get("obligation",{}).get("obligationId") != scope["migration"]["attempt_id"]):
                raise StopRun("copied migration public receipt/registry do not prove R10 committed state")
            write_new(receipt_root / "r10-migration-resume-proof.json", {
                "state":"current","manifest_sha256":TARGET_MANIFEST,
                "reference":reference,"attempt_id":scope["migration"]["attempt_id"],
                "public_receipt_matches_r10":True,"registry_observations":len(observations),
                "last_observation":"committed_terminated_and_released",
                "prepare_called":False,"apply_called":False,"reconcile_called":False})
            completed.append("r10_migration_reused_without_replay")

            # Public .51 identity proof gates every later mutation.
            rlogin = registrar_role
            read_params = dict(params); read_params["user"] = rlogin
            with psycopg.connect(**read_params, autocommit=True, connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    conn.execute("SET TRANSACTION READ ONLY")
                    host = adopt_psycopg_host(conn, statement_timeout_ms=5000,
                        operation_timeout_ms=30000)
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
                    host = adopt_psycopg_host(conn, statement_timeout_ms=5000,
                        operation_timeout_ms=30000)
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
                    host = adopt_psycopg_host(conn, statement_timeout_ms=5000, operation_timeout_ms=30000)
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
                    host = adopt_psycopg_host(conn, statement_timeout_ms=5000, operation_timeout_ms=30000)
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
                    host=adopt_psycopg_host(conn,statement_timeout_ms=5000,operation_timeout_ms=30000)
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
        source_signature_after = root_directory_signature(SOURCE)
        if source_signature_after != source_signature_before:
            raise StopRun("R10 migrated clone root metadata changed during R12 continuation")
        write_new(receipt_root / "r10-migrated-source-preserved.json", {
            "path": str(SOURCE), "bytes": source_bytes, "files": source_files,
            "tree_sha256_before_copy": source_tree_sha,
            "source_root_signature_unchanged_after_attempt": True,
            "content_rehash_after_copy": False})
        write_new(receipt_root / "completed.json", {
            "status":"completed_local_development_only","completed_stages":completed,
            "r10_pgdata_mutated":False,"cloud":False,"publication":False,
            "acknowledgement":False,"attempt_tree_full_rehash":"not performed; copy trees each hashed once",
            "elapsed_seconds":round(time.monotonic()-(deadline-MAX_SECONDS),3)})
    except BaseException as error:
        signal.alarm(0)
        try:
            unchanged = (source_signature_before is not None
                         and root_directory_signature(SOURCE) == source_signature_before)
        except BaseException:
            unchanged = None
        try:
            write_new(receipt_root / "failure.json", {
                "status":"failed_or_interrupted","completed_stages":completed,
                "effects_may_remain":effects_may_remain,
                "failure_type":type(error).__name__,"failure":str(error)[:1500],
                "source_root_signature_unchanged":unchanged,"source_full_tree_hash_repeated":False,"cloud":False,"publication":False,
                "acknowledgement":False})
        except BaseException:
            pass
        raise


if __name__ == "__main__":
    main()

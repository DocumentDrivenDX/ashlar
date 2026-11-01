#!/usr/bin/env python3
"""Effect-free validation of the accepted r6 source and proposed .51 worker inputs."""
from __future__ import annotations

import hashlib
import importlib.metadata as metadata
import importlib.resources
import inspect
import json
import os
from pathlib import Path
import shutil
import stat
import unittest

HERE = Path(__file__).resolve().parent
SCOPE = HERE / "r7-truss051-migration-scope.json"
SEED = HERE / "fresh-local-seed-request.json"
SOURCE = Path("/private/tmp/ashlar-truss048-fresh-local-20261031-r6/pgdata")
EXPECTED_PATHS = {
    "output_root": "/private/tmp/ashlar-r7-truss051-continuation-20261031",
    "supervisor_root": "/private/tmp/ashlar-r7-truss051-continuation-20261031-supervisor",
    "cold_backup_pgdata": "/private/tmp/ashlar-r7-truss051-continuation-20261031/cold-backup-pgdata",
    "migration_clone_pgdata": "/private/tmp/ashlar-r7-truss051-continuation-20261031/migration-clone-pgdata",
    "recovery_registry": "/private/tmp/ashlar-r7-truss051-continuation-20261031/truss-recovery-registry.sqlite3",
    "receipt_root": "/private/tmp/ashlar-r7-truss051-continuation-20261031/receipts",
}
EXPECTED_WHEEL = "eb59141c48e94bb93ab7053e9e8725012eee1749e4432e9c71804d23abbdff57"
SOURCE_MANIFEST = "c4339ac7f09ab5a004c272ab00962107a970938004c1765ec4163e4d5ca9b6f0"
MID_MANIFEST = "35fd797b5cf48dd888c68e53b9e599df20cffe00f8f3d33e1563820ea808d59f"
TARGET_MANIFEST = "288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4"
PG18_DIRS = (
    "global", "pg_wal/archive_status", "pg_wal/summaries", "pg_commit_ts",
    "pg_dynshmem", "pg_notify", "pg_serial", "pg_snapshots", "pg_subtrans",
    "pg_twophase", "pg_multixact", "pg_multixact/members",
    "pg_multixact/offsets", "base", "base/1", "pg_replslot", "pg_tblspc",
    "pg_stat", "pg_stat_tmp", "pg_xact", "pg_logical",
    "pg_logical/snapshots", "pg_logical/mappings",
)


class DuplicateKey(ValueError):
    pass


def unique_pairs(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise DuplicateKey(key)
        out[key] = value
    return out


def strict_json(path: Path):
    return json.loads(path.read_text(), object_pairs_hook=unique_pairs)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def valid_role_name(value: str) -> bool:
    return (type(value) is str and bool(value) and "\0" not in value
            and not value.startswith("pg_") and len(value.encode("utf8")) <= 63)


def tree_inventory(root: Path) -> tuple[int, str, int]:
    digest = hashlib.sha256()
    total = count = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        dirs.sort(); files.sort()
        for name in dirs:
            path = Path(base) / name
            info = path.lstat()
            if (path.is_symlink() or not path.is_dir() or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) & 0o077):
                raise SystemExit(f"unsafe source directory ownership/mode: {path}")
            rel = path.relative_to(root).as_posix().encode()
            digest.update(b"D\0" + rel + b"\0")
        for name in files:
            path = Path(base) / name
            info = path.lstat()
            if (path.is_symlink() or not path.is_file() or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) & 0o077):
                raise SystemExit(f"unsafe source file ownership/mode: {path}")
            rel = path.relative_to(root).as_posix().encode()
            raw = path.read_bytes()
            total += len(raw); count += 1
            digest.update(b"F\0" + rel + b"\0" + str(len(raw)).encode() + b"\0" + hashlib.sha256(raw).digest())
            if total > 512 * 1024 * 1024:
                raise SystemExit("source PGDATA exceeds 512 MiB packet cap")
    return total, digest.hexdigest(), count


scope = strict_json(SCOPE)
seed_raw = SEED.read_bytes()
seed = json.loads(seed_raw, object_pairs_hook=unique_pairs)
if scope["status"] != "approved_one_shot_native" or scope["authorization"] != "one_run_local_native_lifecycle":
    raise SystemExit("R7 scope status/authorization differ from Sol-reviewed one-shot values")
approval_path = HERE / "r7-truss051-native-approval.json"
approval = strict_json(approval_path)
worker_path = HERE / "r7_truss051_local_worker.py"
supervisor_path = HERE / "r7_truss051_supervisor.py"
if (approval.get("status") != "sol_reviewed_one_shot_local_dispatch_candidate"
        or approval.get("review", {}).get("result") != "accepted"
        or approval.get("scope_sha256") != sha(SCOPE.read_bytes())
        or approval.get("worker_sha256") != sha(worker_path.read_bytes())
        or approval.get("supervisor_sha256") != sha(supervisor_path.read_bytes())
        or approval.get("attempts") != 1 or approval.get("cloud") is not False
        or approval.get("authorization_quote") != "User authorized fresh Truss source creation, local seed, and a tested release; this is one bounded local-development attempt only."):
    raise SystemExit("Sol-reviewed standing authorization receipt does not bind exact scope/worker/supervisor")
if scope["source"]["existing_pgdata_is_mutable"] is not False:
    raise SystemExit("original r6 PGDATA must remain immutable")
if scope["source"]["existing_pgdata"] != str(SOURCE):
    raise SystemExit("exact retained r6 PGDATA path mismatch")
if scope["paths"] != EXPECTED_PATHS:
    raise SystemExit("scope output paths differ from exact reviewed paths")
if scope["limits"] != {
    "attempts": 1, "automatic_retries": 0, "wall_seconds": 900,
    "process_group_rss_bytes": 2147483648, "retained_plus_attempt_bytes": 2147483648,
    "attempt_root_bytes": 536870912, "migration_preservation_bytes": 268435456,
    "combined_log_bytes": 16777216, "parallelism": "sequential local only",
    "cloud": False, "cleanup": False,
}:
    raise SystemExit("scope resource/cost limits differ from reviewed bounds")
if scope["source"]["source_manifest_sha256"] != SOURCE_MANIFEST:
    raise SystemExit("r6 source manifest is not the pinned .48 route source")
if (scope["runtime"]["python_path"] != "/private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/bin/python"
        or scope["runtime"]["pythonpath"] != "/private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/lib/python3.12/site-packages"):
    raise SystemExit("exact .51 inspection runtime paths required")
if seed["document_id"] != scope["source"]["catalog_accepted"]["document_id"] or seed["request_key"] != "ashlar-local-dev-typed-graph-seed-r6":
    raise SystemExit("seed request does not match accepted r6 identity")
if len(seed["operations"]) != 7 or sha(seed_raw) != scope["consumer_and_model"]["seed_request_sha256"]:
    raise SystemExit("exact seven-operation seed bytes required")
if sha(seed_raw) != "550768ec3f90d929e5e60c719f31e708183a3cc41b3f558b2da932633c18281d":
    raise SystemExit("seed request hash mismatch")
if (sum(item["op"] == "create_object" for item in seed["operations"]) != 4
        or sum(item["op"] == "create_edge" for item in seed["operations"]) != 3):
    raise SystemExit("seed must contain exactly four objects and three edges")
aliases = {item["alias"] for item in seed["operations"]}
if len(aliases) != 7 or any(item.get(side) not in aliases for item in seed["operations"]
        if item["op"] == "create_edge" for side in ("source", "target")):
    raise SystemExit("seven unique aliases and complete edge endpoints required")
if metadata.version("umf-truss") != "0.0.51":
    raise SystemExit("Truss .51 inspection environment required")
dist = metadata.distribution("umf-truss")
direct = json.loads(dist.read_text("direct_url.json") or "{}")
wheel_hash = direct.get("archive_info", {}).get("hashes", {}).get("sha256")
if wheel_hash != EXPECTED_WHEEL:
    raise SystemExit("Truss .51 wheel digest mismatch")
manifest = importlib.resources.files("truss").joinpath("_catalog_runtime/manifest.json").read_bytes()
if sha(manifest) != TARGET_MANIFEST:
    raise SystemExit("Truss .51 runtime manifest digest mismatch")
from truss._migration_recipe import load_recipe, plan_route
from truss.migrations import RuntimeMigrations
from truss.recovery_registry import HostRecoveryRegistry
from truss import CatalogDocument, CatalogReferenceAssembly, LocalPostgres, ModuleAccessGrant, prepare_acceptance
_, _, outer_sha = load_recipe()
if outer_sha != scope["runtime"]["outer_migration_index_sha256"]:
    raise SystemExit("migration index digest mismatch")
chain = tuple({k: edge[k] for k in ("source", "target", "artifact")}
              for edge in plan_route(load_recipe()[0], SOURCE_MANIFEST))
if chain != tuple(scope["runtime"]["migration_edges"]):
    raise SystemExit("exact .48 to .51 route mismatch")
for method in ("status", "backup_metadata", "prepare", "apply", "reconcile", "receipt"):
    if not callable(getattr(RuntimeMigrations, method, None)):
        raise SystemExit(f"public migration API missing: {method}")
for method_name, required in {
    "prepare": {"attempt_id"}, "apply": {"reference"},
    "reconcile": {"reference"}, "receipt": {"reference"},
}.items():
    if not required <= set(inspect.signature(getattr(RuntimeMigrations, method_name)).parameters):
        raise SystemExit(f"public migration argument mismatch: {method_name}")
if not {"connect", "recovery_registry", "maximum_preservation_bytes", "operation_timeout_ms", "statement_timeout_ms"} <= set(inspect.signature(RuntimeMigrations).parameters):
    raise SystemExit("public migration runtime constructor arguments changed")
if not {"path"} <= set(inspect.signature(HostRecoveryRegistry).parameters):
    raise SystemExit("restart-durable migration recovery registry constructor changed")
if not {"data_directory"} <= set(inspect.signature(LocalPostgres).parameters):
    raise SystemExit("local PostgreSQL constructor changed")
required = {
    "status_in_transaction": set(),
    "report_in_transaction": set(),
    "catalog_view_request": {"definition_count", "encoded_bytes"},
    "catalog_view_in_transaction": {"request"},
    "accepted_document_in_transaction": {"document_id", "maximum_bytes"},
    "registrar_status_in_transaction": set(),
    "configure_module_access_in_transaction": {"grants"},
    "register_feed_consumer_in_transaction": {"consumer_id", "reader_role"},
    "prepare_group_in_transaction": {"records", "relationships"},
    "apply_in_transaction": {"input", "request", "documents"},
    "read_feed_in_transaction": {"consumer_id", "maximum_facts", "maximum_bytes"},
}
for name, keys in required.items():
    params = inspect.signature(getattr(CatalogReferenceAssembly, name)).parameters
    if not keys <= set(params):
        raise SystemExit(f"public API signature mismatch: {name}")
grant = scope["consumer_and_model"]["module_grant"]
ModuleAccessGrant(grant["document_id"], grant["module_id"], grant["reader_role"], grant["writer_role"])
role_names = (scope["consumer_and_model"]["reader_login_role"],
             scope["consumer_and_model"]["module_reader_role"],
             scope["consumer_and_model"]["module_writer_role"],
             scope["consumer_and_model"]["registrar_login_role"])
for role in role_names:
    if not valid_role_name(role):
        raise SystemExit(f"unsafe or unbounded role identifier: {role!r}")
if len(set(role_names)) != len(role_names):
    raise SystemExit("role identities must be distinct")
source_model = (HERE.parents[5] / "docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r4-sdk010-initialize-only/inputs/typed-graph-model.umf.json").read_bytes()
if source_model.count(b'"id": "typed-graph-example"') != 1:
    raise SystemExit("frozen G03 model identity token changed")
model = source_model.replace(b'"id": "typed-graph-example"',
                             b'"id": "ashlar-local-dev-typed-graph-r6-20261031"')
if sha(model) != scope["source"]["catalog_accepted"]["document_sha256"]:
    raise SystemExit("r6 model bytes differ from accepted document hash")
prepared = prepare_acceptance((CatalogDocument(scope["source"]["catalog_accepted"]["document_id"],
    scope["source"]["catalog_accepted"]["document_revision"], model),))
if prepared is None:
    raise SystemExit("pure .51 acceptance preparation failed")

if not SOURCE.is_absolute() or SOURCE.resolve() != SOURCE or SOURCE.is_symlink() or not SOURCE.is_dir():
    raise SystemExit("exact retained r6 PGDATA unavailable or noncanonical")
source_stat = SOURCE.lstat()
if source_stat.st_uid != os.getuid() or stat.S_IMODE(source_stat.st_mode) & 0o077:
    raise SystemExit("retained r6 PGDATA root must be user-owned and private (no group/world permissions)")
if (SOURCE / "PG_VERSION").read_text().strip() != "18":
    raise SystemExit("r6 PGDATA is not PostgreSQL 18")
if (SOURCE / "postmaster.pid").exists():
    raise SystemExit("r6 PGDATA has a postmaster.pid marker")
missing = [relative for relative in PG18_DIRS if not (SOURCE / relative).is_dir() or (SOURCE / relative).is_symlink()]
if missing:
    raise SystemExit(f"r6 PGDATA incomplete PostgreSQL 18 directory skeleton: {missing!r}")
source_bytes, source_tree_sha, source_files = tree_inventory(SOURCE)
expected_source = scope["source"]
if (source_bytes != expected_source["pgdata_bytes_at_packet_review"]
        or source_tree_sha != expected_source["pgdata_tree_sha256_at_packet_review"]
        or source_files != expected_source["pgdata_files_at_packet_review"]):
    raise SystemExit("retained r6 PGDATA differs from exact packet-review tree; stop")
root = Path(scope["paths"]["output_root"])
if any(Path(scope["paths"][key]).exists() or Path(scope["paths"][key]).is_symlink()
       for key in ("output_root", "supervisor_root", "cold_backup_pgdata", "migration_clone_pgdata", "recovery_registry", "receipt_root")):
    raise SystemExit("all continuation output paths must be absent before dispatch")
free = shutil.disk_usage(root.parent).free
required_free = 2 * source_bytes + scope["limits"]["attempt_root_bytes"]
if free < required_free:
    raise SystemExit("insufficient free disk for two cold copies plus bounded attempt")

test_path = HERE / "test_r7_worker_handshake.py"
spec = importlib.util.spec_from_file_location("r7_handshake_tests", test_path)
test_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(test_module)
test_result = unittest.TextTestRunner(stream=__import__("sys").stdout, verbosity=1).run(
    unittest.defaultTestLoader.loadTestsFromModule(test_module))
if not test_result.wasSuccessful():
    raise SystemExit("R7 supervisor/worker handshake test failed")

print(json.dumps({
    "status": "passed_effect_free_r7_truss051_packet_validation",
    "supervisor_handshake_inert_tests": f"passed {test_result.testsRun} cases",
    "native_dispatch": False,
    "postgres_started": False,
    "database_connected": False,
    "acceptance_replayed": False,
    "original_pgdata_mutated": False,
    "pg18_required_directories": len(PG18_DIRS),
    "source_pgdata_bytes": source_bytes,
    "source_pgdata_files": source_files,
    "source_tree_sha256": source_tree_sha,
    "source_process_inventory": "not_attempted_by_effect_free_checker; required immediately before native start",
    "free_bytes": free,
    "required_free_bytes": required_free,
    "truss_wheel_sha256": wheel_hash,
    "runtime_manifest_sha256": sha(manifest),
    "migration_edges": [dict(edge) for edge in chain],
    "migration_api_signatures": {name: str(inspect.signature(getattr(RuntimeMigrations, name))) for name in ("prepare", "apply", "reconcile", "receipt")},
    "consumer_id": scope["consumer_and_model"]["consumer_id"],
    "role_names": [scope["consumer_and_model"][key] for key in ("reader_login_role", "module_reader_role", "module_writer_role", "registrar_login_role")],
    "module_grant": grant,
    "seed_request_sha256": sha(seed_raw),
    "seed_operation_count": len(seed["operations"]),
    "pure_acceptance_preparation": "passed",
    "max_feed_page": {"facts": scope["consumer_and_model"]["feed_maximum_facts"], "bytes": scope["consumer_and_model"]["feed_maximum_bytes"]},
    "acknowledgement": False,
    "publication": False,
}, sort_keys=True))

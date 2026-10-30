"""R16 .51 one-start migration and full-lifecycle candidate.

`run_migration_stage` performs migration/readback within a caller-owned live
PostgreSQL start. `run_one_start_prefix_and_migration` composes that stage with
custody, publication, ACK, seed, continuation, and the prefix-owned stop; both
are effectful entrypoints and require a populated exact owner-approved scope.
No CLI is provided. Repository tests inspect pinned metadata and exercise only
a fully mocked orchestration path; they do not invoke native effects.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.resources
import importlib.util
import inspect
import json
import os
import re
import signal
import subprocess
import time
import uuid
from pathlib import Path

PROFILE = "ashlar-truss051-r16-migration-readback/0.1"
FULL_LIFECYCLE_SCOPE = "ashlar-truss-r16-full-native-lifecycle/0.1"
SOURCE_ARCHIVE_SHA256 = "cb57fdb7d338f345cfad157463fdbe94a2be7dcf69e728c474954c2939c14a0b"
SOURCE_MANIFEST_SHA256 = "fb79cebc5b1388b49d0687c9f77c61187125299758ee7388fd64ba2c9d533348"
TRUSS_WHEEL_SHA256 = "eb59141c48e94bb93ab7053e9e8725012eee1749e4432e9c71804d23abbdff57"
TRUSS_051_OUTER_INDEX_SHA256 = "d1672e85ae0e89137986fc57669186af47e5826366bd430284aba9b7cf52c37d"
TRUSS_050_EMBEDDED_INDEX_SHA256 = "4c6baf0282e3d0b5492f070c9c07d89ff0c9a819e748f3820f60b11c1f51c7aa"
TRUSS_048_MANIFEST_SHA256 = "c4339ac7f09ab5a004c272ab00962107a970938004c1765ec4163e4d5ca9b6f0"
TRUSS_049_INTERMEDIATE_SHA256 = "35fd797b5cf48dd888c68e53b9e599df20cffe00f8f3d33e1563820ea808d59f"
TRUSS_051_NATIVE_MANIFEST_SHA256 = "288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4"
TRUSS_051_RUNTIME_MANIFEST_SHA256 = TRUSS_051_NATIVE_MANIFEST_SHA256
WEFT_VERSION = "0.4.56"
WEFT_WHEEL_SHA256 = "68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d"
SPARK_VERSION = "4.2.0"
DELTA_VERSION = "4.4.1"
PG_CTL_SHA256 = "3021087af95fd33bfd83106c85de47c4756e44aa7edde78446e7593604d1b451"
PG_CTL_VERSION = "pg_ctl (PostgreSQL) 18.6 (Homebrew)"
CARRIER_PROFILE = "ashlar-delta/0.4-xxhash64"
SEED_REQUEST_SHA256 = "56aed4d7a4789f45cdaa7af55a834ff38ca2b4bcb75f44658b3031ba5f8aa771"
ORIGIN_MAP_SHA256 = "c19540a4c1cc3561365cc4ec38a71da44fdc4648b8f27a6540c81eb5233badc5"

FROZEN_CUSTODY = {
    "expected_accepted_source_document_id": "ashlar-current-045-lifecycle-20261021",
    "expected_source_revision": "ashlar-current-045-lifecycle-r1",
    "expected_source_registration_id": "ccfcd423-5893-4df9-8f88-6504e086bd4d",
    "expected_source_sha256": "498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b",
    "expected_catalog_revision": 1,
    "expected_source_report_sha256": "c797191c90c516b0997801a63605d297e5e429b50e922cd7da2d39640d570c9a",
    "expected_complete_view_sha256": "cc36fe489326f73c2eaf153ba2e025feeb76eaa88ffa0d6b675a1f4772264dc7",
    "expected_feed_id": "ashlar-current-045-lifecycle",
    "expected_consumer_id": "ashlar-current-048-lifecycle-20261022",
    "expected_consumer_registration_id": "9f03eb5d-ce72-437f-8b32-0112c130e724",
    # Taken from the R16 future-scope template and corroborated by prior .49 receipts.
    "expected_genesis_checkpoint_sha256": "c74e4c2dd697421f26a378454ec489bcb83501a4997235b670bdb011e4e7be8e",
}

MIGRATION_EDGES = (
    {
        "source": TRUSS_048_MANIFEST_SHA256,
        "target": TRUSS_049_INTERMEDIATE_SHA256,
        "artifact": "edge-c4339ac7f09ab5a004c272ab00962107a970938004c1765ec4163e4d5ca9b6f0-35fd797b5cf48dd888c68e53b9e599df20cffe00f8f3d33e1563820ea808d59f.json",
    },
    {
        "source": TRUSS_049_INTERMEDIATE_SHA256,
        "target": TRUSS_051_NATIVE_MANIFEST_SHA256,
        "artifact": "edge-35fd797b5cf48dd888c68e53b9e599df20cffe00f8f3d33e1563820ea808d59f-288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4.json",
    },
)


class AdmissionError(RuntimeError):
    """Raised when a frozen input or exact receipt does not admit progress."""


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _need(condition: bool, message: str) -> None:
    if not condition:
        raise AdmissionError(message)


def read_regular(path: Path, limit: int) -> bytes:
    if not path.is_absolute() or path.resolve() != path or path.is_symlink() or not path.is_file():
        raise AdmissionError(f"canonical regular file required: {path}")
    if path.stat().st_size > limit:
        raise AdmissionError(f"bounded file limit exceeded: {path}")
    return path.read_bytes()


def write_new(path: Path, raw: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _write_json_new(path: Path, value: object) -> str:
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    write_new(path, raw)
    return sha(raw)


def verify_installed_runtime() -> dict:
    """Read-only validation of installed package identity and the public .51 route."""
    truss_dist = importlib.metadata.distribution("umf-truss")
    weft_dist = importlib.metadata.distribution("umf-weft")
    _need(truss_dist.version == "0.0.51", "Truss 0.0.51 runtime required")
    _need(weft_dist.version == WEFT_VERSION, "selected Weft 0.4.56 runtime required")
    _need(importlib.metadata.version("pyspark") == SPARK_VERSION, "Spark 4.2.0 runtime required")
    _need(importlib.metadata.version("delta-spark") == DELTA_VERSION, "Delta 4.4.1 runtime required")
    direct_url = json.loads(truss_dist.read_text("direct_url.json") or "{}")
    wheel_hash = direct_url.get("archive_info", {}).get("hashes", {}).get("sha256")
    if wheel_hash is None:
        value = direct_url.get("archive_info", {}).get("hash", "")
        wheel_hash = value.removeprefix("sha256=") if value.startswith("sha256=") else None
    _need(wheel_hash == TRUSS_WHEEL_SHA256, "installed Truss wheel digest differs from official .51 wheel")
    weft_url = json.loads(weft_dist.read_text("direct_url.json") or "{}")
    weft_hash = weft_url.get("archive_info", {}).get("hashes", {}).get("sha256")
    if weft_hash is None:
        value = weft_url.get("archive_info", {}).get("hash", "")
        weft_hash = value.removeprefix("sha256=") if value.startswith("sha256=") else None
    _need(weft_hash == WEFT_WHEEL_SHA256,
          "installed Weft wheel digest differs from selected 0.4.56 wheel")
    runtime_manifest = importlib.resources.files("truss").joinpath("_catalog_runtime/manifest.json").read_bytes()
    _need(sha(runtime_manifest) == TRUSS_051_RUNTIME_MANIFEST_SHA256,
          "installed Truss runtime manifest digest mismatch")

    from truss._migration_recipe import load_recipe, plan_route
    from truss.migrations import RuntimeMigrations
    from truss import CatalogReferenceAssembly
    _need("attempt_id" in inspect.signature(RuntimeMigrations.prepare).parameters
          and "reference" in inspect.signature(RuntimeMigrations.apply).parameters
          and "reference" in inspect.signature(RuntimeMigrations.reconcile).parameters
          and "reference" in inspect.signature(RuntimeMigrations.receipt).parameters,
          "public Truss migration prepare/apply/reconcile/receipt methods unavailable")
    for method in ("status_in_transaction", "report_in_transaction",
                   "catalog_view_request", "catalog_view_in_transaction",
                   "accepted_document_in_transaction", "read_feed_in_transaction"):
        _need(callable(getattr(CatalogReferenceAssembly, method, None)),
              f"public Truss source/readback API unavailable: {method}")
    manifest, artifacts, outer_sha = load_recipe()
    _need(outer_sha == TRUSS_051_OUTER_INDEX_SHA256,
          "installed .51 outer migration index digest mismatch")
    embedded = artifacts.get(TRUSS_050_EMBEDDED_INDEX_SHA256 + ".manifest.json")
    _need(type(embedded) is bytes and sha(embedded) == TRUSS_050_EMBEDDED_INDEX_SHA256,
          "installed embedded .50 migration index digest mismatch")
    _need(manifest.get("target") == TRUSS_051_NATIVE_MANIFEST_SHA256,
          "installed .51 migration target manifest mismatch")
    route = plan_route(manifest, TRUSS_048_MANIFEST_SHA256)
    chain = tuple({key: edge[key] for key in ("source", "target", "artifact")} for edge in route)
    _need(chain == MIGRATION_EDGES, "installed public route differs from exact .48 → .49 → .51 route")
    return {
        "truss_version": truss_dist.version,
        "truss_wheel_sha256": wheel_hash,
        "weft_version": weft_dist.version,
        "pyspark_version": SPARK_VERSION,
        "delta_spark_version": DELTA_VERSION,
        "runtime_manifest_sha256": sha(runtime_manifest),
        "outer_index_sha256": outer_sha,
        "embedded_050_index_sha256": sha(embedded),
        "source_manifest_sha256": TRUSS_048_MANIFEST_SHA256,
        "target_manifest_sha256": TRUSS_051_NATIVE_MANIFEST_SHA256,
        "edges": [dict(edge) for edge in chain],
        "public_stage_apis": {
            "prepare": str(inspect.signature(RuntimeMigrations.prepare)),
            "apply": str(inspect.signature(RuntimeMigrations.apply)),
            "reconcile": str(inspect.signature(RuntimeMigrations.reconcile)),
            "receipt": str(inspect.signature(RuntimeMigrations.receipt)),
            "source_document": str(inspect.signature(CatalogReferenceAssembly.accepted_document_in_transaction)),
            "catalog_view": str(inspect.signature(CatalogReferenceAssembly.catalog_view_in_transaction)),
            "pending_feed": str(inspect.signature(CatalogReferenceAssembly.read_feed_in_transaction)),
        },
        "native_migration_invoked": False,
    }


def _validate_scope_approval(scope_path: Path, approval_path: Path, worker_path: Path,
                             prefix_worker_path: Path) -> tuple[dict, bytes]:
    scope_raw = read_regular(scope_path, 256 * 1024)
    approval_raw = read_regular(approval_path, 64 * 1024)
    scope = json.loads(scope_raw)
    approval = json.loads(approval_raw)
    _need(scope.get("profile") == FULL_LIFECYCLE_SCOPE,
          "exact populated R16 full-lifecycle scope required")
    _need(scope.get("authorization") == "one_run_local_native_lifecycle",
          "fresh full-lifecycle authorization for one owned PostgreSQL start required")
    _need(scope.get("limits", {}).get("attempts") == 1
          and scope.get("limits", {}).get("automatic_retries") == 0
          and scope.get("limits", {}).get("cloud_resources") is False,
          "single local attempt and zero automatic retries required")
    source = scope.get("source", {})
    _need(source.get("archive_sha256") == SOURCE_ARCHIVE_SHA256
          and source.get("manifest_sha256") == SOURCE_MANIFEST_SHA256,
          "exact frozen R16 source archive and manifest required")
    archive_raw = read_regular(Path(source.get("archive_path", "")), 16 * 1024 * 1024)
    manifest_raw = read_regular(Path(source.get("manifest_path", "")), 2 * 1024 * 1024)
    _need(sha(archive_raw) == SOURCE_ARCHIVE_SHA256
          and sha(manifest_raw) == SOURCE_MANIFEST_SHA256,
          "frozen R16 source archive or manifest bytes changed")
    source_manifest = json.loads(manifest_raw)
    _need(source_manifest.get("archive_sha256") == SOURCE_ARCHIVE_SHA256
          and source_manifest.get("profile") == "ashlar-r16-selected-source-inputs/0.1",
          "source manifest does not bind the frozen source archive")
    seed = scope.get("seed_custody", {})
    request_path = Path(seed.get("request_path", ""))
    origin_path = Path(seed.get("origin_map_path", ""))
    origin_gate_path = Path(seed.get("origin_gate_path", ""))
    request_raw = read_regular(request_path, 64 * 1024)
    origin_raw = read_regular(origin_path, 64 * 1024)
    origin_gate_raw = read_regular(origin_gate_path, 256 * 1024)
    _need(seed.get("request_sha256") == SEED_REQUEST_SHA256
          and sha(request_raw) == SEED_REQUEST_SHA256
          and source_manifest.get("seed_request", {}).get("sha256") == SEED_REQUEST_SHA256,
          "exact accepted seven-operation R16 seed request bytes required")
    _need(seed.get("origin_map_expected_sha256") == ORIGIN_MAP_SHA256
          and sha(origin_raw) == ORIGIN_MAP_SHA256
          and json.loads(origin_raw) == {
              "authority_scope": "local_truss_acceptance_trial",
              "profile": "ashlar-r16-seed-origin/0.1",
              "purpose": "Truss 0.0.51 local lifecycle qualification; not production source authority"},
          "exact newly proposed R16 local-trial origin map required")
    _need(seed.get("origin_gate_code_sha256") ==
          "80d937e7c8ec9fff51d04cb08ecea27c6c32dd05df6b87d2bb0a87fc62347ebf"
          and sha(origin_gate_raw) == seed.get("origin_gate_code_sha256"),
          "exact frozen R16 origin admission gate required")
    from ashlar_host.local_current_delta4_umf import (
        BRIDGE_SHA256, BUILD_INPUTS_SHA256, REALIZATION_MANIFEST_SHA256,
        ROLE_SEMANTICS_SHA256)
    expected_bridge = {"profile": "ashlar-local-current-delta4-umf-realization/0.1",
        "carrier_profile": CARRIER_PROFILE, "bridge_sha256": BRIDGE_SHA256,
        "resource_sha256": REALIZATION_MANIFEST_SHA256,
        "build_inputs_sha256": BUILD_INPUTS_SHA256,
        "role_semantics_sha256": ROLE_SEMANTICS_SHA256}
    _need(scope.get("bridge") == expected_bridge,
          "exact reusable selected .4 UMF bridge/resource/build/role hashes required")
    destination_id = scope.get("publication", {}).get("destination_id")
    _need(type(destination_id) is str and 1 <= len(destination_id.encode()) <= 1024
          and "\0" not in destination_id
          and scope.get("publication", {}).get("carrier_profile") == CARRIER_PROFILE,
          "fresh explicitly approved local `.4` destination identity required")
    runtime = scope.get("runtime", {})
    _need(runtime.get("truss_version") == "0.0.51"
          and runtime.get("wheel_sha256") == TRUSS_WHEEL_SHA256
          and runtime.get("outer_migration_index_sha256") == TRUSS_051_OUTER_INDEX_SHA256
          and runtime.get("embedded_050_index_sha256") == TRUSS_050_EMBEDDED_INDEX_SHA256
          and runtime.get("runtime_manifest_sha256") == TRUSS_051_RUNTIME_MANIFEST_SHA256
          and runtime.get("weft_version") == WEFT_VERSION
          and runtime.get("weft_wheel_sha256") == WEFT_WHEEL_SHA256
          and runtime.get("pyspark_version") == SPARK_VERSION
          and runtime.get("delta_spark_version") == DELTA_VERSION
          and runtime.get("carrier_profile") == CARRIER_PROFILE
          and runtime.get("external_pg_ctl_sha256") == PG_CTL_SHA256
          and runtime.get("external_pg_ctl_version") == PG_CTL_VERSION,
          "exact pinned Truss .51 / Weft .4 / Spark / Delta profile and migration index hashes required")
    java_home = Path(runtime.get("java_home", ""))
    jars_dir = Path(runtime.get("jars_directory", ""))
    bun_path = Path(runtime.get("bun_path", ""))
    _need(java_home.is_absolute() and java_home.resolve() == java_home
          and jars_dir.is_absolute() and jars_dir.resolve() == jars_dir
          and bun_path.is_absolute() and bun_path.resolve() == bun_path,
          "canonical JDK21, selected Delta JAR directory, and Bun paths required")
    expected_jar_hashes = {
        "delta-spark_4.2_2.13-4.4.1.jar": "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
        "delta-storage-4.4.1.jar": "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
    }
    _need(type(runtime.get("jar_sha256")) is dict
          and runtime["jar_sha256"] == expected_jar_hashes
          and java_home.is_dir() and (java_home / "bin/java").is_file()
          and (java_home / "release").is_file()
          and jars_dir.is_dir()
          and {path.name for path in jars_dir.iterdir()} in
              (set(expected_jar_hashes), set(expected_jar_hashes) | {"inventory.json"})
          and all(sha(read_regular(jars_dir / name, 32 * 1024 * 1024)) == digest
                  for name, digest in expected_jar_hashes.items()),
          "exact configured JDK21 and Delta 4.4.1 jar inventory required")
    _need(sha(read_regular(bun_path, 128 * 1024 * 1024)) == runtime.get("bun_sha256"),
          "scope-bound Bun executable hash required")
    bun_version = subprocess.run([str(bun_path), "--version"], stdin=subprocess.DEVNULL,
                                 capture_output=True, text=True, timeout=5, check=False)
    _need(bun_version.returncode == 0 and bun_version.stdout.strip() == "1.4.3",
          "exact Bun 1.4.3 seed runtime required")
    scope_custody = scope.get("source_custody", {})
    _need(type(scope_custody) is dict and all(scope_custody.get(key) == value
          for key, value in FROZEN_CUSTODY.items()),
          "complete frozen source/feed/consumer/checkpoint custody required")
    workers = scope.get("workers", {})
    migration_worker_sha = sha(read_regular(worker_path, 2 * 1024 * 1024))
    custody_worker_sha = sha(read_regular(prefix_worker_path, 2 * 1024 * 1024))
    _need(workers == {"custody_prefix_sha256": custody_worker_sha,
                      "migration_readback_sha256": migration_worker_sha},
          "full-lifecycle scope must bind exact custody-prefix and migration workers")
    state = scope.get("state_custody", {})
    _need(type(scope.get("output_root")) is str and Path(scope["output_root"]).is_absolute(),
          "scope output_root path required")
    state["output_root"] = scope["output_root"]
    for key in ("restored_pgdata_path", "registry_path", "genesis_page_path"):
        _need(type(state.get(key)) is str and Path(state[key]).is_absolute(),
              f"scope path required: state_custody.{key}")
    root = Path(state["output_root"])
    paths = [root, Path(state["restored_pgdata_path"]),
             Path(state["registry_path"]), Path(state["genesis_page_path"])]
    _need(all(path.resolve() == path for path in paths), "canonical scope paths required")
    _need(root.resolve() == root and Path(state["restored_pgdata_path"]).is_relative_to(root)
          and Path(state["registry_path"]).is_relative_to(root)
          and Path(state["genesis_page_path"]).is_relative_to(root),
          "migration registry and custody receipts must remain under the attempt root")
    socket_directory = Path(runtime.get("socket_directory", ""))
    port = runtime.get("port")
    pg_ctl = Path(runtime.get("pg_ctl_path", ""))
    _need(socket_directory.is_absolute() and socket_directory.resolve() == socket_directory
          and type(port) is int and 1024 <= port <= 65535
          and runtime.get("database") == "postgres"
          and runtime.get("bootstrap_user") == "postgres"
          and pg_ctl.is_absolute() and pg_ctl.resolve() == pg_ctl,
          "approved local socket, port, database, and bootstrap role required")
    _need(sha(read_regular(pg_ctl, 32 * 1024 * 1024)) == PG_CTL_SHA256,
          "external PostgreSQL 18.6 pg_ctl executable hash mismatch")
    version = subprocess.run([str(pg_ctl), "--version"], stdin=subprocess.DEVNULL,
                             capture_output=True, text=True, timeout=5, check=False)
    _need(version.returncode == 0 and version.stdout.strip() == PG_CTL_VERSION,
          "external pg_ctl must identify as the approved PostgreSQL 18.6 build")
    migration = scope.get("migration", {})
    _need(type(migration.get("attempt_id")) is str,
          "one preapproved migration attempt UUID required")
    try:
        _need(str(uuid.UUID(migration["attempt_id"])) == migration["attempt_id"],
              "canonical migration attempt UUID required")
    except (ValueError, AttributeError):
        raise AdmissionError("canonical migration attempt UUID required") from None
    expected_approval = {
        "profile": "ashlar-truss-r16-full-native-lifecycle-approval/0.1",
        "scope_sha256": sha(scope_raw),
        "workers": workers,
        "source_archive_sha256": SOURCE_ARCHIVE_SHA256,
        "source_manifest_sha256": SOURCE_MANIFEST_SHA256,
        "runtime": {
            "truss_version": "0.0.51",
            "wheel_sha256": TRUSS_WHEEL_SHA256,
            "weft_version": WEFT_VERSION,
            "weft_wheel_sha256": WEFT_WHEEL_SHA256,
            "pyspark_version": SPARK_VERSION,
            "delta_spark_version": DELTA_VERSION,
            "carrier_profile": CARRIER_PROFILE,
            "outer_migration_index_sha256": TRUSS_051_OUTER_INDEX_SHA256,
            "embedded_050_index_sha256": TRUSS_050_EMBEDDED_INDEX_SHA256,
            "runtime_manifest_sha256": TRUSS_051_RUNTIME_MANIFEST_SHA256,
            "jar_sha256": expected_jar_hashes,
            "bun_sha256": runtime["bun_sha256"],
        },
        "seed": {"request_sha256": SEED_REQUEST_SHA256,
                 "origin_map_sha256": ORIGIN_MAP_SHA256,
                 "origin_gate_code_sha256": seed["origin_gate_code_sha256"]},
        "bridge": expected_bridge,
        "destination_id": destination_id,
        "scope": "one owned PostgreSQL start; preserve/restore and .48 custody; one exact .48-to-.51 migration; public source/view/page recheck; one no-change genesis Delta publication with independent verification and protected ACK; one approved seven-operation seed; one independently verified actual continuation with protected ACK; stop",
    }
    _need(approval == expected_approval,
          "approval bytes do not exactly bind scope, workers, runtimes, inputs, and full-lifecycle stages")
    return scope, scope_raw


def _verify_prefix_receipt(scope: dict, prefix_raw: bytes) -> tuple[dict, bytes, bytes]:
    state = scope["state_custody"]
    _need(type(prefix_raw) is bytes and len(prefix_raw) <= 256 * 1024,
          "actual bounded in-memory custody-prefix receipt bytes required")
    prefix = json.loads(prefix_raw)
    _need(prefix.get("profile") == "ashlar-truss051-r16-pre-migration-custody/0.1"
          and prefix.get("status") == "custody_prefix_verified_migration_not_invoked"
          and prefix.get("migration_invoked") is False
          and prefix.get("spark_started") is False,
          "successful durable R16 pre-migration custody receipt required while the owned server is live")
    observed = prefix.get("source_custody", {})
    for actual, frozen in (("document_id", "expected_accepted_source_document_id"),
                           ("document_revision", "expected_source_revision"),
                           ("source_registration_id", "expected_source_registration_id"),
                           ("source_sha256", "expected_source_sha256"),
                           ("catalog_revision", "expected_catalog_revision"),
                           ("report_sha256", "expected_source_report_sha256"),
                           ("view_sha256", "expected_complete_view_sha256")):
        _need(observed.get(actual) == FROZEN_CUSTODY[frozen],
              f"pre-migration public .48 custody differs: {actual}")
    _need(type(state.get("genesis_page_path")) is str
          and Path(state["genesis_page_path"]).is_absolute(),
          "approved actual genesis page archive path required")
    before_path = Path(state["genesis_page_path"])
    page_raw = read_regular(before_path, 2 * 1024 * 1024)
    page = prefix.get("genesis_page", {})
    _need(sha(page_raw) == page.get("archive_sha256"),
          "pre-migration genesis page archive does not match stopped custody receipt")
    from ashlar_host.truss_page_archive import decode_truss_page
    decoded = decode_truss_page(page_raw, sha(page_raw), FROZEN_CUSTODY["expected_feed_id"])
    _need(page.get("consumer_id") == FROZEN_CUSTODY["expected_consumer_id"]
          and page.get("registration_id") == FROZEN_CUSTODY["expected_consumer_registration_id"]
          and decoded.consumer_id == page.get("consumer_id")
          and decoded.registration_id == page.get("registration_id")
          and decoded.sha256 == page.get("page_sha256")
          and decoded.writer_xid == page.get("writer_xid")
          and page.get("next_checkpoint_sha256") == FROZEN_CUSTODY["expected_genesis_checkpoint_sha256"],
          "genesis page consumer/checkpoint differs from frozen custody")
    return prefix, prefix_raw, page_raw


def _result_dict(result) -> dict:
    fields = ("outcome", "reference", "attempt_id", "source", "target", "detail",
              "receipt", "current_verification")
    answer = {key: getattr(result, key) for key in fields if hasattr(result, key)}
    return json.loads(json.dumps(answer, sort_keys=True, allow_nan=False))


def _readback_source_view(conn_uri: str, attempt_root: Path) -> dict:
    """Read .51 source and complete-view APIs in the required role transactions."""
    import psycopg
    from truss import (Ok, CatalogRuntimeConfiguration, CatalogViewAvailable,
                       adopt_psycopg_host, create_reference_assembly)
    from ashlar_host.lifecycle import finish

    observed = {}
    for role, access_mode in (("ashlar048_accept_consumer_r1", "read_write"),
                              ("ashlar048_r5_source_reader", "read_only")):
        params = psycopg.conninfo.conninfo_to_dict(conn_uri)
        params["user"] = role
        connection = psycopg.connect(**params, connect_timeout=5,
                                      options="-c statement_timeout=5000")
        connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED "
                           + ("READ WRITE" if access_mode == "read_write" else "READ ONLY"))
        connection.execute("SET LOCAL statement_timeout='5000ms'")
        host = adopt_psycopg_host(connection)
        assembly = None
        primary = None
        try:
            tx = host.adopt_transaction(access_mode=access_mode)
            built = create_reference_assembly(CatalogRuntimeConfiguration(), host)
            if not isinstance(tx, Ok) or not isinstance(built, Ok):
                raise AdmissionError("public .51 source/catalog transaction refused")
            assembly = built.value
            if role == "ashlar048_accept_consumer_r1":
                status = assembly.status_in_transaction(tx.value)
                report = assembly.report_in_transaction(tx.value, 1)
                request = assembly.catalog_view_request(definition_count="65536", encoded_bytes="16777216")
                view = assembly.catalog_view_in_transaction(tx.value, request)
                if not all(isinstance(item, Ok) for item in (status, report, view)):
                    raise AdmissionError("public .51 status/report/complete-view read refused")
                if not status.value.installed or status.value.revision != 1:
                    raise AdmissionError("frozen catalog revision 1 not present after migration")
                report_raw = report.value.original_bytes
                if not isinstance(view.value, CatalogViewAvailable):
                    reason = getattr(view.value, "reason", "unknown")
                    raise AdmissionError(f"public .51 complete view unavailable: {reason}")
                view_raw = view.value.view.original_bytes
                _need(type(report_raw) is bytes and len(report_raw) <= 2 * 1024 * 1024
                      and type(view_raw) is bytes and len(view_raw) <= 16 * 1024 * 1024,
                      "bounded exact public report and complete view bytes required")
                _need(sha(report_raw) == FROZEN_CUSTODY["expected_source_report_sha256"],
                      "post-migration .51 source report hash mismatch")
                _need(sha(view_raw) == FROZEN_CUSTODY["expected_complete_view_sha256"],
                      "post-migration .51 complete view hash mismatch")
                _need(json.loads(view_raw).get("scope") == {"kind": "complete"},
                      "post-migration public catalog view is not complete")
                _need(str(json.loads(view_raw).get("catalogRevision")) == "1",
                      "post-migration public catalog view revision mismatch")
                report_path = attempt_root / "source-report-after-migration.json"
                view_path = attempt_root / "complete-view-after-migration.json"
                write_new(report_path, report_raw)
                write_new(view_path, view_raw)
                observed.update(catalog_revision=1, report_sha256=sha(report_raw),
                                report_bytes_path=str(report_path),
                                complete_view_sha256=sha(view_raw),
                                complete_view_bytes_path=str(view_path))
            else:
                accepted = assembly.accepted_document_in_transaction(
                    tx.value,
                    document_id=FROZEN_CUSTODY["expected_accepted_source_document_id"],
                    maximum_bytes=1_048_576)
                if not isinstance(accepted, Ok):
                    raise AdmissionError("public .51 accepted-source read refused")
                value = accepted.value
                _need(value.document_id == FROZEN_CUSTODY["expected_accepted_source_document_id"]
                      and value.document_revision == FROZEN_CUSTODY["expected_source_revision"]
                      and value.registration_id == FROZEN_CUSTODY["expected_source_registration_id"]
                      and value.sha256 == FROZEN_CUSTODY["expected_source_sha256"]
                      and sha(value.content) == FROZEN_CUSTODY["expected_source_sha256"],
                      "post-migration accepted-source identity or byte hash mismatch")
                source_path = attempt_root / "accepted-source-after-migration.umf.json"
                write_new(source_path, value.content)
                observed.update(document_id=value.document_id,
                                document_revision=value.document_revision,
                                source_registration_id=value.registration_id,
                                source_sha256=sha(value.content),
                                source_bytes_path=str(source_path))
        except BaseException as error:
            primary = error
        def detach():
            if not host.detach():
                raise AdmissionError("public .51 source/catalog host detach refused")
        try:
            finish(primary, ([assembly.dispose] if assembly is not None else []) + [detach])
            connection.rollback()
        finally:
            connection.close()
    return observed


def _readback_page(conn_uri: str, expected_archive: bytes) -> dict:
    """Read the same pending page through the public .51 feed API, without ACK."""
    import psycopg
    from truss import (Ok, CatalogRuntimeConfiguration, adopt_psycopg_host,
                       create_reference_assembly)
    from truss.feed import FeedPage
    from ashlar_host.r04_truss051_feed import read_next_transaction
    from ashlar_host.truss_page_archive import encode_truss_page
    from ashlar_host.lifecycle import finish

    params = psycopg.conninfo.conninfo_to_dict(conn_uri)
    params["user"] = "ashlar048_accept_consumer_r1"
    with psycopg.connect(**params, autocommit=False, connect_timeout=5,
                         options="-c statement_timeout=5000") as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY")
        connection.execute("SET LOCAL statement_timeout='5000ms'")
        host = adopt_psycopg_host(connection)
        assembly = None
        primary = None
        page = None
        try:
            tx = host.adopt_transaction(access_mode="read_only")
            built = create_reference_assembly(CatalogRuntimeConfiguration(), host)
            if not isinstance(tx, Ok) or not isinstance(built, Ok):
                raise AdmissionError("public .51 pending-feed transaction refused")
            assembly = built.value
            page = read_next_transaction(
                assembly, tx.value,
                {"consumer_id": FROZEN_CUSTODY["expected_consumer_id"],
                 "registration_id": FROZEN_CUSTODY["expected_consumer_registration_id"]},
                ok_type=Ok, page_type=FeedPage)
            if not isinstance(page, FeedPage) or page.outcome != "transaction":
                raise AdmissionError("public .51 pending genesis transaction page required")
            actual = encode_truss_page(page, FROZEN_CUSTODY["expected_feed_id"])
            _need(actual == expected_archive, "pending genesis page bytes changed across migration")
        except BaseException as error:
            primary = error
        def detach():
            if not host.detach():
                raise AdmissionError("public .51 feed host detach refused")
        finish(primary, ([assembly.dispose] if assembly is not None else []) + [detach])
        connection.rollback()
    return {"page_sha256": page.sha256, "archive_sha256": sha(expected_archive),
            "writer_xid": page.writer_xid,
            "consumer_id": page.consumer_id,
            "registration_id": page.registration_id,
            "prior_checkpoint_sha256": sha(page.prior.content),
            "next_checkpoint_sha256": sha(page.next.content),
            "acknowledgement_invoked": False}


def run_migration_stage(scope_path: Path, approval_path: Path, worker_path: Path,
                        prefix_worker_path: Path, conn_uri: str,
                        prefix_receipt_raw: bytes) -> dict:
    """Run the .51 migration/readback stage inside the enclosing single PG start.

    The caller owns startup and stop. It must pass the exact connection and the
    actual prefix receipt bytes while that server remains up; this function never
    starts, restarts, or stops PostgreSQL.
    """
    scope, scope_raw = _validate_scope_approval(scope_path, approval_path, worker_path,
                                                prefix_worker_path)
    runtime_evidence = verify_installed_runtime()
    prefix, prefix_raw, before_page_raw = _verify_prefix_receipt(scope, prefix_receipt_raw)
    state = scope["state_custody"]
    root = Path(state["output_root"])
    restored = Path(state["restored_pgdata_path"])
    registry_path = Path(state["registry_path"])
    stage_root = root / "migration-readback"
    _need(root.is_dir() and not root.is_symlink() and root.resolve() == root,
          "exact existing pre-migration attempt root required")
    _need(restored.is_dir() and not restored.is_symlink() and restored.resolve() == restored,
          "exact restored PGDATA from custody prefix required")
    _need(not stage_root.exists() and not stage_root.is_symlink()
          and not registry_path.exists() and not registry_path.is_symlink(),
          "one fresh migration stage and absent recovery registry required; no retry")
    import psycopg
    live_params = psycopg.conninfo.conninfo_to_dict(conn_uri)
    _need("password" not in live_params
          and live_params.get("host") == scope["runtime"]["socket_directory"]
          and str(live_params.get("port")) == str(scope["runtime"]["port"])
          and live_params.get("dbname") == scope["runtime"]["database"]
          and live_params.get("user") == scope["runtime"]["bootstrap_user"],
          "live migration conninfo must be secret-free and match the approved local socket/port/database/role")
    stage_root.mkdir(mode=0o700, parents=False, exist_ok=False)
    _write_json_new(stage_root / "scope-admitted.json", {
        "profile": PROFILE, "scope_sha256": sha(scope_raw),
        "prefix_receipt_sha256": sha(prefix_raw),
        "pre_migration_page_archive_sha256": sha(before_page_raw),
        "runtime_evidence": runtime_evidence,
        "enclosing_single_start_required": True,
        "publication_invoked": False, "acknowledgement_invoked": False,
    })
    try:
        with psycopg.connect(**live_params, autocommit=True, connect_timeout=5) as probe:
            observed_data = Path(probe.execute("SHOW data_directory").fetchone()[0]).resolve()
        _need(observed_data == restored,
              "provided connection does not target exact restored PGDATA")
        connected_result = _run_migration_connected(
            scope, scope_raw, runtime_evidence, prefix_raw, before_page_raw,
            root, stage_root, registry_path, conn_uri)
    except BaseException as error:
        # No retry or later stage follows failure; enclosing owner retains server custody.
        try:
            _write_json_new(stage_root / "stage-failed.json", {
                "profile": PROFILE, "error": f"{type(error).__name__}: {error}"[:1500],
                "scope_sha256": sha(scope_raw), "no_retry": True,
                "later_stages_invoked": False,
            })
        except FileExistsError:
            pass
        raise
    final_receipt = {
        "profile": PROFILE,
        "status": "migration_and_public_readback_verified_stop_before_publication",
        "scope_sha256": sha(scope_raw),
        "prefix_receipt_sha256": sha(prefix_raw),
        "pre_migration_page_archive_sha256": sha(before_page_raw),
        **connected_result,
        "migration_invoked": True,
        "publication_invoked": False,
        "acknowledgement_invoked": False,
        "seed_invoked": False,
        "continuation_invoked": False,
        "postgres_stop_owned_by_enclosing_lifecycle": True,
    }
    _write_json_new(stage_root / "migration-readback-verified.json", final_receipt)
    return final_receipt


def _verify_delta4_bridge(scope: dict) -> dict:
    """Run the existing owner-pinned UMF bridge proposal path without Spark."""
    from ashlar_host.local_current_delta4_umf import (
        LocalUmfDelta4Bridge, ROLE_SOURCE_SHA256, pinned_resources)
    from ashlar_host.managed_current_carriers import load_current_managed_carriers
    from ashlar_host.schema_rows import LOCAL_CURRENT_ROLE_TO_UMF

    _manifest, _bundle, source_bytes, _semantics = pinned_resources()
    carriers = load_current_managed_carriers(CARRIER_PROFILE)
    _need(set(LOCAL_CURRENT_ROLE_TO_UMF.values()) == set(ROLE_SOURCE_SHA256),
          "complete six-role `.4` bridge map required")
    proposals = []
    with LocalUmfDelta4Bridge.open() as bridge:
        for local_role, carrier_role in LOCAL_CURRENT_ROLE_TO_UMF.items():
            raw = source_bytes.get(carrier_role + ".umf.json")
            _need(type(raw) is bytes and sha(raw) == ROLE_SOURCE_SHA256[carrier_role],
                  f"exact managed `.4` source bytes required: {carrier_role}")
            target = str(Path(scope["output_root"]) / "delta-destination" / local_role)
            proposal = bridge.realize(raw, target, carrier_role)
            _need(proposal.get("fields") == carriers.ordered_fields(carrier_role),
                  f"owner-generated `.4` field schema differs: {carrier_role}")
            proposals.append({"local_role": local_role, "carrier_role": carrier_role,
                "source_bytes_sha256": sha(raw), "proposal_sha256": proposal["proposal_sha256"],
                "sql_sha256": sha(proposal["proposal"]["sql"].encode("utf-8"))})
    return {"profile": "ashlar-r16-local-delta4-bridge-probe/0.1",
            "carrier_profile": CARRIER_PROFILE, "native_effects": False,
            "proposals": proposals}


def _verify_graph_manifest(manifest: dict) -> tuple[dict, dict]:
    _need(manifest.get("profile_version") == CARRIER_PROFILE,
          "exact `.4-xxhash64` publication manifest required")
    report = json.loads(manifest["validation_report_json"])
    _need(report.get("physical_carrier_profile") == CARRIER_PROFILE
          and report.get("lookup_hash_profile") == "spark-xxhash64-typed-key/0.1"
          and report.get("umf_revision") == "2ede4c879bea5db13794a6fa30e67c821306c17f",
          "exact `.4` physical carrier, signed hash and accepted UMF profile required")
    profile_sha = report.get("installation_profile_sha256")
    _need(type(profile_sha) is str and re.fullmatch(r"[0-9a-f]{64}", profile_sha),
          "immutable installed Delta profile digest required")
    vector = json.loads(manifest["table_versions_json"])
    required = {f"local.trussgraph.{role}" for role in
                ("object_current", "edge_current", "tombstone", "whole_source_history")}
    _need(type(vector) is dict and set(vector) == required,
          "complete four-role Delta publication vector required")
    return report, vector


def _role_connection_factory(conn_uri: str, user: str):
    import psycopg
    params = psycopg.conninfo.conninfo_to_dict(conn_uri)
    _need("password" not in params, "secret-free local PostgreSQL connection required")
    params["user"] = user
    def connect():
        return psycopg.connect(**params, connect_timeout=5,
                               options="-c statement_timeout=5000")
    return connect


def _validate_seed_report(path: Path, request: dict) -> dict:
    report = json.loads(read_regular(path, 1024 * 1024))
    original = report.get("group_original", {}).get("response", {})
    replay = report.get("committed_receipt_replay", {}).get("response", {})
    results = original.get("semantic", {}).get("results")
    expected = (["create_object"] * len(request.get("records", []))
                + ["create_edge"] * len(request.get("edges", [])))
    _need(type(results) is list and len(results) == 7 and len(expected) == 7
          and len(report.get("objects", [])) == 4 and len(report.get("edges", [])) == 3
          and original.get("semantic") == replay.get("semantic"),
          "committed exact seven-operation seed replay required")
    event_count = 0
    for result, operation in zip(results, expected, strict=True):
        _need(result.get("outcome") == "created" and result.get("operation") == operation,
              "committed seed operation order/outcome differs")
        events = result.get("events")
        _need(type(events) is list and bool(events), "each seed result must retain actual source events")
        event_count += len(events)
    _need(event_count == 7, "exact seven committed source events required")
    return {"objects": 4, "edges": 3, "operations": 7, "events": event_count,
            "report_sha256": sha(read_regular(path, 1024 * 1024)),
            "replay_semantics_verified": True}


def _read_actual_seed_page(conn_uri: str, scope: dict, genesis_page):
    import psycopg
    from truss import (Ok, CatalogRuntimeConfiguration, adopt_psycopg_host,
                       create_reference_assembly)
    from truss.feed import FeedPage
    from ashlar_host.r04_truss051_feed import read_next_transaction
    from ashlar_host.truss_page_archive import encode_truss_page
    from ashlar.truss_native_feed import native_feed_batch
    from ashlar_host.truss_catalog_reader import read_graph_catalog_evidence
    from ashlar_host.truss_graph_admission import TrussGraphAdmission
    from ashlar_host.lifecycle import finish

    params = psycopg.conninfo.conninfo_to_dict(conn_uri)
    params["user"] = "ashlar048_accept_consumer_r1"
    with psycopg.connect(**params, connect_timeout=5,
                         options="-c statement_timeout=5000") as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY")
        connection.execute("SET LOCAL statement_timeout='5000ms'")
        host = adopt_psycopg_host(connection); assembly = None; primary = None; page = None
        try:
            tx = host.adopt_transaction(access_mode="read_only")
            built = create_reference_assembly(CatalogRuntimeConfiguration(), host)
            if not isinstance(tx, Ok) or not isinstance(built, Ok):
                raise AdmissionError("public .51 seed-page transaction refused")
            assembly = built.value
            page = read_next_transaction(assembly, tx.value,
                {"consumer_id": FROZEN_CUSTODY["expected_consumer_id"],
                 "registration_id": FROZEN_CUSTODY["expected_consumer_registration_id"]},
                ok_type=Ok, page_type=FeedPage)
            if not isinstance(page, FeedPage) or page.outcome != "transaction":
                raise AdmissionError("actual post-seed .51 transaction page required")
        except BaseException as error:
            primary = error
        def detach():
            if not host.detach():
                raise AdmissionError("public .51 seed-page host detach refused")
        finish(primary, ([assembly.dispose] if assembly is not None else []) + [detach])
        connection.rollback()

    _need(page.consumer_id == FROZEN_CUSTODY["expected_consumer_id"]
          and page.registration_id == FROZEN_CUSTODY["expected_consumer_registration_id"]
          and page.prior.content == genesis_page.next.content,
          "actual seed page must immediately follow ACKed genesis cursor")
    batch = native_feed_batch(page.content, sha256=page.sha256,
        prior=page.prior.content, next=page.next.content,
        feed=FROZEN_CUSTODY["expected_feed_id"])
    evidence = read_graph_catalog_evidence(
        _role_connection_factory(conn_uri, "ashlar048_accept_consumer_r1"), batch)
    admission = TrussGraphAdmission(batch, evidence)
    _need(evidence.truss_version == "0.0.51"
          and admission.metadata().get("profile") ==
          "ashlar-truss-current-umf010-graph-semantic-admission/0.6",
          "exact public .51 graph semantic admission required for actual seed page")
    request_path = Path(scope["seed_custody"]["request_path"])
    request = json.loads(read_regular(request_path, 64 * 1024))
    group = json.loads(read_regular(Path(scope["state_custody"]["output_root"]) /
                                    "seed-group-report.json", 1024 * 1024))
    original_results = group["group_original"]["response"]["semantic"]["results"]
    source_id = json.loads(page.prior.content).get("installationId")
    _need(type(source_id) is str and bool(source_id),
          "actual Truss installation identity required for seed event comparison")
    expected_events = []
    for item in original_results:
        _need(item.get("outcome") == "created"
              and item.get("operation") in ("create_object", "create_edge"),
              "only committed fresh create events may feed seed publication")
        identity = item["identity"]
        for event in item.get("events", []):
            _need(event.get("xid") == page.writer_xid and event.get("source_epoch") == batch.epoch,
                  "seed event must belong to actual source transaction page")
            expected_events.append((source_id, event["xid"] + ":" + event["seq"],
                "object" if item["operation"] == "create_object" else "edge",
                str(identity["type_id"]), str(identity["id"])))
    observed = []
    for change in admission.changes:
        key = change.state.key
        observed.append((key.source, change.delivery_id, key.kind,
                         str(key.type_id), str(key.id)))
        _need(change.operation == "create", "fresh seed page may contain create operations only")
    _need(len(original_results) == 7 and len(expected_events) == 7
          and sorted(observed) == sorted(expected_events),
          "actual admitted .51 page differs from exact committed seed group")
    raw = encode_truss_page(page, FROZEN_CUSTODY["expected_feed_id"])
    page_path = Path(scope["state_custody"]["output_root"]) / "actual-seed-page.json"
    write_new(page_path, raw)
    return {"page": page, "page_sha256": page.sha256,
        "archive_sha256": sha(raw), "page_path": str(page_path),
        "writer_xid": page.writer_xid,
        "prior_checkpoint_sha256": sha(page.prior.content),
        "next_checkpoint_sha256": sha(page.next.content),
        "admission_profile": admission.metadata()["profile"],
        "admitted_change_count": len(admission.changes)}


def _run_post_migration_graph_lifecycle(scope: dict, conn_uri: str,
                                        migration_result: dict) -> dict:
    """Use existing .51 public feed, graph, Delta, seed and continuation adapters."""
    import psycopg
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
    from ashlar_host.configured_local_runtime_profile import (
        PARAMETER_SUBSTITUTION_SETTING, attest as attest_runtime,
        verify_loaded_delta_class)
    from ashlar_host.truss_note_publication import publish_graph_pages
    from ashlar_host.truss_catalog_reader import read_graph_catalog_evidence
    from ashlar_host.truss_graph_admission import TrussGraphAdmission
    from ashlar_host.truss_page_archive import decode_truss_page
    from ashlar.truss_native_feed import native_feed_batch

    root = Path(scope["output_root"])
    stage_receipts = {}
    source_path = Path(migration_result["source_view_readback"]["source_bytes_path"])
    source_raw = read_regular(source_path, 1_048_576)
    _need(sha(source_raw) == FROZEN_CUSTODY["expected_source_sha256"],
          "captured accepted-source bytes changed before seed")
    source_doc = json.loads(source_raw)
    request_path = Path(scope["seed_custody"]["request_path"])
    request_raw = read_regular(request_path, 64 * 1024)
    request = json.loads(request_raw)
    _need(source_doc.get("id") == request.get("document_id")
          and source_doc.get("umf") == request.get("source_version")
          and scope["source_custody"]["expected_source_revision"] == request.get("document_revision"),
          "accepted public source bytes do not bind the exact approved seed request")

    page_path = Path(scope["state_custody"]["genesis_page_path"])
    page_raw = read_regular(page_path, 2 * 1024 * 1024)
    page = decode_truss_page(page_raw, sha(page_raw), FROZEN_CUSTODY["expected_feed_id"])
    _need(page.sha256 == migration_result["pending_page_readback"]["page_sha256"]
          and sha(page.next.content) == FROZEN_CUSTODY["expected_genesis_checkpoint_sha256"],
          "actual post-migration genesis page differs from independent readback")

    batch = native_feed_batch(page.content, sha256=page.sha256,
        prior=page.prior.content, next=page.next.content,
        feed=FROZEN_CUSTODY["expected_feed_id"])
    evidence = read_graph_catalog_evidence(
        _role_connection_factory(conn_uri, "ashlar048_accept_consumer_r1"), batch)
    admission = TrussGraphAdmission(batch, evidence)
    _need(evidence.truss_version == "0.0.51" and not admission.changes
          and admission.metadata().get("profile") ==
          "ashlar-truss-current-umf010-graph-semantic-admission/0.6",
          "actual genesis page must be an unchanged `.51` graph catalog transaction")

    publication = scope["publication"]
    output = root / "delta-destination"
    warehouse = root / "spark-warehouse"
    _need(not output.exists() and not warehouse.exists(),
          "fresh `.4` destination and Spark warehouse required")
    runtime = scope["runtime"]
    java_home = Path(runtime["java_home"])
    jars_dir = Path(runtime["jars_directory"])
    class RuntimeConfig:
        pass
    runtime_config = RuntimeConfig()
    runtime_config.java_home = java_home
    runtime_config.jar_inventory = runtime["jar_sha256"]
    runtime_config.jars = jars_dir
    _profile, _runtime_report, jars = attest_runtime(runtime_config)
    conf = SparkConf(loadDefaults=False).setMaster("local[1]").setAppName("Ashlar .51 R16 local .4 lifecycle")
    settings = {
        "spark.driver.memory": "512m", "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "1", "spark.databricks.delta.snapshotPartitions": "1",
        "spark.sql.session.timeZone": "UTC",
        PARAMETER_SUBSTITUTION_SETTING: "true", "spark.sql.ansi.enabled": "true",
        "spark.sql.decimalOperations.allowPrecisionLoss": "false",
        "spark.jars": ",".join(map(str, jars)),
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.warehouse.dir": str(warehouse),
    }
    for key, value in settings.items():
        conf.set(key, value)
    context = None; spark = None; primary = None; genesis = None
    try:
        context = SparkContext(conf=conf)
        spark = SparkSession(context)
        spark.sparkContext.setLogLevel("ERROR")
        _need(spark.version == "4.2.0" and spark.sparkContext.master == "local[1]"
              and spark.sparkContext._jvm.java.lang.System.getProperty("java.specification.version") == "21"
              and spark.conf.get(PARAMETER_SUBSTITUTION_SETTING) == "true"
              and spark.conf.get("spark.jars") == ",".join(map(str, jars)),
              "exact local Spark 4.2 / Delta 4.4 / JDK21 runtime required")
        verify_loaded_delta_class(spark, jars)
        result = publish_graph_pages(spark, output,
            connection_factory=_role_connection_factory(conn_uri, "ashlar048_accept_consumer_r1"),
            consumer_id=FROZEN_CUSTODY["expected_consumer_id"],
            feed=FROZEN_CUSTODY["expected_feed_id"], maximum_pages=1,
            wait_seconds=0, maximum_page_bytes=2_097_152,
            destination_id=publication["destination_id"],
            carrier_profile=CARRIER_PROFILE)
        publications = result.get("publications")
        _need(type(publications) is list and len(publications) == 1,
              "exactly one genesis Delta publication required")
        item = publications[0]
        manifest = item.get("manifest")
        _need(type(manifest) is dict and item.get("page_sha256") == page.sha256
              and "consumer-verified destination" in result.get("scope", ""),
              "actual genesis page publication with destination-verified protected ACK required")
        report, vector = _verify_graph_manifest(manifest)
        source_archive = read_regular(Path(item["source_page"]["path"]), 2 * 1024 * 1024)
        _need(sha(source_archive) == item["source_page"]["sha256"],
              "published genesis source-page archive digest mismatch")
        genesis = {"publication_id": manifest["publication_id"],
            "page_sha256": page.sha256, "archive_sha256": item["source_page"]["sha256"],
            "source_page_path": item["source_page"]["path"],
            "publication_intent": item.get("publication_intent"),
            "profile_version": manifest["profile_version"],
            "validation_report_json": manifest["validation_report_json"],
            "table_versions_json": manifest["table_versions_json"],
            "installation_profile_sha256": report["installation_profile_sha256"],
            "destination_id": publication["destination_id"],
            "carrier_profile": CARRIER_PROFILE, "receipt_acknowledged": True,
            "admission_profile": admission.metadata()["profile"],
            "change_count": 0, "vector": vector,
            "bridge_sha256": scope["bridge"]["bridge_sha256"]}
        stage_receipts["genesis"] = _write_json_new(root / "genesis-publication-committed.json", genesis)
    except BaseException as error:
        primary = error
    for stop in ([spark.stop] if spark is not None else []) + ([context.stop] if context is not None else []):
        try:
            stop()
        except BaseException as error:
            if primary is None:
                primary = error
    if primary is not None:
        raise primary
    _need(type(genesis) is dict and genesis.get("receipt_acknowledged") is True,
          "verified ACKed genesis publication required before seed")

    origin_path = Path(scope["seed_custody"]["origin_map_path"])
    origin_raw = read_regular(origin_path, 64 * 1024)
    gate_path = Path(scope["seed_custody"]["origin_gate_path"])
    _need(sha(read_regular(gate_path, 256 * 1024)) == scope["seed_custody"]["origin_gate_code_sha256"],
          "approved R16 origin gate source changed before seed")
    gate_spec = importlib.util.spec_from_file_location("ashlar_r16_origin_gate", gate_path)
    _need(gate_spec is not None and gate_spec.loader is not None,
          "approved effect-free R16 origin gate could not be loaded")
    gate_module = importlib.util.module_from_spec(gate_spec)
    gate_spec.loader.exec_module(gate_module)
    gate_receipt = gate_module.verify_origin_map(origin_path)
    _need(gate_receipt.get("verified") is True
          and gate_receipt.get("review_required") is True
          and gate_receipt.get("execution_authorized") is False
          and gate_receipt.get("sha256") == ORIGIN_MAP_SHA256,
          "candidate origin map must pass the exact pre-run gate and remain review-required")
    _need(sha(origin_raw) == ORIGIN_MAP_SHA256,
          "approved R16 origin map changed before seed")
    params = psycopg.conninfo.conninfo_to_dict(conn_uri)
    params["user"] = "ashlar048_lifecycle_seed_writer_r1"
    seed_config = {"profile": "ashlar-truss-record-group/0.1",
        "connection_uri": psycopg.conninfo.make_conninfo(**params),
        "bun": runtime["bun_path"], "document_path": str(source_path),
        "origin_path": str(origin_path), "document_revision": request["document_revision"],
        "document_id": request["document_id"], "source_version": request["source_version"],
        "request_key": request["request_key"], "records": request["records"],
        "edges": request["edges"]}
    seed_config_path = root / "seed-runtime-config.json"
    _write_json_new(seed_config_path, seed_config)
    seed_report_path = root / "seed-group-report.json"
    from ashlar_host.truss_record_group import run_create_truss_record_group_command
    run_create_truss_record_group_command(seed_config_path, seed_report_path, require_fresh=True)
    seed_receipt = _validate_seed_report(seed_report_path, request)
    seed_receipt.update(request_sha256=SEED_REQUEST_SHA256,
        origin_map_sha256=ORIGIN_MAP_SHA256,
        request_config_sha256=sha(read_regular(seed_config_path, 64 * 1024)),
        genesis_publication_id=genesis["publication_id"], genesis_acknowledged=True)
    stage_receipts["seed"] = _write_json_new(root / "seed-committed.json", seed_receipt)

    admitted = _read_actual_seed_page(conn_uri, scope, page)
    seed_page = admitted.pop("page")
    stage_receipts["page_admitted"] = _write_json_new(root / "page-admitted.json", admitted)
    intent = genesis.get("publication_intent")
    _need(type(intent) is dict and set(intent) == {"path", "sha256"},
          "exact genesis publication intent required for continuation")
    intent_path = Path(intent["path"])
    intent_raw = read_regular(intent_path, 64 * 1024)
    _need(sha(intent_raw) == intent["sha256"], "retained genesis publication intent changed")
    from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
    continuation_config = {"connection_uri": psycopg.conninfo.make_conninfo(
            **{**params, "user": "ashlar048_accept_consumer_r1"}),
        "publication_directory": str(output), "jars": [str(path) for path in jars],
        "feed": FROZEN_CUSTODY["expected_feed_id"], "publication_intent": intent,
        "destination_id": publication["destination_id"], "carrier_profile": CARRIER_PROFILE,
        "runtime_profile": {"profile": "ashlar-configured-local-runtime/0.2",
            "java_home": str(java_home), "jar_inventory": runtime["jar_sha256"]}}
    continuation_config_path = root / "continuation-runtime-config.json"
    _write_json_new(continuation_config_path, continuation_config)
    continuation_report_path = root / "continuation-report.json"
    continuation_result = run_continue_truss_graph_command(
        continuation_config_path, continuation_report_path,
        maximum_pages=1, wait_seconds=0, maximum_page_bytes=2_097_152)
    publications = continuation_result.get("publications")
    _need(continuation_result.get("predecessor") == genesis["publication_id"]
          and type(publications) is list and len(publications) == 1,
          "one actual continuation from the ACKed genesis publication required")
    item = publications[0]; manifest = item.get("manifest"); source = item.get("source_page")
    _need(type(manifest) is dict and type(source) is dict
          and set(source) == {"path", "sha256"},
          "bounded actual continuation publication result required")
    continuation_profile, vector = _verify_graph_manifest(manifest)
    continuation_archive = read_regular(Path(source["path"]), 2 * 1024 * 1024)
    _need(sha(continuation_archive) == source["sha256"],
          "continued source page archive digest mismatch")
    continued_page = decode_truss_page(continuation_archive, source["sha256"],
                                       FROZEN_CUSTODY["expected_feed_id"])
    _need(continued_page.content == seed_page.content
          and continued_page.prior.content == seed_page.prior.content
          and continued_page.next.content == seed_page.next.content
          and continued_page.writer_xid == seed_page.writer_xid
          and "consumer-verified destination" in continuation_result.get("scope", ""),
          "continuation must publish the exact admitted source page before protected ACK")
    _need(continuation_profile["installation_profile_sha256"] == genesis["installation_profile_sha256"],
          "continuation may not change the installed `.4` carrier profile")
    continuation_receipt = {"publication_id": manifest["publication_id"],
        "predecessor": genesis["publication_id"], "page_sha256": continued_page.sha256,
        "archive_sha256": source["sha256"], "profile_version": manifest["profile_version"],
        "validation_report_json": manifest["validation_report_json"],
        "table_versions_json": manifest["table_versions_json"], "vector": vector,
        "destination_id": publication["destination_id"],
        "carrier_profile": CARRIER_PROFILE, "receipt_acknowledged": True,
        "admitted_change_count": admitted["admitted_change_count"],
        "installation_profile_sha256": continuation_profile["installation_profile_sha256"],
        "report_sha256": sha(read_regular(continuation_report_path, 1_048_576))}
    stage_receipts["continuation"] = _write_json_new(root / "continuation-committed.json", continuation_receipt)
    return {"migration_invoked": True, "publication_invoked": True,
        "genesis_acknowledged": True, "seed_invoked": True,
        "continuation_invoked": True, "continuation_acknowledged": True,
        "stage_receipts": stage_receipts,
        "destination_id": publication["destination_id"],
        "carrier_profile": CARRIER_PROFILE,
        "genesis_publication_id": genesis["publication_id"],
        "continuation_publication_id": continuation_receipt["publication_id"]}


def run_one_start_prefix_and_migration(prefix_worker, scope_path: Path,
                                       approval_path: Path, migration_worker_path: Path,
                                       prefix_worker_path: Path, source_root: Path,
                                       helper_path: Path) -> dict:
    """Run the complete frozen lifecycle under one prefix-owned PG start."""
    import copy
    scope, scope_raw = _validate_scope_approval(
        scope_path, approval_path, migration_worker_path, prefix_worker_path)
    runtime_evidence = verify_installed_runtime()
    prefix_scope = copy.deepcopy(scope)
    prefix_scope["profile"] = "ashlar-truss-r16-custody-prefix-scope/0.1"
    prefix_scope["authorization"] = "one_run_local_custody_prefix"
    prefix_scope["source"]["worker_sha256"] = scope["workers"]["custody_prefix_sha256"]
    prefix_scope["source_custody"] = {
        key: scope["source_custody"][key]
        for key in prefix_worker.FROZEN_SOURCE_CUSTODY
    }
    prefix_worker.verify_scope(prefix_scope)

    captured = {"migration": None, "lifecycle": None}
    bridge_probe = _verify_delta4_bridge(scope)
    def helper_call(helper_python, helper, root, conninfo, attempt, *args):
        return prefix_worker._run_helper(helper_python, helper, root, conninfo, attempt, *args)

    def prefix_stage(name: str) -> None:
        if name == "attempt-created":
            _write_json_new(Path(scope["output_root"]) / "delta4-bridge-probe.json", bridge_probe)

    def live_lifecycle(prefix: dict, conn_uri: str) -> dict:
        _need(type(prefix) is dict and type(conn_uri) is str,
              "prefix hook lacks actual .48 source/page result or live connection")
        _need(prefix.get("genesis_page", {}).get("next_checkpoint_sha256") ==
              FROZEN_CUSTODY["expected_genesis_checkpoint_sha256"],
              "prefix hook next checkpoint differs from frozen genesis cursor")
        raw = (json.dumps(prefix, sort_keys=True, separators=(",", ":"),
                          allow_nan=False) + "\n").encode()
        root = Path(scope["output_root"])
        # Fsync the exact pre-effect custody evidence before invoking migration.
        write_new(root / "pre-migration-custody-live.json", raw)
        migration = run_migration_stage(
            scope_path, approval_path, migration_worker_path, prefix_worker_path,
            conn_uri, raw)
        captured["migration"] = migration
        lifecycle = _run_post_migration_graph_lifecycle(scope, conn_uri, migration)
        captured["lifecycle"] = lifecycle
        return lifecycle

    previous_handler = signal.getsignal(signal.SIGALRM)
    def timeout(_signum, _frame):
        raise AdmissionError("600-second complete R16 lifecycle deadline exceeded")
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(scope["limits"]["child_deadline_seconds"])
    try:
        prefix_worker.run_prefix(prefix_scope, source_root, helper_path,
            stage=prefix_stage, operations={"helper": helper_call},
            live_lifecycle=live_lifecycle)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)
    _need(captured["migration"] is not None and captured["lifecycle"] is not None,
          "single-start custody prefix returned without complete lifecycle callback")
    root = Path(scope["output_root"])
    prefix_file = root / "lifecycle-stop-receipt.json"
    prefix_raw = read_regular(prefix_file, 256 * 1024)
    stopped = json.loads(prefix_raw)
    _need(stopped.get("pg_stopped") is True
          and all(stopped.get(name) is True for name in (
              "migration_invoked", "publication_invoked", "genesis_acknowledged",
              "seed_invoked", "continuation_invoked", "continuation_acknowledged")),
          "durable final stop receipt does not confirm every completed stage")
    retained = prefix_worker.tree(root)
    _need(retained["bytes"] <= scope["limits"]["attempt_root_bytes_max"],
          "complete lifecycle retained output exceeds approved byte cap")
    receipt = {
        "profile": FULL_LIFECYCLE_SCOPE,
        "status": "one_start_full_lifecycle_complete_and_stopped",
        "scope_sha256": sha(scope_raw),
        "runtime_evidence": runtime_evidence,
        "delta4_bridge_probe_sha256": sha(read_regular(root / "delta4-bridge-probe.json", 256 * 1024)),
        "prefix_receipt_sha256": sha(prefix_raw),
        "pre_migration_receipt_sha256": sha(read_regular(root / "pre-migration-custody-live.json", 256 * 1024)),
        "migration_readback": captured["migration"],
        "graph_lifecycle": captured["lifecycle"],
        "migration_invoked": True, "publication_invoked": True,
        "genesis_acknowledged": True, "seed_invoked": True,
        "continuation_invoked": True, "continuation_acknowledged": True,
        "single_postgres_start": True,
        "postgres_stopped_by_prefix_finally": stopped.get("pg_stopped") is True,
        "retained_output_bytes": retained["bytes"],
    }
    _write_json_new(root / "one-start-migration-readback.json", receipt)
    return receipt


def _run_migration_connected(scope: dict, scope_raw: bytes, runtime_evidence: dict,
                             prefix_raw: bytes, before_page_raw: bytes, root: Path,
                             stage_root: Path, registry_path: Path, conn_uri: str) -> dict:
    """Use the selected Truss public migration API once, then public readback."""
    import psycopg
    from truss.migrations import RuntimeMigrations
    from truss.recovery_registry import HostRecoveryRegistry

    params = psycopg.conninfo.conninfo_to_dict(conn_uri)
    connect = lambda: psycopg.connect(**params, autocommit=True, connect_timeout=5)
    registry = HostRecoveryRegistry(registry_path)
    _need(getattr(registry, "retention", None) == "restart_durable",
          "Truss restart-durable HostRecoveryRegistry required")
    migration = RuntimeMigrations(connect, registry, maximum_preservation_bytes=268435456,
                                  operation_timeout_ms=30000, statement_timeout_ms=10000)
    status = migration.status()
    backup = migration.backup_metadata()
    _need(status.get("state") == "upgrade_required"
          and status.get("manifestSha256") == TRUSS_048_MANIFEST_SHA256
          and status.get("target") == TRUSS_051_NATIVE_MANIFEST_SHA256,
          "public .51 migration status is not exact frozen .48→.51 upgrade")
    _need(backup.get("sourceManifestSha256") == TRUSS_048_MANIFEST_SHA256
          and backup.get("context") == status.get("context"),
          "public migration backup metadata does not bind exact source/context")
    observation_sha = _write_json_new(stage_root / "migration-observation.json", {
        "profile": PROFILE, "status": status, "backup_metadata": backup,
        "runtime_evidence": runtime_evidence,
        "scope_sha256": sha(scope_raw), "prefix_receipt_sha256": sha(prefix_raw),
        "pre_migration_genesis_archive_sha256": sha(before_page_raw),
        "migration_invoked": False, "publication_invoked": False,
        "acknowledgement_invoked": False, "seed_invoked": False,
    })

    attempt_id = scope["migration"]["attempt_id"]
    prepared = migration.prepare(attempt_id=attempt_id)
    _need(prepared.outcome == "prepared" and prepared.reference
          and prepared.attempt_id == attempt_id
          and prepared.source == TRUSS_048_MANIFEST_SHA256
          and prepared.target == TRUSS_051_NATIVE_MANIFEST_SHA256,
          "public .51 migration prepare did not bind approved source/target/attempt")
    retained = registry.inspect(prepared.reference, status["context"])
    prepared_sha = _write_json_new(stage_root / "migration-prepared.json", {
        "outcome": prepared.outcome, "reference": prepared.reference,
        "attempt_id": prepared.attempt_id, "source": prepared.source,
        "target": prepared.target, "original_obligation": retained["obligation"],
        "observations": retained["observations"],
        "migration_observation_sha256": observation_sha,
    })
    _write_json_new(stage_root / "migration-apply-started.json", {
        "reference": prepared.reference, "attempt_id": attempt_id,
        "single_apply": True, "scope_sha256": sha(scope_raw),
        "prepared_receipt_sha256": prepared_sha,
    })
    apply_error = None
    try:
        applied = migration.apply(prepared.reference)
    except BaseException as error:
        apply_error = f"{type(error).__name__}: {error}"[:1000]
        applied = None
    if applied is not None:
        apply_result_sha = _write_json_new(stage_root / "migration-apply-result.json", {
            **_result_dict(applied), "prepared_receipt_sha256": prepared_sha,
        })
    else:
        apply_result_sha = None
    final = applied
    if applied is None or applied.outcome in ("commit_unknown", "recovery_required", "committed_unverified"):
        _write_json_new(stage_root / "migration-reconcile-started.json", {
            "reference": prepared.reference, "attempt_id": attempt_id,
            "reason": "single same-reference reconciliation after uncertain apply",
            "apply_error": apply_error,
            "apply_result_sha256": apply_result_sha,
            "prepared_receipt_sha256": prepared_sha,
        })
        try:
            final = migration.reconcile(prepared.reference)
        except BaseException as error:
            _write_json_new(stage_root / "migration-reconcile-error.json", {
                "error": f"{type(error).__name__}: {error}"[:1000],
                "reference": prepared.reference, "attempt_id": attempt_id,
                "no_retry": True,
            })
            raise AdmissionError("public migration outcome unresolved; preserve registry and stop") from error
        reconcile_result_sha = _write_json_new(stage_root / "migration-reconcile-result.json", {
            **_result_dict(final), "prepared_receipt_sha256": prepared_sha,
            "apply_result_sha256": apply_result_sha,
        })
    else:
        reconcile_result_sha = None
    _need(final is not None and final.outcome in ("migrated", "already_applied")
          and final.current_verification
          and final.current_verification.get("state") == "current",
          "migration did not reach verified latest state; preserve all receipts and stop")
    receipt = migration.receipt(prepared.reference)
    verified_sha = _write_json_new(stage_root / "migration-verified.json", {
        "outcome": final.outcome, "reference": prepared.reference,
        "attempt_id": attempt_id, "current_verification": final.current_verification,
        "receipt": receipt, "prepared_receipt_sha256": prepared_sha,
        "apply_result_sha256": apply_result_sha,
        "reconcile_result_sha256": reconcile_result_sha,
    })

    # The historical source file is absent. Capture the actual .51 public source
    # bytes, then verify their frozen digest before writing the retained copy.
    source_view = _readback_source_view(conn_uri, root)
    page_after = _readback_page(conn_uri, before_page_raw)
    _need(source_view.get("source_sha256") == FROZEN_CUSTODY["expected_source_sha256"]
          and source_view.get("report_sha256") == FROZEN_CUSTODY["expected_source_report_sha256"]
          and source_view.get("complete_view_sha256") == FROZEN_CUSTODY["expected_complete_view_sha256"],
          "post-migration source/report/view readback differs from frozen custody")
    _need(page_after.get("next_checkpoint_sha256") == FROZEN_CUSTODY["expected_genesis_checkpoint_sha256"],
          "post-migration pending genesis checkpoint differs from frozen custody")
    return {
        "migration_reference": prepared.reference,
        "migration_attempt_id": attempt_id,
        "migration_outcome": final.outcome,
        "migration_receipt_sha256": sha(json.dumps(receipt, sort_keys=True,
                                                    separators=(",", ":"), allow_nan=False).encode()),
        "migration_verified_receipt_sha256": verified_sha,
        "source_view_readback": source_view,
        "pending_page_readback": page_after,
    }

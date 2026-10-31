#!/usr/bin/env python3
"""Effect-free inspection of pinned Truss 0.0.48 public lifecycle APIs."""
from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import inspect
import json
import zipfile
from pathlib import Path

WHEEL = Path(
    "/private/tmp/ashlar-r16-truss048-readonly-wheelhouse/"
    "truss_toolkit-0.0.48-py3-none-any.whl"
)
EXPECTED_WHEEL_SHA256 = (
    "1aedf614ff3d3ff7a61bd4d91887ede650a7135d1239e8564c1c297c0ddc37c3"
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def main() -> None:
    actual_wheel_sha256 = hashlib.sha256(WHEEL.read_bytes()).hexdigest()
    require(actual_wheel_sha256 == EXPECTED_WHEEL_SHA256, "pinned .48 wheel digest mismatch")
    require(importlib.metadata.version("truss-toolkit") == "0.0.48", "wrong installed Truss version")

    import truss
    from truss.catalog_runtime import CatalogReferenceAssembly
    from truss.groups import GroupRequestSelection, GroupSemanticInput
    from truss.preparation import CatalogDocument
    from truss.imports import ImportInput

    apis = {
        "fresh_postgresql18_runtime": truss.LocalPostgres,
        "accepted_model_document": CatalogDocument,
        "prepare_acceptance": truss.prepare_acceptance,
        "accept_catalog_registration": CatalogReferenceAssembly.accept_in_transaction,
        "register_genesis_feed_consumer": CatalogReferenceAssembly.register_feed_consumer_in_transaction,
        "seed_atomic_group": CatalogReferenceAssembly.apply_in_transaction,
        "seed_import": CatalogReferenceAssembly.import_in_transaction,
        "read_actual_feed_page": CatalogReferenceAssembly.read_feed_in_transaction,
    }
    for name, value in apis.items():
        require(callable(value), f"missing public API: {name}")

    signatures = {
        name: str(inspect.signature(value))
        for name, value in apis.items()
        if name not in ("fresh_postgresql18_runtime", "accepted_model_document")
    }
    require("data_directory" in str(inspect.signature(truss.LocalPostgres)), "LocalPostgres path contract changed")
    require("content: bytes" in str(inspect.signature(CatalogDocument)), "CatalogDocument content contract changed")
    require("expected_revision" in signatures["accept_catalog_registration"], "catalog CAS input missing")
    require("consumer_id" in signatures["register_genesis_feed_consumer"], "consumer identity input missing")
    require("GroupSemanticInput" in str(inspect.signature(CatalogReferenceAssembly.apply_in_transaction)),
            "atomic Truss seed API type changed")
    require("ImportInput" in str(inspect.signature(CatalogReferenceAssembly.import_in_transaction)),
            "Truss import seed API type changed")
    require(GroupRequestSelection is not None and GroupSemanticInput is not None and ImportInput is not None,
            "seed request types unavailable")

    with zipfile.ZipFile(WHEEL) as archive:
        for module_name in ("truss", "truss.catalog_runtime", "truss.local_runtime",
                            "truss.groups", "truss.preparation", "truss.imports"):
            module = importlib.import_module(module_name)
            member = module_name.replace(".", "/") + ("/__init__.py" if module_name == "truss" else ".py")
            require(Path(module.__file__).read_bytes() == archive.read(member),
                    f"installed {module_name} differs from pinned wheel")
        runtime = archive.read("truss/local_runtime.py").decode("utf-8")
        catalog = archive.read("truss/catalog_runtime.py").decode("utf-8")
        runtime_profile = json.loads(archive.read("truss/_profiles/local-postgres-runtime.json"))
        metadata = archive.read("truss_toolkit-0.0.48.dist-info/METADATA").decode("utf-8")
        require("Each data directory owns a PostgreSQL 18" in runtime,
                "local runtime PostgreSQL 18 contract not found")
        require(runtime_profile == {
            "distribution": "pgserver-pg18",
            "distributionVersion": "18.6.1",
            "serverVersion": "18.6",
            "sourceSha256": "983ee554ec53dbeb9b70797bef9fcf4e67e117e7e48ca1463cc80b3ff8e8ff3f",
            "repository": "https://github.com/DocumentDrivenDX/pgserver",
            "releaseTag": "pg18.6-pgv0.8.6-r2",
        }, "pinned PostgreSQL local runtime profile changed")
        for pin in ("pgserver-pg18==18.6.1", "fasteners==0.20", "platformdirs==4.12.4", "psutil==7.2.2"):
            require(pin in metadata, f"missing pinned [local] runtime dependency: {pin}")
        require("runtime_feed_cursor(%s,'0')" in catalog,
                "genesis checkpoint implementation signature changed")
        require("enrolls a full-source consumer from genesis" in catalog,
                "consumer genesis contract not found")

    print(json.dumps({
        "profile": "ashlar-truss048-fresh-public-api-check/0.1",
        "effect_free": True,
        "postgres_started": False,
        "database_connected": False,
        "source_accepted": False,
        "consumer_registered": False,
        "seed_submitted": False,
        "migration_run": False,
        "truss_version": importlib.metadata.version("truss-toolkit"),
        "wheel_path": str(WHEEL),
        "wheel_sha256": actual_wheel_sha256,
        "local_runtime_profile": runtime_profile,
        "local_extra_dependencies": ["pgserver-pg18==18.6.1", "fasteners==0.20",
                                     "platformdirs==4.12.4", "psutil==7.2.2"],
        "local_runtime_extra_installed_in_inspection_environment": False,
        "api_signatures": signatures,
    }, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()

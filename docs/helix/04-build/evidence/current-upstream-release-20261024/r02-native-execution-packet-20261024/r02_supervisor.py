#!/usr/bin/env python3
"""Fail-closed, one-attempt supervisor for the bounded R02 local slice.

The first manifest may contain only preparation/publication operations. Query
operations require a separate continuation manifest bound to the resulting
publication vector and frozen Weft artifacts. This supervisor never creates
those artifacts or infers review approval.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time
import uuid


PACKET = Path(__file__).resolve().parent
ROOT = PACKET.parents[5]
LIMIT_SECONDS = 300
LIMIT_PRIVATE = 512 * 1024 * 1024
LIMIT_RECEIPTS = 16 * 1024 * 1024
LIMIT_COMMAND_OUTPUT = 1024 * 1024
DEFAULT_ATTEMPT_PARENT = Path("/private/tmp/ashlar-r02-attempts")
REQUIRED_ROLES = {"object_current", "edge_current", "tombstone", "whole_source_history"}
RUNTIME = {
    "/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-spark_4.2_2.13-4.4.1.jar": "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-storage-4.4.1.jar": "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pyspark/jars/spark-core_2.13-4.2.0.jar": "8e0a03b66338a4a38c9089d9f798ab6a6c6583aefe93f910311704a5ec90ffc4",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pyspark/jars/spark-sql-api_2.13-4.2.0.jar": "d789409a56db600c5160300fcd2c9651bf6f32dd8db1035a9bda17b67543b372",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pyspark/jars/spark-sql_2.13-4.2.0.jar": "ec0a0d699c6c57300ee47ba5d54b4d1f71d3e5401fb90a2bcdcd83b942084dbf",
}
SOURCES = {
    "examples/end-to-end/typed-graph-model.umf.json": "19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b",
    "examples/end-to-end/configured-typed-graph-source.json": "040545b8cc96701a158b8936750c89f97ce991df9652ceeacf6c66fb9d9fc168",
    "examples/end-to-end/typed-graph-source.jsonl": "9edb7566f82324fadb1e7cf1ba0eaf22691096a26209e938c83730d096d4882b",
    "examples/end-to-end/typed-graph-binding.json": "1ab1416d5a36a082c4d5be38dc061377bdae22bb5395fb8cfbec8cee80910863",
}
HOST_SOURCES = {
    "src/ashlar_host/configured_local_cli.py": "72047fe0773286e40ad202402f3d79a43a79dc30409b65f0e8beb365403b23ec",
    "src/ashlar_host/configured_local_runtime.py": "73a6fb85a6d4114a4f706622b86d75d3440cf0618554bd92830f11364ee60420",
    "src/ashlar_host/configured_local_publication.py": "dcbb7b146431351b68b63699637dbe6e94acbcaf770286a2fb1e5a284ba31646",
    "src/ashlar_host/configured_local_query.py": "ff51001d937f10eaff53663a43358e2aede00723c9ade7110d21952ffff19466",
    "src/ashlar_host/configured_weft_compile.py": "d2622531d4f87ec046538a54121049ec6b91d25bc7ded433ba7c4d7ec345019a",
    "src/ashlar_host/configured_local_runtime_profile.py": "082a69af610550bb0db19a8c8a4da09666e6031feb714eca3d07261d0a868033",
    "src/ashlar_host/configured_local_read.py": "340bb76f6aecb2ee0db13967643aa64452e4b6dadef048096edb29ee62af0df5",
}


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def tree_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file() and not p.is_symlink())


def write_state(path: Path, value: dict) -> None:
    with path.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def stop_descendants(process: subprocess.Popen) -> None:
    """Ensure the operation's process group has no surviving child process."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    raise RuntimeError("operation process group did not close")


def verify_pins(manifest: dict) -> None:
    current_revision = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
    if manifest.get("sourceRevision") != current_revision:
        raise ValueError("manifest sourceRevision must equal the exact current reviewed Git revision")
    for name, expected in SOURCES.items():
        path = ROOT / name
        if digest(path) != expected or manifest.get("sourceFiles", {}).get(name) != expected:
            raise ValueError(f"source hash is stale or absent: {name}")
    for name, expected in HOST_SOURCES.items():
        path = ROOT / name
        if digest(path) != expected or manifest.get("hostSourceFiles", {}).get(name) != expected:
            raise ValueError(f"host source hash is stale or absent: {name}")
    for name, expected in manifest.get("packetFiles", {}).items():
        path = ROOT / name if name.startswith("examples/") else PACKET / name
        if not path.is_file() or digest(path) != expected:
            raise ValueError(f"R02 packet implementation hash is stale or absent: {name}")
    required_packet = {"r02_packet_generator.py", "r02_supervisor.py", "r02_invocation_worker.py",
                       "source-oracle.json", "umf-validation.json", "positive-backend03-request.json",
                       "positive-backend03-response.json", "positive-backend03-compiler-receipt.json",
                       "compile-positive-backend03.py", "examples/configured-local/configuration.template.json"}
    if set(manifest.get("packetFiles", {})) != required_packet:
        raise ValueError("packet file inventory is incomplete or contains unexpected files")
    for raw_path, expected in RUNTIME.items():
        path = Path(raw_path)
        if not path.is_file() or digest(path) != expected or manifest.get("runtimeFiles", {}).get(path.name) != expected:
            raise ValueError(f"runtime hash is stale or absent: {path.name}")
    identity = manifest.get("runtimeIdentity", {})
    python = Path("/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python3.12")
    java_home = Path("/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home")
    public_umf = Path("/private/tmp/umf056-public-wheel-verify/venv/bin/python3.12")
    public_weft = Path("/private/tmp/ashlar-weft053-adoption-20261023/venv/bin/python3.12")
    if (identity.get("nativePython") != str(python) or identity.get("javaHome") != str(java_home)
            or identity.get("nativePythonExecutableSha256") != digest(python.resolve(strict=True))
            or identity.get("javaExecutableSha256") != digest((java_home / "bin/java").resolve(strict=True))):
        raise ValueError("selected Python or JDK executable identity changed")
    probe = "import importlib.metadata as m,sys; print(m.version('pyspark')); print(m.version('delta-spark')); print(m.version('weft-sql')); print(sys.version_info.major,sys.version_info.minor,sys.version_info.micro)"
    native = subprocess.check_output([str(python), "-I", "-c", probe], text=True).splitlines()
    if (native != ["4.2.0", "4.4.1", "0.4.48", identity.get("nativePythonVersion")]
            or identity.get("pyspark") != "4.2.0" or identity.get("deltaSpark") != "4.4.1"
            or identity.get("r05WeftSql") != "0.4.48"):
        raise ValueError("selected R05 Python package cohort changed")
    java_release = (java_home / "release").read_bytes()
    if hashlib.sha256(java_release).hexdigest() != identity.get("javaReleaseSha256"):
        raise ValueError("selected JDK release identity changed")
    umf = subprocess.check_output([str(public_umf), "-I", "-c", "import importlib.metadata as m; print(m.version('umf-core'))"], text=True).strip()
    weft = subprocess.check_output([str(public_weft), "-I", "-c", "import importlib.metadata as m; print(m.version('weft-sql'))"], text=True).strip()
    if umf != "0.10.1" or umf != identity.get("umfCore") or weft != "0.4.53" or weft != identity.get("weftSql"):
        raise ValueError("selected public UMF or Weft verification cohort changed")
    installed = identity.get("installedHostModules", {})
    for module, expected in {
        "ashlar_host.configured_local_cli": HOST_SOURCES["src/ashlar_host/configured_local_cli.py"],
        "ashlar_host.configured_local_runtime": HOST_SOURCES["src/ashlar_host/configured_local_runtime.py"],
        "ashlar_host.configured_local_publication": HOST_SOURCES["src/ashlar_host/configured_local_publication.py"],
        "ashlar_host.configured_local_query": HOST_SOURCES["src/ashlar_host/configured_local_query.py"],
        "ashlar_host.configured_weft_compile": HOST_SOURCES["src/ashlar_host/configured_weft_compile.py"],
        "ashlar_host.configured_local_runtime_profile": HOST_SOURCES["src/ashlar_host/configured_local_runtime_profile.py"],
        "ashlar_host.configured_local_read": HOST_SOURCES["src/ashlar_host/configured_local_read.py"],
    }.items():
        entry = installed.get(module)
        if (not isinstance(entry, list) or len(entry) != 2 or digest(Path(entry[0])) != expected
                or entry[1] != expected or "/site-packages/ashlar_host/" not in entry[0]):
            raise ValueError(f"installed Ashlar module bytes differ from selected source: {module}")


def review_gate(manifest_path: Path, manifest: dict, review_path: Path, phase: str) -> str:
    review = json.loads(review_path.read_text(encoding="utf-8"))
    manifest_hash = digest(manifest_path)
    if review.get("decision") != "execute" or review.get("manifestSha256") != manifest_hash or review.get("phase") != phase:
        raise ValueError("independent review does not authorize these exact manifest bytes and phase")
    if review.get("reviewer") != "Sol":
        raise ValueError("review receipt must identify Sol")
    return manifest_hash


def check_limits(attempt: Path, receipts: Path) -> None:
    if tree_size(attempt) > LIMIT_PRIVATE:
        raise ValueError("private attempt exceeded 512 MiB")
    if tree_size(receipts) > LIMIT_RECEIPTS:
        raise ValueError("retained receipts exceeded 16 MiB")


def run_one(argv: list[str], cwd: Path, deadline: float, output_path: Path,
            attempt: Path, receipts: Path) -> None:
    if not argv or any(not isinstance(arg, str) for arg in argv):
        raise ValueError("every operation requires an explicit argv array")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("five-minute total deadline expired")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("xb") as output:
        process = subprocess.Popen(argv, cwd=cwd, stdin=subprocess.DEVNULL,
                                   stdout=output, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        while process.poll() is None:
            if (time.monotonic() >= deadline or output_path.stat().st_size > LIMIT_COMMAND_OUTPUT
                    or tree_size(attempt) > LIMIT_PRIVATE or tree_size(receipts) > LIMIT_RECEIPTS):
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                stop_descendants(process)
                raise TimeoutError("operation exceeded time, private-byte, receipt-byte, or output cap")
            time.sleep(0.1)
        process.wait()
        stop_descendants(process)
    if process.returncode != 0:
        raise RuntimeError(f"operation exited {process.returncode}: {argv[0]}")


def execute(manifest_path: Path, review_path: Path, phase: str) -> None:
    manifest_path = manifest_path.resolve(strict=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("profile") != "ashlar-r02-one-shot/0.1" or manifest.get("phase") != phase:
        raise ValueError("manifest profile or phase mismatch")
    if manifest.get("limits") != {"wallSeconds": 300, "privateBytes": LIMIT_PRIVATE,
                                  "receiptBytes": LIMIT_RECEIPTS, "operationOutputBytes": LIMIT_COMMAND_OUTPUT}:
        raise ValueError("manifest limits must equal the fixed R02 ceilings")
    verify_pins(manifest)
    manifest_hash = review_gate(manifest_path, manifest, review_path.resolve(strict=True), phase)
    if phase == "continuation":
        prior = manifest.get("prepublicationState", {})
        prior_path = Path(prior.get("path", ""))
        if not prior_path.is_file() or prior_path.is_symlink() or digest(prior_path) != prior.get("sha256"):
            raise ValueError("continuation must bind the exact completed prepublication state file")
        prior_state = json.loads(prior_path.read_text(encoding="utf-8"))
        if (prior_state.get("phase") != "prepublication" or prior_state.get("status") != "complete-held-after-publication"
                or prior_state.get("manifestSha256") != prior.get("manifestSha256")
                or prior_state.get("attemptId") != prior.get("attemptId")
                or prior_state.get("sourceRevision") != manifest.get("sourceRevision")
                or prior_state.get("sourceFiles") != manifest.get("sourceFiles")):
            raise ValueError("referenced prepublication attempt is incomplete or has different source custody")
        if (prior_path.name != "one-shot-state.json" or prior_path.parent.name != f"ashlar-r02-{prior.get('attemptId')}"
                or prior_state.get("attemptId") != prior.get("attemptId")):
            raise ValueError("prior state path is not inside its recorded private attempt")
        vector = manifest.get("publicationVector")
        if (not isinstance(vector, dict) or not isinstance(vector.get("id"), str) or not vector["id"]
                or not isinstance(vector.get("manifestUuid"), str) or not vector["manifestUuid"]
                or not isinstance(vector.get("tables"), list)):
            raise ValueError("continuation requires a complete publication vector")
        table_names = set()
        roles = set()
        for table in vector["tables"]:
            if (not isinstance(table.get("name"), list) or len(table["name"]) != 3
                    or any(not isinstance(part, str) or not part for part in table["name"])
                    or not isinstance(table.get("uuid"), str) or type(table.get("version")) is not int or table["version"] < 0):
                raise ValueError("publication vector table identity is incomplete")
            try:
                uuid.UUID(table["uuid"])
            except (ValueError, AttributeError):
                raise ValueError("publication vector table UUID is invalid") from None
            name = tuple(table["name"])
            if name in table_names:
                raise ValueError("publication vector repeats a table identity")
            table_names.add(name)
            roles.add(table["name"][-1])
        try:
            uuid.UUID(vector["manifestUuid"])
        except (ValueError, AttributeError):
            raise ValueError("publication manifest UUID is invalid") from None
        if roles != REQUIRED_ROLES or len(table_names) != len(REQUIRED_ROLES):
            raise ValueError("publication vector must contain exactly the four required Ashlar table roles")
        vector_bytes = json.dumps(vector, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if hashlib.sha256(vector_bytes).hexdigest() != manifest.get("publicationVectorSha256"):
            raise ValueError("complete publication vector hash mismatch")
        if prior_state.get("publicationVector") != vector or prior_state.get("publicationVectorSha256") != manifest.get("publicationVectorSha256"):
            raise ValueError("continuation vector does not match the completed publication attempt")
        artifacts = manifest.get("weftArtifacts", [])
        expected_cases = ["backend03-original", "backend03-positive", "backend02-page1", "backend02-continuation"]
        if [item.get("case") for item in artifacts] != expected_cases:
            raise ValueError("continuation requires the four named R02 query artifacts in packet order")
        for item in artifacts:
            request_item, response_item = item.get("request", {}), item.get("response", {})
            request_file, response_file = Path(request_item.get("path", "")), Path(response_item.get("path", ""))
            if not request_file.is_file() or not response_file.is_file():
                raise ValueError(f"fresh vector-bound Weft files are required: {item.get('case')}")
            request_bytes, response_bytes = request_file.read_bytes(), response_file.read_bytes()
            if hashlib.sha256(request_bytes).hexdigest() != request_item.get("sha256") or hashlib.sha256(response_bytes).hexdigest() != response_item.get("sha256"):
                raise ValueError(f"frozen vector-bound Weft artifact hash mismatch: {item.get('case')}")
            request_value, response_value = json.loads(request_bytes), json.loads(response_bytes)
            binding_raw = request_value.get("target", {}).get("bindingJson", "").encode("utf-8")
            binding = json.loads(binding_raw)
            if binding.get("publication") != vector or response_value.get("bindingSha256") != hashlib.sha256(binding_raw).hexdigest() or response_value.get("status") != "compiled":
                raise ValueError(f"Weft artifacts do not compile against the reviewed complete vector: {item.get('case')}")
        if manifest.get("vectorReviewer") != "Sol" or manifest.get("vectorDecision") != "execute":
            raise ValueError("continuation vector and Weft artifacts lack separate Sol review")
    elif [(op.get("kind"), op.get("case")) for op in manifest.get("operations", [])] != [
        ("initialize", "initialize"), ("publish", "batch-1"), ("publish", "batch-2"),
        ("publish", "batch-3"), ("replay", "batch-3-replay")]:
        raise ValueError("prepublication phase must initialize, publish batches 1-3, and replay batch 3 exactly once")
    if phase == "continuation" and [(op.get("kind"), op.get("case")) for op in manifest.get("operations", [])] != [
        ("singleton", "backend03-singleton"), ("query", "backend03-original"),
        ("query", "backend03-positive"), ("query", "backend02-page1"),
        ("query", "backend02-continuation")]:
        raise ValueError("continuation operations must be the one singleton and four reviewed queries in packet order")

    attempt_id = manifest.get("attemptId")
    if not isinstance(attempt_id, str) or not attempt_id.isascii() or not attempt_id.replace("-", "").isalnum() or len(attempt_id) < 16:
        raise ValueError("attemptId must be a fresh UUID-like identifier")
    configured_base = Path(manifest.get("attemptParent", str(DEFAULT_ATTEMPT_PARENT)))
    if configured_base != DEFAULT_ATTEMPT_PARENT:
        raise ValueError("attemptParent must be the private Ashlar directory under /private/tmp")
    configured_base.mkdir(mode=0o700, parents=True, exist_ok=True)
    if any(path.is_symlink() for path in (configured_base, *configured_base.parents)):
        raise ValueError("attempt parent path must not contain symlinks")
    base = configured_base.resolve(strict=True)
    if base.stat().st_uid != os.getuid() or not stat.S_ISDIR(base.stat().st_mode) or stat.S_IMODE(base.stat().st_mode) != 0o700:
        raise ValueError("attempt parent must be a mode-0700 user-owned directory")
    attempt_raw = Path(manifest.get("attemptRoot", ""))
    if attempt_raw.is_symlink():
        raise ValueError("attempt root cannot be a symlink")
    attempt = attempt_raw.resolve(strict=True)
    if (attempt.parent != base or attempt.name != f"ashlar-r02-{attempt_id}"
            or attempt.stat().st_uid != os.getuid() or not stat.S_ISDIR(attempt.stat().st_mode)
            or stat.S_IMODE(attempt.stat().st_mode) != 0o700):
        raise ValueError("manifest must name its fresh mode-0700 user-owned attempt root")
    if manifest_path.parent.resolve(strict=True) != attempt or review_path.resolve(strict=True).parent != attempt:
        raise ValueError("manifest and review receipt must be retained inside the attempt root")
    if phase == "prepublication":
        for raw in manifest.get("invocations", []):
            path = Path(raw)
            if path.is_symlink() or path.resolve(strict=True).parent != (attempt / "invocations").resolve(strict=True):
                raise ValueError("all invocation templates must be regular files in the private attempt")
        expected_cases = [("initialize", "initialize"), ("batch-1", "publish"),
                          ("batch-2", "publish"), ("batch-3", "publish"),
                          ("batch-3-replay", "replay")]
        templates = manifest.get("invocationTemplates", [])
        if [(x.get("case"), x.get("kind")) for x in templates] != expected_cases:
            raise ValueError("manifest must bind the five exact prepublication invocation templates")
        if [x.get("path") for x in templates] != manifest.get("invocations"):
            raise ValueError("invocation list differs from its hash-bound template inventory")
        for template in templates:
            path = Path(template.get("path", ""))
            if path.is_symlink() or digest(path) != template.get("sha256"):
                raise ValueError("invocation template bytes differ from their reviewed hash")
        for relative, expected in manifest.get("attemptFiles", {}).items():
            path = attempt / relative
            if path.is_symlink() or attempt not in path.resolve(strict=True).parents or digest(path) != expected:
                raise ValueError(f"staged attempt input hash mismatch: {relative}")
        if set(manifest.get("attemptFiles", {})) != {
            "input/configured-typed-graph-source.json", "input/typed-graph-model.umf.json",
            "input/typed-graph-source.jsonl", "input/typed-graph-binding.json",
            "jars/delta-spark_4.2_2.13-4.4.1.jar", "jars/delta-storage-4.4.1.jar", "jars/inventory.json",
        }:
            raise ValueError("staged input inventory is incomplete or unexpected")
        operations = manifest.get("operations", [])
        if [(op.get("case"), op.get("kind"), op.get("templateSha256")) for op in operations] != [
            (item["case"], item["kind"], item["sha256"]) for item in templates
        ]:
            raise ValueError("operation sequence is not tied to the five reviewed invocation templates")
        for operation, template in zip(operations, templates):
            expected_argv = ["/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python3.12", "-I", "-B",
                             str(attempt / "r02_invocation_worker.py"), "--attempt", str(attempt),
                             "--case", template["case"], "--template", template["path"],
                             "--python", "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python3.12",
                             "--java-home", "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
                             "--template-sha256", template["sha256"]]
            if operation.get("argv") != expected_argv:
                raise ValueError("operation argv differs from the fixed local runner worker command")
    receipts = attempt / "receipts"
    if (not receipts.is_dir() or receipts.is_symlink() or receipts.stat().st_uid != os.getuid()
            or stat.S_IMODE(receipts.stat().st_mode) != 0o700 or any(receipts.iterdir())):
        raise ValueError("attempt receipts directory must be a fresh, empty private directory")
    if (attempt / "one-shot-state.json").exists() or (attempt / "one-shot-state.json").is_symlink():
        raise ValueError("attempt root already contains one-shot state")
    run_identity = {"phase": phase, "sourceFiles": manifest.get("sourceFiles"),
                    "publicationVectorSha256": manifest.get("publicationVectorSha256")}
    run_key = hashlib.sha256(json.dumps(run_identity, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    ledger = base / f"ashlar-r02-consumed-{run_key}.json"
    try:
        descriptor = os.open(ledger, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise ValueError("this source/vector phase already has a consumed one-shot attempt") from None
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(json.dumps({"phase": phase, "manifestSha256": manifest_hash,
                                 "attemptId": manifest.get("attemptId"), "status": "consumed"}, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    state = attempt / "one-shot-state.json"
    deadline = time.monotonic() + LIMIT_SECONDS
    try:
        write_state(state, {"phase": phase, "manifestSha256": manifest_hash,
                            "attemptId": attempt_id, "sourceRevision": manifest["sourceRevision"],
                            "sourceFiles": manifest["sourceFiles"], "status": "running"})
        for index, operation in enumerate(manifest.get("operations", [])):
            if time.monotonic() >= deadline:
                raise TimeoutError("five-minute total deadline expired")
            kind = operation.get("kind")
            if phase == "prepublication" and kind not in {"initialize", "publish", "replay"}:
                raise ValueError("unsupported prepublication operation")
            if phase == "continuation" and kind not in {"singleton", "query"}:
                raise ValueError("unsupported continuation operation")
            argv = operation.get("argv")
            run_one(argv, attempt, deadline, receipts / f"operation-{index:02d}.log", attempt, receipts)
            check_limits(attempt, receipts)
        if phase == "prepublication":
            vector_relative = manifest.get("publicationVectorPath")
            raw_vector_path = attempt / vector_relative if isinstance(vector_relative, str) else None
            vector_path = raw_vector_path.resolve() if raw_vector_path is not None else None
            if (raw_vector_path is None or raw_vector_path.is_symlink() or vector_path is None
                    or attempt.resolve() not in vector_path.parents or not vector_path.is_file()):
                raise ValueError("prepublication phase did not produce its complete publication vector artifact")
            vector = json.loads(vector_path.read_text(encoding="utf-8"))
            vector_bytes = json.dumps(vector, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            write_state(state, {"phase": phase, "manifestSha256": manifest_hash,
                                "attemptId": attempt_id, "sourceRevision": manifest["sourceRevision"],
                                "sourceFiles": manifest["sourceFiles"], "publicationVector": vector,
                                "publicationVectorSha256": hashlib.sha256(vector_bytes).hexdigest(),
                                "status": "complete-held-after-publication",
                                "operations": len(manifest.get("operations", []))})
        else:
            write_state(state, {"phase": phase, "manifestSha256": manifest_hash,
                                "attemptId": attempt_id, "sourceRevision": manifest["sourceRevision"],
                                "sourceFiles": manifest["sourceFiles"], "publicationVectorSha256": manifest["publicationVectorSha256"],
                                "status": "complete", "operations": len(manifest.get("operations", []))})
    except BaseException:
        # Keep the one-shot state in place: a failed attempt must never be silently repeated.
        try:
            write_state(state, {"phase": phase, "manifestSha256": manifest_hash,
                                "attemptId": attempt_id, "status": "failed-or-held"})
        except OSError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("prepublication", "continuation"), required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--review", type=Path, required=True)
    args = parser.parse_args()
    try:
        execute(args.manifest, args.review, args.phase)
    except Exception as exc:
        print(f"R02 supervisor refused: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

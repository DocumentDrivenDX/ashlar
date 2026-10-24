"""Shared closed identities for the one-shot configured-local `.4` packet."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat


PACKET = Path(__file__).resolve().parent
MANIFEST = PACKET / "packet-manifest.json"
PACKET_PROFILE = "ashlar-local-configured-delta4-init-packet/0.1"
ALLOWED_ENV = (
    "HOME", "LANG", "LC_ALL", "PATH", "JAVA_HOME", "PYSPARK_PYTHON",
    "PYSPARK_DRIVER_PYTHON", "PYTHONDONTWRITEBYTECODE", "SPARK_LOCAL_IP",
    "TMPDIR", "SPARK_LOCAL_DIRS",
)
INPUTS = (
    "configured-typed-graph-source.json", "typed-graph-model.umf.json",
    "typed-graph-source.jsonl", "typed-graph-binding.json",
)


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"


def sha_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def sha_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def json_file(path: Path) -> object:
    return json.loads(path.read_bytes())


def source_tree_digest(root: Path, relative: str) -> dict:
    directory = root / relative
    files = []
    for path in sorted(directory.rglob("*")):
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"source-tree symlink refused: {path}")
        if stat.S_ISREG(info.st_mode):
            files.append([path.relative_to(root).as_posix(), sha_file(path)])
        elif not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"non-regular source-tree entry refused: {path}")
    raw = json.dumps(files, separators=(",", ":"), ensure_ascii=False).encode()
    return {"fileCount": len(files), "sha256": sha_bytes(raw)}


def packet_tree_digest() -> str:
    files = []
    for path in sorted(PACKET.rglob("*")):
        relative = path.relative_to(PACKET)
        if "__pycache__" in relative.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        if any(part in {"runs", "prepared"} for part in relative.parts):
            continue
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"packet symlink refused: {path}")
        if stat.S_ISREG(info.st_mode):
            files.append([relative.as_posix(), sha_file(path)])
    return sha_bytes(json.dumps(files, separators=(",", ":"), ensure_ascii=False).encode())


def sol_review_path(packet_digest: str) -> Path:
    if type(packet_digest) is not str or re.fullmatch(r"[0-9a-f]{64}", packet_digest) is None:
        raise ValueError("packet digest must be 64 lowercase hexadecimal characters")
    return PACKET.parent / f"sol-review-{packet_digest}.json"


def load_manifest() -> dict:
    value = json_file(MANIFEST)
    if type(value) is not dict or value.get("profile") != PACKET_PROFILE:
        raise ValueError("configured-local `.4` packet manifest refused")
    return value


def verify_inputs(input_root: Path, manifest: dict) -> None:
    expected = manifest["inputSha256"]
    if set(expected) != set(INPUTS):
        raise ValueError("packet input inventory is not closed")
    for name in INPUTS:
        path = input_root / name
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or path.is_symlink() or sha_file(path) != expected[name]:
            raise ValueError(f"packet input digest differs: {name}")
    config = json_file(input_root / INPUTS[0])
    refs = {config[k]["path"]: config[k]["sha256"] for k in ("source", "schema", "bindings")}
    if refs != {name: expected[name] for name in INPUTS[1:]}:
        raise ValueError("source configuration references do not bind the frozen packet inputs")


def verify_source_tree(manifest: dict) -> None:
    root = Path(manifest["ashlarSource"]["root"])
    for relative, expected in manifest["ashlarSource"]["directories"].items():
        if source_tree_digest(root, relative) != expected:
            raise ValueError(f"Ashlar source snapshot differs: {relative}")


def verify_packet_assets(attempt_inputs: Path | None = None) -> dict:
    manifest = load_manifest()
    if attempt_inputs is None:
        attempt_inputs = PACKET / "inputs"
    verify_inputs(attempt_inputs, manifest)
    verify_source_tree(manifest)

    runtime = manifest["runtime"]
    python = Path(runtime["python"])
    python_real = python.resolve(strict=True)
    if not os.access(python, os.X_OK):
        raise ValueError("pinned Python interpreter is not executable")
    import sys
    if (sys.executable != str(python)
            or sha_file(python_real) != runtime["pythonRealExecutableSha256"]
            or sha_file(python.parent.parent / "pyvenv.cfg") != runtime["pyvenvCfgSha256"]):
        raise ValueError("pinned isolated Python interpreter identity changed")

    import importlib.metadata as metadata
    versions = {"pyspark": runtime["pyspark"], "delta-spark": runtime["deltaSpark"],
                "mini-racer": runtime["miniRacer"], "py4j": runtime["py4j"]}
    if sys.version_info[:3] != tuple(map(int, runtime["pythonVersion"].split("."))):
        raise ValueError("packet preflight is running under an unsupported Python version")
    locations = {}
    for distribution, expected in versions.items():
        dist = metadata.distribution(distribution)
        if dist.version != expected:
            raise ValueError(f"pinned Python distribution changed: {distribution}")
        locations[distribution] = str(Path(dist.locate_file("")).resolve())
        if "site-packages" not in locations[distribution]:
            raise ValueError(f"distribution metadata is outside site-packages: {distribution}")
        record = dist.read_text("RECORD")
        if record is None or sha_bytes(record.encode("utf-8")) != runtime["distributionRecordSha256"][distribution]:
            raise ValueError(f"installed distribution RECORD changed: {distribution}")
    if len(set(locations.values())) != 1:
        raise ValueError("pinned Python distributions come from different installations")

    java_home = Path(runtime["javaHome"])
    java = java_home / "bin" / "java"
    if sha_file(java) != runtime["javaExecutableSha256"]:
        raise ValueError("pinned Java executable digest changed")
    release = (java_home / "release").read_text(encoding="utf-8")
    if 'JAVA_VERSION="21' not in release:
        raise ValueError("pinned JDK is not Java 21")

    jars_dir = Path(manifest["runtimePaths"]["jars"])
    expected_jars = runtime["jars"]
    entries = {path.name for path in jars_dir.iterdir()}
    if entries not in (set(expected_jars), set(expected_jars) | {"inventory.json"}):
        raise ValueError("Delta JAR directory inventory changed")
    for name, expected in expected_jars.items():
        path = jars_dir / name
        if path.is_symlink() or not path.is_file() or sha_file(path) != expected:
            raise ValueError(f"Delta JAR digest changed: {name}")

    wheelhouse = Path(manifest["runtimePaths"]["wheelhouse"])
    wheel_hashes = {}
    for name, expected in runtime["localWheels"].items():
        path = wheelhouse / name
        if path.is_symlink() or not path.is_file() or sha_file(path) != expected:
            raise ValueError(f"pinned local wheel changed: {name}")
        wheel_hashes[name] = expected

    producer = manifest["umfProducer"]
    git = Path(producer["git"])
    bun = Path(producer["bun"])
    if sha_file(git) != producer["gitSha256"] or sha_file(bun) != producer["bunSha256"]:
        raise ValueError("UMF producer tool identity changed")
    import subprocess
    cwd = Path(producer["source"])
    head = subprocess.run([str(git), "-C", str(cwd), "rev-parse", "HEAD"],
                          check=True, capture_output=True, text=True, timeout=10).stdout.strip()
    tree = subprocess.run([str(git), "-C", str(cwd), "rev-parse", "HEAD^{tree}"],
                          check=True, capture_output=True, text=True, timeout=10).stdout.strip()
    dirty = subprocess.run([str(git), "-C", str(cwd), "status", "--porcelain"],
                           check=True, capture_output=True, text=True, timeout=10).stdout
    if (head, tree, dirty) != (producer["head"], producer["tree"], ""):
        raise ValueError("selected UMF producer revision/tree/worktree changed")

    return {"python": str(python), "pythonRealPath": str(python_real),
            "pythonVersion": runtime["pythonVersion"], "distributions": versions,
            "distributionLocations": locations, "javaSha256": runtime["javaExecutableSha256"],
            "distributionRecordSha256": runtime["distributionRecordSha256"],
            "jarSha256": expected_jars, "wheelSha256": wheel_hashes,
            "umfProducer": {"head": head, "tree": tree, "dirty": False}}


def verify_umf_bridge(attempt_installation: Path, manifest: dict) -> dict:
    from ashlar_host.local_current_delta4_umf import (
        BRIDGE_SHA256, BUILD_INPUTS_SHA256, REALIZATION_MANIFEST_SHA256,
        UMF_CANDIDATE_COMMIT, LocalUmfDelta4Bridge,
    )
    from ashlar_host.managed_current_carriers import load_current_managed_carriers
    from ashlar_host.schema_rows import LOCAL_CURRENT_ROLE_TO_UMF

    expected = manifest["umfDelta4Bridge"]
    if (BRIDGE_SHA256, BUILD_INPUTS_SHA256, REALIZATION_MANIFEST_SHA256, UMF_CANDIDATE_COMMIT) != (
            expected["bridgeSha256"], expected["buildInputsSha256"],
            expected["realizationManifestSha256"], expected["candidateCommit"]):
        raise ValueError("pinned UMF bridge realization identity changed")
    carriers = load_current_managed_carriers("ashlar-delta/0.4-xxhash64")
    source_bytes = dict(carriers.inputs)
    if sha_bytes(carriers.generated_bytes) != expected["generatedCarriersSha256"]:
        raise ValueError("pinned UMF generated carrier bytes changed")
    proposals = {}
    with LocalUmfDelta4Bridge.open() as bridge:
        if set(LOCAL_CURRENT_ROLE_TO_UMF) != set(manifest["scope"]["roles"]):
            raise ValueError("current six-role local-to-UMF mapping changed")
        for local_role, carrier_role in LOCAL_CURRENT_ROLE_TO_UMF.items():
            source = source_bytes.get(carrier_role + ".umf.json")
            expected_source = expected["sourceRoleSha256"].get(carrier_role)
            if type(source) is not bytes or sha_bytes(source) != expected_source:
                raise ValueError(f"pinned UMF role source changed: {carrier_role}")
            target_path = attempt_installation / local_role
            admitted = bridge.realize(source, str(target_path), carrier_role)
            if admitted["fields"] != carriers.ordered_fields(carrier_role):
                raise ValueError(f"UMF proposal field mapping differs: {local_role}")
            writer = admitted["proposal"].get("writerSettings")
            if writer != expected["proposalWriterSettings"].get(local_role):
                raise ValueError(f"UMF writer setting differs: {local_role}")
            proposals[local_role] = {
                "carrierRole": carrier_role,
                "sourceSha256": admitted["proposal"]["sourceSha256"],
                "sourceBytesSha256": admitted["source_bytes_sha256"],
                "schemaSha256": admitted["proposal"]["schemaSha256"],
                "proposalSha256": admitted["proposal_sha256"],
                "ddlSha256": sha_bytes(admitted["proposal"]["sql"].encode("utf-8")),
            }
    if set(proposals) != set(manifest["scope"]["roles"]):
        raise ValueError("UMF preflight did not realize all six roles")
    return {"profile": expected["profile"], "candidateCommit": expected["candidateCommit"],
            "bridgeSha256": BRIDGE_SHA256, "roles": proposals, "sparkStarted": False}

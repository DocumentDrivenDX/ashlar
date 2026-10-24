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
import subprocess
import sys
import time


PACKET = Path(__file__).resolve().parent
ROOT = PACKET.parents[6]
LIMIT_SECONDS = 300
LIMIT_PRIVATE = 512 * 1024 * 1024
LIMIT_RECEIPTS = 16 * 1024 * 1024
LIMIT_COMMAND_OUTPUT = 1024 * 1024
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


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def tree_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file() and not p.is_symlink())


def verify_pins(manifest: dict) -> None:
    if manifest.get("sourceRevision") != "32f3d65f":
        raise ValueError("sourceRevision must be rebound to the reviewed current source before use")
    for name, expected in SOURCES.items():
        path = ROOT / name
        if digest(path) != expected or manifest.get("sourceFiles", {}).get(name) != expected:
            raise ValueError(f"source hash is stale or absent: {name}")
    for raw_path, expected in RUNTIME.items():
        path = Path(raw_path)
        if not path.is_file() or digest(path) != expected or manifest.get("runtimeFiles", {}).get(path.name) != expected:
            raise ValueError(f"runtime hash is stale or absent: {path.name}")


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
            if (time.monotonic() >= deadline or output.tell() > LIMIT_COMMAND_OUTPUT
                    or tree_size(attempt) > LIMIT_PRIVATE or tree_size(receipts) > LIMIT_RECEIPTS):
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise TimeoutError("operation exceeded time, private-byte, receipt-byte, or output cap")
            time.sleep(0.1)
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
        vector = manifest.get("publicationVector")
        if not isinstance(vector, dict) or not isinstance(vector.get("tables"), list) or not vector["tables"]:
            raise ValueError("continuation requires a complete publication vector")
        table_names = set()
        for table in vector["tables"]:
            if not isinstance(table.get("name"), list) or not table.get("uuid") or type(table.get("version")) is not int or table["version"] < 0:
                raise ValueError("publication vector table identity is incomplete")
            name = tuple(table["name"])
            if name in table_names:
                raise ValueError("publication vector repeats a table identity")
            table_names.add(name)
        vector_bytes = json.dumps(vector, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if hashlib.sha256(vector_bytes).hexdigest() != manifest.get("publicationVectorSha256"):
            raise ValueError("complete publication vector hash mismatch")
        artifacts = manifest.get("weftArtifacts", {})
        request_item, response_item = artifacts.get("request", {}), artifacts.get("response", {})
        request_file, response_file = Path(request_item.get("path", "")), Path(response_item.get("path", ""))
        if not request_file.is_file() or not response_file.is_file():
            raise ValueError("fresh vector-bound Weft request and response files are required")
        request_bytes, response_bytes = request_file.read_bytes(), response_file.read_bytes()
        if hashlib.sha256(request_bytes).hexdigest() != request_item.get("sha256") or hashlib.sha256(response_bytes).hexdigest() != response_item.get("sha256"):
            raise ValueError("frozen vector-bound Weft artifact hash mismatch")
        request_value, response_value = json.loads(request_bytes), json.loads(response_bytes)
        binding_raw = request_value.get("target", {}).get("bindingJson", "").encode("utf-8")
        binding = json.loads(binding_raw)
        if binding.get("publication") != vector or response_value.get("bindingSha256") != hashlib.sha256(binding_raw).hexdigest():
            raise ValueError("Weft artifacts do not bind the reviewed complete publication vector")
        if manifest.get("vectorReviewer") != "Sol" or manifest.get("vectorDecision") != "execute":
            raise ValueError("continuation vector and Weft artifacts lack separate Sol review")
    elif manifest.get("operations", []) and any(op.get("kind") == "query" for op in manifest["operations"]):
        raise ValueError("prepublication manifest may not contain query operations")

    attempt_id = manifest.get("attemptId")
    if not isinstance(attempt_id, str) or not attempt_id.isascii() or not attempt_id.replace("-", "").isalnum() or len(attempt_id) < 16:
        raise ValueError("attemptId must be a fresh UUID-like identifier")
    base = Path(manifest.get("attemptParent", "/private/tmp")).resolve(strict=True)
    if base.stat().st_uid != os.getuid() or base.is_symlink():
        raise ValueError("attempt parent must be a real, user-owned directory")
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
    attempt = base / f"ashlar-r02-{attempt_id}"
    attempt.mkdir(mode=0o700)
    if attempt.stat().st_uid != os.getuid() or attempt.stat().st_mode & 0o077:
        attempt.rmdir()
        raise ValueError("new attempt directory is not private and user-owned")
    state = attempt / "one-shot-state.json"
    deadline = time.monotonic() + LIMIT_SECONDS
    try:
        state.write_text(json.dumps({"phase": phase, "manifestSha256": manifest_hash,
                                     "status": "running"}, sort_keys=True) + "\n", encoding="utf-8")
        receipts = attempt / "receipts"
        receipts.mkdir(mode=0o700)
        for index, operation in enumerate(manifest.get("operations", [])):
            if time.monotonic() >= deadline:
                raise TimeoutError("five-minute total deadline expired")
            kind = operation.get("kind")
            if phase == "prepublication" and kind not in {"initialize", "publish", "singleton"}:
                raise ValueError("unsupported prepublication operation")
            if phase == "continuation" and kind != "query":
                raise ValueError("continuation manifest may contain only query operations")
            argv = operation.get("argv")
            run_one(argv, attempt, deadline, receipts / f"operation-{index:02d}.log", attempt, receipts)
            check_limits(attempt, receipts)
        state.write_text(json.dumps({"phase": phase, "manifestSha256": manifest_hash,
                                     "status": "complete", "operations": len(manifest.get("operations", []))}, sort_keys=True) + "\n", encoding="utf-8")
        if phase == "prepublication":
            state.write_text(json.dumps({"phase": phase, "manifestSha256": manifest_hash,
                                         "status": "complete-held-after-publication", "operations": len(manifest.get("operations", []))}, sort_keys=True) + "\n", encoding="utf-8")
    except BaseException:
        # Keep the one-shot state in place: a failed attempt must never be silently repeated.
        try:
            state.write_text(json.dumps({"phase": phase, "manifestSha256": manifest_hash,
                                         "status": "failed-or-held"}, sort_keys=True) + "\n", encoding="utf-8")
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

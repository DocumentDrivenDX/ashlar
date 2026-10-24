#!/usr/bin/env python3
"""Effect-free readiness check for the bounded R02 local-native packet.

This checks retained repository inputs and compiler-only receipts. It never
imports Spark, starts Java, writes files, inspects processes, or runs a native
publication. Native execution remains a separate, gated follow-up after R12
releases the local slot and a fresh .4 publication exists.
"""
from __future__ import annotations

import hashlib
import gzip
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[6]
EVIDENCE = Path(__file__).resolve().parent
PUBLIC_TARGET = ROOT / "docs/helix/04-build/evidence/current-upstream-release-20261023/weft053-r02-spark42-public-target-20261024"
PARITY = ROOT / "docs/helix/04-build/evidence/current-upstream-release-20261021/local-current/managed-current-uc-program-parity-v0.4.50.json"

PINS = {
    "examples/end-to-end/typed-graph-model.umf.json": "19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b",
    "examples/end-to-end/configured-typed-graph-source.json": "040545b8cc96701a158b8936750c89f97ce991df9652ceeacf6c66fb9d9fc168",
    "examples/end-to-end/typed-graph-source.jsonl": "9edb7566f82324fadb1e7cf1ba0eaf22691096a26209e938c83730d096d4882b",
    "examples/end-to-end/typed-graph-binding.json": "1ab1416d5a36a082c4d5be38dc061377bdae22bb5395fb8cfbec8cee80910863",
}
TARGETS = {
    "backend03": {
        "request_sha256": "db32c552f3de31fc8f4f11f0b1a7edfb0645026ddbe2a72b6b3f4ea6ac2e74d5",
        "response_sha256": "be2e32908d9175c5206aa9a4a9c5383b8aa6eaee282b295bd9d9e890c5d4e7c9",
        "backend": "ashlar.databricks.paths-keys-having",
        "version": "0.4.1-count-star-having-spark4.2-candidate",
        "profile": "spark4.2.0-delta4.4.1-paths-keys-having-candidate",
    },
    "backend02_page1": {
        "request_sha256": "3206e3c16f7721699a61974e0419b015bb0728a4d6b697e734f68a3e873ae729",
        "response_sha256": "879af9e863ef97016106a33ec95bb80a35095340cb1aa0cb2bfb5e68c2e3cdcb",
        "backend": "ashlar.databricks",
        "version": "0.2.0-local-spark4.2-pages-candidate",
        "profile": "spark4.2.0-delta4.4.1-candidate",
    },
    "backend02_continuation": {
        "response_sha256": "d96f63e9786f4b8d4e495708f9799edd8b9c7a66efbcbc88e0fee5eab89e98c4",
        "backend": "ashlar.databricks",
        "version": "0.2.0-local-spark4.2-pages-candidate",
        "profile": "spark4.2.0-delta4.4.1-candidate",
    },
}


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def check() -> dict:
    inputs = {}
    for relative, expected in PINS.items():
        raw = (ROOT / relative).read_bytes()
        actual = sha256(raw)
        if actual != expected:
            raise ValueError(f"pinned input digest changed: {relative}")
        inputs[relative] = {"sha256": actual, "bytes": len(raw)}

    model = read_json(ROOT / "examples/end-to-end/typed-graph-model.umf.json")
    if model.get("umf") != "0.10.0":
        raise ValueError("typed graph model semantics must remain UMF 0.10.0")
    source_config = read_json(ROOT / "examples/end-to-end/configured-typed-graph-source.json")
    if source_config.get("schema", {}).get("sha256") != PINS["examples/end-to-end/typed-graph-model.umf.json"]:
        raise ValueError("configured source no longer pins the selected UMF model")
    if source_config.get("source", {}).get("sha256") != PINS["examples/end-to-end/typed-graph-source.jsonl"]:
        raise ValueError("configured source no longer pins the event fixture")
    if source_config.get("bindings", {}).get("sha256") != PINS["examples/end-to-end/typed-graph-binding.json"]:
        raise ValueError("configured source no longer pins the graph binding")

    events = [json.loads(line) for line in (ROOT / "examples/end-to-end/typed-graph-source.jsonl").read_text(encoding="utf-8").splitlines()]
    event_rows = [row for row in events if row.get("kind") == "event"]
    batch_ids = [row["batch_id"] for row in events if row.get("kind") == "begin"]
    if len(event_rows) != 13 or len(batch_ids) != 3 or any(row.get("operation") not in {"create", "replace", "delete"} for row in event_rows):
        raise ValueError("bounded fixture must retain 13 events across three complete batches")
    if {row["operation"] for row in event_rows} != {"create", "replace", "delete"}:
        raise ValueError("fixture must include create, update/replace, and delete operations")

    summary = read_json(PUBLIC_TARGET / "replay-summary.json")
    if summary.get("weft_sql") != "0.4.53":
        raise ValueError("public target receipt must record Weft 0.4.53")
    if sha256((PARITY).read_bytes()) != "dd5cef72e28b120f6a81d79d941068cbc3b099f4276dcc386c631bb06f721887":
        raise ValueError("upstream parity request source changed")
    target_receipts = {}
    retained = {
        "backend03": "backend03-response.json.gz",
        "backend02_page1": "backend02-page1-response.json.gz",
        "backend02_continuation": "backend02-continuation-response.json.gz",
    }
    for name, expected in TARGETS.items():
        value = summary[name]
        backend = value["backend"]
        if value.get("status") != "compiled" or value.get("response_sha256") != expected["response_sha256"]:
            raise ValueError(f"public Weft compiler receipt changed: {name}")
        if (backend.get("backendId"), backend.get("backendVersion"), backend.get("targetProfile")) != (
            expected["backend"], expected["version"], expected["profile"]
        ):
            raise ValueError(f"public Weft target declaration changed: {name}")
        context = value["target_context"]
        if context.get("engineVersion") != "4.2.0" or context.get("sessionSettings", {}).get("deltaVersion") != "4.4.1":
            raise ValueError(f"R02 runtime target mismatch: {name}")
        if sha256(gzip.decompress((PUBLIC_TARGET / retained[name]).read_bytes())) != expected["response_sha256"]:
            raise ValueError(f"retained public Weft response bytes changed: {name}")
        if name != "backend02_continuation" and value.get("request_sha256", value.get("original_request_sha256")) != expected["request_sha256"]:
            raise ValueError(f"source query request changed: {name}")
        target_receipts[name] = {"response_sha256": value["response_sha256"], "target": backend}

    return {
        "profile": "ashlar-r02-local-native-preflight/0.1",
        "status": "prepared_native_held",
        "scope": "Effect-free static readiness only; no Spark, Java, publication, native read, process inspection, or cloud operation.",
        "selected_runtime": {"weft_sql": "0.4.53", "umf_distribution": "0.10.1", "umf_semantics": "0.10.0", "spark": "4.2.0", "delta": "4.4.1", "jdk": "21", "carrier": "ashlar-delta/0.4-xxhash64"},
        "fixture": {"events": len(event_rows), "batches": batch_ids, "inputs": inputs},
        "compiler_only_receipts": target_receipts,
        "execution_gate": "The local Spark slot was reported free after the bounded R05 action; await approved process inventory, Sol's independent packet review and explicit execution direction, then freeze fresh .4 publication-bound request/artifact bytes before native execution.",
        "limitations": ["Compiler receipt bindings use ashlar-delta/0.3 and are not native or publication-bound R02 evidence.", "This preflight does not admit the UMF distribution at runtime or establish Spark/Delta execution.", "No Truss, source tuple, or ACK authority is established."],
    }


if __name__ == "__main__":
    try:
        print(json.dumps(check(), sort_keys=True, indent=2, ensure_ascii=False) + "\n", end="")
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        print("R02 local-native static preflight refused", file=sys.stderr)
        raise SystemExit(2) from None

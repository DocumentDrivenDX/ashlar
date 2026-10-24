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
PACKET = ROOT / "docs/helix/04-build/evidence/current-upstream-release-20261024/r02-native-execution-packet-20261024"
R05 = ROOT / "docs/helix/04-build/evidence/canonical-graphframes-holder-candidate-20261023/r05-native-action-r13"

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
R05_RUNTIME_INVENTORY_SHA256 = "973d63580aec4a5f7f396befef68d19823fcf669811e3c6957346f746e6e3505"
R05_RESULT_SHA256 = "c7d627be28af5a71192b0bb7c14edfda9d564e5084db4473de68921234b5c4ee"
UMF_VALIDATION_SHA256 = "c37640b318db03c879ec11a54d68712ffc38e1f3c5955052000a64efddcc38d6"
SOURCE_ORACLE_SHA256 = "f04f74668ac9adc46ac9928b01a4155ccad3aab6421cd297d018b7e45348aab0"
POSITIVE_BACKEND03 = {
    "request_sha256": "8e89f94a3933e5f072b794ddee011dc954654b29485637f2d95254f6e9b56605",
    "response_sha256": "481e324b6a1245566762b7a9dd213c2497e28bb0a96d8fb20541b3ce4b2ad6e7",
    "backend": "ashlar.databricks.paths-keys-having",
    "version": "0.4.1-count-star-having-spark4.2-candidate",
    "profile": "spark4.2.0-delta4.4.1-paths-keys-having-candidate",
}
R05_JARS = {
    "/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-spark_4.2_2.13-4.4.1.jar": "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-storage-4.4.1.jar": "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pyspark/jars/spark-core_2.13-4.2.0.jar": "8e0a03b66338a4a38c9089d9f798ab6a6c6583aefe93f910311704a5ec90ffc4",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pyspark/jars/spark-sql-api_2.13-4.2.0.jar": "d789409a56db600c5160300fcd2c9651bf6f32dd8db1035a9bda17b67543b372",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pyspark/jars/spark-sql_2.13-4.2.0.jar": "ec0a0d699c6c57300ee47ba5d54b4d1f71d3e5401fb90a2bcdcd83b942084dbf",
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

    # Reuse the exact R05 Spark/Delta runtime only after independently hashing
    # the retained JAR bytes and checking their source receipt identities.
    runtime_inventory = R05 / "runtime-tree-inventory.json"
    runtime_result = R05 / "native-attempt-r14-worker-result.json"
    if sha256(runtime_inventory.read_bytes()) != R05_RUNTIME_INVENTORY_SHA256 or sha256(runtime_result.read_bytes()) != R05_RESULT_SHA256:
        raise ValueError("R05 runtime receipt bytes changed")
    r05 = read_json(runtime_result)
    if r05.get("status") != "bounded-one-hop-passed" or (r05.get("runtime", {}).get("spark"), r05.get("runtime", {}).get("delta"), r05.get("runtime", {}).get("jdk")) != ("4.2.0", "4.4.1", "21.0.13"):
        raise ValueError("R05 runtime tuple does not match the R02 selected candidate")
    jar_hashes = {}
    for raw_path, expected in R05_JARS.items():
        path = Path(raw_path)
        actual = sha256(path.read_bytes())
        if actual != expected:
            raise ValueError(f"R05 Spark/Delta JAR digest changed: {path.name}")
        jar_hashes[path.name] = actual

    umf_receipt_path = PACKET / "umf-validation.json"
    if sha256(umf_receipt_path.read_bytes()) != UMF_VALIDATION_SHA256:
        raise ValueError("public UMF validation receipt bytes changed")
    umf_receipt = read_json(umf_receipt_path)
    if umf_receipt.get("distribution", {}).get("version") != "0.10.1" or umf_receipt.get("distribution", {}).get("wheelSha256") != "49459bb2dbc37ac9b62b269e9664bac1483439c7d4c79f0b07855e9c8546c823":
        raise ValueError("UMF validation receipt does not use the selected public wheel")
    if umf_receipt.get("input", {}).get("sha256") != PINS["examples/end-to-end/typed-graph-model.umf.json"] or umf_receipt.get("result") != {
        "valid": True, "complete": True, "diagnostics": [], "canonicalBytes": 4082,
        "canonicalSha256": PINS["examples/end-to-end/typed-graph-model.umf.json"], "canonicalMatchesInput": True,
    }:
        raise ValueError("public UMF validation result does not close the pinned model")

    oracle_path = PACKET / "source-oracle.json"
    if sha256(oracle_path.read_bytes()) != SOURCE_ORACLE_SHA256:
        raise ValueError("source-derived oracle artifact bytes changed")
    oracle = read_json(oracle_path)
    oracle_inputs = oracle.get("inputs", {})
    if oracle_inputs != {
        "modelSha256": PINS["examples/end-to-end/typed-graph-model.umf.json"],
        "configurationSha256": PINS["examples/end-to-end/configured-typed-graph-source.json"],
        "sourceJsonlSha256": PINS["examples/end-to-end/typed-graph-source.jsonl"],
        "bindingSha256": PINS["examples/end-to-end/typed-graph-binding.json"],
    }:
        raise ValueError("source-derived oracle input hashes do not match the frozen fixture")
    expectations = oracle["firstPublicationSourceDerivedExpectations"]
    if expectations["backend03GroupedPaths"]["queryRequestSha256"] != TARGETS["backend03"]["request_sha256"] or expectations["backend02SdkPage"]["queryRequestSha256"] != TARGETS["backend02_page1"]["request_sha256"]:
        raise ValueError("source oracle request hashes differ from the original compiler requests")
    by_id = {(int(row["type_id"]), int(row["id"])): row for row in event_rows if row["operation"] == "create"}
    singleton = expectations["objectSingletons"]
    if len(singleton) != 4 or any(json.loads(by_id[(row["typeId"], row["entityId"])]["props_json"]) != {str(k): v for k, v in row["properties"].items()} for row in singleton):
        raise ValueError("source-derived singleton expectations do not match first-batch source events")
    first_batch = event_rows[:8]
    objects = {
        (int(row["type_id"]), int(row["id"])): json.loads(row["props_json"])
        for row in first_batch if row["entity_kind"] == "object"
    }
    edges = [row for row in first_batch if row["entity_kind"] == "edge"]
    edge_rows = [
        {"typeId": int(row["type_id"]), "edgeId": int(row["id"]),
         "from": [int(row["endpoints"][0]["type_id"]), int(row["endpoints"][0]["id"])],
         "to": [int(row["endpoints"][1]["type_id"]), int(row["endpoints"][1]["id"])]}
        for row in edges
    ]
    if edge_rows != expectations["directedEdges"] or any(tuple(e["from"]) not in objects or tuple(e["to"]) not in objects for e in edge_rows):
        raise ValueError("source-derived directed edge identities/endpoints mismatch")
    groups = {}
    for (type_id, entity_id), props in objects.items():
        if type_id != 17:
            continue
        terminals = []
        for edge in edge_rows:
            if edge["typeId"] != 31 or edge["from"] != [17, entity_id]:
                continue
            middle_id = edge["to"][1]
            for second in edge_rows:
                if second["typeId"] == 32 and second["from"] == [17, middle_id]:
                    terminals.append(second["to"])
        if terminals:
            key = props["21"]
            groups[key] = {"person_key": key, "paths": len(terminals),
                           "distinctTerminalKeys": len({objects[tuple(end)]["24"] for end in terminals})}
    grouped = expectations["backend03GroupedPaths"]
    derived_groups = sorted(groups.values(), key=lambda row: row["person_key"].encode("utf-8"))
    derived_rows = [[row["person_key"], row["paths"], row["distinctTerminalKeys"]]
                    for row in derived_groups if row["paths"] > 0]
    if grouped["sourceDerivedPreHavingGroups"] != derived_groups or grouped["exactExpectedResultRows"] != derived_rows:
        raise ValueError("source-derived grouped HAVING oracle mismatch")
    original_request = json.loads(PARITY.read_text(encoding="utf-8"))["cases"]["backend03-grouped-having"]["local50"]["requestJson"]
    original_response = json.loads(gzip.decompress((PUBLIC_TARGET / "backend03-response.json.gz").read_bytes()))
    if sha256(original_request.encode("utf-8")) != TARGETS["backend03"]["request_sha256"]:
        raise ValueError("original Backend03 request bytes changed")
    original_value = json.loads(original_request)
    if original_value.get("sql", "").count("HAVING COUNT(*) > 0") != 1 or original_response.get("logicalPlan", {}).get("having", [{}])[0].get("threshold", {}).get("value") != "0":
        raise ValueError("original Backend03 public request/response threshold is not exactly COUNT(*) > 0")
    if grouped.get("having") != "COUNT(*) > 0" or grouped.get("exactExpectedResultRows") != [["person-1", 2, 1]]:
        raise ValueError("original Backend03 source-derived positive result bag is incorrect")
    positive_request_raw = (PACKET / "positive-backend03-request.json").read_bytes()
    positive_response_raw = (PACKET / "positive-backend03-response.json").read_bytes()
    positive_request = json.loads(positive_request_raw)
    positive_response = json.loads(positive_response_raw)
    positive_variant = grouped.get("positiveVariant", {})
    positive_backend = positive_response.get("backend", {})
    positive_target = (positive_backend.get("backendId"), positive_backend.get("backendVersion"), positive_backend.get("targetProfile"))
    if sha256(positive_request_raw) != POSITIVE_BACKEND03["request_sha256"] or sha256(positive_response_raw) != POSITIVE_BACKEND03["response_sha256"]:
        raise ValueError("positive Backend03 compiler artifact digest changed")
    if positive_variant.get("requestSha256") != POSITIVE_BACKEND03["request_sha256"] or positive_variant.get("responseSha256") != POSITIVE_BACKEND03["response_sha256"]:
        raise ValueError("positive Backend03 oracle digests changed")
    positive_sql = positive_request.get("sql", "")
    expected_positive_sql = original_value.get("sql", "").replace("COUNT(*) > 0", "COUNT(*) > 1")
    if positive_sql != expected_positive_sql or positive_request.get("target") != original_value.get("target") or positive_request.get("source") != original_value.get("source"):
        raise ValueError("positive Backend03 request must change only the original HAVING threshold")
    if positive_response.get("status") != "compiled" or positive_target != (POSITIVE_BACKEND03["backend"], POSITIVE_BACKEND03["version"], POSITIVE_BACKEND03["profile"]):
        raise ValueError("positive Backend03 Weft response target/status mismatch")
    if positive_response.get("logicalPlan", {}).get("having", [{}])[0].get("threshold", {}).get("value") != "1":
        raise ValueError("positive Backend03 Weft logical plan did not retain threshold 1")
    if positive_variant.get("expectedResultRows") != [["person-1", 2, 1]] or [row for row in derived_rows if row[1] > 1] != positive_variant.get("expectedResultRows"):
        raise ValueError("positive Backend03 source-derived result bag mismatch")
    sdk = expectations["backend02SdkPage"]
    ordered_people = sorted(((props["21"], props["22"]) for (tid, _), props in objects.items() if tid == 17), key=lambda row: row[0].encode("utf-8"))
    if sdk["firstPageVisibleRows"] != [list(row) for row in ordered_people[:2]] or sdk["lookaheadKey"] != ordered_people[2][0] or sdk["continuationVisibleRows"] != [list(row) for row in ordered_people[2:4]]:
        raise ValueError("source-derived SDK page oracle mismatch")

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
        "effect_free_closure": {
            "public_umf_validation": {"receipt_sha256": UMF_VALIDATION_SHA256, "status": "valid_and_complete", "canonical_sha256": PINS["examples/end-to-end/typed-graph-model.umf.json"]},
            "r05_runtime_reuse": {"runtime_inventory_sha256": R05_RUNTIME_INVENTORY_SHA256, "result_sha256": R05_RESULT_SHA256, "spark": "4.2.0", "delta": "4.4.1", "jdk": "21.0.13", "jars": jar_hashes},
            "source_oracle_sha256": SOURCE_ORACLE_SHA256,
            "publication_bound_weft_compile": "must be compiled and frozen only after the fresh .4 native publication supplies its exact UUID/version vector; do not invent or reuse the .3 binding response bytes",
        },
        "compiler_only_receipts": target_receipts,
        "positive_backend03_compiler_artifact": {"request_sha256": POSITIVE_BACKEND03["request_sha256"], "response_sha256": POSITIVE_BACKEND03["response_sha256"], "expected_rows": [["person-1", 2, 1]]},
        "execution_gate": "The local Spark slot was reported free after the bounded R05 action; await approved process inventory, Sol's independent packet review and explicit execution direction. After publishing the fresh .4 vector, compile with public Weft 0.4.53, durably freeze/hash the exact requests, bindings, artifacts, and responses, and obtain independent vector-pin review before any SQL execution.",
        "limitations": ["Compiler receipt bindings use ashlar-delta/0.3 and are not native or publication-bound R02 evidence.", "This preflight does not admit the UMF distribution at runtime or establish Spark/Delta execution.", "No Truss, source tuple, or ACK authority is established."],
    }


if __name__ == "__main__":
    try:
        print(json.dumps(check(), sort_keys=True, indent=2, ensure_ascii=False) + "\n", end="")
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        print("R02 local-native static preflight refused", file=sys.stderr)
        raise SystemExit(2) from None

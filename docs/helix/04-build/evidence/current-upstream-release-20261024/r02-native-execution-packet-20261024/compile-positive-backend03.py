#!/usr/bin/env python3
"""Compile one positive R02 grouped-path request with public Weft only."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import weft


ROOT = Path(__file__).resolve().parents[6]
PARITY = ROOT / "docs/helix/04-build/evidence/current-upstream-release-20261021/local-current/managed-current-uc-program-parity-v0.4.50.json"
OUT = Path(__file__).resolve().parent
ORIGINAL_SHA256 = "db32c552f3de31fc8f4f11f0b1a7edfb0645026ddbe2a72b6b3f4ea6ac2e74d5"
POSITIVE_SQL = (
    "SELECT p.person_key, COUNT(*) AS paths, "
    "COUNT_DISTINCT_PATH_TARGETS(x) AS targets FROM Person p "
    "CROSS JOIN EXPAND_PATHS(p.knows, located) AS x "
    "GROUP BY p.person_key HAVING COUNT(*) > 1 ORDER BY p.person_key"
)
ORIGINAL_SQL = POSITIVE_SQL.replace("COUNT(*) > 1", "COUNT(*) > 0")


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def main() -> None:
    parity = json.loads(PARITY.read_text(encoding="utf-8"))
    original = parity["cases"]["backend03-grouped-having"]["local50"]["requestJson"]
    if sha(original.encode("utf-8")) != ORIGINAL_SHA256:
        raise SystemExit("original Backend03 request digest changed")
    original_value = json.loads(original)
    if original_value.get("sql") != ORIGINAL_SQL:
        raise SystemExit("original Backend03 SQL differs from the hash-bound public request")
    original_value["sql"] = POSITIVE_SQL
    request = json.dumps(original_value, separators=(",", ":"), ensure_ascii=False)
    parsed = json.loads(request)
    if parsed.get("sql") != POSITIVE_SQL:
        raise SystemExit("positive request did not preserve the exact graph query shape")
    response = weft.compile_paths_keys_json(request)
    result = json.loads(response)
    backend = result.get("backend", {})
    expected = (
        "ashlar.databricks.paths-keys-having",
        "0.4.1-count-star-having-spark4.2-candidate",
        "spark4.2.0-delta4.4.1-paths-keys-having-candidate",
    )
    actual = (backend.get("backendId"), backend.get("backendVersion"), backend.get("targetProfile"))
    if result.get("status") != "compiled" or actual != expected:
        raise SystemExit("public Weft did not compile the positive request to the selected candidate target")
    if result.get("logicalPlan", {}).get("having", [{}])[0].get("threshold", {}).get("value") != "1":
        raise SystemExit("public Weft positive grouped HAVING plan did not retain threshold 1")

    request_raw = request.encode("utf-8")
    response_raw = response.encode("utf-8")
    (OUT / "positive-backend03-request.json").write_bytes(request_raw)
    (OUT / "positive-backend03-response.json").write_bytes(response_raw)
    receipt = {
        "profile": "ashlar-r02-positive-backend03-public-compile/0.1",
        "compiler": {"distribution": "weft-sql", "version": "0.4.53", "api": "compile_paths_keys_json"},
        "originalRequestSha256": ORIGINAL_SHA256,
        "requestSha256": sha(request_raw),
        "responseSha256": sha(response_raw),
        "status": result["status"],
        "backend": backend,
        "targetContext": result.get("targetContext"),
        "sql": POSITIVE_SQL,
        "publicationBinding": json.loads(parsed["target"]["bindingJson"]).get("publication"),
        "qualification": "Effect-free compiler artifact only. The inherited ashlar-delta/0.3 binding is not a fresh .4 publication pin and this response must be regenerated against the actual .4 vector after publication before any SQL execution.",
    }
    (OUT / "positive-backend03-compiler-receipt.json").write_text(
        json.dumps(receipt, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(receipt, sort_keys=True, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

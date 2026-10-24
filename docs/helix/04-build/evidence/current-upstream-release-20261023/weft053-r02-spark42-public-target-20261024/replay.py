#!/usr/bin/env python3
"""Replay the three compiler-only public Weft 0.4.53 R02 target checks."""
import hashlib
import gzip
import importlib.metadata
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[6]
EVIDENCE = Path(__file__).resolve().parent
SOURCE = ROOT / "docs/helix/04-build/evidence/current-upstream-release-20261021/local-current/managed-current-uc-program-parity-v0.4.50.json"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def compact(value) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def save(name: str, raw: str):
    data = raw.encode("utf-8")
    compressed = gzip.compress(data, mtime=0)
    (EVIDENCE / f"{name}.gz").write_bytes(compressed)
    return json.loads(raw), digest(data), digest(compressed)


def main():
    import weft

    if importlib.metadata.version("weft-sql") != "0.4.53":
        raise SystemExit("expected public Weft 0.4.53")
    cases = json.loads(SOURCE.read_text(encoding="utf-8"))["cases"]
    backend03_request = cases["backend03-grouped-having"]["local50"]["requestJson"]
    backend02_request = cases["backend02-page1"]["local50"]["requestJson"]
    if digest(backend03_request.encode("utf-8")) != "db32c552f3de31fc8f4f11f0b1a7edfb0645026ddbe2a72b6b3f4ea6ac2e74d5":
        raise SystemExit("original Backend03 request bytes changed")
    if digest(backend02_request.encode("utf-8")) != "3206e3c16f7721699a61974e0419b015bb0728a4d6b697e734f68a3e873ae729":
        raise SystemExit("original Backend02 request bytes changed")
    backend03, backend03_hash, backend03_gzip_hash = save(
        "backend03-response.json", weft.compile_paths_keys_json(backend03_request)
    )

    page_program = {
        "version": "weft.engine-page/0.1",
        "compiler": "ashlar-spark4-pages",
        "request": json.loads(backend02_request),
        "pageSize": 2,
    }
    page_program_raw = compact(page_program)
    first, first_hash, first_gzip_hash = save(
        "backend02-page1-response.json", weft.compile_engine_page_json(page_program_raw)
    )
    continuation, continuation_hash, continuation_gzip_hash = save(
        "backend02-continuation-response.json",
        weft.check_engine_page_json(compact(first), backend02_request, '["person-2"]'),
    )

    expected = [
        (backend03, "ashlar.databricks.paths-keys-having", "0.4.1-count-star-having-spark4.2-candidate", "spark4.2.0-delta4.4.1-paths-keys-having-candidate"),
        (first.get("artifact", {}), "ashlar.databricks", "0.2.0-local-spark4.2-pages-candidate", "spark4.2.0-delta4.4.1-candidate"),
        (continuation.get("artifact", {}), "ashlar.databricks", "0.2.0-local-spark4.2-pages-candidate", "spark4.2.0-delta4.4.1-candidate"),
    ]
    for result, backend_id, version, profile in expected:
        actual = result.get("backend", {})
        if result.get("status") != "compiled" or (
            actual.get("backendId"), actual.get("backendVersion"), actual.get("targetProfile")
        ) != (backend_id, version, profile):
            raise SystemExit("compiled response did not retain its exact expected target")

    summary = {
        "weft_sql": importlib.metadata.version("weft-sql"),
        "weft_import": str(Path(weft.__file__).resolve()),
        "backend03": {
            "api": "compile_paths_keys_json",
            "request_sha256": digest(backend03_request.encode("utf-8")),
            "response_sha256": backend03_hash,
            "response_gzip_sha256": backend03_gzip_hash,
            "status": backend03.get("status"),
            "backend": backend03.get("backend"),
            "target_context": backend03.get("targetContext"),
        },
        "backend02_page1": {
            "api": "compile_engine_page_json",
            "original_request_sha256": digest(backend02_request.encode("utf-8")),
            "wrapped_request_sha256": digest(page_program_raw.encode("utf-8")),
            "response_sha256": first_hash,
            "response_gzip_sha256": first_gzip_hash,
            "status": first.get("status"),
            "backend": first.get("artifact", {}).get("backend"),
            "target_context": first.get("artifact", {}).get("targetContext"),
            "page": first.get("page"),
        },
        "backend02_continuation": {
            "api": "check_engine_page_json",
            "after_json": '["person-2"]',
            "response_sha256": continuation_hash,
            "response_gzip_sha256": continuation_gzip_hash,
            "status": continuation.get("status"),
            "backend": continuation.get("artifact", {}).get("backend"),
            "target_context": continuation.get("artifact", {}).get("targetContext"),
            "after": continuation.get("after"),
            "page": continuation.get("page"),
        },
    }
    rendered = json.dumps(summary, indent=2, ensure_ascii=False) + "\n"
    (EVIDENCE / "replay-summary.json").write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()

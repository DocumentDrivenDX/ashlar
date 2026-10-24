#!/usr/bin/env python3
"""One private ordinal-1 R02 session under the selected live Weft .53 cohort."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import time


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def write_new(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        if os.write(fd, raw) != len(raw):
            raise OSError("short write")
        os.fsync(fd)
    finally:
        os.close(fd)


def read_pinned_json(path: Path, expected_sha256: str) -> dict:
    raw = path.read_bytes()
    if sha(raw) != expected_sha256:
        raise ValueError("pinned R02 invocation bytes changed")
    value = json.loads(raw)
    if type(value) is not dict:
        raise ValueError("pinned R02 invocation must be a JSON object")
    return value


def make_query_models():
    from pydantic import BaseModel, ConfigDict, create_model
    config = ConfigDict(extra="forbid", strict=True)
    parameters = create_model("R02PageParameters", __base__=BaseModel, __config__=config)
    result = create_model("R02PageResult", __base__=BaseModel, __config__=config,
        person_key=(str, ...), person_name=(str, ...))
    return parameters, result


def refusal_location(exc: BaseException) -> str:
    """Report only exception type and innermost source location, never payloads."""
    frame = exc.__traceback__
    while frame is not None and frame.tb_next is not None:
        frame = frame.tb_next
    source = f"{Path(frame.tb_frame.f_code.co_filename).name}:{frame.tb_lineno}" if frame else "unknown:0"
    return f"{type(exc).__module__}.{type(exc).__name__} at {source}"


def run(manifest_path: Path, repository: Path) -> None:
    manifest = json.loads(manifest_path.read_bytes())
    attempt = manifest_path.parent.resolve(strict=True)
    if (manifest.get("profile") != "ashlar-r02-live-alias-continuation/0.1"
            or manifest.get("attemptRoot") != str(attempt)
            or manifest.get("invocation", {}).get("ordinal") != 1
            or manifest.get("reviewStages") != ["pre-sql", "post-page1"]):
        raise ValueError("R02 continuation invocation identity refused")
    for relative, expected in manifest["runtimeSourceFiles"].items():
        if sha((repository / relative).read_bytes()) != expected:
            raise ValueError("R02 runtime source pin changed")
    worker = Path(__file__).resolve()
    if sha(worker.read_bytes()) != manifest["invocation"]["workerSha256"]:
        raise ValueError("R02 worker bytes differ from packet")
    sys.path.insert(0, str(repository / "src"))
    from dataclasses import replace
    from ashlar.weft_053_distribution import Weft053Distribution, Weft053Paths
    from ashlar_host.configured_local_cli import parse_configured_local
    from ashlar_host.configured_local_runtime import run_configured_local_r02_weft053
    from ashlar_host.configured_sdk_pages import ConfiguredR02Weft053Selection, ConfiguredSdkPagesConfig
    from ashlar_host.path_capture import PathCaptureConfig

    prior = manifest["priorPublication"]
    prior_root = Path(prior["attemptRoot"])
    state_path, vector_path = Path(prior["statePath"]), Path(prior["vectorPath"])
    if sha(state_path.read_bytes()) != prior["stateSha256"] or sha(vector_path.read_bytes()) != prior["vectorFileSha256"]:
        raise ValueError("preserved publication state or vector changed")
    vector = json.loads(vector_path.read_bytes())
    vector_canonical = json.dumps(vector, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    if vector != prior["vector"] or sha(vector_canonical) != prior["vectorSha256"]:
        raise ValueError("preserved publication vector identity changed")

    template_path = Path(prior["baseInvocationPath"])
    template = read_pinned_json(template_path, prior["baseInvocationSha256"])
    installation = prior_root / "installation" / "installation.json"
    template["installation_sha256"] = sha(installation.read_bytes())
    template["output"] = str(attempt / "output")
    template["operation"] = "publish"
    template["ordinal"] = 1
    invocation = parse_configured_local(canonical(template))
    compiler = Weft053Distribution(Weft053Paths(Path(manifest["runtime"]["weftIndex"]),
        manifest["runtime"]["weftIndexSha256"]))
    parameters_type, result_type = make_query_models()
    query = ConfiguredSdkPagesConfig(compiler,
        "SELECT p.person_key,p.person_name FROM Person p", parameters_type, result_type,
        parameters_type(), 2, 1000, 120, PathCaptureConfig(1000, 65536, 1048576))
    query.__post_init__()
    worker_started = time.monotonic()
    runtime_metadata = dict(manifest["runtime"])
    runtime_metadata["reviewDeadlineMonotonic"] = worker_started + 230.0
    runtime_metadata["wholeProcessDeadlineMonotonic"] = worker_started + 270.0
    selection = ConfiguredR02Weft053Selection(attempt, manifest["attemptId"],
        manifest["sourceRevision"], prior["sourceRevision"],
        manifest["runtimeSourceFiles"]["src/ashlar_host/configured_sdk_pages.py"],
        manifest["packetFiles"]["r02_live_alias_gate.py"], runtime_metadata,
        manifest["sourceOracleSha256"])
    runtime = invocation.runtime
    runtime_result = run_configured_local_r02_weft053(runtime, ordinal=1, query=query, selection=selection)
    if (runtime_result.get("profile") != "ashlar-configured-local-query-result/0.1"
            or runtime_result.get("operation") != "query" or runtime_result.get("ordinal") != 1
            or runtime_result.get("protected_ack") is not False or runtime_result.get("completed_batches", 0) < 1
            or type(runtime_result.get("query")) is not dict):
        raise ValueError("configured-local query result envelope differs from the held ordinal-1 operation")
    answer = runtime_result["query"]
    returned_vector = {name: list(pair) for name, pair in answer["vector"].items()}
    expected_vector = {".".join(item["name"]): [item["uuid"], item["version"]]
        for item in vector["tables"]}
    if returned_vector != expected_vector or answer.get("scopeKind") != "ashlar-delta/0.3":
        raise ValueError("held ordinal-1 read scope differs from the preserved publication vector")
    result = {"profile": "ashlar-r02-weft053-continuation-result/0.1", "status": "complete",
        "attemptId": manifest["attemptId"], "sourceRevision": manifest["sourceRevision"],
        "priorPublicationSourceRevision": prior["sourceRevision"],
        "publicationVectorSha256": prior["vectorSha256"], "scopeOrdinal": 1,
        "scopeCount": 1, "aliasScopeCount": 1, "reviewStages": ["pre-sql", "post-page1"],
        "scopeClosed": True, "protectedAck": False, "result": answer}
    raw = canonical(result)
    if len(raw) > 1024 * 1024:
        raise ValueError("R02 continuation result exceeds receipt cap")
    write_new(attempt / "continuation-result.json", raw)


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--repo", type=Path, required=True)
    args = parser.parse_args()
    try:
        run(args.manifest, args.repo)
    except Exception as exc:
        print(f"R02 live-alias worker refused: {refusal_location(exc)}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

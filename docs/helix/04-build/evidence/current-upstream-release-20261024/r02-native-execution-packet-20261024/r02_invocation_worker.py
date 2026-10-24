#!/usr/bin/env python3
"""Materialize one hash-bound invocation and run the configured-local CLI."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
import uuid


ROLES = {"object_current", "edge_current", "tombstone", "whole_source_history"}
MAX_OUTPUT = 1024 * 1024
MAX_RECEIPT = 262144


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"


def write_new(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        if os.write(fd, raw) != len(raw):
            raise OSError("short write")
        os.fsync(fd)
    finally:
        os.close(fd)


def under(root: Path, path: Path) -> Path:
    resolved = path.resolve(strict=True)
    if root.resolve(strict=True) not in resolved.parents:
        raise ValueError("invocation path escaped the private attempt root")
    if path.is_symlink():
        raise ValueError("symlinked invocation input refused")
    return resolved


def contained_future(root: Path, path: Path) -> Path:
    if not path.is_absolute() or root.resolve(strict=True) not in path.parent.resolve(strict=True).parents and path.parent.resolve(strict=True) != root.resolve(strict=True):
        raise ValueError("configured path escaped the private attempt root")
    if path.is_symlink():
        raise ValueError("symlinked configured path refused")
    return path


def verify_source_config(path: Path) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    for key in ("source", "schema", "bindings"):
        target = path.parent / value[key]["path"]
        if not target.is_file() or target.is_symlink() or sha256(target) != value[key]["sha256"]:
            raise ValueError(f"source configuration input changed: {key}")


def extract_vector(attempt: Path, result: dict, installation_sha: str) -> dict:
    installation = attempt / "installation" / "installation.json"
    if sha256(installation) != installation_sha:
        raise ValueError("installation bytes changed after the invocation bound them")
    registry = json.loads(installation.read_text(encoding="utf-8"))
    targets = registry.get("targets")
    if (registry.get("profile") != "ashlar-configured-local-installation/0.2"
            or not isinstance(targets, list) or len(targets) != 6):
        raise ValueError("initialized registry does not contain the exact six original target identities")
    by_role = {}
    for target in targets:
        table = target.get("table")
        role = table.rsplit(".", 1)[-1] if isinstance(table, str) else ""
        if role in by_role or role not in {*ROLES, "manifest", "attempts"}:
            raise ValueError("initialized registry target roles are duplicate or unexpected")
        if table != f"local.configured.{role}" or target.get("path") != str(attempt / "installation" / role):
            raise ValueError("registry target is not the stable configured-local physical target")
        if not isinstance(target.get("uuid"), str) or len(target["uuid"]) != 36:
            raise ValueError("initialized registry has an invalid target UUID")
        try:
            if str(uuid.UUID(target["uuid"])) != target["uuid"]:
                raise ValueError("initialized registry target UUID is not canonical")
        except ValueError:
            raise ValueError("initialized registry target UUID is invalid") from None
        by_role[role] = target
    if set(by_role) != {*ROLES, "manifest", "attempts"}:
        raise ValueError("initialized registry is missing a required table role")
    if len({item["uuid"] for item in by_role.values()}) != 6:
        raise ValueError("initialized registry target UUIDs are not unique")
    if (result.get("operation") != "replay" or result.get("replayed") is not True
            or result.get("ordinal") != 3 or result.get("completed_batches") != 3):
        raise ValueError("final replay result does not prove the third batch was complete")
    manifest = result.get("manifest")
    if not isinstance(manifest, dict) or manifest.get("profile_version") != "ashlar-delta/0.3":
        raise ValueError("final operation did not return the original publication manifest")
    versions = json.loads(manifest.get("table_versions_json", "{}"))
    expected_keys = {f"local.configured.{role}" for role in ROLES}
    if set(versions) != expected_keys or any(type(v) is not int or v < 0 for v in versions.values()):
        raise ValueError("published manifest does not have the complete four-role version vector")
    vector = {
        "id": manifest.get("publication_id"),
        "manifestUuid": by_role["manifest"]["uuid"],
        "tables": [
            {"name": by_role[role]["table"].split("."), "uuid": by_role[role]["uuid"],
             "version": versions[f"local.configured.{role}"]}
            for role in sorted(ROLES)
        ],
    }
    if not isinstance(vector["id"], str) or not vector["id"]:
        raise ValueError("publication id is missing")
    return vector


def run(args: argparse.Namespace) -> None:
    attempt = args.attempt.resolve(strict=True)
    if attempt.is_symlink() or attempt.stat().st_uid != os.getuid() or stat.S_IMODE(attempt.stat().st_mode) != 0o700:
        raise ValueError("attempt root is not a private user-owned directory")
    template_path = under(attempt, args.template)
    if sha256(template_path) != args.template_sha256:
        raise ValueError("invocation template hash differs from the reviewed supervisor manifest")
    template = json.loads(template_path.read_bytes())
    if template.get("profile") != "ashlar-configured-local/0.2" or template.get("installation_id") != "r02-" + attempt.name.removeprefix("ashlar-r02-"):
        raise ValueError("invocation template does not belong to this R02 attempt")
    verify_source_config(under(attempt, Path(template["source_configuration"])))
    if template.get("operation") == "initialize":
        if template.get("installation_sha256") is not None:
            raise ValueError("initialize must have a null installation digest")
    else:
        installation = under(attempt, attempt / "installation" / "installation.json")
        template["installation_sha256"] = sha256(installation)
    for key in ("installation", "output", "jars", "source_configuration"):
        contained_future(attempt, Path(template[key]))
    if Path(template["installation"]).resolve() != (attempt / "installation").resolve():
        raise ValueError("installation root changed")
    under(attempt, Path(template["jars"]) / "delta-spark_4.2_2.13-4.4.1.jar")
    under(attempt, Path(template["jars"]) / "delta-storage-4.4.1.jar")
    output = Path(template["output"])
    if output.exists() or output.is_symlink():
        raise ValueError("per-operation output already exists")
    case = args.case
    final_path = attempt / "invocations" / f"final-{case}.json"
    raw = canonical(template)
    if len(raw) > MAX_RECEIPT:
        raise ValueError("invocation exceeds the configured receipt ceiling")
    write_new(final_path, raw)
    prepared = {"case": case, "configurationPath": str(final_path), "configurationSha256": hashlib.sha256(raw).hexdigest(),
                "installationSha256": template["installation_sha256"], "status": "frozen-before-cli"}
    write_new(attempt / "receipts" / f"{case}-prepared.json", canonical(prepared))
    env = {"PATH": "/usr/bin:/bin", "HOME": os.environ.get("HOME", "/nonexistent"),
           "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "JAVA_HOME": args.java_home,
           "PYSPARK_PYTHON": args.python, "PYSPARK_DRIVER_PYTHON": args.python,
           "PYTHONDONTWRITEBYTECODE": "1", "SPARK_LOCAL_IP": "127.0.0.1",
           "TMPDIR": str(attempt / "tmp")}
    stdout_path = attempt / "receipts" / f"{case}-cli-stdout.json"
    stderr_path = attempt / "receipts" / f"{case}-stderr.log"
    with stdout_path.open("xb") as stdout_stream, stderr_path.open("xb") as stderr_stream:
        process = subprocess.Popen([args.python, "-I", "-B", "-m", "ashlar", "run-configured-local",
                                    "--configuration", str(final_path)], cwd=attempt,
                                   env=env, stdin=subprocess.DEVNULL, stdout=stdout_stream,
                                   stderr=stderr_stream, start_new_session=False)
        while process.poll() is None:
            if stdout_path.stat().st_size + stderr_path.stat().st_size > MAX_OUTPUT:
                raise ValueError("configured-local CLI exceeded its streaming output cap")
            time.sleep(0.05)
        process.wait()
    if stdout_path.stat().st_size + stderr_path.stat().st_size > MAX_OUTPUT:
        raise ValueError("configured-local CLI output exceeded its receipt cap")
    if process.returncode:
        raise RuntimeError(f"configured-local CLI refused with status {process.returncode}")
    result = json.loads(stdout_path.read_bytes())
    report_path = Path(template["output"]) / "report.json"
    if not report_path.is_file() or report_path.is_symlink() or report_path.read_bytes() != canonical(result).rstrip(b"\n"):
        # The CLI's result report is compact JSON without the canonical trailing newline.
        if not report_path.is_file() or json.loads(report_path.read_bytes()) != result:
            raise ValueError("CLI stdout and its durable result report do not match")
    write_new(attempt / "receipts" / f"{case}-result.json", canonical(result))
    if case == "batch-3-replay":
        vector = extract_vector(attempt, result, template["installation_sha256"])
        write_new(attempt / "publication-vector.json", canonical(vector))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", type=Path, required=True)
    parser.add_argument("--case", required=True)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--java-home", required=True)
    parser.add_argument("--template-sha256", required=True)
    args = parser.parse_args()
    try:
        run(args)
    except Exception as exc:
        print(f"R02 invocation worker refused: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

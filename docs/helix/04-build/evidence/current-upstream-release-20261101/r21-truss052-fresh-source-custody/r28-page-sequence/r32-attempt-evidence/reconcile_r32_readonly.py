#!/usr/bin/env python3
"""Read-only reconciliation of the retained R32 Delta/SQLite/source receipts."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

ROOT = Path("/private/tmp/ashlar-r32-truss052-page-sequence-20261101")
SUPERVISOR = Path("/private/tmp/ashlar-r32-truss052-page-sequence-20261101-supervisor")
OUTPUT = Path(__file__).with_name("r32-readonly-reconciliation.json")


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def info(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path), "bytes": len(data), "sha256": sha(data)}


def main() -> None:
    failure_path = ROOT / "receipts/failure.json"
    page_path = ROOT / "receipts/genesis-page-selection.json"
    source_path = ROOT / "receipts/resume-source-reconciliation.json"
    supervisor_path = SUPERVISOR / "supervisor-receipt.json"
    failure = json.loads(failure_path.read_bytes())
    page = json.loads(page_path.read_bytes())
    source = json.loads(source_path.read_bytes())
    supervisor = json.loads(supervisor_path.read_bytes())
    if failure.get("source_ack_attempted") is not False or page.get("acknowledgement") is not False:
        raise SystemExit("Retained receipts do not prove the source ACK was unattempted")
    if supervisor.get("process_group_remaining") != [] or supervisor.get(
            "postgres_cluster_cleanup", {}).get("remaining") != []:
        raise SystemExit("Supervisor does not prove owned process cleanup")

    roles = {}
    for directory in sorted((ROOT / "delta").iterdir()):
        if not directory.is_dir() or directory.name.startswith("."):
            continue
        history = []
        log = directory / "_delta_log"
        if log.exists():
            for commit in sorted(log.glob("*.json")):
                raw = commit.read_bytes()
                actions = [next(iter(json.loads(line))) for line in raw.splitlines() if line]
                history.append({"version": int(commit.stem), "sha256": sha(raw),
                                "actions": actions})
        roles[directory.name] = {
            "reservation_present": (directory / ".ashlar-local-installation-reservation.json").is_file(),
            "history": history,
            "parquet_files": sum(1 for _ in directory.rglob("*.parquet")),
        }

    database = ROOT / "delta/operations.sqlite"
    connection = sqlite3.connect(f"file:{database}?mode=ro&immutable=1", uri=True)
    try:
        tables = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        counts = {name: int(connection.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0])
                  for name in tables if name.startswith("local_")}
    finally:
        connection.close()
    local_files = {}
    for pattern in ("receipts/*", "delta/publication-intent-*.json", "delta/source-page-*.json"):
        for path in sorted(ROOT.glob(pattern)):
            if path.is_file():
                local_files[str(path.relative_to(ROOT))] = info(path)

    result = {
        "profile": "ashlar-r32-truss052-selector-refusal-reconciliation/0.1",
        "attempt_root": str(ROOT), "supervisor_root": str(SUPERVISOR),
        "scope_sha256": supervisor["scope_sha256"],
        "worker_sha256": supervisor["worker_sha256"],
        "supervisor_sha256": supervisor["supervisor_sha256"],
        "failure": {"type": failure["failure_type"], "message": failure["failure"],
                    "source_ack_attempted": failure["source_ack_attempted"],
                    "receipt": info(failure_path)},
        "public_source": {
            "public_readbacks": source["public_readbacks"],
            "acceptance_replayed": source["acceptance_replayed"],
            "consumer_enrollment_replayed": source["consumer_enrollment_replayed"],
            "genesis_group_replayed": source["genesis_group_replayed"],
            "page_sha256": source["page_sha256"],
            "page_acknowledgement": page["acknowledgement"],
            "page_receipt": info(page_path), "source_receipt": info(source_path),
        },
        "supervisor": {key: supervisor.get(key) for key in (
            "status", "returncode", "wall_seconds", "peak_supervised_process_rss_bytes",
            "postgres_postmaster_pid_observed", "process_group_remaining",
            "postgres_cluster_cleanup", "effects_may_remain")},
        "supervisor_receipt": info(supervisor_path),
        "delta_roles": roles, "sqlite_counts": counts,
        "local_attempt_files": local_files,
        "conclusion": {
            "graph_data_publication": False,
            "source_acknowledgement_attempted": False,
            "postgres_started_and_stopped_on_attempt_clone": True,
            "spark_started": True,
            "attempt_root_reusable": False,
            "effects": "Six-role Delta installation metadata/reservations exist, but no Parquet data or effect journal rows were observed.",
            "cause": "The Truss report encodes revision as JSON text while CatalogRegistration exposes revision as an integer; the selector compared them without converting the integer to its public JSON representation.",
        },
    }
    OUTPUT.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"output": str(OUTPUT), "sha256": sha(OUTPUT.read_bytes()),
                      "delta_roles": sorted(roles), "sqlite_counts": counts}, sort_keys=True))


if __name__ == "__main__":
    main()

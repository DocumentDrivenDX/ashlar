#!/usr/bin/env python3
"""Read-only reconciliation of retained R30 Delta/SQLite/attempt receipts."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path


ROOT = Path("/private/tmp/ashlar-r30-truss052-page-sequence-20261101")
SUPERVISOR_ROOT = Path("/private/tmp/ashlar-r30-truss052-page-sequence-20261101-supervisor")
OUTPUT = Path(__file__).with_name("r30-readonly-reconciliation.json")


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def record(path: Path) -> dict:
    raw = path.read_bytes()
    return {"path": str(path), "bytes": len(raw), "sha256": sha(raw)}


def main() -> None:
    failure = json.loads((ROOT / "receipts/failure.json").read_bytes())
    supervisor_path = SUPERVISOR_ROOT / "supervisor-receipt.json"
    supervisor = json.loads(supervisor_path.read_bytes())
    roles = {}
    for directory in sorted((ROOT / "delta").iterdir()):
        if not directory.is_dir() or directory.name.startswith("."):
            continue
        history = []
        log = directory / "_delta_log"
        if log.exists():
            for entry in sorted(log.glob("*.json")):
                raw = entry.read_bytes()
                actions = [next(iter(json.loads(line))) for line in raw.splitlines() if line]
                history.append({"version": int(entry.stem), "sha256": sha(raw),
                                "actions": actions})
        roles[directory.name] = {
            "reservation_present": (directory / ".ashlar-local-installation-reservation.json").is_file(),
            "history": history,
            "parquet_files": sum(1 for _ in directory.rglob("*.parquet")),
        }

    database = ROOT / "delta/operations.sqlite"
    connection = sqlite3.connect(f"file:{database}?mode=ro&immutable=1", uri=True)
    try:
        names = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        tables = {name: int(connection.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0])
                  for name in names if name.startswith("local_")}
    finally:
        connection.close()

    retained = {}
    for pattern in ("receipts/*", "delta/publication-intent-*.json", "delta/source-page-*.json"):
        for path in sorted(ROOT.glob(pattern)):
            if path.is_file():
                retained[str(path.relative_to(ROOT))] = record(path)
    if failure.get("source_ack_attempted") is not False:
        raise SystemExit("R30 receipt does not prove source ACK was unattempted")
    if supervisor.get("process_group_remaining") != []:
        raise SystemExit("R30 supervisor does not prove process-group quiescence")
    cleanup = supervisor.get("postgres_cluster_cleanup", {})
    if cleanup.get("remaining") != []:
        raise SystemExit("R30 supervisor does not prove PostgreSQL cluster cleanup")

    result = {
        "profile": "ashlar-r30-readonly-reconciliation/0.1",
        "attempt_root": str(ROOT),
        "supervisor_root": str(SUPERVISOR_ROOT),
        "scope_sha256": supervisor["scope_sha256"],
        "worker_sha256": supervisor["worker_sha256"],
        "supervisor_sha256": supervisor["supervisor_sha256"],
        "failure": {"type": failure["failure_type"], "message": failure["failure"],
                    "ack_attempted": failure["source_ack_attempted"],
                    "receipt": record(ROOT / "receipts/failure.json")},
        "supervisor": {"status": supervisor["status"],
                       "wall_seconds": supervisor["wall_seconds"],
                       "peak_rss_bytes": supervisor["peak_supervised_process_rss_bytes"],
                       "process_group_remaining": supervisor["process_group_remaining"],
                       "postgres_cleanup": cleanup,
                       "effects_may_remain": supervisor["effects_may_remain"],
                       "receipt": record(supervisor_path)},
        "delta_roles": roles,
        "sqlite_counts": tables,
        "retained_attempt_files": retained,
        "conclusion": {
            "durable_graph_publication": False,
            "acknowledgement_attempted": False,
            "attempt_root_reusable": False,
            "cause": "The exact selected LocalDeltaTransport contains six installation roles; source selection incorrectly demanded that its registry contain only the four vector data roles.",
            "effects": "Installation reservations and v0/v1 Delta metadata are retained. No graph row files, operation journal rows, or source ACK were observed.",
        },
    }
    OUTPUT.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"output": str(OUTPUT), "sha256": sha(OUTPUT.read_bytes()),
                      "delta_roles": sorted(roles), "sqlite_counts": tables}, sort_keys=True))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Read-only admission check for the PostgreSQL 18 initdb directory skeleton."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

PROFILE = "ashlar-truss051-r21-pg18-layout-preflight/0.1"

# PostgreSQL 18 REL_18_STABLE src/bin/initdb/initdb.c, subdirs[].
PG18_INITDB_DIRS = (
    "global",
    "pg_wal/archive_status",
    "pg_wal/summaries",
    "pg_commit_ts",
    "pg_dynshmem",
    "pg_notify",
    "pg_serial",
    "pg_snapshots",
    "pg_subtrans",
    "pg_twophase",
    "pg_multixact",
    "pg_multixact/members",
    "pg_multixact/offsets",
    "base",
    "base/1",
    "pg_replslot",
    "pg_tblspc",
    "pg_stat",
    "pg_stat_tmp",
    "pg_xact",
    "pg_logical",
    "pg_logical/snapshots",
    "pg_logical/mappings",
)


def inspect_pg18_layout(pgdata: Path) -> dict:
    """Inspect a stopped-copy candidate without changing it or invoking PostgreSQL."""
    problems = []
    if not pgdata.is_absolute() or pgdata.resolve() != pgdata or pgdata.is_symlink():
        problems.append("PGDATA path must be canonical, absolute, and not a symlink")
    if not pgdata.is_dir():
        problems.append("PGDATA must be a directory")
    version_file = pgdata / "PG_VERSION"
    if version_file.is_symlink() or not version_file.is_file():
        problems.append("PG_VERSION must be a regular file")
        version = None
    else:
        version = version_file.read_text().strip()
        if version != "18":
            problems.append("PG_VERSION must identify PostgreSQL 18")

    missing = []
    invalid = []
    for relative in PG18_INITDB_DIRS:
        path = pgdata / relative
        if path.is_symlink():
            invalid.append(relative)
        elif not path.is_dir():
            if path.exists():
                invalid.append(relative)
            else:
                missing.append(relative)
    if invalid:
        problems.append("required initdb paths are not real directories: " + ", ".join(invalid))
    if missing:
        problems.append("required PostgreSQL 18 initdb directories are missing: " + ", ".join(missing))
    pid_present = (pgdata / "postmaster.pid").exists()
    if pid_present:
        problems.append("postmaster.pid is present; input is not an admitted stopped copy")
    return {"profile": PROFILE, "pgdata": str(pgdata), "postgres_major": version,
            "required_directory_count": len(PG18_INITDB_DIRS),
            "missing_directories": missing, "invalid_directories": invalid,
            "postmaster_pid_present": pid_present, "problems": problems,
            "admitted": not problems}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pgdata", required=True, type=Path)
    args = parser.parse_args(argv)
    result = inspect_pg18_layout(args.pgdata)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["admitted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

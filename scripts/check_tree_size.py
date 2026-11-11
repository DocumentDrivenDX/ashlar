#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fail when run output or other bulk files are committed.

Checks the tracked tree: no file over 1 MB unless allowlisted, total under the budget,
and no source module that reads the repository's docs tree at runtime.
"""
import fnmatch
import os
import re
import subprocess
import sys

MAX_FILE = 1_000_000
MAX_ALLOWED_FILE = 2_000_000
BUDGET = 45_000_000
ALLOW = (
    "examples/domain-packs/inventory.json",
    "src/ashlar_host/resources/evolution/source-closure.json",
    "tests/fixtures/golden/*.gz",
    "tests/fixtures/golden/*/*.gz",
    "tests/fixtures/golden/*/*/*.gz",
    "tests/fixtures/golden/*/*/*/*.gz",
    "tests/fixtures/golden/*/*/*/*/*.gz",
    "tests/fixtures/golden/*/*/*/*/*/*.gz",
)
SRC_FORBIDDEN = re.compile(r"parents\[\d\]\s*/\s*['\"]docs/helix")


def main():
    root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    names = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).decode().split("\0")
    total = 0
    problems = []
    for name in filter(None, names):
        path = os.path.join(root, name)
        if not os.path.isfile(path):
            continue
        size = os.path.getsize(path)
        total += size
        if size > MAX_FILE and not any(fnmatch.fnmatch(name, pattern) for pattern in ALLOW):
            problems.append(f"{name}: {size} bytes exceeds {MAX_FILE}")
        elif size > MAX_ALLOWED_FILE:
            problems.append(f"{name}: {size} bytes exceeds the allowlist ceiling {MAX_ALLOWED_FILE}")
        if name.startswith("src/") and name.endswith(".py"):
            with open(path, encoding="utf-8", errors="replace") as handle:
                for number, line in enumerate(handle, 1):
                    if SRC_FORBIDDEN.search(line):
                        problems.append(f"{name}:{number}: product code reads the repository docs tree at runtime")
                        break
    if total > BUDGET:
        problems.append(f"tracked tree is {total} bytes, over the {BUDGET} byte budget")
    for problem in problems:
        print(problem, file=sys.stderr)
    print(f"tracked tree: {total} bytes")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())

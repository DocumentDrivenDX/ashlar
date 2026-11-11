# SPDX-License-Identifier: MIT OR Apache-2.0
"""Read recorded golden inputs from tests/fixtures/golden.

Large JSON files are stored gzipped with a ``.gz`` suffix. ``read_golden`` returns the
original bytes either way, so hashes over the content keep working.
"""
import gzip
from pathlib import Path

GOLDEN = Path(__file__).resolve().parent / "fixtures" / "golden"


def golden_path(rel):
    return GOLDEN / rel


def read_golden(rel):
    path = GOLDEN / rel
    if path.exists():
        return path.read_bytes()
    packed = GOLDEN / (str(rel) + ".gz")
    if packed.exists():
        return gzip.decompress(packed.read_bytes())
    raise FileNotFoundError(f"golden fixture {rel} not found (also tried .gz)")

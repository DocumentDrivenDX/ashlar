#!/usr/bin/env python3
"""Freeze the exact existing typed-graph request using Ashlar's pure planner."""
import hashlib
import json
import sys
from pathlib import Path

_parents = Path(__file__).resolve().parents
_root = next(parent for parent in _parents if (parent / ".helix.yml").is_file())
sys.path.insert(0, str(_root / "src"))

from ashlar.typed_graph_source import build_typed_graph_request
from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration


def main() -> int:
    input_root = Path(sys.argv[1]).resolve()
    output = Path(sys.argv[2]).resolve()
    configuration = load_typed_graph_jsonl_configuration(
        input_root / "configured-typed-graph-source.json"
    )
    raw = build_typed_graph_request(configuration)
    if output.exists() or output.is_symlink():
        raise SystemExit("request output must be fresh")
    output.write_bytes(raw)
    print(json.dumps({"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env bash
# The README quickstart. CI runs this against the built wheel; users run the same commands
# with `pip install umf-ashlar`. Usage: scripts/quickstart.sh [PIP_TARGET]   (default: umf-ashlar)
set -euo pipefail
target="${1:-umf-ashlar}"
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

python3 -m venv "$work/venv"
"$work/venv/bin/python" -m pip install --quiet "$target"
ashlar="$work/venv/bin/ashlar"

"$ashlar" --help > /dev/null
"$ashlar" inspect-source --feed example --epoch one \
  < "$repo/examples/end-to-end/local-string-source.jsonl" > "$work/batches.jsonl"
test "$(wc -l < "$work/batches.jsonl")" -eq 3
"$ashlar" inspect-configured-source "$repo/examples/end-to-end/configured-source.json" \
  | grep -q '"state":"inspected-configured-source"'
echo "quickstart ok: $("$work/venv/bin/python" -c 'from importlib.metadata import version; print("umf-ashlar", version("umf-ashlar"))')"

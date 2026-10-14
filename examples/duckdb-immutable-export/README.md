# Query a saved graph with DuckDB

Use a checked Ashlar wheel and an existing immutable graph export. This workflow
reads the saved release and custody pair. It starts no Spark session, connects to
no source service and performs no ingestion or source acknowledgement.

Build the wheel from a checked repository revision, then install its DuckDB extra
into a fresh environment:

```sh
python3 -m pip wheel --no-deps --wheel-dir dist .
python3 -m venv .venv
.venv/bin/python -m pip install './dist/ashlar_graph_toolkit-0.1.0.dev0-py3-none-any.whl[duckdb]'
```

Choose the original export result and its independently retained SHA-256. The
result must still reference its original release and custody files. Supply fresh
absolute output paths whose parent directory already exists:

```sh
.venv/bin/ashlar configure-graph-export \
  --export-report /absolute/path/export.json \
  --export-report-sha256 ORIGINAL_EXPORT_REPORT_SHA256 \
  --output /absolute/path/fresh-selection.json

.venv/bin/ashlar query-duckdb-export \
  --configuration /absolute/path/fresh-selection.json \
  --output /absolute/path/fresh-duckdb-result.json \
  --maximum-hops 2
```

The configuration contains only the selected release and custody paths and their
hashes. The query checks complete canonical carrier bags, one-hop edge
occurrences, out-degrees including isolates, two-hop edge-pair occurrences and
path counts versus distinct terminal-node counts. Result order is unspecified;
compare bags and original graph identities rather than display labels.

DuckDB uses one thread, a 64 MiB memory limit and disabled external access. The
finite adapter permits at most 10,000 nodes, edges and returned rows per query;
exceeding a bound refuses rather than truncating an admitted result. Results
become available only after the owned connection and saved-export owners close.

The retained `truss-graph-5` example has two nodes and distinct source edge IDs `3`
and `4`, both self-edges on one node. Its expected two-hop edge pairs are `(3,3)`, `(3,4)`, `(4,3)` and
`(4,4)`: four paths reach one distinct terminal node. The other node is isolated,
with zero outgoing edges and zero two-hop paths. Each graph key also preserves
source and type identity; the source IDs alone are not the graph keys.

Support is finite saved STRING/null carrier topology on DuckDB 1.5.0. Opaque
property JSON is retained without numeric-property interpretation. This does not
qualify Weft SQL, current publication/source authority, native Delta reads or a
remote/cloud graph engine. A source-qualified export remains meaningful as a
saved snapshot even when the live source changes; this workflow does not assert
that it is the current head.

The [installed CLI check](../../docs/helix/04-build/evidence/duckdb-parallel-installed-cli-20261013/result.json) retains an actual DuckDB 1.5.0 run against that parallel/self-loop/isolate export, with complete original carrier parity and unchanged input hashes.

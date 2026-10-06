# Native atomic scheduled stream

Executed six scheduled 100k-object batches on the same existing shared
serverless Photon 2X-Small dbw-aidev-cus warehouse, SQL channel 2026.38, using the
cache-disabled SQL driver. New isolated catalog-managed object, serving,
property-journal and applied-receipt tables participate in each BEGIN ATOMIC.
The manifest remains a separate post-validation publication of actual table
versions. No workspace preview or warehouse settings changed.

| Batch | Processing s | Oldest / newest modeled freshness s |
| --- | --- | --- |
| 1 | 34.98 | 44.99 / 34.99 |
| 2 | 29.89 | 64.87 / 54.87 |
| 3 | 28.79 | 83.66 / 73.66 |
| 4 | 24.77 | 98.43 / 88.43 |
| 5 | 26.79 | 115.22 / 105.22 |
| 6 | 26.12 | 131.34 / 121.34 |

All six atomic applies and manifests succeeded. Changed-set cardinality,
canonical/serving raw properties and retained text, entity versions, journal
count and decoded old/new value checks ran inside the transaction; matching
post-commit validation also passed. These are scoped synthetic update checks,
not full publisher/source-feed conformance. Edges, deletion/replay conflicts,
reader load and interrupted version-vector recovery remain untested.

The client deliberately stops when newest modeled freshness exceeds 120s. This
happened after batch 6 was published; no further arrivals were submitted. The
run is recorded as stopped_backlog_bound, not a successful sustained-rate test.
The <=60s freshness gate fails and backlog grows despite atomicity. Neither the
131s final delay nor receipt presence should be hidden by resetting arrival
clocks or resubmitting successful operations.

Median stage wall time was 5.70s (5.28s engine), atomic apply 15.33s (14.94s
engine), repeated object/serving validation 2.69s and repeated journal validation
1.52s. Median manifest insertion was 0.84s. Stage timing includes synthetic
SHA-based payload generation, which a real producer would normally perform
before delivery. The duplicate validation is conservative evidence overhead,
not a chosen production requirement. These costs must be separated in a future
production-shaped input test rather than silently excluded from this run.

This intervention changes transport, commit coordination, validation placement
and work count versus prior runs. It proves a useful atomic mechanism but is
not a clean per-feature performance comparison or a throughput improvement.
The appropriate next experiments use prebuilt producer payloads, avoid redundant
post-commit validation after equivalent in-transaction checks, and measure
whether narrow serving projections can avoid rewriting canonical bags twice.
Any narrower projection needs an explicit canonical residual mapping; it cannot
silently drop properties or change the query contract.

Raw evidence: `SPIKE-001-table-layout/out/native/ashlar_stream_20261005_e3/`,
55 completed native statement metrics, all six version vectors, receipts and
validation results. Harness `native_stream_atomic.py`. Six successful source
positions do not prove a long-run p95, 100k/s burst handling, billion-node scale,
external graph-reader compatibility, or all CONTRACT-001 failure semantics.

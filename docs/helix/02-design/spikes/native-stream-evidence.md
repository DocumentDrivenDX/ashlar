# Scheduled native publication stream

Two bounded runs on existing shared dbw-aidev-cus serverless Photon 2X-Small
compute, SQL channel 2026.38, persistent SDK 0.102.0. Both start from the same
fixed 1M-object seed, schedule six disjoint 100k changed-object batches at ten
second intervals, and retain their original scheduled arrival clocks. This
models 600k changes over one minute at 10k/s. It is not a live producer or a
long-running steady-state admission test. Each batch changes roughly 2KB of
property text per object and preserves retained content.

| Batch | Serial processing s | Serial oldest/newest freshness s | Parallel processing s | Parallel oldest/newest freshness s |
| --- | --- | --- | --- | --- |
| 1 | 27.89 | 37.89 / 27.89 | 18.31 | 28.31 / 18.31 |
| 2 | 27.85 | 55.75 / 45.75 | 19.52 | 37.84 / 27.84 |
| 3 | 26.22 | 71.96 / 61.96 | 19.31 | 47.15 / 37.15 |
| 4 | 25.32 | 87.28 / 77.28 | 17.93 | 55.08 / 45.08 |
| 5 | 26.60 | 103.90 / 93.90 | 18.35 | 63.43 / 53.43 |
| 6 | 24.54 | 118.44 / 108.44 | 20.23 | 73.66 / 63.66 |

Freshness includes waiting behind earlier batches, staging CTAS, canonical
MERGE, exact old/new JSON property journal insertion, serving MERGE, table
version discovery, complete changed-set validation and manifest insertion.
Oldest/newest source arrival bounds span the ten-second modeled batch window.
The conservative publication endpoint is receipt of successful manifest response
at the client. Clocks do not restart when queued batches begin processing.

All twelve publications passed changed-row cardinality (100k), canonical/serving
property and retained equality, entity-version checks and exact decoded journal
old/new value checks. These are scoped object-update correctness results, not
full CONTRACT-001 conformance. No edge changes, deletions, conflict/replay cases,
source feed adapter, failure-safe publisher or concurrent consumers were tested.

The second intervention runs canonical, journal and serving writes concurrently
with separate persistent clients and adds explicit changed-ID ranges to MERGE
and target validation. These writes are independent because staged old values
are captured before mutation. Every manifest still follows successful completion
of all three writes and validation. This does not make the separate table writes
atomic or prove safe recovery after a partial failure. It combines two changes;
the speedup cannot be attributed independently to parallelism versus pruning.

The intervention improved throughput, but neither run sustains a ten-second
arrival interval. Approximate processing capacities are 3.8k and 5.3k changed
objects/s, respectively. Backlog keeps growing; both exceed the <=60s freshness
gate within this short input schedule. Six different-position batches do not
supply a reliable population p95. No burst-duration admission or billion-node
scalability follows, and post-input draining must not be mistaken for sustained
10k/s processing.

Raw evidence lives under `SPIKE-001-table-layout/out/native/` in
`ashlar_stream_20261005_e1` (serial) and `ashlar_stream_20261005_e2` (intervention).
Harnesses are `native_stream.py` and `native_stream_parallel.py`. The latter keeps
worker statement logs under `workers/` and a combined `all-statements.json`;
metrics must be matched by statement ID. All schemas are isolated synthetic
fixtures, no warehouse settings changed, and cost attribution remains open.

The evidence argues for testing a publication mechanism that reduces write and
validation work, and a controlled compute profile. Current documentation also
exposes a separate catalog-commit transaction candidate, absent from the tested
tables' feature lists. Its beta availability, native rollback behavior, protocol
and external reader compatibility must be tested before any design change:
[catalog commits](https://learn.microsoft.com/en-us/azure/databricks/tables/features/catalog-commits),
[transactions](https://learn.microsoft.com/en-us/azure/databricks/transactions/).
No catalog feature or workspace preview setting has been enabled by these runs.

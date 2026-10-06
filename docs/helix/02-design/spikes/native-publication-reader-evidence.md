# Full-schema publication with concurrent pinned readers

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.
Schema: `client_dev.ashlar_stream_20261005_e10`.

The full-contract 200k/20s candidate repeats 1.2M changes across 600k objects,
with overlapped staging, experimental catalog-managed canonical/journal/receipt
atomic writes and immutable manifests. Objects use the full source/type/id
schema and 16MiB target. Two independent cache-disabled driver sessions read
a captured seed snapshot while publication proceeds. Each reader attempts one
request per 0.5s (slower if its request exceeds that period), using a 30-key pool
across all changed ranges. Exact returned bags, retained text, identity and
entity version must match the captured seed snapshot, including after updates.

Both readers completed 286 successful requests, with every exact carrier check
passing. Excluding repetition zero from each reader yields 570 measurements.
All six transaction and publication checks passed; every later immutable
snapshot hydrated 200k changed rows with zero mismatches. All 645 submitted
statements succeeded. Metrics were refreshed by existing query IDs without
repeating operations.

| Batch | Processing seconds | Oldest freshness seconds |
| --- | ---: | ---: |
| 1 | 28.46 | 48.47 |
| 2 | 21.18 | 49.65 |
| 3 | 20.74 | 50.39 |
| 4 | 20.87 | 51.26 |
| 5 | 21.64 | 52.90 |
| 6 | 20.85 | 53.75 |

The full 20s accumulation window is included. Queue delay reaches 12.90s;
the two-minute schedule stays below 60s but does not admit long-run headroom.

Reader overlap classification uses caller start/end interval intersection with
the recorded atomic-apply caller interval, not an engine-internal contention
trace. These measurements describe concurrent requests, not causal attribution
to a particular internal lock or scheduler.

| Reader subset | Requests | Engine p95 | Caller p95 | Remote-I/O requests |
| --- | ---: | ---: | ---: | ---: |
| All measured reads | 570 | 202ms | 570ms | 53 |
| Atomic-write overlap | 427 | 253ms | 612ms | 51 |
| Outside atomic-write interval | 143 | 107ms | 469ms | 2 |
| Atomic overlap, no remote bytes | 376 | 151ms | 552ms | 0 |
| Atomic overlap, remote bytes | 51 | 480ms | 838ms | 51 |

All measured subsets have zero result-cache hits. Even the no-remote subset
fails the 100ms warm engine and 250ms caller gates. Remote requests are not
representative population cold p95, although their measured caller tail is
below 1s here. Compilation p95 is 286ms across overlapping reads, so file
pruning alone cannot explain the observed caller tail.

The candidate preserves old snapshot isolation under this bounded workload
but **does not meet the combined ingest/read performance objective** on the
existing warehouse. Idle warm-key passes must not substitute for this result.
Next compare ordinary Delta writes with version-manifest publication against
catalog-managed commits, retaining exact version-pinned reads and checks.
Compute separation is another possible intervention requiring an explicit
resource bound; no warehouse configuration was changed in this test.

This tests a fixed old snapshot, not latest-manifest discovery, reader policy,
replay/deletes, source transaction reconstruction or failure recovery. Degree-
skewed edges, external consumers, long-run/burst rates, cost attribution and
billion-node/5B-edge admission remain open.

Harness: `SPIKE-001-table-layout/native_stream_readers.py`. Evidence:
`out/native/ashlar_stream_20261005_e10/`, including completed stream/reader
summaries, publisher/staging/reader records, combined statements, refreshed
history and `reader-latency-summary.json`.

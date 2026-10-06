# Overlapping staging and publication evidence

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`. Compute settings were unchanged.
Isolated synthetic schema: `client_dev.ashlar_stream_20261005_e6`.

This repeats the narrow-serving experiment with one independent staging driver
session and one serial publisher. Six prebuilt 100k-object batches become ready
at ten-second intervals; the staging worker waits for each scheduled arrival
and copies its producer table. The publisher waits for that exact completed
stage before its atomic canonical/journal/serving/receipt write. Future stages
may overlap publication but do not advance the manifest or reset arrival clocks.
The stage worker has one thread and the publisher one session on the same
existing warehouse. No additional compute was created.

Exact canonical bags and retained content, promoted scalars, entity versions,
journal count and decoded old/new values are checked inside each transaction.
Exact table-version discovery and manifest publication remain timed. The same
bounded id-clustered fixture schema and 128MiB file target are used; this is not
full canonical-schema integration or a 16MiB layout benchmark.

| Batch | Processing seconds | Oldest freshness seconds | Newest freshness seconds |
| --- | ---: | ---: | ---: |
| 1 | 24.10 | 34.11 | 24.11 |
| 2 | 18.31 | 42.43 | 32.43 |
| 3 | 19.21 | 51.64 | 41.64 |
| 4 | 17.57 | 59.21 | 49.21 |
| 5 | 18.61 | 67.83 | 57.83 |
| 6 | 18.43 | 76.26 | 66.26 |

All publications succeeded. After the timed stream, every immutable canonical/
serving version pair hydrated 100k changed rows with zero exact-carrier/scalar/
version mismatches. Six singleton hydrations matched complete producer bags
and retained text. Those checks are outside the freshness clock and do not
constitute singleton latency admission. All 71 statements succeeded; their
complete execution telemetry was refreshed by existing IDs without rerunning.

Median staging wall time was 5.64s, atomic apply 15.92s, each version discovery
0.47s and manifest publication 0.88s. Later batches no longer include staging
on their critical path, but atomic apply alone exceeds the ten-second arrival
interval. The run still accumulates backlog and fails <=60s freshness. Its
76.26s final oldest freshness is lower than the prior sequential narrow run's
105.32s; these sequential shared-warehouse runs are not a paired causal estimate.

Next investigate a version-pinned scalar projection over canonical state to
avoid maintaining a second physical current-state table, while keeping the
exact canonical carrier and original property journal. The independent edge-ID
lookup versus adjacency-clustering tradeoff also needs native evidence; object
ingest improvements do not prove edge performance.

This is a synthetic one-minute schedule, not a long-run p95, live producer,
burst, reader-concurrency, replay/deletion, recovery or billion-node admission.
General-purpose cluster inventory was also checked via the workspace SDK and
returned no clusters; no cluster was started for an in-region caller test.
Caller latency from that location remains unmeasured, and larger-stage spend
and compute constraints remain pending.

Harness: `SPIKE-001-table-layout/native_stream_pipeline.py`. Evidence:
`SPIKE-001-table-layout/out/native/ashlar_stream_20261005_e6/`, including
`schedule.json`, `stream-summary.json`, publisher/staging statements,
`all-statements.json`, and refreshed `query-history.json`.

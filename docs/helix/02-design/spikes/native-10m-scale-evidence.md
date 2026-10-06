# Native 10M-node canonical layout evidence

Observed 2026-10-05 on `dbw-aidev-cus`, existing `data-gateway` warehouse
`2439e1f2e37ac563`, SQL channel 2026.38. No compute settings changed.
Schema: `client_dev.ashlar_scale_20261005_i1`.

The run used CONTRACT-003's 13-column canonical object table, liquid clustering
and statistics on `(source_system,type_id,id)`, and a 16MiB target file size.
A fixed 1M-object payload pool was reused ten times with synthetic fixture IDs.
This is not independent high-entropy payload generation across 10M nodes.
Only one source and one type were exercised; no edges were populated.

Population completed in 101.9s and full optimization in 79.0s. The resulting
table contains 419 files and 6,435,892,447 active bytes. Count and distinct ID
checks both returned 10,000,000, with IDs 1 through 10,000,000. Full stored
property/retained carrier and logical-key parity returned zero mismatches.
These preparation timings are not incremental publication freshness evidence.

The persistent SQL driver disabled result caching. Fifty measured composite-key
lookups returned the expected singleton and logical key, with zero result-cache
hits. Engine p50/p95 was 96/295ms; caller p50/p95 was 331/522ms; compilation
p50/p95 was 121/147ms. Median files read was one, maximum two; 417–418 files
were pruned. Eleven requests read remote bytes, so this is a **mixed-I/O**
sample, not a pure warm-cache test. The deterministic keys sampled IDs up to
5,236,451 rather than the entire 10M-ID range. Neither overall latency gate
passes; a repeated warm control is required to isolate cache effects.

This proves native canonical identity pruning and stored-carrier equality at
10M nodes under the stated distribution. It does not admit billion-node scale,
5B edges, sustained ingest, concurrent publication, or external graph readers.
Small liquid files remain a candidate, with file-count overhead and warm caller
latency unresolved. Larger scale remains subject to the pending spend ceiling.

Reproduction: `SPIKE-001-table-layout/native_scale_10m.py`. Evidence:
`SPIKE-001-table-layout/out/native/ashlar_scale_20261005_i1/` contains
`run.json`, `statements.jsonl`, refreshed `query-history.json`, and `summary.json`.
All 60 submitted statements completed successfully; metric refresh reused
their existing IDs and did not resubmit the workload.

## Repeated-key warm control

`native_scale_warm.py` completed three passes through the same 51 keys on the
existing table, with repetition zero excluded from each measurement group.
All returned carriers matched exactly across passes. All 150 measured queries
had zero remote bytes, 100% I/O-cache reads, and zero result-cache hits.

| Pass | Samples | Engine p95 | Caller p95 | Compile p95 | Server p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Prime | 50 | 115ms | 387ms | 141ms | 271ms |
| Repeat | 50 | 93ms | 337ms | 128ms | 228ms |
| Repeat 2 | 50 | 94ms | 331ms | 121ms | 221ms |

The two later passes meet the engine gate at 10M nodes under this bounded warm
workload, but every pass misses the 250ms caller gate. Even the I/O-cached
prime pass misses the engine gate, so I/O cache alone does not explain the
first-pass overhead. Compilation remains material. This supports continued
evaluation of the candidate rather than attributing the mixed-I/O regression
solely to table scale. It does not prove production p95 or concurrent admission.
Evidence is in `out/native/ashlar_scale_warm_20261005_i2/` (155 completed
statements, telemetry refreshed by existing IDs).

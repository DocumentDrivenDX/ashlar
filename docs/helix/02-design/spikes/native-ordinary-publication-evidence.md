# Ordinary Delta publication comparison

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.
Schema: `client_dev.ashlar_stream_20261005_e11`.

This repeats the full-schema, two-reader 200k/20s workload on ordinary Delta
tables, removing catalog-managed commits. `BEGIN NOT ATOMIC` applies individual
commits, with the same exact prior-state, canonical, retained, version, journal
count and decoded old/new checks. A version-vector manifest is published only
after successful checks. Scalar serving reads the pinned canonical version.
Per the [documented compound-statement semantics](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/control-flow/compound-stmt),
earlier statements are not rolled back after a later failure. This test does
not prove interruption handling or authorize unpinned latest-table reads.

Six 200k batches model 10k changes/s, including the full 20s accumulation window.
Three 200k-ID ranges repeat twice. Two cache-disabled driver sessions read a
fixed seed snapshot on a 30-key pool, paced at most two requests/s per session.
Source generation is prebuilt; staging overlaps publication. All exact checks
and six later snapshot hydrations pass. Both readers complete 300 requests,
all matching captured seed values. All 673 statements succeed; complete metrics
were refreshed by existing query IDs without rerunning operations.

| Batch | Processing seconds | Oldest freshness seconds |
| --- | ---: | ---: |
| 1 | 27.86 | 47.87 |
| 2 | 21.66 | 49.53 |
| 3 | 20.49 | 50.02 |
| 4 | 20.10 | 50.12 |
| 5 | 28.82 | 58.95 |
| 6 | 20.87 | 59.82 |

Queue delay reaches 18.95s. Every batch stays below 60s in this short schedule,
but the tail leaves almost no margin and does not admit sustained throughput.

| Reader subset | Requests | Engine p95 | Caller p95 | Remote requests |
| --- | ---: | ---: | ---: | ---: |
| All measured reads | 598 | 147ms | 488ms | 26 |
| Apply overlap | 452 | 168ms | 537ms | 23 |
| Apply overlap, no remote bytes | 429 | 145ms | 490ms | 0 |
| Apply overlap, remote bytes | 23 | 472ms | 730ms | 23 |

Repetition zero from each reader is excluded. Overlap means caller-interval
intersection with the non-atomic apply interval; it is not an internal scheduler
trace. All subsets have zero result-cache hits. Compilation p95 for overlapping
reads is 215ms. No-remote reads still fail the 100ms engine and 250ms caller gates.
The 23 remote requests do not establish population cold p95.

The prior catalog-managed run measured 151ms engine/552ms caller p95 in its
no-remote overlap subset and 53.75s maximum oldest freshness. These sequential
shared-warehouse tests are not a paired causal estimate. Ordinary writes do not
close the combined gate and cannot be selected solely as a latency optimization.
Preservation and manifest-based consistency remain useful, but partial-write
recovery and fenced publication require independent proof.

Next test the explicit burst scenario and edge publication before increasing
scale. Compute isolation and an in-region caller remain potential latency
interventions requiring resource bounds; current compute settings were untouched.
Long-run arrivals, latest-manifest readers, replay/deletion, external engines,
cloud-cost attribution and billion-node/5B-edge admission remain unresolved.

Harness: `SPIKE-001-table-layout/native_stream_ordinary.py`. Evidence:
`out/native/ashlar_stream_20261005_e11/`, including completed stream/reader
summaries, combined statement records, refreshed query history and reader
latency summary.

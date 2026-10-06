# Multi-key reads after scheduled fenced batches

2026-10-05; SPIKE-001 bounded native evidence on unchanged dbw-aidev-cus warehouse. Fixed canonical edge version 10 after four scheduled publications, no additional OPTIMIZE. The active canonical table contains **210 files / 4,143,248,465 bytes**, clustered by lookup_hash.

Three phases repeat 51 keys spread across ten source/relationship domains, with native IDs 101-based to avoid deliberately deleted controls. Independent Python formulas reconstruct typed endpoints, source-slot multiplicity, hash and property 201 payloads, choosing original, q2 or scheduled payload by exact ID bands. Every projected ten-column carrier matches and repeats exactly. This key sequence differs from the earlier q3 baseline; no exact matched-workload causal comparison is claimed.

| Phase | Measured n | Engine p95 ms | Caller p95 ms | Median files |
| --- | ---: | ---: | ---: | ---: |
| Prime | 50 | 98 | 447.5 | 6 |
| Repeat | 50 | 103 | 389.3 | 6 |
| Repeat2 | 50 | 95 | 378.8 | 6 |

Rep zero excluded from percentiles but retained in correctness. Result-cache hits zero; remote-read samples across all 153 calls **0**. Compilation p95 171/194/182ms is material to caller time but this does not prove an irreducible floor for another runtime/client or in-region call path. Warm engine passes two phases and misses one; caller fails all. Favorable warm reads do not admit cold/billion-scale behavior or concurrency under writes. Six-file pruning after repeated publications requires maintenance testing; no reclustering was hidden outside this read experiment.

Harness `SPIKE-001-table-layout/native_scheduled_post_reads.py`; raw own queries and exact carriers, refreshed same-ID history, terminal scope and percentile summary under `out/native/ashlar_scheduled_post_reads_20261005_r14/`. Delayed fields refreshed without reruns. All 154 recorded statements complete.

[Ingest clock attribution](native-fenced-scheduled-evidence.md) identifies 7.36–8.83s post-change checks, but atomic apply plus publication already exceeds 20s arrivals. Next controlled broadcast validation and larger integrated batch comparison, preserving checks and counting maintenance; new resources remain subject to pending bounds. Goal active; UMF deferred.

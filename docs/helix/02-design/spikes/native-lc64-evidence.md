# Wide canonical 64MiB file-target comparison

2026-10-05 workspace date, native UTC October 6; SPIKE-001 on unchanged dbw-aidev-cus warehouse. Separate candidate `client_dev.ashlar_lc64_20261005_r42.edge_current` copies all 17 canonical fields from verified LC edge version26, with identical entropy, hash clustering, native residual predicates, zstd, catalog-managed commits, row tracking and deletion-vector features. Target changes 16MiB → 64MiB; candidate is a fresh copy at version0 with different write/history lineage, not a matched-history causal experiment. Copy costs 52.161s. No full parity scan touches candidate data before singleton reads.

Actual active files are **452 → 128**, bytes **5,887,489,651 → 5,901,234,211**. Follow-up file metadata aggregation (after all reads/parity) reports physical file min/median/p95/max bytes:

| Configured target | Min | Median | p95 | Max |
|---|---:|---:|---:|---:|
| 16MiB | 5,046,409 | 11,814,271 | 19,938,519 | 21,627,426 |
| 64MiB | 30,082,932 | 46,318,196 | 55,424,041 | 70,779,781 |

Target size is not an exact physical size. Per-file metadata groups total exactly to each table's active file bytes/count.

Same 51 updated native keys across ten source/relationship domains, spanning both previous maintained batches; identical full 17-field projection and alternating layout order. Prior source sample and independently calculated new 2KB property201 tokens/feed/epoch/positions establish expected carriers. All read rounds match completely. Fifty measured observations per phase exclude its first key, nearest-rank p95:

| Phase | 16MiB engine/caller p95 ms | 64MiB engine/caller p95 ms |
|---|---:|---:|
| Prime | 114 / 390 | 666 / 921 |
| Repeat | 82 / 520 | 95 / 381 |
| Repeat2 | 74 / 367 | 86 / 357 |

Median files read are 16MiB four / 64MiB one. Zero result-cache hits. 64MiB prime includes 40/50 remote-I/O reads; remote subset caller p95 **921ms**, max remote bytes 71,036,201. This passes the bounded running-compute first-touch <=1s screen, not a controlled cache-eviction or general cold-scale admission. Baseline prime has only five remote queries, so prime is not a fair cold ranking. Each repeat has one small remote read; final repeat2 has zero remote bytes on both layouts. Warm engine passes both; all warm caller gates fail 250ms. Final compile p95 16MiB174ms / 64MiB177ms and server total p95 261ms / 262ms show no clear planning benefit from fewer files in this control. Small caller differences do not establish a winner.

After read phases, exhaustive full outer native-identity parity passes **10,019,981** rows, zero missing/differing canonical fields, and unique typed keys at the copy. Hidden row-tracking IDs are a new lineage, not asserted equal across tables. The copy has no graph publication descriptor or inherited journal yet and is not admitted for ingest, combined workload, billion-scale or external engines.

Harness `SPIKE-001-table-layout/native_lc64_reads.py`; complete first-touch/read queries/results, full parity, details/history, own telemetry and terminal scope under `out/native/ashlar_lc64_reads_20261005_r42/`. Physical file-distribution statements/results under `out/native/ashlar_lc64_file_distribution_20261005_r43/`. Summary's numeric `detail` group contains metadata commands and is not a singleton sample; only `lc16-*` / `lc64-*` phases above are used for gates.

Next bounded matched guarded ingest to test whether the smaller file population reduces validation/apply cost; include inherited history, actual vectors and maintenance if this candidate proceeds. No repeated read test is warranted without a changed workload or concern. Caller, sustained/burst, scale/resource/cost and graph-engine gates remain open. Goal active; UMF deferred.

# Narrow serving projection evidence

Observed 2026-10-05 on dbw-aidev-cus, SQL channel 2026.38, existing
`data-gateway` warehouse `2439e1f2e37ac563`. Compute settings were unchanged.
Schema: `client_dev.ashlar_stream_20261005_e5`.

This intervention replaces the full serving bag copy with six columns:
`id,source_system,type_id,group_label,counter,entity_version`. Exact property
maps and unknown retained content remain in canonical objects. The property
journal still stores decoded-checkable old/new values for changed property 103.
The fixture has one fixed source/type and non-null scalar promotions; it does
not establish generic promotion policy or edge support. Objects use the prior
bounded fixture schema and 128MiB id-clustered layout, not the full CONTRACT-003
canonical schema or the 16MiB singleton candidate.

The same six prebuilt 100k-object batches become ready at ten-second intervals.
Staging, canonical/journal/narrow-serving/receipt atomic writes, inside-transaction
cardinality/value/version validation, exact table-version discovery and manifest
publication remain timed. Source payload generation is outside the arrival clock.
No live producer, long-running arrivals, reader load, replay/deletes or recovery
claim follows from this synthetic one-minute schedule.

| Batch | Processing seconds | Oldest freshness seconds | Newest freshness seconds |
| --- | ---: | ---: | ---: |
| 1 | 26.05 | 36.06 | 26.06 |
| 2 | 25.13 | 51.19 | 41.19 |
| 3 | 24.14 | 65.33 | 55.33 |
| 4 | 22.57 | 77.90 | 67.90 |
| 5 | 22.52 | 90.43 | 80.43 |
| 6 | 24.90 | 105.32 | 95.32 |

All six publications and transaction checks succeeded. After the timed stream,
each published canonical/serving version pair was hydrated and compared with
its exact producer batch: 100k rows and zero mismatches in every snapshot.
Six singleton hydrations also returned exact complete bags and retained text.
These hydration checks are correctness evidence outside the freshness clock,
not singleton performance admission. All 70 submitted statements succeeded;
their complete execution metrics were refreshed by existing IDs.

Median staging wall time was 5.61s; atomic apply 16.15s; each version discovery
0.44s; manifest publication 0.87s. The candidate still accumulates backlog and
fails <=60s freshness at the modeled 10k entities/s. Final freshness is lower
than the full-copy control's 128.27s, but these sequential runs are not a paired
causal or production p95 estimate.

## Active storage comparison

Six read-only metadata calls captured the following active data-file bytes:

| Layout | Canonical objects | Serving | Journal |
| --- | ---: | ---: | ---: |
| Full copy, e4 | 702,159,097 | 719,508,891 | 1,260,884,346 |
| Narrow, e5 | 747,992,163 | 8,010,565 | 1,260,873,496 |

Serving shrinks approximately 90x. Both serving tables still have 13 files;
the journal has 17 files in each run. Total active bytes for these three tables
fall from approximately 2.68GB to 2.02GB. Retained old versions, producer/staging
tables, transaction metadata, receipts and manifests are excluded, so this is
not the complete storage bill. Canonical file sizes differ despite equivalent
logical inputs, and no cloud dollar cost is inferred.

Narrow projections merit further evaluation for scale/storage, with an explicit
canonical residual path. Publication throughput still requires a separate
intervention. Next compare staging overlapped with publication while retaining
arrival clocks and transaction checks; then evaluate avoiding a second physical
current-state copy using a version-pinned scalar projection. None of these
results approves a weaker freshness, preservation or billion-node gate.

Harness: `SPIKE-001-table-layout/native_stream_narrow.py`. Run evidence:
`SPIKE-001-table-layout/out/native/ashlar_stream_20261005_e5/`. Active-size
comparison: `out/native/ashlar_narrow_storage_20261005_e5/`.

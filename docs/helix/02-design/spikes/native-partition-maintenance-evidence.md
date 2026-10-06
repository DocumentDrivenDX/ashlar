# Counted maintenance and changed-key comparison

2026-10-05 workspace date, native UTC October 6; SPIKE-001, unchanged existing dbw-aidev-cus compute. One four-bucket Z-order pass precedes one incremental liquid-clustering OPTIMIZE, reversing the prior apply order. Both operate on the r32 changed-edge snapshots. No journal maintenance or new graph descriptor is published here.

Partition maintenance takes **59.145s**, canonical version **2 → 3**, removing 328 files / 5,447,600,644 bytes and adding 312 files / 5,335,823,854 bytes. All 300k marked rows are removed from deletion vectors. LC takes **16.257s**, canonical version **20 → 21**, removing eight files / 68,204,980 bytes and adding four files / 66,408,207 bytes; it removes 5,302 marked rows. End state is 408 LC files / 5,446,673,634 bytes versus 312 partition files / 5,335,823,854 bytes. The two commands have different cleanup scope: incremental LC does not perform the full-table rewrite of this Z-order pass. This is one observed maintenance choice, not a tuned continuous policy or a proof of inherently better cost.

Same 51 changed native keys across ten domains, identical full 17-field projection and alternating layout order; all returned rows match the pre-maintenance changed-key evidence exactly. Three phases, each excludes its first key (50 measured observations, nearest-rank p95):

| Phase | LC engine/caller p95 ms | Partition engine/caller p95 ms |
|---|---:|---:|
| Prime | 89 / 360 | 511 / 761 |
| Repeat | 81 / 383 | 79 / 354 |
| Repeat2 | 78 / 378 | 86 / 385 |

Median files read LC four / partition one. Zero result-cache hits; final warm phases restore the bounded engine screen on both while every caller phase fails 250ms. Prime cache conditions differ after the much larger partition rewrite; this is not a fair controlled cold ranking. Repeated reads/cache and varying shared load prevent attributing the entire before/after latency change to maintenance alone. Fewer files do not itself predict lower latency.

The cost matters: the preceding single-batch partition publication was 29.914s with only 0.086s slack under the assumed 30s source-window/60s screen. A 59.145s maintenance operation cannot be treated as free or inferred to fit that pipeline. LC publication was already above its processing budget. These separately measured operations are not an integrated freshness clock; asynchronous policy, contention and sustained arrivals still need testing.

Harness `SPIKE-001-table-layout/native_partition_maintenance_reads.py`; exact queries/results, maintenance output/history, phases, same-ID refreshed metrics and scope under `out/native/ashlar_partition_maintenance_reads_20261005_r37/`. These new maintenance versions are experimental: current graph descriptors still pin edge20 / partition edge2. Full 10M-row preservation including hidden metadata and controlled publication of equivalent new vectors remain next, before any release-level read improvement claim.

Next exhaustive maintenance parity and guarded equivalent-vector publication, then repeated-batch ingest with counted maintenance and readers. No caller, sustained/burst, billion-node, resource/cost or external-engine admission. Goal active; UMF deferred.

Follow-up `native_partition_maintenance_publish.py` completes full-fixture preservation for both layouts: 10,019,981 rows, zero missing rows or differences across all 17 canonical fields plus hidden row identity/commit version (and the partition bucket on that side), unique typed identities at both old/new snapshots. A cooperative barrier holds logical publication during validation; actual equivalent vectors are appended as `r38-lc` (edge21/journal18) and `r38-partition` (edge3/journal1), retaining node3/adjacency2/degree1/tombstone1, all prior progress and revisions. Both prior descriptors remain, and barriers end clear at sequence4. This closes the maintenance preservation/publication gap above for this fixture, without granting source/permissions/compatibility or latency admission. Exact query/result records, descriptor readbacks, progress and terminal pass under `out/native/ashlar_partition_maintenance_publish_20261005_r38/`.

Next integrate repeated 30s arrivals, maintenance before publication and fixed-vector readers. Previously measured maintenance and publication durations must not be silently excluded from freshness; the independent component timings do not substitute for that integrated clock.

Preparation for the integrated comparison now completes via `native_maintained_stage_prepare.py`, evidence under `out/native/ashlar_maintained_stage_prepare_20261005_r39/`. Two common disjoint stages each contain 300k entities across all ten native domains, from complete LC version21 carriers: native IDs 240001–270000 and 270001–300000. Counts establish unique typed identities and event ordinals, every new property201 payload is 2048 characters, and independently calculated Python SHA256 payload checks pass one selected identity in each domain per batch. Stage preparation is outside the future arrival clock; no apply, maintenance or publication is executed in this preparation. The next short schedule must include maintenance before publishing equivalent actual vectors, old pinned readers and all exact guards. Two arrivals can expose queue growth/failure but cannot establish sustained admission if they pass.

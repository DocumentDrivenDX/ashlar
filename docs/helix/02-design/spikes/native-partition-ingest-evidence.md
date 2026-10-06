# Paired guarded edge publication comparison

2026-10-05 workspace date, native UTC October 6; SPIKE-001 on unchanged existing dbw-aidev-cus warehouse. Common prebuilt 300k-entity stage r31, full 17-field prior parity and inherited journal multiset established before the clock. Liquid clustering starts edge 19/journal 17; four-bucket partition/Z-order starts edge 1/journal 0 in its separate table namespace. No concurrent readers, sustained arrival schedule or maintenance within this trial.

Each layout executes the same guarded pending-barrier transaction: exact prior fields and unique typed identities; delete/insert changed canonical rows; full property journal old/new encoding and origins; exact post-state and journal guards; durable receipt. The partitioned insert also derives the physical bucket. Actual row commit metadata resolves 300k non-null markers to one commit version per changed table. Publication then inserts the complete graph vector and clears the pending barrier atomically. The baseline includes shared unchanged node/adjacency/degree/tombstone snapshots; the partitioned vector substitutes its canonical edge and inherited journal tables. No guessed version or data replay.

| Measured component | Liquid clustering | Partition/Z-order |
|---|---:|---:|
| Atomic data caller duration | 45.003s | 24.046s |
| Resolve edge + journal | 1.046s | 1.203s |
| Publication + barrier clear | 4.395s | 4.661s |
| Total processing | 50.449s | 29.914s |
| Oldest freshness assuming a 30s source window | 80.449s | 59.914s |

The source window is an explicit comparison assumption, not measured source delivery. Partition barely satisfies that single-batch freshness screen; LC fails. One sequential observation per layout is not a percentile or sustained 10k/s admission, and running LC first plus different physical journal histories prevents causal attribution to partitioning alone. Both retain the same logical journal history, commit/row-tracking features and complete guards. Maintenance/rewrite amplification and post-write singleton latency are unmeasured here.

Published vectors: LC edge **20**, journal **18**; partition edge **2**, journal **1**; both node 3, adjacency 2, degree 1, tombstone 1. Independent pinned verification passes each layout's 300k changed canonical/journal records, 300k unique new journal origins, **9,719,981 untouched rows across all 17 canonical fields**, 10,019,981 total unique typed keys, partition bucket derivation, actual descriptor vectors and cleared barriers (sequence 2 each). Inherited-journal multiset parity was proved before apply; exhaustive post-apply inherited-history equality and full receipt/progress/revision checks remain separate. No production ordering/permissions or external-engine claim follows.

Harnesses `SPIKE-001-table-layout/native_partition_guarded_apply.py` and `native_partition_apply_verify.py`; all submitted SQL, terminal results, exact vectors, timings, progress and summaries under `out/native/ashlar_partition_guarded_apply_20261005_r32/` and `out/native/ashlar_partition_apply_verify_20261005_r33/`. Complete history is retained; no VACUUM.

Next verify inherited history and descriptor metadata, measure post-write singleton behavior, and count each layout's required maintenance before a longer repeated-batch comparison. Caller, sustained/burst freshness, full scale, resource/cost and graph-engine gates remain open. Goal active; UMF deferred.

Follow-up `native_partition_history_verify.py` now passes exact two-direction EXCEPT ALL equality of all 2,820,023 inherited journal rows after apply on each published journal snapshot. It also verifies every descriptor's profile, actual vector, full progress map, five source revisions, validation report and non-null publication timestamp, and the durable receipt's stage, count, progress and revisions. Evidence and terminal pass are under `out/native/ashlar_partition_history_verify_20261005_r34/`. This closes the bounded inherited-history/metadata verification gap above; source guarantees, production permissions, rate and compatibility admission remain open.

Post-ingest `native_partition_post_reads.py` passes all 17-field comparisons in three alternating 51-key rounds at LC edge20 and partition edge2, without maintenance. Coverage inspection finds **zero changed IDs** in this reused key set: this is an untouched-row-after-ingest control, and the harness's changed-value branch is unexercised. Changed canonical/journal correctness is established by r33, but changed-key singleton latency remains unmeasured. Evidence under `out/native/ashlar_partition_post_reads_20261005_r35/`:

| Phase | LC engine/caller p95 ms | Partition engine/caller p95 ms |
|---|---:|---:|
| Prime | 110 / 478 | 96 / 399 |
| Repeat | 108 / 441 | 120 / 437 |
| Repeat2 | 77 / 374 | 80 / 366 |

Each phase excludes its first key (50 measured observations, nearest-rank p95). Median files remain LC two / partition one. Zero result-cache hits throughout; prime and final repeat2 have zero remote bytes, while repeat has two LC / three partition queries with remote bytes. Final warm engine passes on both, all caller gates fail. Current active files/bytes: LC 412 / 5,448,470,407, partition 328 / 5,447,600,644. This small repeated-key control does not admit a combined workload or establish a causal winner. Next deliberately sample updated IDs across all ten native domains, then count maintenance and repeat the ingest schedule.

Deliberate changed-key follow-up `native_partition_changed_reads.py` completes at the same published snapshots without maintenance. All 51 unique native keys are updated IDs 210001–240000, spanning all ten source/relationship domains. Every full 17-field carrier matches the validated prior stage with independently calculated new property201 and lookup hash, expected entity/feed/epoch/position, and a non-null timestamp that stays exact across repeated reads. Evidence and complete own-query telemetry under `out/native/ashlar_partition_changed_reads_20261005_r36/`; asynchronous metrics refreshed for the same IDs without rerunning queries.

| Changed-key phase | LC engine/caller p95 ms | Partition engine/caller p95 ms |
|---|---:|---:|
| Prime | 157 / 432 | 136 / 439 |
| Repeat | 102 / 398 | 102 / 375 |
| Repeat2 | 103 / 412 | 113 / 415 |

Again 50 measured observations per phase. Median files are LC three / partition two. No result-cache hits; both repeat phases have zero remote bytes. Prime includes remote reads on 20 LC / four partition queries, so it is not a fair controlled cold ranking. Both warmed changed-key engine screens exceed 100ms and every caller screen exceeds 250ms. The earlier untouched-key pass therefore cannot admit post-ingest singleton performance. A transient approval-review capacity failure prevented the first launch; the same read-only launch passed review on retry, with no prior execution. Next counted layout maintenance and matched changed-key reads, then repeated ingest; no sustained or billion-scale claim.

# Post-ingest edge singleton reads and counted reclustering

2026-10-05 workspace date; native UTC October 6. SPIKE-001 on existing dbw-aidev-cus warehouse, unchanged settings. Current edge snapshot 16 contains 10,019,981 logical edges after structural and property batches. Three rounds of the same 51 native-domain keys precede one incremental `OPTIMIZE`; three rounds follow at version 19. Each query retains hash pruning plus exact source/relationship/native-ID predicates and returns all 17 carrier fields plus hidden row identity and commit version.

Every returned property201 token and typed endpoint matches an independent fixture calculation. All subsequent reads exactly match the complete first-round rows, including retained JSON, nullable ordering, origins, versions, publication time and hidden metadata. This is 51-key preservation evidence, not exhaustive maintenance preservation or a production recovery proof.

Nearest-rank p95 over 50 observations per phase, excluding the first observation of each phase:

| Phase | Engine p95 ms | Caller p95 ms | Median files |
|---|---:|---:|---:|
| Before prime | 107 | 439 | 6 |
| Before repeat | 107 | 440 | 6 |
| Before repeat2 | 118 | 529 | 6 |
| After prime | 511 | 823 | 2 |
| After repeat | 140 | 455 | 2 |
| After repeat2 | 98 | 444 | 2 |

All phases have zero result-cache hits. All before phases and final after repeat2 have zero remote-read bytes. After prime includes remote reads of rewritten files, and after repeat includes some remote bytes. Thus the final warm engine screen passes while all caller phases fail the 250ms target. Post-maintenance prime satisfies the bounded 1s running-compute caller screen, but this is not a controlled cache eviction or a general cold-read admission. Final compile p95 is 233ms and server total p95 349ms; reducing files alone does not close caller latency on this client/warehouse.

One submitted incremental OPTIMIZE takes **54.977s** and creates three Delta commits (17–19), all linked to its own statement ID in history. Active files increase **344 → 388**, while active bytes fall **6,121,000,083 → 5,119,063,958** and deletion-vector marked rows fall **2,800,021 → 1**. File count alone does not describe pruning quality. Retain all prior versions; no VACUUM. This maintenance is not free and is not integrated into a sustained ingest schedule. No freshness improvement or billion-node admission follows from it.

Harness `SPIKE-001-table-layout/native_edge_recluster_reads.py`; raw queries, all exact rows, own statement telemetry, maintenance output/history, scope and phase summary under `out/native/ashlar_edge_recluster_reads_20261005_r27/`. Missing asynchronous metrics were refreshed for the same IDs without rerunning queries.

Follow-up `native_verify_edge_recluster.py` passes an exhaustive full outer native-identity join over versions 16/19: 10,019,981 matched rows, zero missing rows on either side and zero differences across all 17 canonical fields plus hidden row identity/commit version. Independent counts establish 10,019,981 unique typed keys at both snapshots. Raw evidence and terminal pass under `out/native/ashlar_edge_recluster_verify_20261005_r28/`. This establishes maintenance preservation for this whole fixture, not external-reader or ingest admission.

Next compare a bounded hash-bucket partition plus Z-order candidate against liquid clustering, including ingest amplification and metadata pruning. Any bucket is a derived physical field; native identity and complete carriers remain authoritative. Caller, sustained/burst ingest, cost, full scale and external-engine gates stay open. Goal active; UMF deferred.

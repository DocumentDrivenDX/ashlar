# ID-clustered physical replacement experiment

2026-10-05; SPIKE-001 experimental write-organization result, not layout approval.

## Documented basis and scope

Microsoft's [liquid-clustering documentation](https://learn.microsoft.com/en-us/azure/databricks/tables/clustering) lists insert/append operations for clustering on write, not MERGE, and specifies a 64MB threshold for one clustering column on managed tables, versus 512MB for three. [Optimized-write documentation](https://learn.microsoft.com/en-us/azure/databricks/tables/tune-file-size) says MERGE optimized writes/compaction remain enabled; a repartition hint is not evidence that write locality will persist.

The private fixture has one source and type. `native_insert_clustering.py` changes its physical clustering from source_system,type_id,id to **id**, then replaces 200k scattered rows using **DELETE plus INSERT in one BEGIN ATOMIC**, with exact canonical carriers retained. All logical IDs and typed keys stay unchanged. Journal semantics remain a property update: physical replacement is not a source lifecycle deletion. This may expose different events through Delta CDF than MERGE; no CDF consumer equivalence is claimed. Transactional visibility depends on the already qualified catalogManaged Beta fixture. No general mixed-source/type layout or native Truss feed claim follows.

## Measured publication and preservation

Source snapshot **22**; clustering configuration becomes version 23; publication captures canonical **24**, journal **12**. Selection `2800000<=pmod((id-1)*104729,10000000)<3000000` gives 200k distinct IDs spread across all 100 range segments. One 20s accumulation models 10k/s, with producer construction excluded, and staging/transaction validation/writes/version discovery/manifest included. The one-time configuration change is setup, not per-publication service time.

Oldest freshness **54.30s**, newest **34.30s**, processing **34.29s**: one batch passes 60s but processing exceeds its 20s interarrival. Atomic wall **27.32s**, reported execution **26.98s**, aggregate read bytes **13,766,787,768**, rows scanned **31,600,713**, cache percentage **91%**. Intermediate DELETE metrics report 200k deleted rows, zero copied rows/files removed, 401 deletion vectors added and 2.389s execution. WRITE reports 200k rows and eight files / 213,751,698 bytes. Post canonical detail is **636 files / 7,854,731,381 bytes**.

All 200k changed canonical/journal rows pass exact post-publication verification, and all **9.8M untouched rows match all 13 canonical fields** against snapshot 22. Canonical typed keys remain 10M distinct; journal event keys are 200k distinct for this batch. The harness completes successfully. Raw evidence: `out/native/ashlar_insert_cluster_20261005_n8/`.

## Immediate singleton readback

`native_insert_cluster_reads.py` pins version **24**, without an extra OPTIMIZE. It uses the same 51-key domain and six-column projection as the preceding controls, verifies identities/logical keys and updated fixture ordering, and checks exact returned-carrier equality across prime/repeat/repeat2. Rep zero is excluded. All 50 measured samples in every phase have zero remote read bytes and zero result-cache hits.

| Phase | Engine p95 ms | Caller p95 ms | Median files read |
| --- | ---: | ---: | ---: |
| Prime | 97 | 413.2 | 4 |
| Repeat | 107 | 437.7 | 4 |
| Repeat2 | 91 | 391.2 | 4 |

Files read are **3–5**, read-byte p95 **74,832,172**. This batch preserves bounded pruning after the maintained baseline, rather than the earlier 65–67-file mutated layout. It does not prove clustering-on-write implementation details or that locality survives repeated batches. Engine p95 is not consistently below 100ms; caller p95 fails 250ms in all phases. Raw run: `out/native/ashlar_insert_cluster_reads_20261005_n9/`. Metrics were refreshed for the same 154 completed statements, without rerunning reads for telemetry.

## Next decision

Compare a single derived **composite physical lookup key** that covers source, type and native ID, so a one-column candidate can be tested with overlapping IDs across sources/types. Such a physical accelerator must not replace native identity: retain exact source/type/id predicates and full canonical carriers, and test derivation/integrity explicitly. Then test repeated publications with concurrent reads and maintenance accounted for. ID-only clustering on this one-domain fixture must not become the generic contract by inference.

Sustained ingest, 100k/s burst, consistently passing warm/caller reads, concurrent-reader p95, native source feed/recovery, external graph-engine protocols, attributable spend and 1B-node/5B-edge scale remain open. UMF binding remains deferred. The full goal stays active.

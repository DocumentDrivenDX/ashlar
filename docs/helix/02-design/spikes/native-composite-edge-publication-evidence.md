# Composite edge publication and lookup evidence

2026-10-05; SPIKE-001 experiment. Layout and production admission remain unapproved.

The unchanged dbw-aidev-cus data-gateway serverless PRO 2X-Small warehouse published one synthetic 200k-edge property batch across five sources and two relationship domains, after 20 seconds of modeled accumulation. Native IDs overlap across domains. The 17-column canonical hash-clustered table retains exact native tuple predicates; the hash serves pruning only. Property 201 changes; endpoints and the seven-column adjacency remain unchanged. Physical DELETE/INSERT is not an asserted source lifecycle or CDF equivalence. UMF binding remains deferred.

Oldest freshness **53.664s** passes the provisional 60s single-batch screen. Processing **33.654s exceeds the 20s arrival interval**, so this does not establish sustained 10k changes/s. Atomic apply took 25.076s wall / 24.623s engine, reporting 6.001GB aggregate reads and 31.601M rows, 87% cache.

All 200k changed rows and property journal records validate; all 9.8M untouched rows preserve every canonical field. All 10M typed edge identities remain distinct and all 200k journal origins are unique. Both paced old-vector readers preserve exact projected carriers across 213 reads. Separate metadata readback validates journal kind/revision/time and the complete manifest, including both node and edge feed progress.

The published manifest pins fully qualified node p1r1 version **3**, edge q1 **3**, adjacency q1 **0**, and edge property journal q1 **1**. This property-only change requires no structural adjacency mutation; it does not establish structural release maintenance, publisher fencing or crash recovery.

## Singleton performance

Query metrics use completed statement IDs; no timing reruns or result-cache hits. Each reader excludes its initial repetition. Overlap means caller interval overlaps atomic apply, not a causal attribution or exact internal execution interval.

| Workload | Measured reads | Engine p95 ms | Caller p95 ms |
| --- | ---: | ---: | ---: |
| All pinned reader calls | 211 | 123 | 458.4 |
| Atomic-overlap calls | 100 | 134 | 450.0 |
| Atomic overlap with zero remote bytes | 89 | 123 | 445.6 |
| Outside atomic overlap | 111 | 99 | 458.4 |
| Immediate post-write prime | 50 | 85 | 400.6 |
| Immediate post-write repeat | 50 | 75 | 368.1 |
| Immediate post-write repeat2 | 50 | 83 | 362.5 |

Concurrent lookups read one file. Post-publication lookups at fixed edge version 3, without another OPTIMIZE, read a median two files. Their independently reconstructed property payloads and typed endpoint carriers match exactly across all three 51-key phases; repetition zero remains in correctness checks but is excluded from percentiles. Cached post-write engine passes the 100ms screen; caller fails 250ms throughout, and both targets fail during atomic overlap even excluding remote reads. Active canonical files increase from 128 to 144, totaling 3,262,610,879 bytes. One publication is not evidence of steady maintenance behavior.

## Reproducibility and disposition

Harnesses: `SPIKE-001-table-layout/native_composite_edge_publication.py`, `native_composite_edge_post_reads.py`, `summarize_composite_edge_publication.py`, and `summarize_client_reads.py`. Raw evidence under `SPIKE-001-table-layout/out/native/`: `ashlar_composite_edge_publication_20261005_q2`, `ashlar_composite_edge_post_reads_20261005_q3`, and `ashlar_composite_edge_metadata_verify_20261005_q2`.

Next test structural insert/delete publication maintaining canonical edges, adjacency, degree, journal and tombstones under a pinned graph descriptor, including parallel-edge multiplicity and hub admission. Sustained/burst ingest, caller latency, full 1B-node/5B-edge scale, native source semantics/recovery and external graph engines remain open. Existing compute remains the bounded test resource; the pending resource/cost bound governs new isolation or larger stages. The goal remains active.

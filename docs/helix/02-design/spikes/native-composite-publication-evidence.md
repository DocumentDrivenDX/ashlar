# Composite-key publication with concurrent pinned readers

2026-10-05; SPIKE-001 native experiment, not layout approval.

## Workload and publication

`native_composite_publication.py` uses the mixed-domain p1r1 10M-node candidate: five sources, two types each, overlapping IDs 1..1M, and one SHA256 physical pruning column. It changes 20k scattered native IDs in every domain, **200k typed identities total**, with 20s synthetic accumulation at modeled 10k/s. Producer construction and priming of 50 changed-domain keys are outside the clock. Two persistent readers run at at most 2 queries/s each, pinning the old version and verifying the seven-column exact-carrier projection on every result. Existing dbw-aidev-cus data-gateway PRO 2X-Small compute/settings remain unchanged.

Physical DELETE/INSERT occurs in BEGIN ATOMIC, retaining property-update journal semantics. Exact source/type/id predicates remain mandatory; hash never replaces identity. Prior/post checks include physical key derivation and exact carriers. Canonical origin becomes fixture-multi/e/1; event ordinals encode the ten fixture domains plus native ID, preventing cross-domain collisions. This is a synthetic whole-entity batch profile, not a native Truss ordering/catalog claim. No Delta CDF equivalence to MERGE is assumed.

Publication completes at **57.24s oldest freshness / 37.24s newest freshness**, narrowly passing one 60s screen. Processing **37.24s** exceeds 20s interarrival, so this is not sustained capacity. Atomic wall **29.55s**, reported execution **29.09s**; aggregate reads **11,016,609,791 bytes**, rows scanned **31,601,024**, cache percentage **90%**. Captured vector: canonical **3**, journal **1**, from old canonical **2**. Post detail: **272 files / 6,997,051,796 bytes**. These observations do not isolate concurrent-reader causality or establish billed cost.

## Integrity and concurrent reads

All **218 old-vector reads** match their primed carriers. Post-publication checks prove all 200k changed canonical/journal rows, **9.8M untouched rows across all 14 fields**, 10M distinct typed canonical identities and 200k distinct journal origin keys. Separate read-only checks verify entity kind, revision, raw source-time marker and nonnull journal publication time; complete manifest readback matches its exact vector, progress, all five source revisions and validation report. These independent checks occur after freshness timing.

Reader metrics exclude rep zero per client. Overlap is intersection of caller intervals with the atomic-call interval, not reconstructed internal execution or a causal attribution.

| Reader subset | n | Engine p95 ms | Caller p95 ms | Remote-reading samples |
| --- | ---: | ---: | ---: | ---: |
| All measured reads | 216 | 143 | 469.7 | 19 |
| Atomic-call overlap | 109 | 151 | 567.0 | 19 |
| Overlap, no remote bytes | 90 | **133** | **450.0** | 0 |
| Outside overlap | 107 | 98 | 446.5 | 0 |

Every measured read touches **one file** and uses no result cache. Both warm engine/caller targets fail in the no-remote overlap subset. Good pruning alone is insufficient on the measured shared compute. The outside-overlap group includes accumulation and publication tail, not an isolated paired baseline.

Raw runs: `out/native/ashlar_composite_publication_20261005_p2/` (289 recorded statements), metadata readback under `ashlar_composite_metadata_verify_20261005_p2/`. No write was retried.

## Immediate post-write singleton control

`native_composite_post_reads.py` pins canonical **3**, with no additional OPTIMIZE, using the same 51-domain-key shape and seven-column projection as p1. Exact returned carriers repeat; native tuple, logical key and derived key match. Each measured read touches **two files**, with zero remote bytes/result-cache hits. Rep zero is excluded in each phase.

| Phase | Engine p95 ms | Caller p95 ms |
| --- | ---: | ---: |
| Prime | 101 | 387.7 |
| Repeat | 90 | 354.1 |
| Repeat2 | 83 | 341.3 |

Both repeated engine screens pass; all caller screens fail. This proves bounded pruning through one mixed-domain publication, not repeated-update/maintenance stability. Telemetry refresh uses the same 154 statement IDs. Raw run: `out/native/ashlar_composite_post_reads_20261005_p3/`.

## Next gate

Inspect already-running compute options to separate publication/read contention without changing shared settings or provisioned resources. New isolated/in-region or larger-scale resources require the outstanding resource/cost bound. Continue repeated publications and an edge-identity candidate with exact typed endpoints, independent edge IDs and narrow adjacency; do not infer those behaviors from this object-only result.

Sustained 10k/s, 100k/s burst, caller latency, warm engine latency during writes, 1B-node/5B-edge scale, native feed/replay/deletion/recovery and external graph protocol admission remain open. The candidate preserves the core meaning, but the full goal is not achieved. UMF binding remains deferred.

# Matched guarded ingest on 64MiB and 16MiB targets

2026-10-05 workspace date, native UTC October 6; SPIKE-001, unchanged dbw-aidev-cus warehouse. Common 300k-entity stage (native IDs300001–330000 across ten domains), independent 2KB property201 payloads and exact full prior parity on both layouts. LC64 inherits the complete 3,720,023-row journal multiset; two-direction EXCEPT ALL returns zero differences. Setup: journal copy23.637s, stage17.863s, outside the publication clock. One sequential guarded batch per layout, LC64 first; no readers or maintenance during timed apply.

Identical pending-barrier transaction checks full prior carriers, replaces changed canonical rows, writes exact property history, validates full post carriers and unique origins, and records a durable receipt. Actual row commit metadata resolves each changed table; complete graph vector publication clears the barrier atomically. Canonical schema remains17 fields, with hash pruning plus native tuple authority. No source lifecycle/CDF equivalence or UMF binding is inferred from physical replacement.

| Component | LC64 | LC16 |
|---|---:|---:|
| Atomic data caller duration | 33.977s | 46.862s |
| Resolve edge + journal | 1.105s | 1.131s |
| Publication/barrier clear | 4.767s | 4.722s |
| Total processing | 39.853s | 52.719s |
| Oldest freshness assuming a 30s source window | 69.853s | 82.719s |

Both miss60s even without maintenance/readers. Processing exceeds the30s interval; no sustained-rate admission follows. LC64 is faster in this one observation, but fresh-copy lineage/journal layout and sequential order prevent attributing the difference solely to file target. Setup is not measured source delivery; the30s window is explicit synthetic comparison input.

Published LC64 edge1/journal1 and LC16 edge27/journal21, each with unchanged node3/adjacency2/degree1/tombstone1 and full prior progress/revisions plus fixture-file-target position1. Independent pinned verifier passes300k changed/journal records per layout,9,719,981 untouched rows across all17 fields per layout,10,019,981 unique total typed identities,300k unique new origins,actual descriptor vectors and cleared barriers atsequence2. Complete inherited-history equality after apply and descriptor/receipt metadata readbacks remain separate follow-up; pre-copy history and in-transaction guard passes do not substitute for those checks.

Harnesses `native_lc64_ingest_prepare.py`, `native_lc64_guarded_apply.py`, `native_lc64_apply_verify.py` under `SPIKE-001-table-layout/`. Raw evidence/terminal summaries under `out/native/ashlar_lc64_ingest_prepare_20261005_r44/`, `ashlar_lc64_guarded_apply_20261005_r45/`, `ashlar_lc64_apply_verify_20261005_r46/`. No shared compute change, vacuum or blind replay.

Next finish inherited-history/metadata follow-up, then test an exact conditional MERGE guard rather than repeating the failed full prior-scan plus replacement path. Candidate proof obligation: only update rows whose complete prior carrier matches; reject an already-present new batch marker before apply; require every staged typed identity and exact new marker/carrier in post checks; preserve journal old values and unique origins; missing/stale/duplicate cases must roll back. This is an untested intervention requiring native negative-case evidence, not permission to omit validation. It may fuse prior validation into the DML predicate; no speedup or admission is assumed. Caller, sustained/burst, scale/cost and external-engine gates stay open. Goal active.

Follow-up `native_lc64_history_verify.py` now passes exact post-apply inherited3,720,023-row journal multisets and complete descriptor/receipt profile, vectors, progress, revisions, reports, timestamp, stage and count on both layouts. Evidence under `out/native/ashlar_lc64_history_verify_20261006_r47/`. This closes the bounded history/metadata gap above, without changing the freshness failures. [Conditional MERGE controls](native-conditional-merge-evidence.md) subsequently pass one clean and16 actual rollback cases; full-batch performance and broader marker/epoch semantics remain untested.

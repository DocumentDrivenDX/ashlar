# Large retained-fixture read comparison r415–r417

Owner requested a large local test followed by Databricks. CONTRACT-001–003 and proposed ADR-001 govern this read-only comparison under the selected Unity Catalog Delta architecture. UMF binding stays deferred. These runs reuse already fully qualified synthetic fixtures and immutable versions; they do not repeat ingest, bootstrap or full multiset parity. Full-field fingerprints are diagnostic aggregates, not collision-free equivalence proofs. Complete prior preservation evidence remains required.

| Measurement | Local machine | Existing Databricks warehouse |
| --- | --- | --- |
| Current carriers scanned | 4M nodes /20M edges | 8M nodes /39.97M live edges |
| Runtime | Spark3.5.3 /Delta3.2.1 /Java21,8workers,32GiB driver | Existing2XSmall warehouse2439e1f2e37ac563; Photon |
| Immutable pins | Both0 | N6 /E6; overlay comparison E5+O1 |
| Exact sampled reads | 64 generated full carriers | 288 full20field carriers/absence,48same stratified identities,2passes,3families |
| Elapsed | 60.741s | 257.404s |

Local active data is40,770,243,854bytes in2048files. Both full scans compute an aggregate over every field and check counts, identity hashes and valid property JSON across every carrier. Every check passes. The local source had complete multiset, endpoint and storage-profile validation in r86. All64new point results independently match the deterministic generator. First Spark startup failed at the sandbox Java gateway socket before table access; the authorized retry succeeds. Both startup receipt and successful run are retained.

Native full scans likewise compute fingerprints over all current fields and validate every tuple hash/property JSON and exact expected counts. Every check passes. All302native statements are final/successful, with all costs included:83,998,490,499reported read bytes,0remote write bytes,0spill bytes, within200GB/900s bounds. These counters are not a price or a retained-disk inventory. No table or publication pointer changed.

| Repeat-pass p95 (48 native queries per family) | Caller | Engine | Compile |
| --- | ---: | ---: | ---: |
| Direct E6 | 385.95ms | 92ms | 168ms |
| Override/fallback E5+O1 | 702.12ms | 228ms | 372ms |
| Qualified max_by E5+O1 | 718.09ms | 216ms | 395ms |

All repeat-pass native queries report zero remote reads; all point results are uncached. This follows full scans and earlier fixture use, so it is not controlled cold-data evidence. Across both passes, direct caller/engine p95 is535.51ms/234ms; override1005.95ms/455ms; max_by922.60ms/411ms. The first-pass cohorts include12/13/19remote queries respectively. Local repeat-pass caller p95 is87.56ms nodes/109.39ms edges (16queries each). Local in-process calls and native SQL RPC timings are different boundaries; no causal platform speed comparison follows.

The native audit independently regenerates all288expected complete carriers/deletion outcomes, binds source and runner hashes, verifies exact native query text and terminal states, checks full-scan outputs, plans, percentile counts and cumulative costs. All six plans are fully Photon supported. Direct/override have no shuffle; max_by has one shuffle/two aggregates. Fewer operators alone did not remove overlay latency. Both overlay queries require the existing independently proven dominance/conflict-free input qualification; they are not general-purpose out-of-order readers.

## Table-design disposition

Retain the current direct unpartitioned hash-liquid-clustered canonical Delta read baseline, with hash and complete Truss identity predicates. The measured native repeat engine result meets the provisional100ms screen on this cohort; caller250ms remains missed. This tunes the selected Delta architecture rather than gating it. These serial synthetic results do not establish cold service latency, concurrency or actual1B-node/5B-edge support.

The overlay remains an ingest experiment, not an admitted replacement for canonical current tables. Earlier accepted100k overlay append took8.023s, while the earlier third full six-role publication took257.486s; these clocks have different scopes and do not establish a freshness win. Preserve raw records, permanent property journal, tombstones, typed endpoints, original carriers and explicit projection versions. No Fabric dependency is introduced into singleton reads; PuppyGraph/GraphFrames/Fabric interoperability limits are unchanged.

Next execute the bounded16MiB overlay file-shaping plan r414 and preserve complete100kmarkers/old pins before paired reads. Then decide whether overlay publication and compaction can provide a useful ingest/read balance. Real source authority, worker fencing, source ACK, sustained ingest and billion-scale capacity remain open; do not spend another iteration merely regenerating identical fixtures.

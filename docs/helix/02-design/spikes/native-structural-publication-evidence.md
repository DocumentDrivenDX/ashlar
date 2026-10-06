# Structural edge publication evidence

2026-10-05; SPIKE-001 bounded synthetic experiment on unchanged dbw-aidev-cus data-gateway serverless PRO 2X-Small.

One BEGIN ATOMIC applies 20 edge deletions and 20,001 hub insertions to canonical edges, narrow adjacency, affected degree groups, lifecycle journal, tombstones and receipt. Adjacency's catalog-managed feature was enabled only on this private fixture. Atomic caller duration **15.048s**. The original descriptor followed complete checks at **55.197s** from apply start, with no modeled accumulation interval; neither figure proves the 10k/s sustained or 100k/s burst target.

Published table snapshots: canonical edges **4**, adjacency **2**, degree **1**, journal **2**, tombstones **1**, fixed nodes **3**. Full canonical/adjacency parity covers **10,019,981** distinct typed edge identities. Full degree comparison against a regrouping of the complete adjacency reports zero mismatches. All 20,001 inserted carriers match all 17 staged fields, and all 9,999,980 surviving prior carriers retain all 17 fields. Deleted identities are absent. Both full typed-endpoint anti-joins report zero unresolved endpoints. All 20,021 lifecycle event origins are unique; full old/new journal carriers compare exactly against the stages, preserving null order fields in JSON.

Maintained hub work is **120,003**, matching independently projected arithmetic and exceeding the 100k logical budget. This run calculates work; it does not yet execute refusal/path-fetch controls or establish a physical scan cap. Parallel counterpart retention, ordinary paths, pinned old-reader behavior during structural writes and failure recovery require further explicit controls.

## Descriptor correction

The running harness emitted r3-1 with an empty schema revision map. This is an incomplete descriptor and must not be treated as an accepted release. The harness source is corrected for future runs, while original statement evidence remains unchanged. `verify_structural_descriptor.py` verifies lifecycle presence/version/kind/revision/source-time flags and every tombstone, then appends r3-2 with the five source revisions and the same fixed table vector and three fixture feed progress entries. It reads back the complete accepted vector, progress and revision map. The separate terminal summary passes with accepted publication **r3-2**; no mutation was blindly retried.

Harnesses `native_structural_publish.py` and `verify_structural_descriptor.py`; raw statement/results under `SPIKE-001-table-layout/out/native/ashlar_structural_publish_20261005_r3/` and `ashlar_structural_descriptor_20261005_r4/`. Synthetic whole-entity lifecycle journal uses null property ID and opaque full-row JSON; this is not an implemented native Truss feed adapter. The native property-level journal remains separate and retained.

Next verify actual hub refusal, ordinary/parallel path effects and pinned snapshot stability; then benchmark structural publication reads and fault recovery. Caller latency, sustained/burst ingest, maintenance, 1B-node/5B-edge scale and external graph execution remain unproven. UMF binding remains deferred; goal active.

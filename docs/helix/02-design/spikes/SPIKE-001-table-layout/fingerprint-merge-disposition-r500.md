# Actual MERGE fingerprint disposition — r496–r500

Do not add the enforced carrier fingerprint to the default canonical layout or backfill the40M canonical table on this evidence. The r495 read-only projection improvement did not transfer to MERGE I/O with full preserved-carrier writes/CDF:

| Single actual MERGE | Fingerprint guard | Full20-field guard |
| --- | ---: | ---: |
| Reported read bytes | 2,354,369,643 | 2,354,420,737 |
| Reported remote read bytes | 27,917 | 79,011 |
| Engine time | 6,533ms | 8,023ms |
| SDK caller time | 7,239.642ms | 8,773.485ms |
| Reported write bytes | 14,419,251 | 14,419,345 |
| Spill | 0 | 0 |

Read difference is0.00217%, effectively equal at this scope. One fingerprint-first/full-second pair is not causal latency evidence. Both mostly hit the storage cache. Native metadata time is5,028ms/6,069ms and Photon task totals7,779ms/7,380ms, respectively; these overlapping nested metrics are not added to execution time. The read-only plan's reduced projection is insufficient to predict MERGE phases. Retain the candidate for separately justified standalone validation; do not claim lower ingest I/O or extrapolate the engine-time pair to full publication.

The accepted private fingerprint MERGE applied5,609 updates and618 deletions to the2,498,646-row r493 fixture. Before applying, a changed property token spelling and a missing predecessor each failed atomically without advancing version1. CDF enablement was its own version2 commit. The single accepted MERGE produced version3;11,836 native change rows exactly matched all20 before/after/delete fields against the immutable prepared source, including exact property/retained strings, source cursors, nulls, endpoints and versions. Every CDF fingerprint matched recomputation, with correct commit version. The final2,498,028 rows were unique by full typed key and every stored fingerprint matched all20 fields. No source ACK, production pointer or canonical role changed.

The comparator used a **new** private UC shallow clone of the exact r493 version2 predecessor, retaining the32 source files without copying any data files. Its actual UUID ise2585c5f-9dde-40d8-af9f-c2ce07e82944. Clone0 had complete21-field parity with the predecessor, including the fingerprint. Its original full-field guard applied exactly the same immutable6,227 source rows at clone1. Full21-field result parity with r493 version3 and full21-field/type CDF parity for all11,836 changes passed. Hidden Delta row tracking IDs are not claimed equivalent across clone lineages. The clone has its own history/UUID and does not inherit the source version numbers or old CDF history. [Databricks clone documentation](https://learn.microsoft.com/en-us/azure/databricks/tables/operations/clone) explains that shallow clones reference source data and copy schema/properties/invariants without copying table history.

Two tiny semantic controls on the independently enforced one-row r491 fixture isolate rules beyond the digest: an exact matching predecessor with a nonadvancing after version, and an exact matching predecessor with a valid successor version but changed after identity. Both failed with the native fingerprint guard error and left head4 unchanged. Its before/after scalar reads used the result cache; data atomicity is supported by terminal failed writes and uncached unchanged-head observations, not by describing those scalar reads as an uncached reread. No timing conclusion uses them.

All37 native handles across r497/r498/r499 have final metrics, exact native SQL binding, frozen code/source hashes and recorded expected failed statuses. Total reported reads15,678,642,738B, writes28,838,596B, spill0. Elapsed83.737s/101.892s/17.417s stayed within their separate bounds. All preparation, comparison and refusal costs are included. See [audit](out/fingerprint-merge-audit-r500.json).

Physical private heads are now r493 version3, full_guard_clone_r498 version1 and r491 version4. Older evidence remains explicitly pinned to its immutable versions; it is not proof of a current head1. The proposed canonical ashlar-delta/0.3 remains unchanged, preserving the close Truss property-map/current/journal/raw/tombstone model and avoiding an unproved extra generatedColumns consumer dependency. Reader/writer3/7 and external-engine interoperability remain separately scoped.

The next material optimization must act on the actual publisher critical path or physical maintenance rather than assume read-only projection savings transfer to MERGE. Use existing immutable publication inputs/role versions to quantify concurrent full-content validation costs before generating another batch; metadata-only parallelism already lowered preflight but did not lower the233.213s ready clock. A stronger layout/capacity intervention still needs evidence because the69.027s canonical current MERGE alone exceeds the provisional60s publication target. Shared warehouse resizing remains pending explicit human approval. Warm/cold singleton,10k/s sustained,100k/s burst, real fencing and1B-node/5B-edge admission remain open; the UC Delta architectural commitment and deferred UMF binding remain intact.

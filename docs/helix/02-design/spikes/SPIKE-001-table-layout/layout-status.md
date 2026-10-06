# Physical layout candidate and evidence status — 2026-10-06

This is a spike evidence index governed by CONTRACT-003 and proposed ADR-001, not approval or production support. [Package manifest](layout-package-candidate.json) hashes the design files and seven scoped native summaries.

The proposed 0.3 schema now adds source_record and direct source cursor/origin references; complete 0.3 native DDL execution passes; synthetic reference validation now passes; real producer and recovery remain pending. Existing 0.2 evidence remains scoped to its original schema. The owner selected Unity Catalog Delta. Current-state tables use native typed identities and exact property/retained JSON text; derived identity hashes provide lookup pruning without replacing full identity checks. Liquid clustering with a 64MiB target is the initial tuning candidate. Optional forward/reverse adjacency and degree tables support structural queries without duplicating full bags. Every consumed table is bound to an actual-version publication vector. Typed engine releases are separate rebuildable projections. UMF bindings remain deferred.

| Requirement | Evidence | Remaining limit |
| --- | --- | --- |
| Executable table shape | r65: 12 native 0.2 tables; r73: 13 native 0.3 tables | Draft profile, publisher-enforced semantic checks |
| Pinned publication/read | r66: tiny 0.2 graph; r74: 0.3 reference validation and vector including raw source | No full producer/recovery protocol |
| Typed graph export | r67/r68: exact native projection parity | No external-engine execution or activation |
| Property history representation | r69: exact missing/null/value tokens | Native producer mapping and reconstruction pending |
| Complete uninterpreted feed content | r71: five record kinds and exact bytes | Supplemental carrier outside 0.2; real transport pending |
| Retry and conflict refusal | r72: atomic single-table MERGE | Concurrent uniqueness/fencing unproved |
| Native singleton query path | r75: generated pinned SQL with bound exact identity parameters | Four-row correctness screen; no new latency/scale claim |
| Scale planning | capacity-planning-v02.json | Assumptions, not measured 1B/5B admission |

## Operational measurements

Older large experiments use their recorded experimental schemas, not the tiny 0.2 fixture. They inform physical tuning but do not establish end-to-end 0.2 publisher performance.

| Measurement | Observed result | Qualification |
| --- | --- | --- |
| r58 exact-key final warm singleton | engine p95 82ms; caller p95 368.043ms; median 2 files; 0 result-cache hits | 50 timed reads in final phase; bounded 10M-edge fixture, maintained version 5 |
| r56 post-maintenance first-touch phase | caller p95 777.471ms; engine p95 511ms | 50 reads; remote-I/O qualification is in underlying evidence, not a general cold-start SLA |
| r52 conditional 300k apply/publication | processing 31.968s; oldest freshness 61.968s under assumed 30s source window | Stage prebuilt; one batch, no readers; not sustained 10k/s |
| r56 counted maintenance | 33.993s | Separate from r52 clock; old published vector stays pinned; no concurrent-rate claim |

Sources: [r58 summary](out/native/ashlar_hash_candidate_reads_20261006_r58/summary.json), [r56 summary](out/native/ashlar_conditional_maintenance_20261006_r56/summary.json), [r52 summary](out/native/ashlar_conditional_apply_20261006_r52/summary.json), and the maintenance details in [conditional evidence](../native-conditional-merge-evidence.md). Warm caller latency and freshness do not satisfy the historical provisional targets. The owner treats these as measurements for tuning, not an architectural veto. Sustained ingest, burst recovery, concurrency and billion-node performance remain unproved. No larger experiment or new compute is implied by this package.

## Remaining implementation boundaries

1. Version the real source adapter: tuple cursor, safe watermark, revision ordering, delivery-ID derivation, exact transport encoding and snapshot handshake. Truss's draft contracts at `6d87fce` inform this; they do not prove implementation.
2. Connect durable raw-source capture, current/property-history validation, actual version resolution and immutable publication under a qualified serialized/fenced recovery protocol. Retain old published versions through partial work and recovery; acknowledge source only after durable publication.
3. Qualify engine runtimes against the same pinned release corpus. GraphFrames mapping code is syntax-checked only; PuppyGraph connection/Delta feature compatibility and Fabric OneLake refresh remain unexecuted.
4. Decide history retention/consumer registration and projection coverage before scale growth. Capacity estimates show these may dominate current-state storage. Require explicit resource/cost bounds for any larger native run.


## Runtime availability check

[Read-only runtime inventory](out/runtime-availability-20261006.json) on 2026-10-06 finds no Databricks Spark cluster or local graph-engine container. The existing local PostgreSQL container belongs to another project and was not inspected or changed. No executable Truss producer was found in the scoped worktree inventory. Fabric tenant/capacity availability remains uninspected. No downloads, compute provisioning or scale expansion were started. Native SQL primitives remain available; real producer/engine and full-scale qualification require separately available runtimes and bounded resource plans.


A [prepared scale-experiment plan](scale-experiment-plan.md) now specifies bounded sensitivity scope, evidence, deadline/cancellation recovery and resource admission. Owner selected small tests and continued design work; larger-scale expansion is deferred. No larger write, download or compute provisioning has started; an observation timeout cannot be used to restart an unknown write.


The existing maintained edge baseline is revalidated at version 5: 136 files and 6.18 GB, with a stable version before/after metadata reads. [r77](out/native/ashlar_scale_baseline_inventory_20261006_r77/summary.json) is scope-limited metadata evidence; compression and 0.3-width/whole-graph extrapolation remain unqualified. Owner selected bounded testing only; larger-scale expansion remains deferred.


Recovery design now specifies interruption outcomes and identifies missing durable receipt bindings in the 0.3 DDL. See CONTRACT-003, “Publication recovery requirements for the 0.3 layout.” The next small implementation spike should exercise retained-stage recovery and lost acknowledgement under one serialized publisher; concurrent takeover remains unqualified. No schema mutation or native compute was used for this design iteration.


[r78](out/native/ashlar_serialized_recovery_20261006_r78/summary.json) now executes the serialized recovery slice on three native object rows with an isolated supplemental binding. Full-row preservation, fresh-client recovery, identical publication retry and absence of data reapply pass. The next recovery slice is partial multi-table output, preserving the prior graph publication through repair; stale-owner prevention and real-source proof remain separate qualifications.


ADR-001 now separates six durable graph/feed roles, three optional structural projections, two typed examples and two coordination examples. CONTRACT-003 specifies invalidation and validated reuse of unchanged projection versions. This is design guidance for reducing needless per-batch writes; no throughput improvement has been measured. Tuple-feed journal clustering remains a recorded tuning gap.


[r79 partial-output recovery](out/native/ashlar_partial_recovery_20261006_r79_resume/summary.json) now passes edge/forward-adjacency repair and old-publication preservation on three edges. Canonical output is not reapplied. A terminal stage parser failure and the inventory/version-checked resume are retained. Next connect these recovery primitives to a qualified complete source boundary and the full consumed table vector; reverse/degree/history/source updates and writer authority remain unqualified.

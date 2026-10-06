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


Local [read-plan locator checks](out/publication-locator-check.json) now verify complete requested table inventory, every version entry, defensive snapshot copying and exact SQL pin retention. Thirteen malformed cases are refused. Query-builder regression checks and browser-target bundling pass; native SQL and browser execution were not repeated. Descriptor authority, decoding duplicate members and complete plan dependency discovery remain caller responsibilities.


[Capacity sensitivity 0.3](out/capacity-planning-v03.json) adds per-table file rounding, 16/64MiB targets, half/full mean fill, 0/1/2 adjacency copies, raw retention and property-event fanout. It also records uniform scattered-update occupancy independently of actual Delta rewrite cost. All values are arithmetic assumptions; larger compute remains deferred and no scale admission is inferred.


[Exact source cursor checks](out/source-cursor-check.json) pass six ordering pairs and sixteen refusals, including values above 2^53, unsigned-range xid text, strict watermark equality and namespace mismatch. These are local synthetic range checks, not real Truss checkpoint or transaction-completeness evidence. No native schema or compute was changed.


[r80 bounded payload observation](out/native/ashlar_payload_sample_20261006_r80/summary.json) finds distinct 2,058-byte single-property bags but uniform 28-byte retained content in 256 version-pinned rows. Byte variety and schema variety are now separate requirements in the prepared workload plan. The convenience sample and local gzip ratios do not establish native compression, full-fixture entropy or billion-scale capacity. One read, no writes/new compute.


[Varied local corpus](out/varied-corpus/summary.json) now provides 64 nodes/192 edges, eight declared property shapes and 256 complete synthetic raw envelopes. Canonical columns match 0.3 DDL and exact raw-to-carrier parity/typed closure checks pass locally. Native ingestion, producer transaction history and engine execution remain untested; no compute was used.


[r81](out/native/ashlar_varied_fidelity_20261006_r81/summary.json) strengthens 0.3 storage fidelity with exhaustive 64-object/192-edge/256-raw-record parity across eight property shapes using bound transport. Native typed identity/hash/reference/endpoint checks pass. Three private tables at version 1; no graph publication, property history, producer or performance/scale admission claim.


The proposed [durable receipt table](sql/publication-receipt-candidate.sql) now has an explicit intent/complete envelope contract in CONTRACT-003. It binds retained input, source boundaries, predecessor, authority context and validated actual output versions. This is a separate unexecuted receipt profile; 0.3 DDL and prior recovery fixture evidence retain their original scope.


[r82 receipt carrier](out/native/ashlar_receipt_20261006_r82/summary.json) passes native DDL, exact two-record replay and atomic byte-conflict refusal. This extends receipt storage evidence, not complete intent/phase/authority validation or a production publisher. Two target and two stage records only.


[r83 release protocol inventory](out/native/ashlar_release_protocol_20261006_r83/summary.json) finds reader3/writer7 with deletionVectors/v2Checkpoint on every unchanged r68 release. CONTRACT-003 now requires actual reader-feature/access/publication qualification; simple scalar release shape alone does not establish engine support. Twelve metadata reads, no writes or feature changes.


[r84 representative encoding](out/native/ashlar_hash_encoding_20261006_r84/summary.json) proves native SQL/Python JSON text and hash parity on 64 ASCII/int64 edge cases. It records the DEL escaping pitfall for Python defaults; no scans, writes or performance benchmark was run.


[Full-scope completion audit](out/goal-completion-audit-20261006.json) reviews 22 requirement groups against pinned evidence. Completion remains unproved: native layout/carrier primitives have scoped evidence, while real-source recovery/authority, actual engines, combined sustained/burst performance and billion-scale capacity remain open. Owner keeps UC Delta fixed and larger runs deferred. Next prioritize existing source/runtime integration; an optional endpoint/configuration question is pending. Additional isolated fixtures cannot close those qualifications.


Merged Truss main at d3dcdde has been inspected and hash-pinned. Its draft layout specifies shared ID allocation and unique relationship/source/target edges; generic repeated-ID/parallel fixtures are now explicitly distinguished from legal native producer state. Current merged contracts do not supply complete-feed worker implementation or engine/runtime access. No compute was used.


[Local GraphFrames execution](out/graphframes-local-20261006.json) passes Spark 3.5.3,
Delta Lake 3.2.1 and GraphFrames 0.12.3 on six checksum-pinned exported rows,
rematerialized as four local Delta tables. The existing v0.2 adapter preserves
three vertices, three edges, two parallel edges, one self-loop, one isolate,
three two-hop paths and exact JSON carriers. An unpublished append is excluded
by VERSION AS OF 0. This closes the local runtime/mapping question; it does not
qualify direct UC access, native deletion-vector/v2-checkpoint interoperability,
Truss producer legality, performance or billion-scale capacity. No remote
compute was started. The optional runtime endpoint question is no longer a
blocker for this integration.

Reproduce with Python 3.11, Java 17 and an isolated environment containing
pyspark==3.5.3, delta-spark==3.2.1, graphframes-py==0.12.3,
typing-extensions==4.16.0 and numpy==2.4.6. Set JAVA_HOME to Java 17,
PYSPARK_PYTHON to that environment's Python and SPARK_LOCAL_IP=127.0.0.1;
run `python graphframes_local.py`. Maven dependencies are resolved on first run.
Tables and JVM dependency cache are created in a fresh temporary directory.
The evidence file is regenerated. Startup and tiny-graph execution are not
singleton latency measurements.

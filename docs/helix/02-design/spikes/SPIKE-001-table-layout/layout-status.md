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


[Local 24M-carrier scale run](out/local-scale-20261006/summary.json) completes
4M objects and 20M edges with full-field multiset parity, then 200k scattered
edge updates with exact update parity and unchanged old-version state. Initial
writes take 9.58s/24.81s and use 256 files per table: 391,857,089 object bytes
and 2,058,134,273 edge bytes. The MERGE takes 18.37s and rewrites all 20M edges,
copying 19.8M unchanged rows and replacing 256 files with 86. This OSS Delta
3.2.1 run uses hash-range-sorted Parquet Zstd, no deletion vectors/native liquid
clustering, so rewrite amplification is not a Databricks conclusion. Serial
50-read local caller p95 is 304.14ms; four-reader p95 is 782.21ms, after hash
preparation and with warm OS data. There is no cold or engine-only claim.

The same 4M/20M/200k generator is now submitted on existing Databricks compute
with native liquid clustering. The owner explicitly authorized this larger
local-then-native comparison. Payloads have semantic variety but repeated digest
blocks compress heavily: about 98 object/103 edge stored bytes per carrier.
Do not extrapolate those widths into realistic consumer or billion-scale cost.
History/raw publication and structural projection amplification remain separate
requirements. Generic parallel pairs are not a legal Truss producer-profile claim.


The owner-authorized [large local/native comparison](scale-comparison.md) is
complete at 4M nodes/20M edges. Native DVs avoid copying unchanged rows; a measured
FULL maintenance invocation is a no-op. A synthetic non-ID property wrapper was
found and corrected, with exact 200k-row parity and full numeric-ID-map validation
on both systems. The corrected native version-2 serial/four-reader p95 is
218/247ms engine and 525/570ms caller, with final uncached metrics for all reads.
Earlier cached REST controls and invalid update semantics are explicitly scoped
out of performance/profile claims. Full publication and billion-scale admission
remain unproved; the goal stays active.


[Higher-entropy iteration](entropy-scale-status.md) passes 40.77GB local current
carriers at 4M/20M, including exact full-field and synthetic Truss storage-profile
checks. GraphFrames 0.12.3 executes the existing version-pinned adapter at that
scale and counts 100M two-hop paths. Native nodes pass; native edge full validation
is still active. Paired native bindings retain exact int64 values but do not close
latency budgets. Explicit same-handle cancellation now bounds long REST phases;
lagged shared billing quantities are recorded without invented experiment cost.


Higher-entropy native phase is now terminal and [audited](out/native/ashlar_entropy_20261006_r86/audited-summary.json):
40.71GB / 576 files, all 24M current carriers exact, both version0, typed endpoint
closure and known Truss allocation/pair rules checked. Writes take 91/653s and
complete field comparisons 91/729s on unchanged serverless 2X-Small compute.
No cancellation, throughput/publication, singleton or billion-scale admission
follows. Local GraphFrames 24M integration passes separately; UC feature/read
compatibility remains separate.

Native higher-entropy singleton evidence: [r88](out/native/ashlar_entropy_reads_20261006_r88/audited-summary.json), 150 uncached full-carrier reads against the 20M-edge / 33.95 GB baseline. One file per read; four-client engine/caller p95 156/455 ms. See [scope and cohort qualifications](entropy-scale-status.md).

Large incremental publication: [r89](out/native/ashlar_entropy_publication_20261006_r89/audited-summary.json) validates a 200k scattered-property slice, full 20M-row canonical parity, changed raw origins and exact journal. Its synthetic manifest explicitly leaves baseline origins unqualified. [r90](out/native/ashlar_entropy_post_reads_20261006_r90/audited-summary.json) shows post-update four-client p95 248/522 ms and 17 files/read versus baseline 1. See [incremental validation design](incremental-publication-design.md) and [measurement scope](entropy-scale-status.md).

Routine maintenance: [r91](out/native/ashlar_entropy_maintenance_20261006_r91/audited-summary.json) rewrites 333 MB in 13 s and passes exact 20M-row parity; [r92](out/native/ashlar_entropy_maintained_reads_20261006_r92/audited-summary.json) prunes 2 files/read and measures four-client engine/caller p95 161/463 ms. No new maintained descriptor is installed. [r93](out/native/ashlar_entropy_changed_validation_20261006_r93/audited-summary.json) measures exact affected-row and narrow-key prior-property checks, including lexical/presence negatives; stored-batch validation is not ingest freshness admission.

Large-token publication: [r94](out/native/ashlar_entropy_wide_journal_20261006_r94/audited-summary.json) publishes 100k hot-set updates in 78 s, with 639 MB new journal data and exact native affected-carrier/history checks. Independent [r95 wire oracle](out/native/ashlar_wire_timestamp_20261006_r95/summary.json) exposes legacy derived published_at truncation (r89:307µs; r94:870µs) and verifies the explicit UTC-microsecond encoder. Old payloads are retained and cannot substantiate complete all-field reconstruction; native Delta timestamps and token text remain intact. Sustained/burst and 1B/5B admission remain open.

Finite admission schedule: [r96](out/native/ashlar_scheduled_publication_20261006_r96/version-aware-preflight/audited-summary.json) passes three exact 100k-member causal hot-set batches with the corrected wire encoder. Complete-input-to-manifest times are 84/153/233 s; modeled per-record freshness p95 is 234 s. Queueing grows under the finite modeled 10k/s, 10k/s, 100k/s windows. Actual inputs are pre-staged; no sustained producer-rate or reader-load admission follows. Background maintenance requires actual captured version vectors. See [schedule qualifications and results](entropy-scale-status.md).

Read-only validation tuning: [r97](out/native/ashlar_raw_validation_20261007_r97/persistent-uncached/audited-summary.json) rejects negative controls but shows no latency win from combining parity/membership. [r98](out/native/ashlar_raw_batch_filter_20261007_r98/audited-summary.json) finds a useful batch predicate: 1.33 GB read versus 3.64–4.03 GB and lower caller time in both alternating uncached pairs. This does not remeasure publication freshness. [Authority and membership prerequisites](incremental-publication-design.md) remain explicit.

Actual filtered publisher: [r99](out/native/ashlar_filtered_publication_20261007_r99/audited-summary.json) completes three exact 100k-member batches. A terminal added-metadata column error is recovered before canonical apply without repeating writes; first-batch timing is qualified. Two uninterrupted full processing samples are 61.62/66.47 s, with no new freshness p95 or sustained-rate admission. Native journal statistics omit the batch column and parity reads have no file pruning. See [measured limits and next physical candidate](entropy-scale-status.md).

Journal batch statistics: [r100](out/native/ashlar_journal_batch_statistics_20261007_r100/audited-summary.json) backfills the added batch column and preserves all 900k rows, file layout and protocol. New version12 checks report 26 file reads / 74 pruned versus old version10's 100 / 0, with similar bytes and latency. The proposed DDL includes the statistic; existing publications remain pinned to their old versions. No new freshness or sustained-rate admission follows.

Parallel publisher/read contention: [r101](out/native/ashlar_parallel_publication_20261007_r101/audited-summary.json) publishes one exact 100k-member batch in 64.19 s. The append pair occupies 25.82 s; overlap does not improve the observed full-path sample. All 246 old/new pinned singleton carriers remain exact. The 30-key cohort's caller p95 rises from 422 ms idle to 921 ms during publication, with 798 ms for the no-remote subset and file-read p95 15. Shared-resource and maintenance policies remain unqualified for the provisional targets; [scope and design implications](entropy-scale-status.md) are explicit.

Updated-key maintenance: [r102](out/native/ashlar_maintenance_reads_20261007_r102/audited-summary.json) replaces 16 update files with four in 16.88 s, preserving all 100k affected carriers and global identity counts. One statement emits commits 14/15. Paired file-read p95 drops 15→3; maintained engine/caller p95 is 122/420 and 116/358 ms, still outside provisional warm targets. All 150 exact reads are uncached with zero remote reads. Existing manifest version13 remains unchanged; no maintained descriptor or freshness admission follows.

Maintained-candidate contention: [r103](out/native/ashlar_maintained_contention_20261007_r103/audited-summary.json) publishes 100k changes in 63.94 s and passes all 248 exact candidate/new-release reads. Candidate version15 retains file-read p95 three, but loaded caller p95 is 1,096 ms (784 ms without remote reads). New publication version16 again reads 15 files at p95. Maintenance does not close shared-resource latency. The next priority is actual engine/catalog and producer integration plus justified scale/resource bounds; further isolated hot-set repetitions are not the default next step.

Singleton transport evidence: [r114 final audit](out/native/ashlar_singleton_rpc_r114/audited-summary.json)
qualifies30 exact warm reads, p95 engine111ms/caller385.05ms, one RPC/read and
less than1ms p95 outside RPC. No polling-delay fix or billion-scale claim follows.

Adjacency maintenance: [r115 final audit](out/native/ashlar_hub_maintenance_r115/audited-summary.json)
passes20M-row parity through100117scattered moves and OPTIMIZE. Page files change
8/4/1→9/5/2→1/1/1 while optimized bytes become2.16MB at every cursor. Do not
choose maintenance from file counts alone; production entropy remains untested.

Higher-entropy adjacency: [r116 final audit](out/native/ashlar_hub_entropy_r116/audited-summary.json)
passes20M exact rows, typed closure, unique keys and36 pages. Built8files/138MB
become3files/137MB after OPTIMIZE; deep-page bytes17MB→51MB with higher observed
latency. Compare smaller adjacency target files before choosing maintenance.

Adjacency file target: [r117 final audit](out/native/ashlar_hub_filesize_r117/audited-summary.json)
compares16/64MiB on identical20M rows with full parity and36 exact pages.16MiB
retains8files (no rewrite),64MiB coalesces into3; deep reads17MB versus51MB.
Carry16MiB as a forward-adjacency candidate pending scattered-update maintenance.

Correction: [r115 closure failure](out/native/ashlar_hub_closure_r118/correction.json)
finds100117dangling targets; its changed graph does not qualify graph integrity.
[Corrected r119 audit](out/native/ashlar_hub_maintenance_r119/audited-summary.json)
passes all20M rows, unique pairs, typed target closure and54 pages.16MiB FULL
maintenance retains19MB large-hub deep reads, costing29.291s; publication
freshness remains unqualified.

Canonical adjacency: [r120 audit](out/native/ashlar_canonical_adjacency_r120/audited-summary.json)
passes full20M eight-column parity, both typed endpoints, independent identities
and30 sparse-neighborhood pages.16MiB candidate builds in8.699s,16files/206MB.
Exact E13/E16 structural equivalence qualifies snapshot reuse; the reviewed
vector is not activated and full publication remains open.

Publication with adjacency reuse: [r121 audit](out/native/ashlar_isolation_r121/audited-summary.json)
passes100k full affected carriers, raw/journal and full20M structural parity,
then publishes E17/N0/R13/J15/T0/forward1. Ready-input64.615s, oldest modeled
74.615s; fresh singleton p95 engine110/caller361.48ms.
[Integrity follow-up](out/native/ashlar_isolation_r121/integrity-custody/audited-summary.json)
passes19.9M immutable row custody and complete inherited raw/journal equality.
Wide unchanged-payload parity timed out; post-check costs exclude freshness.
The pending r107 execution preflight needs the new predecessor; no extra compute
was provisioned. Provisional performance and1B/5B admission remain unmet.

Validation scheduling: [r122 audit](out/native/ashlar_validation_lanes_r122/audited-summary.json)
passes8 identical raw/journal checks; parallel spans10.0–10.2s versus serial
query sums11.1–14.1s, with higher summed concurrent query time. Read-only replay
supports a bounded scheduling candidate, not publication p95 admission.

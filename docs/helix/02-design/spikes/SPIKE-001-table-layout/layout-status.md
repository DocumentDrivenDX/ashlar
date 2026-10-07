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

Parallel publication: [r123 audit](out/native/ashlar_isolation_r123/audited-summary.json)
passes100k affected20-field carriers, raw/journal and20M structural parity before
E18/N0/R14/J16/T0/forward1 publication. Ready-input62.943s, modeled oldest72.943s;
fresh singleton p95 engine102/caller351.76ms. All provisional targets remain
missed; validation scheduling alone is insufficient.

Changed-set validation: [r125 audit](out/native/ashlar_changed_structure_r125/audited-summary.json)
passes6 exact100k structural differentials and rejects9 corruptions. Output
checks5.5–5.8s offer only modest gain over6.5s full scan. Finite recorded-lineage
guard tests pass; outside-membership/source/owned-SQL/baseline obligations block
a general publication replacement. Failed r124 grouping control is retained.

Structural proof composition: [r126 result](out/composed-structure-r126.json)
combines the validated E17 adjacency baseline, full-carrier checks and owned
E17→E18 apply lineage; three local tests reject11 missing/altered evidence cases.
Fixed-fixture qualification only, no new native workload or performance claim.

Owned publisher templates: [r127 qualification](out/property-template-r127.json)
reproduces5 native r123 queries exactly and passes5 local tests with26 rejection
cases. Template-based structural reuse proof is ready for a bounded synthetic
integration experiment; no native execution or performance claim this iteration.

Integrated owned proof: [r128 audit](out/native/ashlar_isolation_r128/audited-summary.json)
publishes E19/N0/R15/J17/T0/forward1 after exact100k carrier/raw/journal checks
and composed lineage proof. Independent20M structural parity passes afterward.
Ready-input60.874s, modeled oldest70.874s; warm singleton p95 engine115/caller
394.57ms. Targets remain unmet; next tune measured physical writes and append
costs rather than adding redundant structural validation scans.


Eligibility-statistics screen (r129/r130): [audited native measurements](out/native/ashlar_write_pruning_r130/audited-summary.json)
compare three balanced eligible-key joins against canonical E19 and an isolated
shallow clone with additional entity_version/apply_batch_id skipping statistics.
Each returns exactly100k keys. Total query files fall535→19, bytes416–447MB→13.4MB,
and engine time491–635ms→243–294ms; result cache is disabled and remote bytes0.
These totals include the stage and are not target-only file counts. The full20M
key/filepath/row-index comparison passes, assuming stable logical schema and
Delta immutable files; no fresh wide-payload comparison or payload rewrite.
Statistics recomputation costs23.694s caller time and reads1.351GB, outside any
publication clock. Canonical tables and publication remain unchanged.

Read-only r129 metadata confirms configured Zstd on edge_current, source_record
and property_journal; this does not independently verify historical file codecs.
Edge_current has532 files/34.280GB, source_record74/8.217GB, journal72/7.672GB.
The SQL warehouse/Unity Catalog targetFileSize setting controls OPTIMIZE;
Databricks' documented clustering-on-write operation list does not include
MERGE. Sources: [file-size controls](https://learn.microsoft.com/en-us/azure/databricks/tables/tune-file-size)
and [liquid clustering](https://learn.microsoft.com/en-us/azure/databricks/tables/clustering),
consulted2026-10-06. Do not infer MERGE write sizing from the64MiB setting.

Next: qualify an actual update-only MERGE with eligibility predicates in ON and
these statistics, including exact full-carrier and unchanged-membership checks,
before changing the owned publisher templates or canonical settings. This SELECT
screen does not qualify actual MERGE latency, mixed-revision production admission,
publication p95, singleton SLOs or billion-scale performance. UC Delta remains the
architecture; performance measurements inform tuning, not an architecture veto.


Actual update-only MERGE pruning: [r131 audit](out/native/ashlar_merge_pruning_r131/audited-summary.json)
compares two original canonical E18 shallow clones using the same100k r128 stage.
Control retains eligibility in WHEN MATCHED; candidate adds prior-version/batch
statistics and moves eligibility into ON, with no INSERT clause. Total query
reads554→38 files,2.023GB→358.444MB; engine6.814→4.184s and caller7.461→4.575s.
The candidate's one-time statistics recomputation costs22.267s caller/1.351GB read,
separate from MERGE. Counts include stage reads, not exclusively target files.
All measured queries are final and result-uncached; candidate MERGE remote reads
1249B, control0; no spill. This is one sequential pair, changing both statistics
and predicate placement, not a factorial attribution or p95 estimate.

Both MERGEs update exactly100k rows with zero insert/delete/copied rows. Exact
20-field stage/output comparison passes; global20M IDs and100k new-version rows
pass. All19.9M untouched logical keys/filepaths/row indices are identical under
stable schema and Delta immutable-file assumptions; no fresh wide untouched
payload comparison. Canonical E19/publication remain unchanged. Intended checks
still read7.512GB in both paths, so pruning does not eliminate validation costs.

Next: qualify mixed eligible/ineligible/unmatched input semantics on a bounded
native control before adapting owned templates and the integrated publisher.
Account separately for initial statistics maintenance and ongoing write stats.
No production admission, publication p95, read SLO or billion-scale qualification
is established by this pair. Performance targets remain open.


Mixed eligibility control: [r132 audited native evidence](out/native/ashlar_merge_semantics_r132/audited-summary.json)
passes five exact checks comparing both update-only predicate placements across
8 unique inputs/7 existing rows. Only eligible id1 updates; stale version, wrong
predecessor, null eligibility/hash, mismatched compound identity and unmatched
ID sharing another hash remain untouched. Full20-field expected output and paired
parity pass. Synthetic nullable fields exercise SQL three-valued logic beyond
canonical NOT NULL constraints; this is not multiwriter/fencing admission.

Owned PropertyApply now accepts explicit eligibility_placement='on', with the
historical 'matched' default preserved. The new option reproduces the r131 native
MERGE byte-for-byte and therefore binds through the existing exact-SQL proof
interface. Six template/proof unit tests pass, including historical equivalence,
new native-query equivalence and invalid placement rejection. No canonical write
or new publication occurred. Next integrate the opt-in template and statistics
into the publisher, measuring maintenance separately and preserving raw/journal,
exact carrier and structural proof requirements. All performance gates remain
unproven.


Integrated pruned publication: [r133 audited evidence](out/native/ashlar_isolation_r133/audited-summary.json)
adds eligibility statistics to canonical edge_current (maintenance E19→E21), then
publishes100k changed entities with owned ON eligibility and explicit adjacency
reuse. Descriptor r133-b1 pins E22/N0/R16/J18/T0/forward1. Statistics setup23.179s,
stage preparation7.281s and full20M baseline6.249s occur before the publisher clock
and are recorded separately; no claim that setup is free or needed every batch.
Exact20-field intended/output carriers, raw bytes/digests/UTC instant, property
journal, global IDs and composed owned lineage proof pass before publication.
Independent full20M structural parity passes afterward6.221s outside that clock.

Ready-input→verified descriptor52.952s; oldest modeled arrival62.952s for the10s
uniform10k/s admission window. This misses the60s oldest-record target and is a
single synthetic batch, not publication p95 or sustained10k/s/burst admission.
The observed ready-input interval improves from r12860.874s, without isolating
all timing causes across runs. Parallel raw/journal append23.997s and validation
10.905s remain major costs. Actual MERGE4.230s caller/3.943s engine,40 total read
files/359.506MB,16 output files/327.002MB,100k updated rows. New writes retain
configured skipping statistics; no statistics recompute runs between apply and
output validation. Initial maintenance and steady-write costs must stay distinct.

Thirty exact fresh20-field singleton reads: warm p95 engine107ms/caller372.50ms,
15 files and no remote reads, final result-uncached. Both singleton gates remain
missed. No untouched wide-payload comparison, real producer completeness/fencing,
concurrent read admission, sustained arrival throughput, cold-data p95 or1B/5B
qualification follows from this experiment. UC Delta remains chosen architecture.
Next investigate measured raw/journal append cost and throughput; shrinking an
admission batch alone would not prove sustained10k/s when service time exceeds
its arrival window. Preserve exact record and property-history requirements.


Stored-carrier append isolation: [r134 native audit](out/native/ashlar_append_layout_r134/audited-summary.json)
replays the same100k captured r133 raw and journal rows from immutable R16/J18 into
four scratch tables, both Zstd. Raw3-key/journal4-key liquid-clustered parallel
pair8.791s versus unclustered4.665s. Raw caller8.790→4.663s, journal7.859→3.943s;
engine8.333→4.381s and7.350→3.510s respectively. Same source-read bytes in each
comparison: raw685.206MB/79 files, journal641.312MB/5 files. No spill, all measured
queries final and result-uncached. Clustered outputs6 files each versus8 each
unclustered. Total four-output bytes2,589,507,841; no resource resize/new compute.

Complete symmetric UTF8-byte-normalized comparisons pass for every raw/journal
field and both layouts,100k rows each. Exact-check caller costs19.618/17.462s
clustered and19.935/17.154s unclustered, separate from append intervals. Only one
sequential pair per layout; empty scratch tables are not the growing production
history layout. Already-serialized input excludes source serialization. The
observed8.791s clustered replay versus23.997s r133 capture pair motivates isolating
encoding/input-layout work, not assigning the entire difference to serialization.
Do not infer steady throughput, publication p95, or billions of history rows.

[Cleanup evidence](out/native/ashlar_append_layout_r134/cleanup/summary.json)
records identity-checked drops of all four owned scratch tables. UC retention
still applies; no immediate storage reclamation/VACUUM claim. Canonical E22,
R16/J18 and descriptor r133-b1 remain unchanged. Next measure exact serialization
cost separately and test deferred clustering against history-read pruning and
maintenance costs before changing canonical raw/journal layout. Native singleton,
publication freshness/rate, concurrency and1B/5B admission gates remain open.


Serialization screen: [r135 final native audit](out/native/ashlar_serialization_r135/audited-summary.json)
forces consumption of every full wire byte using100k count/byte-length/digest
aggregates in three balanced pairs. Encoding stage0 engine877–1182ms versus
stored R16 payload591–626ms; caller1126–1491ms versus822–864ms. All aggregates
agree; independent exact UTF8 lexical comparison passes4.698s caller, not merely
digest equality. Result cache disabled, remote bytes0. Encoding reads4 files/
665.733MB; stored bytes79 files/672.444MB, so different input layout prevents exact
CPU attribution. These timings do not explain the full23.997s r133 capture pair;
pre-encoding is not yet justified as the primary fix. No new data/table writes.

[Read-only write plans](out/native/ashlar_write_plans_r136/summary.json) capture
raw serialization INSERT, journal INSERT and stored raw replay through EXPLAIN
only. All three expose AppendDataExecV1 command wrappers and report unsupported
Photon wrappers; internal sort/shuffle/write stages are absent. Do not infer
that all runtime tasks fall back or attribute latency from this incomplete plan.
All three EXPLAIN histories final; no INSERT execution occurred.

Qualification correction for r134: inspection of its saved DESCRIBE DETAIL
shows clustered scratch tables include rowTracking, while unclustered scratch
tables do not. Both retain deletionVectors/v2Checkpoint/Zstd. Therefore the
8.791→4.665s pair changes a feature bundle, not clustering alone. Historical
exact raw/journal preservation remains valid; performance attribution to liquid
clustering alone is unqualified. Next replay with row tracking held constant,
then investigate constraints/growing-table versus empty-table write costs and
history-read pruning before altering canonical layout. E22/R16/J18/r133-b1 remain
unchanged. No performance gate or billion-scale admission is established here.


Matched-row-tracking append comparison: [r137 native audit](out/native/ashlar_append_layout_r137/audited-summary.json)
explicitly enables row tracking on both Zstd layouts and preflights tracking plus
cluster keys before writing. Same immutable100k R16/J18 inputs, one sequential
parallel pair each: clustered8.743s versus unclustered4.379s. Raw caller8.741→4.378s,
journal7.438→3.999s; engine8.262→4.087s and6.842→3.490s. Same source read bytes
per role, no spill, final result-uncached. Outputs6 versus8 files per role; full
symmetric UTF8-exact raw/journal comparison passes100k rows per output. This
removes the r134 tracking mismatch, but remains an empty-table replay, not a
production growing-table or sustained-rate estimate.

[Twenty exact full-row reads](out/native/ashlar_append_reads_r138/audited-summary.json)
on those same tables use five SHA-ranked immutable keys per role, alternating
layouts. Clustered reads1 file/89–123MB; unclustered8 files, raw657.716MB and
journal640.251MB. Raw engine123–150ms clustered versus118–139ms unclustered;
journal120–388ms versus113–131ms. Small remote reads occur on three clustered
journal reads (100/48,939/11,379B); all other read remote bytes0. Cached storage
and a journal latency outlier preclude a cold-read or general latency benefit
claim. These are history/raw lookups, not canonical singleton SLO evidence.
Replay source ordering and100k-row scale limit extrapolation. The pruning loss
makes removing clustering globally premature despite faster scratch append.

[Cleanup](out/native/ashlar_append_layout_r137/cleanup/summary.json) drops all four
identity-checked owned scratch tables; platform retention applies, no VACUUM or
immediate physical reclamation claim. Canonical E22/R16/J18/r133-b1 unchanged.
Retain raw/journal clustering in the candidate layout. Next test materialized
exact write inputs with preparation counted inside publication timing: stored
replay is faster than direct computed capture, but the net materialization cost
must be measured rather than moved outside the clock. Preserve exact carriers,
history and origin checks. All native singleton/freshness/rate/concurrency and
1B/5B admission gates remain open.


Materialized-write publication: [r139 final audit](out/native/ashlar_isolation_r139/audited-summary.json)
publishes100k synthetic property changes using immutable raw/journal CTAS inputs,
with materialization counted inside the publication clock. Descriptor r139-b1
pins E23/N0/R17/J19/T0/forward1. Initial eligibility statistics reused; prepared
source stage7.319s and full20M structural baseline6.969s before that clock remain
recorded separately. Exact20-field affected carriers, lexical patch, raw bytes/
digests/origins/UTC instant, property journal/global IDs and owned structural
reuse proof pass before publication; independent full20M parity passes6.235s
after publication, outside the clock. Canonical raw/journal clustering retained.

Materialization pair10.123s plus append8.351s =18.474s versus r133 direct pair
23.997s, but validation16.124s versus10.905s erases the phase saving. Complete
ready-input interval53.452s versus52.952s; oldest modeled arrival63.452s, still
above60s. One batch/nonmatched runtime variability does not establish a causal
regression, p95, sustained10k/s or100k/s burst. Do not promote this materialization
candidate based on faster append alone; it adds intermediate storage/I/O without
an observed complete-clock benefit. Keep direct pruned publication as the current
execution candidate, retaining this experiment for reproduction.

Thirty exact full20-field fresh reads: p95 engine156ms/caller410.65ms,15 files;
five queries have remote reads. This is a mixed-cache post-publication sample,
not a wholly warm or controlled cold-data distribution. Targets remain unmet or
unproven; no billion-scale admission. [Cleanup](out/native/ashlar_isolation_r139/write-input-cleanup/summary.json)
drops two owned100k intermediates after confirming version0, membership and
absence from the published version vector. Retention applies; no VACUUM/immediate
physical reclamation claim. Next address scattered post-MERGE files for native
singleton reads and investigate bounded physical maintenance/input layout rather
than repeatedly adding capture stages. Preserve property history and exact values.


Incremental singleton maintenance: [r140 final audit](out/native/ashlar_singleton_optimize_r140/audited-summary.json)
uses an isolated original E23 shallow clone (before0/after2). One OPTIMIZE command
creates a rewrite and a no-op OPTIMIZE commit; retain both in r141 delta history.
It removes16 files/326.999747MB and adds4/326.482281MB, total532→520 files;
caller13.340s/engine11.741s,658.854MB read. No FULL rewrite, source E23 unchanged.
Thirty SHA-ranked affected full20-field reads before: engine p95124ms/caller
422.70ms,18 files/414.852MB, remote0. Immediately after:574ms/832.02ms,3 files/
160.379MB, five remote queries. This mixed-cache result cannot establish a warm
latency benefit or controlled cold-data admission.

Full20M keyed SHA256 wire comparison times out180s after96.497GB read, and remains
failed evidence. It is not retried or counted as passing preservation. [r141
proof](out/native/ashlar_singleton_custody_r141/summary.json) instead passes exact
UTF8-normalized equality for all100k rewritten20-field carriers and19.9M untouched
key/filepath/row-index custody, plus20M globally unique IDs. Custody relies on
stable schema and immutable Delta-file semantics, not fresh wide-payload equality.
Bounded diagnostics must not imply the failed full digest passed.

[Warm repeat r142](out/native/ashlar_singleton_warm_r142/audited-summary.json)
checks the same30 exact keys after rewritten-carrier verification warmed files:
p95 engine101ms/caller363.10ms,3 files/163.869MB, remote0, final result-uncached.
Both agreed warm gates still missed; this is one finite repeat, no general
concurrency or billion admission. Maintenance improves file/byte pruning but
adds13.340s work and initially introduces cold rewritten files; no integrated
publication freshness claim. Treat it as a tuning candidate, not an admitted
operational schedule. Next isolate query compile/client overhead and assess
post-MERGE write distribution before adding maintenance to every publication.
[Cleanup](out/native/ashlar_singleton_optimize_r140/cleanup/summary.json) drops
only the identity-verified owned clone; source/publication r139-b1 remains pinned
E23/N0/R17/J19/T0/forward1. Platform retention applies; no VACUUM claim. All full
goal performance/scale requirements remain open.


Persistent-client/compile floor: [r143 final audit](out/native/ashlar_client_floor_r143/audited-summary.json)
passes30 exact SELECT1 queries on one connection, result cache disabled, final
histories. Caller p95154.13ms; compile29ms, engine19ms, server total54ms. Paired
caller-minus-server p95106.13ms. Historic optimized r142 full-carrier singleton:
caller363.10ms, compile145ms, engine101ms, server259ms; paired caller-minus-server
111.56ms and caller-minus-execution257.12ms. Residuals computed per query before
ranking; never add or subtract separately ranked p95s. Outside-server residual
includes transport/client and metric-boundary differences, not just network;
caller-minus-execution is diagnostic, not a zero-engine latency prediction.

This small control shows client/compile work must be addressed alongside file
pruning; it does not qualify singleton performance. SELECT1 differs in plan and
payload from20-field Delta queries. Existing workstation→Central US endpoint
measurement is the recorded caller context; a service colocated with the warehouse
is an unmeasured deployment alternative, not an inferred faster result or a
changed250ms target. Preserve current gates while recording caller placement,
compilation and parameterized-query behavior as explicit tuning dimensions.
No new tables, writes, compute, publication or scale admission. E23/R17/J19 and
r139-b1 unchanged. Next test bounded query/plan choices with exact identity and
publication pinning retained, and post-MERGE distribution controls; do not treat
a scalar query or relaxed pinning as the native singleton architecture.


Typed parameter partial experiment: [r144 final audit](out/native/ashlar_typed_reads_r144/audited-summary.json)
retains E23 snapshot/hash/source/Rel/id predicates and full20-field carriers,
alternating decimal-text CAST versus Python integer parameters. Only12 pairs
complete (24 exact carrier matches), all result-uncached/remote0. Descriptive
completed-query nearest-rank p95/max: CAST caller469.78ms/engine126ms/compile254ms;
typed366.23ms/116ms/155ms,18 files each. These exclude the stalled25th request,
so cannot be used as latency admission or a completed30-key comparison. No
signed64 boundary or general connector precision admission; retain current casts.

Server-only pending cast-12 query01f1c1fb-304e-1e2b-bb03-de1d807c805f was FINISHED/
final in239ms while the client stayed stalled for several minutes. A separate
history observation also initially waited; basic workspace HTTP reachability303
was insufficient to establish authenticated SQL health. [Termination evidence](out/native/ashlar_typed_reads_r144/termination.json)
records stopping only identity-verified owned Python PID8866 after the authoritative
server terminal observation (exec session79617 exit143). No blind restart, duplicate
SQL, native write or assertion of the undelivered result. Exact culprit within
client/gateway/response handling remains unknown; this is not slow Delta execution.

This adds a transport reliability requirement to the next bounded client experiment:
finite request wall timeout, durable request correlation/server-query recovery,
and no re-execution solely on observation/response timeout. Current query builder
and agreed gates unchanged. No candidate is promoted from this incomplete sample.
E23/N0/R17/J19/T0/forward1 and r139-b1 unchanged; no scratch data or new compute.
All full goal performance/scale obligations remain open. Continue physical write
and native-read tuning while preserving caller-layer failures in end-to-end evidence.


Bounded correlated reader: [r145 final audit](out/native/ashlar_bounded_reads_r145/audited-summary.json)
runs an owned read-only subprocess with60s total worker wall bound (including
connect/auth),10s configured sockets, one connector request attempt,10s retry
budget/no redirects and15s SQL statement timeout. Unique run/label SQL correlation
and parameters are persisted before submission; no generic timeout replay. Thirty
exact E23 pinned20-field reads pass in14.044s total worker time, all final result-
uncached/remote0. Caller p95397.28ms/engine159ms/compile144ms,18 files. Gates still
missed; comments/control settings and runtime sample differ from previous runs,
so no causal overhead comparison or transport fault admission.

[Same-query status recovery](out/native/ashlar_bounded_reads_r145/recovery/summary.json)
uses history GET only to recover the previously undelivered r144 server ID as
FINISHED/final, without resubmission. Status recovery does not recover result
bytes or assert the lost carrier. The caller must fail closed on ambiguous request
correlation; new request labels identify pending work when a handle is unavailable.
Whole-worker timeout covers reads only, not subsequent SDK history audits.

[Offline timeout field control](out/native/ashlar_bounded_reads_r145/timeout-field-control.json)
confirms installed HTTP timeout setter10000ms propagates to10s pool timeout. Base
and subclass share class name THttpClient/private field; an initial separate-field
concern was resolved, not a discovered timeout bug. Installed source fingerprints
are recorded; controls are internal/version-specific, not general API guarantees.
No induced socket/auth/trickle-response fault was tested. The observed healthy run
therefore verifies acceptance/configuration and exact reads, not all timeout paths.
No table writes/new compute/publication; canonical E23/R17/J19/r139-b1 unchanged.
Next use bounded correlated reads for post-MERGE distribution experiments. Keep
native singleton, publication/rate, concurrency, cold-data and1B/5B gates open.


Post-MERGE source-distribution control: [r146 final audit](out/native/ashlar_merge_pruning_r146/audited-summary.json)
compares two original E22 clones, same100k stage r139 and inherited eligibility
statistics/ON predicates. Candidate adds REPARTITION_BY_RANGE(16,lookup_hash) to
the source SELECT only; no assertion that the writer preserves source ordering.
Both native MERGEs pass exact20-field affected values,100k updates/zero insert,
delete,copied rows,20M global identity and19.9M untouched immutable row custody.
No canonical or publication write. Custody assumes stable schema/immutable files.

Control MERGE caller4.021s/engine3.568s; hinted6.680s/6.127s. Actual changed-data
file groups6 versus16 (100k live rows each). Min/max hash spans: control0.490–
0.99996 of256-bit hash space, candidate0.99901–0.99996; this is measured range
coverage, not proof of internal sort ordering or hint preservation. All candidate
ranges remain nearly full-space and overlap, so source hint does not deliver the
intended selective file ranges in this experiment. Do not promote it to owned
publisher templates.

Two separately bounded correlated read children each pass30 exact pinned carriers
within60s process limits. Control p95 engine109ms/caller386.69ms,8 files; candidate
105ms/390.18ms,18 files, no remote reads, final result-uncached. Both gates missed;
one sequential pair cannot establish timing causality or general read distributions.
Full exact output validation3.542/3.453s and untouched custody13.024/13.083s are
separate diagnostic costs, not publication measurements. No source/burst/sustained
rate or billion-scale admission. [Cleanup](out/native/ashlar_merge_pruning_r146/cleanup/summary.json)
checks recorded latest MERGE statement identity/version and drops only both owned
clones. Canonical E23/R17/J19/r139-b1 unchanged; platform retention/no VACUUM.
Next compare physical key/maintenance choices with explicit ingest/read tradeoffs;
source partition hints alone did not solve post-MERGE file overlap. Retain the
chosen UC Delta architecture and current agreed gates; all full-goal obligations
remain open.


### r147 physical key screen and candidate DDL reconciliation

[r147 native audit](out/native/ashlar_key_screen_r147/audited-summary.json) compares
lookup_hash clustering with (source_system,id) on the same 100k complete carriers
from stage r139. Exact symmetric 20-field/UTF-8 carrier equality and unique IDs
pass for both tables. Both OPTIMIZE FULL commands were no-ops (version 0 retained,
zero rewritten bytes); the 16 MiB target did not cause a demonstrated rebuild.
Actual hash/source-id layouts contain 16/14 files and 335.655/335.319 MB.

Thirty alternating exact singleton reads per layout prune one file each. Hash
p95 engine/caller is 103/338.45 ms; source-id is 135/366.79 ms with two remote-read
queries versus zero for hash. Result cache is disabled. Neither comparison admits
the latency targets. This 100k screen does not establish full-scale source/type
diversity, incremental write behavior or billion-scale performance; retain hash
as the candidate. Both owned tables were identity/version checked and dropped;
canonical E23/R17/J19 and the r139-b1 publication remain unchanged.

The candidate DDL now includes edge entity_version/apply_batch_id statistics,
reflecting r130/r131/r133 evidence. CONTRACT-003 records the bounded evidence,
backfill cost, update-only eligibility scope and remaining obligations. Logical
schema and the 64 MiB reference target remain unchanged. The revised complete
13-table DDL has not been re-executed. Next resolve the publication maintenance
policy and service-side reader deployment using the existing large fixture;
retain UC Delta as selected architecture and keep ingest/read metrics explicit.


### r148 publication capacity and maintenance policy

The [reproducible model](out/publication-capacity-r148.json) derives a serialized
capacity sensitivity of 1,889 changed entities/s from the one r133 100k publication
(52.947 s processing). Holding this sample constant at modeled 10k/s arrivals
increases queue wait by 42.947 s per batch; uniform first-batch record-age p95 is
62.447 s. No sustained throughput, actual arrivals or service p95 is established.
A first 100k/s burst batch appears under 60 s in this model but subsequent queue
growth invalidates burst admission. Separate maintenance adds cost rather than
closing this capacity gap; its warmed reads still miss the agreed targets.

The [draft maintenance policy](publication-maintenance-policy.md) preserves pinned
publication vectors, distinguishes cleanup from source progress and requires
qualification before a maintenance-only manifest can be exposed. ADR-001 links
the policy and qualifies revised-DDL execution scope. Next run a bounded three-batch
queue diagnostic on isolated outputs, targeting publisher service before more
clustering screens. This iteration runs arithmetic locally and performs no native
write or compute provisioning. Full goal remains active and unproven.


### r149 native three-batch queue diagnostic

[Final native audit](out/native/ashlar_queue_r149/audited-summary.json) executes
three serialized 100k complete synthetic property batches on isolated E23/R17/J19
shallow clones, existing warehouse 2439e1f2e37ac563, runtime
19.8.x-aarch64-photon-scala2.13. Releases are modeled at 10/20/30 s (10k/s), not
real source arrivals. No readers, maintenance, resizing or new compute.

| Batch | Processing seconds | Queue wait seconds | Modeled record-age p95 seconds |
| --- | --- | --- | --- |
| 1 | 50.139 | 0.0001 | 59.639 |
| 2 | 54.654 | 40.507 | 104.661 |
| 3 | 49.974 | 85.567 | 145.041 |

Combined 300k uniform modeled per-record freshness p95 is 144.041 s. The oldest
record in batch 1 is 60.139 s old. Distinguish these quantiles from service p95,
which three samples do not establish. Input drained at 165.541 s after controller
epoch, 135.541 s after final modeled input completion. First-batch near-pass does
not admit sustained 10k/s or 100k/s burst; queue growth is actually observed under
the modeled release schedule, not just the prior fixed-cost arithmetic.

Exact20 affected fields and token changes, exact raw bytes/digests/origin/UTC
instant and property journal pass per batch. All20M unique IDs and owned structural
reuse proofs pass; independent full20M structural parity passes after the clock.
Final19.9M untouched physical custody passes under stable schema/immutable-file
assumptions, not fresh wide-payload equality or unknown-writer evidence. The second
and third batches count a full structural baseline scan inside processing; batch1
baseline is before the epoch. This extra proof cost is explicit, not attributed to
layout degradation. Prepared input is outside clock; real source staging remains
unmeasured. Source/history appends and validation are parallel separate lanes,
while logical publication is serialized. Carrier entropy remains synthetic.

Append pairs take13.696/10.670/10.596 s; raw/journal validation pairs take
18.358/19.221/11.707 s; MERGE caller3.953/3.767/3.540 s. Costs across preparation,
publication and final audit:122.642 GB recorded read_bytes and6.856 GB remote
writes (decimal). These counters are not distinct storage footprint or billing.
No whole-graph wide digest scan was repeated. Each statement is bounded180 s;
controller stops admitting SQL after600 s, not a guaranteed global process kill;
connector socket/retry bounds and durable submission correlation remain explicit.

[Cleanup](out/native/ashlar_queue_r149/cleanup/summary.json) UUID/version-checks
and drops all seven owned tables. Canonical E23/R17/J19/r139-b1 remains unchanged;
no VACUUM. The evidence prioritizes reducing publisher validation/capture service
cost with equivalent preservation proof over further singleton key changes.
Next compare one bounded precomputed-wire validation path against exact wire
checks, counting materialization inside publication; do not promote it without
measured end-to-end benefit and an explicit proof-strength comparison. The prior
r139 materialization regression remains relevant failed-candidate evidence.
UC Delta remains selected; sustained/burst/concurrent/cold/billion gates stay open.


### r150 exact wire witness screen: rejected

[Final native audit](out/native/ashlar_wire_validation_r150/audited-summary.json)
compares three alternating exact raw-wire validation pairs using immutable stage
r139 v0 and published source_record v17/r139-b1. An isolated100k-row Zstd wire
witness preserves complete20-field-plus-old_json UTC/microsecond encoding; full
symmetric exact witness comparison passes, as do100k unique delivery memberships
and all six exact raw UTF8/digest/origin validations. No canonical, raw, journal,
MERGE or publication write occurs; only one temporary witness is created.

Control caller5.220/5.030/6.216 s; stored5.436/4.844/5.107 s. Engine control
4.750/4.579/5.207 s; stored4.911/4.227/4.651 s. Control91files versus stored95;
control all remote0, first stored19.902MB remote, other stored remote0. Actual
witness8files673.355MB/reader3/writer7; source-stage and witness physical layouts
are different. Do not attribute timing differences to serialization alone.

Materialization7.473 s plus stored validation yields a hypothetical12.317–12.909 s
single-use cost versus5.030–6.216 s direct checks. Independent exact witness
verification12.774 s raises that sensitivity to25.091–25.683 s. These sums are
separate samples, not a measured publication. Digest does not replace exact raw
UTF8 comparison. No demonstrated benefit: reject this validation witness path;
retain the earlier r139 whole-publication materialization regression as evidence.

Total recorded reads11.515GB/remote writes0.673GB, not billable amounts or distinct
storage footprint. Each statement90 s; socket/retry bounded and submissions
correlated. [Cleanup](out/native/ashlar_wire_validation_r150/cleanup/summary.json)
UUID/version-checks and drops the witness; canonical E23/R17/J19/r139-b1 unchanged,
no VACUUM. All native metric IDs are final and result-uncached; three cached-data
pairs do not admit service/freshness/concurrency/billion targets.

Next screen explicit source-feed/epoch bounds in the raw validation query using
its existing source_feed/source_epoch/delivery_id statistics. Such bounds must be
proved from immutable stage membership and retain missing-row and exact-origin
refusals. This targets raw-file pruning without copying complete wire payloads or
weakening preservation proof. If it fails, do not keep adding per-batch copies.
UC Delta remains selected and the full performance goal remains active.


### r151 explicit raw origin bounds: pruning candidate

[Final native audit](out/native/ashlar_origin_validation_r151/audited-summary.json)
compares three alternating100k exact raw validations over stage r139 v0/source
records v17. A stage-origin GROUP BY proves one non-null feed/epoch and100k rows;
static feed/epoch filters supplement the existing apply_batch_id predicate.
Because every stage row has that exact origin and the join already requires it,
these filters preserve the logical match set. They do not replace delivery-ID,
missing-row, wire UTF8/digest/origin/UTC instant validation or global raw-membership
checks. Six exact comparisons pass. Two deliberately incorrect static origin
bounds each produce100k failures, confirming that missing rows are not suppressed.

Control caller6.148/4.885/5.408 s versus bounded4.919/4.470/5.095 s; engine
5.631/4.567/4.738 versus4.440/4.083/4.480 s. File reads91→12 in every pair;
read bytes1.353→1.323GB (decimal), all remote0/result-uncached. File-count gains
are much larger than byte savings; no assumption that payload serialization or
publication bottlenecks are solved. Three cached-data pairs do not establish
service p95, sustained rate, cold reads or timing causality. Stage-origin proof
cost is separate and must count inside any integrated publication comparison.

Read-only existing warehouse2439e1f2e37ac563; no materialization, mutations,
maintenance or compute changes. Canonical anchor E23/r139-b1 remains unchanged.
All phases plus refusal controls record8.057GB reads/zero remote writes, not
billing. Statements90 s, refusal controls30 s, bounded connector retries/socket
and durable correlation. Initial audit encountered finalization lag for two
FINISHED refusal-control IDs; [finalization note](out/native/ashlar_origin_validation_r151/audit-finalization-note.json)
records same-ID history refresh without SQL replay. Final histories all final.

The [executed bounded query](out/native/ashlar_origin_validation_r151/bounded-query.sql)
is a scoped owned candidate, not a generic consumer SQL API. Next incorporate
origin-proof validation into an owned query builder, rejecting unverified,
non-singleton or null origins, then count origin proof and exact validation in
one isolated publisher comparison. Preserve multi-origin fallback and real source
semantics; never assume all future batches share one epoch. This is the first
useful raw-validation pruning change in this sequence, but no agreed full gate is
admitted. UC Delta remains selected and the full goal remains active.


### r152 owned origin guard and integrated publication

The owned [raw validation helper](raw_validation_queries.py) consumes the exact
successful immutable stage0 origin query and requires one nonempty feed/epoch,
exact expected count and a native query ID. [Guard evidence](out/raw-validation-guard-check.json)
matches the executed r151 SQL after equivalent literal spelling and rejects11
adversarial query/version/result/count/null/multi-origin/different-stage cases.
The helper is inside a trusted controller; its records/dataclass are not an
untrusted authentication boundary or generic producer fencing. Unsupported
origin grouping is refused by this scoped helper; retain the prior unbounded-by-
origin exact validation as an explicit multi-origin fallback in future integration.

[Integrated native audit](out/native/ashlar_queue_r152/audited-summary.json) runs
one100k synthetic property publication on isolated E23/R17/J19 shallow clones,
existing warehouse2439e1f2e37ac563/runtime19.8.x-aarch64-photon-scala2.13.
Modeled10k/s release window is10 s; no actual source/rate or simultaneous readers.
Origin proof costs0.337 s inside the clock. Processing50.378 s, complete-input
queue wait0.00027 s, modeled record-age p95 59.878 s and oldest age60.378 s.
The first r149 batch took50.139 s: this single sequential comparison does not
show an end-to-end gain or establish a service/freshness p95 distribution.
A one-batch modeled near-pass cannot admit sustained10k/s or100k/s bursts.

Raw exact validation caller13.652 s/engine13.111,5files/1.474GB; journal symmetric
validation17.749/16.999 s,10files/2.885GB, keeps the parallel validation critical
path at17.753 s. MERGE4.182/3.775 s,40files/359.516MB. Reduced raw file scans do
not solve the journal validation cost. Exact20 affected fields/token patch,
raw origin/digest/UTF8/UTC instant and exact property journal pass, alongside20M
unique IDs and owned structural-reuse proof. Independent full20M structural
oracle and19.9M untouched physical custody pass after the clock under stable
schema/immutable-file assumptions. No fresh wide-payload equality claim for all
untouched rows, real producer completeness/fencing/ACK or billion admission.

Preparation/publication/final audit totals44.998GB recorded reads/2.285GB remote
writes, not billing or distinct storage footprint. Controller admits SQL for at
most600 s; per-statement180 s/socket/retry bounds remain explicit. Final history
IDs are all final. [Cleanup](out/native/ashlar_queue_r152/cleanup/summary.json)
UUID/version-checks and removes all five owned tables; canonical E23/R17/J19 and
r139-b1 remain unchanged. No new compute or VACUUM.

Keep guarded origin bounds as a scoped pruning option, with no claimed throughput
improvement. Next compare a single-pass exact journal comparison against the
symmetric EXCEPT query, preserving all columns, UTF8 equality, missing/null flags,
row multiplicity and membership refusals. Measure its own duplicate-key checks;
no digest substitution or moved validation clock. UC Delta remains selected and
all full-goal sustained/read/concurrent/cold/billion gates remain open.


### r153 exact journal multiset aggregation: rejected for latency

[Native audit](out/native/ashlar_journal_validation_r153/audited-summary.json)
compares three alternating100k journal validation pairs over immutable stage
r139 v0 and property journal v19/r139-b1. Candidate unions normalized expected
and actual rows with signed multiplicities, groups by all21 fields and refuses
nonzero balances. Every text field uses hex UTF8 bytes; timestamps, booleans,
numerics and nulls remain native. No digest or approximate equality substitutes
for full values. Zero imbalance means exact multiset equality within this bounded
200k-row profile; nonzero result counts differ from EXCEPT counts but both refuse.

Both paths pass exact100k equality. Eleven tiny typed native controls are refused
by both: duplicate/missing row, old-value null, missing flag, Unicode lexical
normalization difference, new value, native cursor, source epoch, microsecond
instant, event ordinal and property ID. Expected stage membership/uniqueness and
source authority remain separate publisher responsibilities; equal duplicated
expected/actual bags alone would not prove those invariants. This helper is owned
synthetic query code, not a generic SQL/authentication or source-contract API.

Control caller7.156/6.346/6.653 s versus candidate9.283/9.416/9.071 s; engine
6.076/5.560/5.894 versus8.504/8.727/8.403 s. Files24→12; reads2.602GB→
1.301–1.305GB; spill0 throughout. Remote bytes control0/0/40,076; candidate
18.409MB/0/3.927MB, so not strict all-warm paired admission. All result caches
disabled, final native IDs and balanced order. Do not infer internal CPU/shuffle
causality without a physical profile or extrapolate these pairs to service p95.
The lower scan volume does not produce a latency benefit here. Reject candidate
for publisher promotion; preserve the existing symmetric exact validator.

Existing warehouse2439e1f2e37ac563, same published synthetic fixture; no tables,
new compute, maintenance or publication writes. All comparison/refusal phases
record11.717GB reads/zero remote writes, not billing. Statements90 s and durable
submission/socket/retry limits remain explicit. No cleanup needed. A local syntax
error was corrected before any SQL submission; the native run was not replayed.

Next screen a full outer comparison keyed by unique journal delivery/property/
event identity, counting uniqueness proof and all21 exact fields. Multiplicity
refusal must survive; otherwise retain symmetric EXCEPT. Bound it to the same
read-only100k corpus before another integrated publication. The r15250 s service
and r149 queue-growth results remain unmet sustained capacity evidence; optimizing
scan counts alone does not meet the full goal. UC Delta remains selected.


### r154 keyed exact journal comparison: rejected

[Native audit](out/native/ashlar_journal_validation_r154/audited-summary.json)
compares three alternating100k journal pairs over stage r139 v0 and property
journal v19/r139-b1. Candidate full-outer joins feed/epoch/delivery/property/event
keys, compares all21 columns with UTF8-normalized texts and includes per-side
window counts for duplicate refusal. This is an owned unique-event synthetic
profile; it does not select native Truss event identity. The source-profile key
and authority requirements of CONTRACT-003 remain unqualified for real feeds.

All exact equality checks and11 typed native corruption refusals pass on both
paths. Candidate also refuses equal duplicated expected/actual bags: intentionally
stricter than multiset equality because this profile requires unique events.
Missing-row detection uses the non-null window count, not nullable value columns.
Nulls, missing flags, Unicode lexical differences, native cursor/origin, exact
microseconds, event ordinal/property identity and changed values stay distinguishable.

Control caller6.618/6.673/6.589 s versus candidate11.389/11.497/11.999 s; engine
5.941/5.924/5.870 versus10.635/10.658/11.318 s. Files24→12, reads2.602–2.606GB→
1.301–1.305GB, spill0. Remote bytes control0/0/18.443MB versus candidate1.969MB/
0/3.961MB. Balanced order, final native IDs, result cache disabled; this is not
strict all-warm service-p95 or causal internal-profile evidence. The included
uniqueness checks halve scans but do not reduce latency. Reject publisher
promotion and retain symmetric exact EXCEPT validation.

Read-only existing warehouse2439e1f2e37ac563; no resources/tables or publication
changed. All phases11.719GB recorded reads/zero remote writes, not billing.
Statement90 s and bounded submission/socket/retry controls. No cleanup needed.
Preserve both r153/r154 failed alternatives rather than claiming fewer scans are
faster or changing the exact preservation obligation.

Next test three independent physical write lanes: current apply, raw append and
journal append, followed by the same complete exact checks and manifest barrier.
Only one logical publisher may own the fixture; consumers remain pinned to the
old manifest until all new vector members pass. A failed lane may leave unpublished
physical versions and requires inspection/recovery, never progress advancement or
blind mutation replay. Do not advertise native multi-table transactions or real
fencing from this experiment. Count all checks, history collection and manifest
confirmation in publication time. Keep the isolated100k/current20M bound and
existing compute; this addresses elapsed service without weakening validation.
The full goal and all sustained/read/concurrent/cold/billion gates remain open.


### r155 independent write overlap: bounded candidate, modest improvement

[Native audit](out/native/ashlar_queue_r155/audited-summary.json) runs one100k
property publication over isolated E23/R17/J19 shallow clones on existing
warehouse2439e1f2e37ac563/runtime19.8.x-aarch64-photon-scala2.13. Current MERGE,
raw append and journal append run through three separate client lanes; logical
publisher remains serialized. Exact pre-apply intent precedes all writes, and
all existing raw/history/current/identity/structural checks precede the manifest.
No native multi-table transaction, real feed fencing or recovery admission.

Processing48.761 s, modeled10 s input window, oldest record58.763 s and uniform
record-age p95 58.263 s. The prior r152 serial-apply publication took50.378 s;
this is one sequential comparison with different cache/load conditions and an
extra observer, not a causal win or measured freshness/service p95 distribution.
At100k batches, holding this sample constant still implies only about2,051
changed entities/s; sustained10k/s and100k/s burst requirements remain unproved.
Do not substitute first-batch latency for queue/rate admission.

Overlapped write barrier15.161 s: raw15.156, journal11.690 and MERGE7.826 s caller.
Prior serial-apply MERGE4.182 s: overlap also slows individual operations. Exact
raw/journal validation barrier17.541 s remains dominant; the saved clock time is
modest. Full20 affected fields/UTF8 exact values/raw digest and origin/native
cursor/property journal/20M IDs and owned structural proof pass. Independent
full20M structural parity and19.9M unchanged physical custody pass after the
publication clock under stable schema/immutable-file assumptions.

A [read-only observer](out/native/ashlar_queue_r155/observer/summary.json) completes
before manifest submission and sees manifest count0,100k applied current rows and
100k rows retained at old pinned version0. This proves one unpublished physical
window and old snapshot retention, not full consumer concurrency or a failed-
lane recovery protocol. The final audit includes an explicit scope correction to
its inherited no-concurrent-reader wording; actual observer IDs/costs are retained.
All saved native IDs are final. Total preparation/publication/observer/final audit
48.941GB recorded reads/2.286GB remote writes, not distinct storage or billing.
Controller600 s admission deadline/per-statement180 s and connector submission
bounds remain explicit; no new compute/resize or whole-graph payload digest.

[Cleanup](out/native/ashlar_queue_r155/cleanup/summary.json) UUID/version-checks and
drops all five owned tables; canonical E23/R17/J19/r139-b1 remains unchanged.
Keep write overlap as an experimental option, not an admitted publisher default.
Next prove failed-lane manifest refusal on a much smaller isolated fixture, then
consider overlap of independent validation lanes without weakening any checks.
Do not repeat the20M custody scan for a tiny protocol control. Resource/rate,
singleton, cold/concurrent reads, external-runtime and1B/5B obligations stay open;
UC Delta architecture remains selected.


### r156 failed raw-lane publication refusal

[Final native audit](out/native/ashlar_failure_r156/audited-summary.json) uses a
1,000-edge property slice with isolated current/raw/journal/manifest/stage tables,
existing warehouse2439e1f2e37ac563. Three separate clients complete native MERGE,
raw append and journal append. Raw records deliberately append one whitespace byte
to each full wire payload and store the matching SHA256 of that corrupt value.
JSON parsing and digest consistency alone therefore cannot establish exact source
bytes. Exact origin-bounded raw validation reports all1,000 mismatches.

Current20-field output and property journal equality pass. Controller validation
refuses the new manifest before any submission; saved records contain no
publish-new statement. The complete old manifest remains byte-for-byte equal,
and all1,000 old pinned carriers match immutable stage r139 v0 with UTF8-normalized
strings. New physical rows stay unmanifested. CONTRACT-003 now links this evidence
and explicitly distinguishes validation refusal from crash/ambiguous transport,
durable receipt, recovery and producer-fence qualification.

This is a protocol control, not a performance screen, full graph or endpoint-
closure/billion admission. Existing preserved typed endpoints/current carriers
are used; no20M structural/custody scan is repeated. Source subset extraction and
old snapshot verification still read original large files: recorded reads18.007GB,
remote writes26.948MB across preparation/control/audit, not billing or distinct
storage footprint. Initial local syntax correction preceded all SQL; native
mutations were not replayed. Statements60 s, final old-snapshot check30 s,
connector socket/retry and durable correlation retained; all native IDs final.

Unpublished versions were recorded/inspected, then [cleanup](out/native/ashlar_failure_r156/cleanup/summary.json)
UUID/version-checks and drops all five owned tables. This is cleanup, not durable
recovery. Canonical E23/R17/J19/r139-b1 stays unchanged; no new compute or VACUUM.
Next test overlap of exact current/output validation with raw/journal validation
on the existing100k fixture, keeping all publication checks and costs inside the
barrier. Full throughput/singleton/cold/concurrent/external-runtime/1B/5B targets
remain open, and UC Delta architecture remains selected.

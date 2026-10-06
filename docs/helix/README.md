# Ashlar project documentation

**Owner direction (2026-10-06):** Unity Catalog Delta is the architectural commitment. Latency measurements inform tuning and capacity; they do not gate this choice. The brief Real-Time availability check found no workspace preview or creation option, so no warehouse was provisioned. Prior benchmark failures remain evidence, not an architecture veto. Next prioritize final table design and consumer mappings.

**Iteration policy (2026-10-06):** The owner now explicitly requests a large local test followed by Databricks. The active phase is 4M nodes / 20M edges / 200k scattered updates. Commit and push each completed iteration. Billion-node/runtime admission remains unproved.

**State (2026-10-05):** Focused HELIX framing drafts exist. Owner direction is a
open-source, domain-independent property-graph toolkit integrating UMF, with
Databricks gold Delta as its first target. The first
milestone is a well-supported schema package and an evidenced graph query path.
No physical schema, implementation, or compatibility result is approved.

| Activity | State | Entry point |
| --- | --- | --- |
| 00 Discover | Updated direction and evidence | [Vision](00-discover/product-vision.md), [input](00-discover/vision-input.md), [generalized build brief](00-discover/build-brief.md), [research/scope disposition](00-discover/research.md) |
| 01 Frame | Draft PRD, four features, four stories | [PRD](01-frame/prd.md), [concerns](01-frame/concerns.md) |
| 02 Design | Concrete table Contract, proposed layout ADR and local spike; native spike executed; production choices unproven | [Publication boundary](02-design/contracts/CONTRACT-001-publication-boundary.md), [read boundary](02-design/contracts/CONTRACT-002-consumer-read-boundary.md) |
| 03 Test | Consumer corpus unexecuted; scoped native layout/query evidence passes | [Conformance plan](03-test/consumer-conformance-plan.md), [fixtures](03-test/fixtures/consumer-conformance.json) |
| 04 Build | Not started | Design-spike harnesses only; no library implementation |
| 05 Deploy | Not started | No resources provisioned |
| 06 Iterate | Not started | No release |

| Requirement | Feature | User story |
| --- | --- | --- |
| FR-1 | [FEAT-001: UMF graph profile](01-frame/features/FEAT-001-umf-graph-profile.md) | [US-001: Review mapping](01-frame/user-stories/US-001-review-graph-mapping.md) |
| FR-2 | [FEAT-002: Databricks schema package](01-frame/features/FEAT-002-databricks-schema-package.md) | [US-002: Validate package](01-frame/user-stories/US-002-validate-schema-package.md) |
| FR-4 | [FEAT-004: Incremental gold publication](01-frame/features/FEAT-004-incremental-gold-publication.md) | [US-004: Trust replayed publication](01-frame/user-stories/US-004-trust-replayed-publication.md) |
| FR-3 | [FEAT-003: Graph query path](01-frame/features/FEAT-003-graph-query-path.md) | [US-003: Query typed graph](01-frame/user-stories/US-003-query-typed-graph.md) |

**Consumer input:** [Hot-store publication](00-discover/hot-store-publication-input.md)
informs new FR-4/FEAT-004/US-004 and extended QUERY-06–QUERY-09/US-003 criteria.
The local discovery-branch input was read for this pass; remote merged state was
not verified. Its proposed identity and producer guarantees remain proposals.

**Table design:** [CONTRACT-003](02-design/contracts/CONTRACT-003-delta-graph-tables.md),
[proposed ADR-001](02-design/adr/ADR-001-delta-canonical-and-serving-layout.md),
[SPIKE-001](02-design/spikes/SPIKE-001-table-layout.md). Local DuckDB screening
passes parity at 300k nodes/900k edges. [Native evidence](02-design/spikes/native-layout-evidence.md)
records successful DDL and parity through 3M nodes/9M edges on dbw-aidev-cus; GraphFrames has scoped local execution; PuppyGraph and Fabric remain
unexecuted. Singleton lookup is native Databricks, independent of Fabric. The
owner selected 1B nodes with more edges; Fabric mappings are bounded projections.

**Comparison evidence:** Bounded hash-bucket partitioning plus Z-order versus
liquid clustering includes ingest cost. Exhaustive maintenance parity passes
all 10,019,981 edge rows, complete fields, hidden metadata and unique typed keys.

**Next action:** Prioritize actual source/runtime integration using the [full-scope audit](02-design/spikes/SPIKE-001-table-layout/out/goal-completion-audit-20261006.json), [layout status](02-design/spikes/SPIKE-001-table-layout/layout-status.md) and [candidate package](02-design/spikes/SPIKE-001-table-layout/layout-package-candidate.json). Native 0.3 varied-carrier and exact hash evidence now pass, as do scoped serialized recovery/receipt controls. Real Truss complete-boundary/authority implementation, full publication protocol and actual graph-engine execution remain unqualified. Local Spark 3.5.3 / Delta 3.2.1 / GraphFrames 0.12.3 integration now passes the scoped pinned-release checks. PuppyGraph/Fabric and direct UC reader-feature interoperability remain unqualified. The authorized larger local/Databricks physical comparison is described in the [scale plan](02-design/spikes/SPIKE-001-table-layout/scale-experiment-plan.md).

**Historical spike status (superseded as a work plan):** The maintenance-inclusive comparisons below now fail.
Final verification passes on both layouts, including 9,419,981 untouched carriers,
600k changed records and 3,120,023 inherited journal rows. [64MiB copy](02-design/spikes/native-lc64-evidence.md)
now passes full 10M-carrier parity, reduces active files452→128 and median lookup
files4→1. Remote first-touch caller p95 921ms passes the bounded screen, warm
engine86ms passes, but warm caller357ms fails and compile timing does not clearly
improve. [Matched guarded ingest](02-design/spikes/native-lc64-ingest-evidence.md)
now passes changed/untouched carriers and actual vectors, but freshness69.85s
LC64 /82.72s LC16 still fails without maintenance/readers. Inherited history and
full metadata follow-up now passes. [Conditional MERGE controls](02-design/spikes/native-conditional-merge-evidence.md)
pass one clean and16 actual rollback cases. Epoch isolation and three separate
publisher-ID controls now pass, preserving older producer history and rejecting
publisher marker reuse with complete rollback. Full300k conditional apply now completes in31.97s, oldest freshness61.97s
still fails. Exact changed/untouched carriers, inherited history, row identities
and vector/barrier checks pass. Full metadata and deliberate changed-key reads now pass correctness; final
warm engine91ms passes,caller363ms fails,median17 files. Counted maintenance33.99s reduces median reads17→2 files, but final caller421ms
still fails; separate freshness cost totals95.96s. Maintenance version5 remains
unpublished. Exhaustive18-field/hidden-metadata maintenance parity now passes. Next revised
policy/execution path. Paired simpler hash-candidate SQL modestly lowers planning
but caller344ms still fails (exact368ms); retain exact native-key SQL;
sustained/full-scale remain open.
[Partition/Z-order comparison](02-design/spikes/native-partition-zorder-evidence.md)
passes full canonical parity at 10M edges and all matched full-field reads.
Final warm engine p95 is 84ms for both layouts; caller 380ms partition / 393ms LC
fails the target. Partition reads one median file versus two, without a proved
caller solution. Copy costs 41.8s, Z-order 52.8s; paired incremental ingest and
counted maintenance remain next.
[Paired guarded publication](02-design/spikes/native-partition-ingest-evidence.md)
passes 300k changed/journal records and 9,719,981 untouched complete carriers per
layout. Processing is LC 50.45s / partition 29.91s; assuming a 30s source window,
freshness is 80.45s / 59.91s. One sequential batch is not sustained admission or
a causal layout win. Full inherited-history and descriptor/receipt metadata
follow-up now passes on both layouts. Untouched-key post-ingest reads pass warm
engine (77ms LC / 80ms partition) but fail caller (374ms / 366ms). The reused
sample includes no updated IDs. Deliberate changed-key reads now pass complete
carrier checks but fail warm engine (103ms LC / 113ms partition) and caller
(412ms / 415ms). [Counted maintenance](02-design/spikes/native-partition-maintenance-evidence.md)
takes 59.15s partition versus 16.26s incremental LC, with very different rewrite
scope. Experimental maintenance snapshots pass sampled full carriers and warm
engine (78ms / 86ms), but caller still fails. Exhaustive full-fixture parity
including hidden metadata now passes, and equivalent vectors publish edge21 LC /
edge3 partition with unchanged progress/revisions and clear barriers. Next
repeated-batch maintenance/read testing includes maintenance in freshness.
[Maintenance-inclusive arrivals](02-design/spikes/native-maintained-schedule-evidence.md)
now fail the combined screen: LC freshness104.33/149.85s, partition131.81/191.66s,
with growing queues. Actual writer-overlap reader p95 is engine209ms/caller1.38s
LC and engine307ms/caller952ms partition. All guarded changed records and old
pinned reader carriers pass. Exhaustive final untouched/history/descriptor
verification is next, then a different policy/layout; neither tested policy is admitted.
[Post-ingest reclustering](02-design/spikes/native-edge-recluster-evidence.md) takes
55s and reduces median singleton files six to two. Final warm engine p95 98ms
passes the bounded screen; caller 444ms still fails. All 51 full carrier/hidden
metadata comparisons pass; the follow-up proves full-fixture preservation but no sustained admission.
[Statistics trial](02-design/spikes/native-optimizer-stats-evidence.md) preserves
all 300k exact checks but shows no clear timing improvement; current plans recognize
catalog statistics while matched pinned plans still report them missing.
[Join-plan comparison](02-design/spikes/native-post-plan-evidence.md) rejects a
slower forced journal sort-merge (partly outside Photon). Existing broadcast is
faster; both initial plans report missing canonical/journal optimizer stats.
[Integrated barrier trial](02-design/spikes/native-barrier-graph-evidence.md)
passes 600k changed/journal records, 9,419,981 untouched full carriers and both
descriptors, but freshness fails at 75.58/87.82s. Actual version resolution costs
1.14s per batch; atomic data work 36.3–39.6s dominates. No guessed vectors or data
replays. Caller/scale/sustained/burst/external-engine gates remain open; new
resources need pending bounds and UMF remains deferred.

Publication ordering, deletion, history, revision barriers, progress and bounded
reads can continue with opaque logical identifiers. Hold exact UMF versions,
vocabulary, mappings and extension design until the relevant capabilities merge.
No UMF schema or binding was authored in this pass.

Drafting design can proceed with explicit proposals, but drafts do not constitute
owner approval or justify production/compatibility claims. Tests must cite stable
story AC IDs and actually exercise them before criteria can be marked satisfied.
Transactional engines, action execution, GraphQL, MCP, and UI are deferred.

HELIX catalog: installed plugin 0.14.1 `workflows/graph.yml`; `.helix.yml` binds
only project artifacts. No methodology catalog is copied into this repository.

**Ingest screening:** [Native publication evidence](02-design/spikes/native-ingest-evidence.md) and [eight-client reads](02-design/spikes/native-concurrency-evidence.md) add bounded object updates, journal/serving parity and old-version reads. Singleton latency still fails; sustained freshness and billion-node admission remain unproven.

**Latency/pruning:** [Persistent-client controls](02-design/spikes/native-latency-floor-evidence.md) expose a failed latency floor on current compute; [multi-file evidence](02-design/spikes/native-pruning-evidence.md) proves identity pruning and carrier parity while both latency gates remain unmet.

**Cold/continuous screening:** [Remote-I/O reads](02-design/spikes/native-cold-read-evidence.md) and [scheduled streams](02-design/spikes/native-stream-evidence.md) fail cold latency and freshness respectively. Parallel independent writes improve the stream but do not sustain its ten-second arrival interval.

**Driver/atomicity:** [Driver evidence](02-design/spikes/native-driver-evidence.md) passes bounded warm engine screening while caller latency remains unmet. [Catalog commits](02-design/spikes/native-atomic-evidence.md) support scoped native two-table commit/rollback in this workspace; full publication and external-reader compatibility remain open.

**Latest candidate evidence:** [Driver cold reads](02-design/spikes/native-driver-cold-evidence.md) favor 16MiB for further singleton testing. [Atomic stream](02-design/spikes/native-atomic-stream-evidence.md) still fails sustained freshness; [catalog-managed lookups](02-design/spikes/native-catalog-lookup-evidence.md) do not improve latency.

**10M canonical scale:** [Evidence](02-design/spikes/native-10m-scale-evidence.md) passes identity/full-carrier parity in 419 files. Repeated uncached-result, I/O-cached lookups reach 93–94ms engine p95 but 331–337ms caller p95. Engine screening passes after repeated keys; caller and billion-node admission remain open.

**Prebuilt producer:** [Ingest control](02-design/spikes/native-prebuilt-ingest-evidence.md) retains all transaction preservation checks and publishes six batches, but freshness still reaches 128s. Removing payload generation and duplicate validation does not make the full-copy serving layout sustain ten-second arrivals.

**Narrow serving:** [Evidence](02-design/spikes/native-narrow-serving-evidence.md)
passes all six exact snapshot hydrations and reduces active serving bytes about
90x. Freshness still reaches 105s. The explicit canonical residual variant remains
experimental; next compare staging overlap and version-pinned scalar projection.

**Pipeline:** [Evidence](02-design/spikes/native-pipeline-ingest-evidence.md)
preserves all checks and six published snapshot hydrations. Overlapping staging
reduces final oldest freshness to 76s, still above 60s. Atomic apply remains the
throughput bottleneck; pinned scalar projection and edge layouts are next.

**Pinned scalar projection:** [Evidence](02-design/spikes/native-pinned-projection-evidence.md)
preserves complete canonical/journal meaning and passes six snapshot/projection
checks. The short schedule ends at 57.5s oldest freshness, but queue delay grows
and atomic apply exceeds the arrival interval; sustained throughput is still open.

**Full schema:** [Evidence](02-design/spikes/native-full-contract-ingest-evidence.md)
passes six exact snapshots, all manifest descriptors and 1.2M unique journal keys.
Freshness remains below 53s in the two-minute run; queue growth and little
headroom leave sustained performance unproved.

**Edge layout:** [Evidence](02-design/spikes/native-edge-layout-evidence.md)
preserves complete carriers, typed endpoints and parallel-edge identity at 1M
edges. ID-first canonical clustering cuts singleton bytes about 17.6x; selective
narrow adjacency is a promising companion. Caller latency and edge-scale/ingest
admission remain unproved.

**Concurrent publication reads:** [Evidence](02-design/spikes/native-publication-reader-evidence.md)
preserves exact snapshots and bounded ingest freshness, but no-remote reads
overlapping writes fail at 151ms engine/552ms caller p95. The combined workload
is not admitted; compare ordinary Delta plus immutable manifests next.

**Ordinary Delta comparison:** [Evidence](02-design/spikes/native-ordinary-publication-evidence.md)
passes preservation and fixed-snapshot checks but still fails concurrent read
latency at 145ms engine/490ms caller p95 without remote I/O. Maximum freshness
is 59.8s; the publication mode does not close the combined gate.

**Burst:** [Evidence](02-design/spikes/native-burst-evidence.md) preserves all
1M changed rows but misses 60s freshness for the provisional ten-second 100k/s
scenario: newest 60.6s, oldest 70.6s. Current compute does not admit that burst.

**Edge property ingest:** [Evidence](02-design/spikes/native-edge-ingest-evidence.md)
passes 200k exact edge updates, journal uniqueness and pinned adjacency hydration
at 45.4s oldest freshness. Endpoints are fixed; steady combined rates, adjacency
mutation and a complete node/edge publication descriptor remain unproved.

**Graph descriptor and traversal:** [Exact vector](02-design/spikes/native-graph-vector-evidence.md)
pins all four graph tables and retains typed endpoints, parallel pairs and isolates.
[Traversal](02-design/spikes/native-fixed-vector-traversal-evidence.md) preserves
path multiplicity and meets bounded two-hop latency, but scans over 2M rows;
work-budget enforcement and degree-skewed admission remain open.

**Traversal guard:** [Evidence](02-design/spikes/native-traversal-budget-evidence.md)
rejects the high-degree case before path fetch and preserves regular parallel
paths and isolates. It bounds an explicit logical expansion metric, not physical
scans; regular guarded latency is 2.14s and production admission remains open.

**Server-side guard:** [Comparison](02-design/spikes/native-server-guard-evidence.md)
passes every result/refusal check across four shapes but regular caller p95
remains 2.57s. Fusing admission into a relational query is the next comparison;
logical/physical work and production-scale admission remain separate.

Latest traversal evidence: [fused admission probe](02-design/spikes/native-fused-guard-evidence.md) passes the bounded 2-second caller screen (regular p95 1.53s), retaining explicit hub refusal and fixed snapshots. The table-layout goal remains active; next prioritize scattered-update ingest and derived degree/adjacency publication, with singleton latency and billion-scale admission still open.


**Large-test iteration:** [Local/native 24M-carrier comparison](02-design/spikes/SPIKE-001-table-layout/scale-comparison.md) is complete. Native DVs avoid OSS whole-table update copying; corrected property-ID maps pass exact parity. Warm version-2 singleton budgets still fail (serial 218ms engine/525ms caller; four-reader 247ms/570ms). No billion-scale or full publication admission is claimed.


**Higher-entropy iteration:** [40.77GB local / GraphFrames evidence](02-design/spikes/SPIKE-001-table-layout/entropy-scale-status.md) passes 4M nodes/20M edges and 100M GraphFrames paths, with the known Truss shared-ID/unique-pair storage rules represented. Native larger-payload full validation remains active. The full performance/publication/billion-scale goal remains open.


**Native higher-entropy completion:** [Audited 40.71GB / 576-file native result](02-design/spikes/SPIKE-001-table-layout/out/native/ashlar_entropy_20261006_r86/audited-summary.json) passes full 4M-node/20M-edge carrier and synthetic Truss storage-rule checks. It extends preservation/file-count evidence; publication/performance and 1B/5B remain unqualified.

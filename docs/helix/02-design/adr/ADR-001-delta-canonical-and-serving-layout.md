---
ddx:
  id: ADR-001
  type: adr
  activity: design
  status: proposed
  authoring:
    home: repo
  links:
  - id: FEAT-002
    kind: informed_by
  - id: FEAT-003
    kind: informed_by
---

# ADR-001: Generic Delta canonical tables with typed serving projections

| Date | Status | Deciders | Confidence |
| --- | --- | --- | --- |
| 2026-10-06 | Physical design proposed; Unity Catalog Delta selected by owner | Ashlar owner | Exact preservation and bounded native workload evidence; billion-node capacity unmeasured |

## Context

The owner requires native Databricks singleton reads, a 1B-node graph with more
edges, proximity to Truss, and straightforward PuppyGraph, GraphFrames and
Microsoft Fabric Graph mappings. Exact UMF capabilities are deferred. Truss's
accepted generic layout uses canonical objects with property-ID maps, typed
edge endpoints, separate retained data and a property journal. Its PostgreSQL
partition/index/FK mechanisms do not transfer to Delta.

## Decision

The owner selects Unity Catalog managed Delta as the storage architecture.
Latency is measured for tuning and capacity planning; missing a provisional
benchmark does not reopen that choice or block table-design work.

Propose generic `object_current` and `edge_current` Delta tables as canonical
current state, preserving source catalog IDs and exact property/retained text.
Add rebuildable per-type/per-relationship scalar-column serving tables where
workload or graph mapping requires them. Native singleton lookup reads canonical
Delta directly at an evidenced publication boundary.

Use identity-oriented liquid clustering for canonical singleton access. The
current measured implementation candidate computes a SHA-256 `lookup_hash`
from the exact source/type/id tuple and clusters by that value; the native tuple
remains the semantic key and an exact SQL predicate. The hash is rebuildable
physical metadata, never identity, uniqueness enforcement or endpoint authority.
For edges use relationship/id identity, not endpoint-only clustering. Endpoint
access belongs in a narrow, rebuildable adjacency projection carrying edge IDs
and both typed endpoint tuples; reverse access is a separately justified layout.

Start with no explicit canonical partition directories. The four-bucket
partition/Z-order comparison does not justify a universal billion-scale bucket
count and has higher measured cleanup cost in the paired fixture. A 64MiB target
is a reasonable initial hash-clustered tuning candidate for fewer files, not a
normative file-size bound or a proved ingest winner. Record actual file sizes,
pruning, deletion vectors and total storage; preserve the 16MiB comparison for
workloads where smaller reads matter. Change these settings without changing the
logical contract. Do not schedule full cleanup after every batch: measured
maintenance costs require an independently tuned policy.

The proposed ashlar-delta/0.3 DDL now includes derived identity hashes, optional
adjacency/degree tables, raw source records and direct cursor references. All 13
table CREATEs have scoped native structural evidence. Later edge-statistics tuning
is evidenced on existing native tables; the revised complete DDL has not been
re-executed as a package. The 0.2 and 0.1 DDL remain historical
baselines; production source, recovery and graph-engine qualification are open.
The 13-table spike inventory is a reference surface, not a requirement to deploy
all projection examples or rewrite every table for every source batch.

Include the journal batch ID in data-skipping statistics alongside its origin/id
columns. Native r100 backfill preserves all 900k rows and protocol/file layout,
and reduces paired validation file reads from 100 to 26. It does not materially
reduce measured bytes or latency. Keep statistics backfill cost and actual pinned
versions explicit; old manifests do not gain the new snapshot automatically.
This physical tuning does not change the logical journal or qualify freshness.

The [publication and maintenance policy candidate](../spikes/SPIKE-001-table-layout/publication-maintenance-policy.md)
separates physical cleanup from logical progress and records the measured publisher
capacity gap. Its maintenance-manifest mode and queue experiment remain unqualified;
existing immutable publication vectors must not be repointed.

## Alternatives

| Option | Benefits | Costs | Evaluation |
| --- | --- | --- | --- |
| Generic property bags only | Closest fixed table surface to Truss | JSON parsing and poor property statistics; awkward scalar graph mapping | Keep as canonical, not sole analytic surface |
| Typed canonical tables for every type | Native scalar filtering and direct graph mapping | Canonical schema migration per type; diverges from Truss | Not proposed as canonical |
| Shared canonical plus scalar serving projections | Preserves Truss semantics; selected types map cleanly | Extra storage, synchronization and revision checks | Proposed, pending native evidence |
| Shared promoted columns only | Fewer serving tables | Global wide sparse schema, unrelated-type evolution | Benchmark alternative, not default |
| Property EAV as canonical Delta | Property journal resemblance | Reassembly and join amplification for object reads | Journal only initially; not benchmarked as canonical |

## Consequences

Canonical schema evolution remains generic. Hot analytical properties incur
versioned projection columns and validation; unknown data remains in canonical
retained text. Typed projections carry independent edge IDs and typed endpoint
keys, so parallel edges and isolated nodes survive. Projection inventory and
publication manifest add operational overhead.

Native Databricks reads remain independent of external graph-engine refresh.
Fabric is a bounded optional export: its documented approximately 2B total graph
elements cannot substantiate the full 1B-node-plus-more-edges target. A full graph
may use native SQL/PuppyGraph/Spark only after their independent scale evidence.

## Risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Singleton latency or cost grows at 1B nodes | Capacity or service objectives missed | Measure cold/warm/concurrent reads and bytes/files; tune execution/layout while retaining UC Delta and exact identity |
| Clustering features rejected by external Delta reader | Integration fails | Verify exact protocol/features; explicit immutable compatible export |
| Serving copies and reverse edges dominate cost | Storage/refresh budgets missed | Promote only measured workload; count canonical+all projections+retained versions |
| Truss property feed mapped as whole-document writes | Lost version/transaction semantics | Preserve journal; explicit adapter and differential corpus |
| Independent table writes leak partial state | Incorrect query answers | Version manifest and pinned reads; engine-specific immutable release proof |

## Validation

[Native evidence](../spikes/native-layout-evidence.md) records successful DDL,
carrier/version probes and bounded uncached query measurements. The initial
provisional singleton latency gate is not satisfied on the measured path; this
record remains proposed for its physical/schema details; Unity Catalog Delta
is already selected by the owner.


[SPIKE-001](../spikes/SPIKE-001-table-layout.md) supplies local result-parity and
columnar screening. [CONTRACT-003](../contracts/CONTRACT-003-delta-graph-tables.md)
defines the proposed table surface. Complete the table design through versioned DDL, publication failure semantics,
source fidelity and explicit engine mapping contracts. Record latency, ingest,
maintenance and scale measurements with their workload/compute scope; they
inform tuning and resource sizing rather than veto the storage architecture.
External connector support is claimed only after connector-specific evidence.
Reconsider projection inventory and maintenance policy when their costs grow.

## Concern Impact

The large higher-entropy update now has [routine maintenance evidence](../spikes/SPIKE-001-table-layout/out/native/ashlar_entropy_maintenance_20261006_r91/audited-summary.json):
13 seconds rewrites only the 333MB update layer, retains baseline deletion
vectors, and passes exact parity across all 20M edges. [Maintained reads](../spikes/SPIKE-001-table-layout/out/native/ashlar_entropy_maintained_reads_20261006_r92/audited-summary.json)
prune two files instead of seventeen. This supports evaluating routine
incremental clustering before full cleanup, with overlap/read-amplification
and retained-storage measurements; it does not establish a universal threshold
or per-batch policy. Existing descriptors retain their old versions. A consumer
needs a new validated descriptor to benefit from the maintained snapshot.

The [incremental publication validation candidate](../spikes/SPIKE-001-table-layout/incremental-publication-design.md)
proposes validated immutable predecessors plus complete authoritative change sets
for routine batches, with exhaustive bootstrap/periodic audits. It does not make
a full-graph comparison a per-batch requirement at 1B/5B scale. Replacing an
exhaustive check requires independent proof of source completeness, writer
authority, affected-row preservation and projection coverage. The synthetic
large baseline lacks retained raw origins, so changed-row origin evidence cannot
promote it to a fully qualified production publication.

Preservation requires exact bags, retained content and projection residuals.
Enforcement remains explicit: Delta semantic keys/relationships are publisher
checks. Scope stays on storage/query proof; UMF authoring and production resource
provisioning remain separate. No Truss language or runtime ADR is inherited.

## References

- [Truss ADR-002](/Users/erik/Projects/truss/docs/helix/02-design/adr/ADR-002-storage-strategy.md)
- [Databricks clustering](https://docs.databricks.com/aws/en/tables/clustering)
- [Fabric limitations](https://learn.microsoft.com/en-us/fabric/graph/limitations)

## Additional experiment evidence

The following records preserve historical measurements and candidate evaluations.
Their earlier latency-gate and next-experiment language is superseded by the
2026-10-06 owner direction and delivery sequence in this ADR.

The [REST controls](../spikes/native-latency-floor-evidence.md) fail both warm latency budgets on that execution path. Subsequent [SQL-driver evidence](../spikes/native-driver-evidence.md) reaches uncached warm engine p95 76–96ms on the bounded 1M-object fixtures; caller p95 remains above 250ms. A transport/query-shape matrix confirms that UUID alone does not explain the difference. The [multi-file comparison](../spikes/native-pruning-evidence.md) validates real id pruning, including 31 of 32 liquid files, while exact carriers remain equal. Keep this ADR proposed: caller, driver cold/concurrent reads, ingest and billion-node/cost admission remain open. Diagnostic routing buckets are not part of the proposed canonical contract.

The [catalog-commit probe](../spikes/native-atomic-evidence.md) confirms native two-table commit and intentional rollback with exact stored carriers. This is a separate experimental publication candidate, absent from the existing canonical DDL. Its full progress/version-vector recovery and external-reader compatibility must be proved before adoption; no workspace preview setting changed.

[10M-node canonical evidence](../spikes/native-10m-scale-evidence.md) now proves
full stored-carrier and identity parity with source/type/id clustering in 419
files. Repeated-key driver controls reach engine p95 93–94ms with zero remote
bytes and zero result-cache hits; caller p95 remains 331–337ms. This supports
continued evaluation of small liquid files without accepting a production
layout. The source/type distribution, concurrent readers, edge identity and
adjacency workloads, incremental publication, and billion-node file-count
behavior remain unproved at this scale. Catalog-managed tables were slower in
the paired singleton test, so their potential atomicity benefit must be judged
separately from read latency.

[Canonical edge comparison](../spikes/native-edge-layout-evidence.md) now favors
evaluating source/relationship/id clustering for canonical edge singletons,
with selective narrow endpoint adjacency for traversal. In the bounded 1M-edge
fixture it prunes 17 of 18 files rather than scanning all nine endpoint-clustered
files, preserves exact carriers and parallel-edge identity, and reduces median
singleton bytes about 17.6x. Both layouts still miss caller latency. This refines
the next experimental candidate, not the accepted design or normative DDL;
edge publication, multi-file adjacency, degree skew and billion-edge admission
must be demonstrated before selection.

[Concurrent publication reads](../spikes/native-publication-reader-evidence.md)
now contradict combined-workload latency admission on current compute: exact
old snapshots remain intact and bounded ingest freshness stays below 54s, but
no-remote reads overlapping catalog-managed writes reach 151ms engine p95 and
552ms caller p95. Keep the ADR proposed. Compare manifest-published ordinary
Delta operations and explicitly bounded compute separation before choosing a
publication mechanism; small idle read results cannot close this gate.


## Delivery sequence after the spike

1. Qualify the proposed CONTRACT-003/0.3 surface against the real source profile.
   The versioned DDL and tiny native fixtures exist; complete extraction,
   interpretation and recovery are still pending. Keep publisher receipts/fences
   separate from producer origins.
2. Implement one bounded publisher/read path with exact carrier validation,
   actual committed version resolution, durable recovery and pinned snapshots.
   Use the proven controls as evidence, not a claim that real Truss feed ordering
   or production permissions are already solved.
3. Specify per-engine node/edge mapping, exact keys, scalar casts/residuals,
   publication binding and supported Delta feature profile. GraphFrames consumes
   pinned Spark frames; PuppyGraph and Fabric need their own evidenced release
   adapters. Fabric remains a bounded export.
4. Publish an operational measurement sheet for singleton/traversal, ingest,
   maintenance, bytes/files and capacity sensitivities at 1B nodes/5B edges.
   Separate observed results, estimates and unmeasured scale; avoid further
   latency-only loops or optional Real-Time provisioning as prerequisites.


## Draft Truss feed coverage discovered after 0.2

The Truss draft feed/journal contracts at `spec/change-feed-and-groups`, `6d87fce`, carry origin metadata, revision documents, provenance and reservations in addition to property events. The 0.2 Delta journal is insufficient as the sole replication record. Propose a supplemental exact source-record carrier (CONTRACT-003), separately versioned before adoption, preserving complete transport envelopes and native tuple cursors. Current tables stay canonical serving state; raw feed retention preserves uninterpreted input and does not imply that every operation can be applied. Unknown operations that affect current-state interpretation stop publication rather than being silently skipped. UMF binding remains deferred.

Raw-record idempotence is keyed by a qualified feed/epoch/delivery ID, with byte-exact cursor/kind/payload/revision conflict detection. A single-table Delta MERGE is a candidate append/refusal operation, not a concurrency-safe unique constraint. A serialized/fenced writer or independently proved protocol is required before production replay safety; cross-table current/history/manifest publication remains its own protocol. Source checkpoints advance only after the relevant publication is durable.


## Deployment inventory and write amplification

The logical graph uses six durable table roles: object_current, edge_current,
property_journal, source_record, tombstone and publication_manifest. source_record
is required for the proposed native feed profile's complete envelope retention;
legacy fixtures have separately scoped semantics. Journal and raw capture preserve
different information and neither substitutes for the other. Empty tombstone or
journal tables still participate when the chosen publication/read profile uses them.

Three structural tables are optional: adjacency_forward, adjacency_reverse and
degree_summary. Select direction and relationship coverage from actual workloads;
a missing projection is an unsupported query capability, not evidence of zero
edges. The two node_type_a/edge_ab tables are illustrative typed projections.
Deploy versioned per-engine releases only for the selected graph subset and cast
profile. External releases may refresh less often than native publications, but
must expose the publication they actually represent.

publisher_fence and apply_receipt are two coordination examples. A recoverable
publisher needs durable authority and application evidence even when its selected
mechanism stores them elsewhere. The 0.3 receipt lacks complete predecessor/output
bindings; r78 uses an isolated supplemental record and does not qualify that DDL
as a complete recovery protocol. Initial experiments remain serialized with no
automatic writer takeover.

Rewrite only tables whose data changed. Every consumed table still appears in the
publication vector. A property-only update writes its canonical
carrier, accepted journal events and raw input; it need not rewrite unchanged
adjacency or degree rows. Endpoint or lifecycle changes require structural updates
and endpoint validation at the final source boundary. Typed projections change
only when their selected content or schema changes. A manifest may reuse an
unchanged table's prior committed version after validation; versions across tables
need not have equal numbers or commit times.

This limits avoidable copying at the 1B-node/5B-edge planning scale while retaining
complete canonical and source evidence. It does not reduce the required history or
prove throughput. Raw feed, journal fanout, old pinned versions, reverse adjacency
and engine release copies must all enter capacity estimates. No expiry/VACUUM or
full-scale admission follows from the current estimates. Tuple-native sources
leave source_position null; the existing journal clustering consequently has an
unused component for that profile. Delivery-ID/source-cursor clustering needs a
separate workload comparison before changing the executed DDL.


## Billion-scale file and history sensitivity

[Capacity sensitivity 0.3](../spikes/SPIKE-001-table-layout/out/capacity-planning-v03.json)
separately rounds estimated files for each physical table. At 1B objects/5B edges,
assumed compressed current carriers of 512–2,048 bytes, 96-byte adjacency rows,
two adjacency copies and a full 64MiB mean file imply 60,083–197,412 active files
for those four tables. Half-full files or a 16MiB target increase that inventory.
This excludes Delta logs/checkpoints, old pinned versions, history and releases;
the target property does not guarantee the assumed mean size.

At continuous 10k changed entities/s for 30 days, assuming one 1KiB compressed
raw envelope per entity and 1/4/10 separate 1KiB property events per entity gives
53.084/132.710/291.963 decimal TB for journal plus raw capture. The corresponding
estimated full-64MiB file counts are 791,016/1,977,540/4,350,587. Real feed records,
revision documents and retained versions can add more. These assumptions are not
observed compression, an adopted retention horizon or a spending authorization.
Journal fanout and source record cardinality must come from the qualified feed.

The model also shows a scattered-update sensitivity: 300k independently uniform
edge changes touch an expected 99.96%/86.00% of baseline edge file groups in the
512/2,048-byte full-64MiB scenarios. This is a mathematical occupancy estimate;
it says nothing about deletion-vector cost or how many files Delta rewrites.
Identity clustering may prune singleton reads while dispersed updates still
visit many files. Larger experiments should measure this distinction and metadata
planning cost instead of inferring ingest scalability from small selective reads.
No billion-scale runtime admission or history expiry follows from this model.


## Authorized 24M-carrier local/native comparison

[Scale comparison](../spikes/SPIKE-001-table-layout/scale-comparison.md) measures
4M objects / 20M edges and 200k scattered updates. Native DVs avoid the OSS
100x output-row amplification (zero unchanged copies versus 19.8M). Retain that
capability in the native candidate; consumer compatibility belongs to the
separately qualified release. A FULL optimization changed no files/version, so
maintenance effectiveness must be observed rather than promised. Corrected
property-map version-2 uncached reads fail provisional warm budgets: serial
218ms engine/525ms caller, four-client 247ms/570ms. Architecture remains UC Delta.
The complete current-carrier column surface and endpoint closure are measured;
real source/history/publication, realistic entropy and billion-scale admission
remain open. CTAS here does not prove production constraint enforcement.

## Subsequent scale and consumer findings

The higher-entropy iteration measures about 41 GB for 4M nodes / 20M edges both
locally and natively, with full-field preservation and typed closure. Native
100k large-token publication and singleton overlap are now evidenced; their
strict provisional budgets remain unsatisfied.
[r103](../spikes/SPIKE-001-table-layout/out/native/ashlar_maintained_contention_20261007_r103/audited-summary.json)
keeps the reader at three files at p95 after maintenance, yet publication overlap
increases caller p95 to about 1.10 s; quiet new-publication reads are about 374 ms.
Its finite pre-staged batch takes about 64 s after complete input readiness,
excluding source preparation. This does not admit sustained 10k/s or burst rates.
Further layout pruning alone is insufficient evidence for shared-compute latency.
The next performance comparison should isolate reader and publisher resources
or measure a declared larger compute profile, with explicit cost bounds and the
same exact-carrier workload; no resource change is implied by this document.

Actual PuppyGraph 1.13.0 mapping passes locally via an immutable DuckDB carrier
fixture. It requires ordinary-property aliases for identity carriers. Those
aliases are serving-only and leave canonical Truss-like tables unchanged.
Its observed rejection of an in-place model/catalog change means release
activation must be independently qualified. Direct UC access currently fails
the metastore external-access prerequisite. Keep canonical DV/row-tracking
features; neither a DuckDB success nor a permission failure establishes Delta
feature compatibility. GraphFrames already executes the 24M-element local
release; bounded Fabric and direct UC graph-engine tests remain open.

[r108 high-degree adjacency sensitivity](../spikes/SPIKE-001-table-layout/out/native/ashlar_hub_adjacency_r108/audited-summary.json)
adds measured support for a separate narrow endpoint-clustered candidate:
20M synthetic edges in240MB/8files, with900004/100004-edge hubs pruning to one
file for count and first100-row page. Typed closure, unique IDs, unique relationship
endpoint pairs and full projection equality pass. First-page reads still consume
37.4/5.1MB and do not prove deep-page ordering/cost. This is an altered synthetic
graph, not a published canonical projection or billion-scale support. Keep
identity clustering on canonical edges and evaluate endpoint/query ordering on
the optional adjacency surface separately.

### Adjacency ordering qualification boundary

[r112 native relationship-first pages](../spikes/SPIKE-001-table-layout/out/native/ashlar_hub_contract_pages_r112/audited-summary.json)
preserve `(rel_type_id,edge_id)` continuation exactly on the synthetic20M-edge
hub graph. Both eight-file layouts scan all8files at each tested relationship
cursor. Earlier edge-ID-only8/4/1file pruning is not evidence for the contractual
order. Keep relationship-first semantics and native edge identities; source/type/
endpoint clustering and optional relationship clustering must be evaluated on
that query shape. The seven-column fixture omits structural_version and does not
qualify the complete reference adjacency DDL or published structural coverage.
No canonical identity-layout change, cursor-order change or billion-scale
admission follows. The proposed reference DDL remains unchanged.

[r113 full-shape forward adjacency](../spikes/SPIKE-001-table-layout/out/native/ashlar_hub_contract_pages_r113/audited-summary.json)
adds20M-row eight-column parity, including a constant synthetic structural version,
and36 exact contractual page checks. Relationship-range writes with the proposed
forward clustering prune8/4/1files at successive relationship cursors; the
edge-range control stays at8files. This supports matching writer file statistics
to `(rel_type_id,edge_id)` order, not a universal partition count or batch width.
Keep reference DDL and canonical identity clustering unchanged. The explicit
range-batch mechanism, exceptionally low entropy, different control column count,
CTAS constraints, unbound synthetic structural revision and noisy caller timings
limit the evidence. Automatic maintenance, scattered updates, reverse access
and real source/publication authority still require qualification.


### Full20M optional bucket-copy preservation evidence

[r167–r169 audited parity](../spikes/SPIKE-001-table-layout/out/native/ashlar_bucket_parity_r169/audited-summary.json) proves all20 logical fields of canonical E23 equal all20M rows in the owned64-bucket table at version4. Every text field uses UTF8 binary equality; null-safe typed comparison covers the other fields. Global nonnull uniqueness and exact disjoint joined counts establish complete membership, with25 native changed-value refusals and two membership/duplicate counterexamples. This closes the initial copy's wide-value obligation for that fixed snapshot. It does not replace canonical hash liquid clustering, qualify the CTAS constraint surface as canonical DDL, select a billion-scale bucket count, prove later maintenance/update preservation, or improve any measured singleton/ingest gate. The owned copy remains unpublished and retained for the next bounded physical comparison; Truss-compatible logical identities and maps remain unchanged, with UMF deferred.


### Full-size bucket/ZORDER comparison remains experimental

[r170–r175 comparison](../spikes/SPIKE-001-table-layout/out/native/ashlar_bucket_full_reads_r175/comparison-summary.json) now measures the full20M owned bucket copy before and after actual partition-local ZORDER:528 files become448, point-read file p95 becomes2→1, and28 no-remote post-repeat reads have engine p9599ms/caller416.783ms. The all30 post-repeat sample has engine151ms and2 remote reads. Caller latency still misses the provisional250ms target, and cache/load differences prevent a causal partitioning claim. A33.156GB physical rewrite is a distinct maintenance cost; its native read telemetry is incomplete. Initial copy4 is exhaustively value-equal to E23; wide parity at rewritten8 remains pending. Retain canonical hash liquid clustering and the close-to-Truss logical surface;64 buckets and64MiB are experiment settings, not a1B/5B selection.


### Full20M update cost favors retaining the canonical LC candidate

[r176/r177](../spikes/SPIKE-001-table-layout/out/native/ashlar_bucket_update_r176/audited-summary.json) applies one same-length wide100k property update: LC clone MERGE4.110s/326.515MB/0 copied rows, maintained bucket MERGE37.018s/5.254GB/2,987,212 unchanged copied rows plus381DVs. Both changed-carrier20-field checks and20M global IDs pass;0 inserts/deletes. Differing file histories prevent a partition-only causal claim, but the measured write amplification keeps bucket64 experimental and canonical hash LC unchanged. The75GB/4GB local cost guard stopped after77.071GB reads/6.244GB writes, before point reads; no native failure or replay occurred. Final bucket-wide preservation (including preceding ZORDER) remains required; copied rows cannot be justified by unchanged physical-file custody. Single-MERGE cost is not publisher/sustained-rate admission, and no UMF binding is added.


### Final preservation evidence r178–r180

The final bucket version9 now passes full20M/20-field exact equality against E23 plus intended stage0 after ZORDER and the100k update, including copied unchanged rows. The LC clone0/MERGE1 passes19.9M unchanged identity/file/row-position custody, combined with prior exact100k changed-output checks. The bucket wide validation exceeded its140GB read plan at177.558GB; the preserved controller stop is a cost failure, while all four native comparisons passed. LC custody used4.273GB reads. No SQL was replayed, no canonical publication changed, and no latency, sustained-rate or billion-scale admission follows. Canonical hash LC remains proposed and bucket64 experimental. Evidence: [bucket audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_bucket_final_parity_r178/audited-summary.json), [LC custody](../spikes/SPIKE-001-table-layout/out/native/ashlar_bucket_lc_custody_r179/summary.json).


### Range-aligned hot-output candidate r187–r188

A fresh owned20M LC clone applies the same100k wide stage with a6-range lookup_hash input hint, preserving all20 changed fields,19.9M unchanged physical custody and20M unique identities. Actual six hot-file ranges narrow to16–18% each; same20-key operational read p95 improves from8files/403.803MB/179ms engine/433.354ms caller to3files/139.231MB/104ms/365.700ms. MERGE writes326.548MB,0 copied rows, caller6.903s; whole run stays within budget. Canonical publication is unchanged. This supports a range-aligned writer tuning candidate, not a production range count, randomized causal SLO claim or revised architecture. Both warm gates, integrated sustained/burst publication, cold and1B/5B remain unproved. Evidence: [owned native audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_lc_range_update_r187/audited-summary.json). Qualify final file shape rather than assuming a source hint controls every writer path; UMF binding remains deferred.


### Integrated range-input qualification limit r191–r194

A resumed isolated100k publisher passes exact raw/wire/origin/current20-field/journal/20M structural and identity checks plus19.9M unchanged physical custody; its owned manifest is verified, canonical publication unchanged. Measured processing59.883s and modeled record-age p9569.386s fail the60s freshness target for this finite controller run; no sustained-rate admission follows. The6-range MERGE source hint produces16 nearly global hot-file ranges in the integrated path, unlike standalone r187. Thus the hint is not a reproducible writer-output contract and the earlier singleton improvement cannot be advertised for this publication. Canonical UC Delta/hash LC remains selected/proposed; range shaping remains experimental pending writer-behavior qualification. Evidence: [publication audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_queue_resume_r192/audited-summary.json), [actual output shape](../spikes/SPIKE-001-table-layout/out/native/ashlar_publication_shape_r194/summary.json). UMF binding and full performance/scale gates remain open.


### Writer isolation evidence r195–r196

Two new sequential E23 shallow clones apply the prior eight-file and fresh four-file100k inputs, both producing6 disjoint narrow live hash ranges. Exact20-field changed values,19.9M unchanged physical custody and20M unique IDs pass; cross-stage native identities/endpoints/hashes match. Four source files alone do not explain the integrated16 broad files, but concurrency causation and writer determinism remain unproved. Canonical UC Delta/hash LC remains selected/proposed and range hints experimental. Next compare isolated current emission after raw/journal writes, preserving the full validation/manifest barrier and measuring its freshness cost. Evidence: [writer isolation audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_lc_writer_isolation_r195/audited-summary.json). No singleton, sustained, cold or1B/5B qualification transfers from this write-only comparison; UMF binding remains deferred.

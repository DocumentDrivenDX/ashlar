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

The existing candidate DDL still represents the earlier native-key clustering
baseline. Its next revision must explicitly version the derived hash profile and
add adjacency/degree DDL rather than imply those experimental columns already
belong to the normative schema. Native DDL, exact preservation and publisher
checks qualify that revision; speed remains a reported metric.

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

1. Revise CONTRACT-003 and its versioned DDL together: generic current tables,
   exact property journal, tombstones and immutable manifest; identity hash as
   derived metadata; narrow typed adjacency and degree summaries as optional
   projections. Keep publisher receipts/fences separate from producer origins.
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

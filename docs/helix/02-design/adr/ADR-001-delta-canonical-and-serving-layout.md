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

## Context

Ashlar must support low-latency singleton lookup directly on Databricks and a
planning graph of 1B nodes with more edges. The physical layout should remain
close to Truss while mapping explicitly to PuppyGraph, GraphFrames and Microsoft
Fabric Graph. Truss-style property-ID maps, typed endpoints, retained content and
property history must survive replication. PostgreSQL indexes, foreign keys and
partition mechanisms do not transfer automatically to Delta.

## Decision

Use Unity Catalog managed Delta tables as canonical storage. Performance
measurements guide physical tuning and capacity planning; they do not determine
whether this storage architecture is used.

Use generic `object_current` and `edge_current` carriers, preserving native
source/type/entity identities, exact property values and uninterpreted retained
content. Add rebuildable typed scalar projections for selected workloads and
engine mappings. Native singleton lookup reads canonical Delta at a pinned,
readable publication boundary, independently of external graph refresh.

### Physical access and maintenance

Start canonical tables without explicit partition directories and use
identity-oriented liquid clustering. Derive `lookup_hash` with SHA-256 from the
exact source/type/id tuple; retain the complete tuple as the semantic key and
exact lookup predicate. Hashes provide pruning, never identity, uniqueness or
endpoint authority. Edge identity includes relationship and edge ID; endpoints
alone cannot identify parallel edges. Use bound query parameters.

Use 64MiB as an initial file-size tuning target, not a correctness constraint.
Select partitioning, Z-order alternatives, file targets and statistics from
whole-workload costs, including reads, ingest, maintenance and retained storage.
Include journal batch IDs and identity columns in data-skipping statistics.
Trigger maintenance from file overlap and read amplification rather than running
full cleanup after every batch or blindly visiting every hash range.

Preserve predictive optimization. Publication readability must depend on the
actual optimization configuration and retained snapshot guarantees. Physical
maintenance cannot repoint an existing immutable publication: exposing a
maintained snapshot requires a separately validated publication.

### Table roles and source preservation

The durable data roles are `object_current`, `edge_current`, `property_journal`,
`source_record`, `tombstone` and `publication_manifest`. Complete raw source
records and accepted semantic journal events preserve different information;
neither substitutes for the other. Preserve transport envelopes, source cursors,
revision documents, provenance and uninterpreted operations. An unknown operation
that can affect current-state interpretation stops publication.

Raw-record idempotence uses qualified feed/epoch/delivery identity with byte-exact
cursor/kind/payload/revision conflict detection. Delta MERGE is not a concurrency
safe unique constraint. Require serialized, fenced writer authority or another
independently proved protocol. Advance source checkpoints only after the relevant
publication is durable; remote acknowledgement requires its own recovery proof.

Optional structural roles are forward adjacency, reverse adjacency and degree
summaries. Choose direction and relationship coverage from query requirements.
An unavailable projection means an unsupported capability, never zero edges.
Typed node/edge projections carry independent edge IDs and complete typed endpoint
keys, retaining isolated nodes, self-loops and parallel edges.

Publisher authority and apply receipts must be durable, with complete predecessor,
input and output bindings, even if the chosen mechanism stores them outside Delta.
Enforce semantic uniqueness, relationship validity and final-boundary endpoint
integrity through the publisher; descriptive model constraints are insufficient.

### Publication and write amplification

Bind every consumed table by native identity and committed version in an immutable
publication vector. Table versions need not share numbers or commit times. Rewrite
only changed roles: a property-only update need not rewrite unchanged adjacency,
degree or unrelated projections. An unchanged role may reuse its previously
validated committed version.

Use immutable validated predecessors and complete authoritative change sets for
incremental publication. Replacing exhaustive comparison requires proof of source
completeness, writer authority, affected-row preservation and projection coverage.
Use exhaustive bootstrap and periodic audits without making full-graph comparison
a requirement for every batch.

### Engine releases and scale

Publish engine-specific immutable releases with explicit publication bindings,
identity mappings, scalar casts, residual content and Delta feature compatibility.
GraphFrames consumes pinned Spark frames. PuppyGraph and Fabric require independent
release activation, readback, rollback and refresh semantics. Export compatible
snapshots when a reader cannot consume canonical Delta features directly.

Keep Fabric exports within the actual target's supported element and capability
limits; do not assume a bounded export supports the full planning graph. External
releases may refresh less often than native publications, but must expose the
publication they represent.

Account for canonical data, raw records, journal fanout, tombstones, forward/reverse
adjacency, typed releases, retained versions, staging, logs and failed work in
capacity and cost estimates. The 5B-edge planning assumption is not a measured
production history profile. Retention and expiry require reader-pin and recovery
proof; capacity estimates cannot authorize destructive cleanup.

## Alternatives

| Option | Benefits | Costs | Evaluation |
| --- | --- | --- | --- |
| Generic property bags only | Closest fixed table surface to Truss | JSON parsing and poor property statistics; awkward scalar graph mapping | Keep as canonical, not sole analytic surface |
| Typed canonical tables for every type | Native scalar filtering and direct graph mapping | Canonical schema migration per type; diverges from Truss | Not proposed as canonical |
| Shared canonical plus scalar serving projections | Preserves Truss semantics; selected types map cleanly | Extra storage, synchronization and revision checks | Proposed physical layout |
| Shared promoted columns only | Fewer serving tables | Global wide sparse schema, unrelated-type evolution | Optional workload-specific projection |
| Property EAV as canonical Delta | Property journal resemblance | Reassembly and join amplification for object reads | Use for semantic history |

## Consequences

Canonical schema evolution remains generic. Selected analytical properties add
versioned scalar columns and projection validation. Unknown content remains
preserved in canonical retained text. The publication manifest and engine releases
add synchronization and storage costs in exchange for explicit, reproducible
query boundaries.

## Risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Singleton latency or cost grows at 1B nodes | Capacity or service objectives missed | Measure cold/warm/concurrent reads and bytes/files; tune execution/layout while retaining UC Delta and exact identity |
| Clustering features rejected by external Delta reader | Integration fails | Verify exact protocol/features; explicit immutable compatible export |
| Serving copies and reverse edges dominate cost | Storage/refresh budgets missed | Promote only measured workload; count canonical+all projections+retained versions |
| Truss property feed mapped as whole-document writes | Lost version/transaction semantics | Preserve journal; explicit adapter and differential corpus |
| Independent table writes leak partial state | Incorrect query answers | Version manifest and pinned reads; engine-specific immutable release proof |

## Validation

Verify complete carrier, identity, endpoint and multiplicity preservation;
source replay/conflict handling; interrupted publication recovery; pinned reads;
retention; and engine-specific release behavior. Measure singleton, traversal,
ingest and maintenance performance with workload, compute and cost scope. Keep
new checks small and reuse existing measurements. Connector support requires
actual connector-specific execution evidence.

The table contract is [CONTRACT-003](../contracts/CONTRACT-003-delta-graph-tables.md).
Historical measurements and implementation observations are retained in
[build evidence](../../04-build/evidence/documentation-history-20261009/ADR-001.original.txt).

## References

- [Truss ADR-002](/Users/erik/Projects/truss/docs/helix/02-design/adr/ADR-002-storage-strategy.md)
- [Databricks clustering](https://docs.databricks.com/aws/en/tables/clustering)
- [Fabric limitations](https://learn.microsoft.com/en-us/fabric/graph/limitations)

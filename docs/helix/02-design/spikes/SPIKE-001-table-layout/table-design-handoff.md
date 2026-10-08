# Table design handoff

This consolidates proposed CONTRACT-003/ADR-001 for the FEAT-002 schema-package
slice and US-002. Exact schema and shared interfaces remain owned by
[CONTRACT-003](../../contracts/CONTRACT-003-delta-graph-tables.md),
[publication CONTRACT-001](../../contracts/CONTRACT-001-publication-boundary.md)
and [read CONTRACT-002](../../contracts/CONTRACT-002-consumer-read-boundary.md).
No new runtime language, wire format, source authority or UMF binding is selected.
Owner direction stops further testing; this handoff requires no benchmark run.

## Concrete table inventory

Use the column definitions in [ashlar-delta/0.3 DDL](sql/delta-layout-v03.sql).
The13 CREATEs are a reference surface; the initial package separates these roles:

| Table | Logical key / purpose | Initial physical layout | Deployment scope |
| --- | --- | --- | --- |
| object_current | Exact source_system/type_id/id; independent nodes, property map and retained content | Unpartitioned Delta, liquid cluster lookup_hash,64MiB target,zstd | Baseline |
| edge_current | Exact source_system/rel_type_id/id; both typed endpoints, independent parallel edges | Unpartitioned Delta, liquid cluster lookup_hash,64MiB target,zstd | Baseline |
| source_record | source_feed/source_epoch/delivery_id; exact cursor, original payload and digest | Liquid cluster feed/epoch/delivery | Baseline |
| property_journal | Entity/version/property event with source origin and event ordinal; missing/null preserved | Liquid cluster feed/epoch/position/id; batch statistics | Baseline |
| tombstone | Typed entity identity plus version and origin; stale resurrection protection | Liquid cluster source/type/id | Baseline |
| publication_manifest | Immutable publication descriptor naming complete table-version vector and source progress | Ordinary Delta; small control table | Baseline |
| adjacency_forward | One row per independent edge, source and target typed tuples | Liquid cluster source/source_type/source_id/relationship | First traversal projection |
| adjacency_reverse | Same edge set, alternate physical ordering | Liquid cluster source/target_type/target_id/relationship | Only for reverse traversal |
| degree_summary | Typed identity/relationship/direction counts | Liquid cluster source/type/id/relationship | Only for evidenced count workload |
| node_type_a / edge_ab | Illustrative scalar type/relationship projections | Clustering selected for target workload | Examples, instantiated for chosen consumer schema |
| publisher_fence / apply_receipt | Coordination and stage receipt candidates | Ordinary Delta | Selected publisher protocol only; DDL does not prove fencing |

The exact native tuple is identity. lookup_hash is derived pruning metadata,
never a uniqueness constraint. Do not allocate new production IDs using the
synthetic append profile: that profile exists only to avoid fixture collisions.
Keep producer catalog IDs unchanged. JSON text retains lexical numbers, missing
versus null, unknown fields and nested content; promoted scalar columns never
replace the canonical source. Validate and report unsupported scalar mappings.

## Writes, visibility and reads

Accept a declared source profile before decoding updates. Retain original source
records and compare exact replay/conflict evidence. Apply current/history/delete
changes under the version and refusal rules of CONTRACT-001. Validate uniqueness,
typed endpoint closure, source references, property semantics and the complete
accepted batch. Delta NOT NULL is write-enforced; logical keys, endpoint integrity,
cardinality and cross-table consistency require publisher checks. Informational
constraints must be labelled accordingly.

Record physical commits independently for each role, then publish one complete
immutable descriptor only after validating all selected versions. A failed or
interrupted stage leaves the previous publication visible. Reader requests fix
one descriptor and validate table identity, schema revision and retained version;
substitute neither latest physical heads nor a mixture of publications. Maintenance
advances physical heads without implicitly advancing logical source progress.

Native singleton lookup uses the fixed Delta version with bound exact source,
type and id predicates and the derived hash predicate. Resolve and allowlist table
identifiers and integer version literals from the descriptor; never interpolate
caller strings as SQL structure. Zero visible rows means absent/hidden according
to CONTRACT-002. Duplicate current identities are invalid state, not an arbitrary
first-row response. Authentication, effective row/property policy and cursor
binding require real execution-adapter enforcement; metadata alone proves none.

Retention must preserve all active publication pins, replay/conflict evidence,
history and tombstones. No VACUUM schedule or production retention interval is
selected. Rollback selects a previously validated retained descriptor; it does
not undo independent physical commits or rewrite producer progress blindly.
Production writer authority/fencing remains a prerequisite for production use,
not a reason to repeat physical benchmarks.

## Graph-tool mapping

Build separate immutable scalar releases from one publication. Retain stable
node_key and edge_key derived from exact tuples with an unambiguous encoding;
preserve a reversible tuple mapping. Include isolated nodes and parallel edges.
GraphFrames uses vertices.id and edges.src/dst plus explicit edge identity;
PuppyGraph uses node types and relationship/endpoint-type labels;
Fabric uses independently verified bounded OneLake releases. History, retained
unknown content, tombstones and source progress remain canonical references.
Use the existing [release mapping profile](adapters/release-r66/mapping-plan.json)
and CONTRACT-003 target limits; its historical state label is not current runtime
qualification. Current [layout status](layout-status.md) owns evidence limits.
Direct UC protocol/feature support is not implied by local engine success.
Fabric is not the singleton path or an unbounded6B-element canonical deployment.

## Physical policy and measured limits

Keep identity-hash liquid clustering as the initial canonical layout. No fixed
partition count, per-type directory fanout,256MiB promotion, or mandatory full
OPTIMIZE after each batch follows from the comparisons. Overlap-driven maintenance
is an operational candidate whose cost must be accounted separately. Existing
64MiB targets do not promise actual emitted sizes. Hash pruning and endpoint
traversal need different physical orderings, which motivates narrow projections.

The scale intent remains1B nodes/5B edges. Native proofs cover the existing8M/
39.93M publication and independent8M-new-node/16M-new-edge staging extents;
these are not one complete16M-node/80M-edge published graph. Warm caller/engine,
freshness, controlled cold reads and billion-scale support remain failed or
unproved in their recorded scopes. Keep the250ms caller/100ms engine,1s cold and
60s freshness values as provisional tuning targets, not architecture gates.
[Final testing disposition](scale-testing-disposition-r696.md) records costs,
resource footprint and the stop; no further experiments are scheduled.

## Next implementation slice

Extract a reviewable SQL schema package from the0.3 reference: baseline tables,
optional adjacency, optional examples and coordination profile clearly separated.
Include the constraint/enforcement matrix, exact hash recipe and pinned native
singleton example governed by CONTRACT-003. The deployment artifact must require
an explicit target catalog/schema and must not auto-apply to existing data.

Then implement the descriptor resolver and publisher boundary separately, each
tracing to the existing read/publication contracts. Runtime and real producer
profile remain explicit choices before those components are built. Preserve
US-002 AC1–AC7 traceability: DDL/fixtures, constraint refusals, breaking migration,
interruption visibility, enforcement labels, semantic mappings and measured
native reads. Existing evidence is reused; no new test invocation is authorized.
Do not treat draft requirements as implementation completion or support approval.

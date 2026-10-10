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

## Module Boundaries

**Source Applicability**: source; the portable Python library and host adapters
have distinct semantic, custody and transport responsibilities.

| Module | Responsibility / Owned Types | Public API | Allowed Dependencies | Forbidden Dependencies |
| --- | --- | --- | --- | --- |
| `src/ashlar/schema.py`, `catalog.py`, `binding.py`, `semantic_policy.py`, `typed_source_policy.py` | Retained model/catalog bindings and public-UMF receipt custody | SchemaIntake, binding/policy admission | Portable value codecs and original UMF receipts | Runtime SDKs, tools, private UMF semantic implementation |
| `src/ashlar/source.py`, `apply.py`, `whole_entity.py`, `source_checkpoint.py` | Source batches, prospective graph state and qualified feed checkpoints | SourceBatch, plan_apply, checkpoint binding | Model/binding types, portable structural codecs | Live engine clients, ambient credentials, host orchestration |
| `src/ashlar/publication.py`, `publisher.py`, `stored_publisher.py`, `recovery.py`, `manifest.py` | Immutable descriptors, attempt state and original-operation recovery | resolve_publication, publisher/backend protocols | Source state and explicit storage/policy ports | Tools, SDK discovery, telemetry as durable authority |
| `src/ashlar/native.py`, `staging.py`, `attempt_store.py`, `schema_registry.py`, `pins.py`, `authority.py`, `retention_policy.py` | Native SQL, pin/authority and retained-store adapters | Executor/store/policy APIs named in CONTRACT-004 | Core descriptors and injected native transport | Source semantic reinterpretation, tools, automatic permissive policy |
| `src/ashlar/weft_binding.py`, `weft_query.py`, `weft_decode.py`, `graph_release.py`, `singleton.py` | Qualified consumer bindings, buffered decoding and graph-release projection | read_weft, graph release and singleton APIs | Resolver, admitted model and explicit transport/policy ports | Producer ACK mutation, host result repair, compiler semantic substitution |
| `src/ashlar/durable_publisher.py`, `effect_validation.py`, `protocol.py`, `schema_policies.py`, `quarantine.py`, `maintenance.py`, `retention.py`, `retention_policy.py`, `profile_custody.py`, `lineage.py`, `origin.py`, `assertions.py`, `report_parts.py`, `outbox.py` | Durable effect/authority composition, maintenance, profile custody and protocol codecs | Named module APIs governed by CONTRACT-001–005 | Core records and injected policy/transport interfaces | Tool imports, ambient SDK construction, silently permissive authority |
| `src/ashlar/truss_input.py`, `truss_feed.py`, `csv_source.py` | External source framing and exact retained input translation | Source adapter APIs | Core source/schema records and supplied public receipts | Shadow semantic validators, implicit source installation discovery |
| `src/ashlar/__init__.py`, `__main__.py` | Package exports and command dispatch | Explicit package surface, CLI main | Owned library modules and CLI | Engine SDK construction or new semantic ownership |
| `src/ashlar/cli.py`, `source_config.py`, `*_source.py`; `tools/` adapters | Entrypoint construction, named source profiles and private native experiments | CLI/source conversion and named runner entrypoints | Public core APIs and explicitly owned SDK adapters | Core import of tools; newly introduced private cross-module access |

**Integration Owners**: UMF -> schema/typed-source policy receipt boundaries;
Truss -> `truss_input.py`/`truss_feed.py`; Weft -> `weft_binding.py`/`weft_query.py`;
Delta SQL -> `native.py` and host transport; PostgreSQL ACK ->
`tools/protected_source_ack.py`; graph engines -> named `tools/run_*` and
`tools/check_*` adapters. Vendor/source translation stays at those boundaries.

**Construction Policy**: the CLI or named host entrypoint builds configuration,
transport and policy ports once and injects them. Concrete SQL coupling in native
adapters is deliberate; no container or interface-per-function is required.
Core invariants and exact shared surfaces remain owned by CONTRACT-001–005.

**Boundary Check**: `python3 tools/check_module_boundaries.py`; enforce the
project map with actual Python AST imports and an individually identified
existing-debt inventory. Use the same command locally, in pre-commit and CI.
Baseline entries identify exact importing file, target, symbol, owner and removal
trigger; no directory exemption admits new violations. Dynamic imports, runtime
reflection and value/type ownership require explicit semantic review; static
import success alone cannot establish encapsulation.

## Configuration and diagnostic boundaries

Entrypoints validate a single typed immutable configuration before constructing
SDK clients or performing effects. For the new application configuration composition, record each nonsecret setting's origin:
explicit arguments > environment/secret files > development-only .env >
environment TOML > default TOML > field defaults. Existing explicitly qualified runtime profiles retain their selected configuration
contract; new precedence cannot silently reinterpret them. Operator-owned endpoint,
warehouse, source installation, authority and supported profile fields are
required; never silently select shared/default compute. Secrets use secret types
and never enter printable settings, fingerprints, SQL diagnostics or subprocess
capture. Library calls receive explicit configuration/policy ports rather than
reading process environment. Browser configuration is a separate public,
browser-safe closed boundary and cannot contain credentials.

OpenTelemetry (OTel) governs diagnostics, independently of durable attempt/manifest/ACK custody.
Publication, source admission, resolver-held read and ACK/reconciliation are
operation spans; durable phase changes and refusal categories are events;
aggregate outcomes are low-cardinality metrics. Preserve optional valid trace
context, use links for independently retried work, and never invent trace IDs.
The host owns one export route and bounded JSONL run capture with safe evidence
references. Redact/allowlist before every sink, reserve bounded record/queue
sizes, disclose sampling/drop/capture limits, and attempt bounded flush at close.
Exporter failure cannot authorize ACK or change commit classification. Protocol
stdout stays clean; stderr shows meaningful outcomes. Raw model/rows, SQL values,
credentials and source payloads are excluded from telemetry by default. Exact
signal schema/version, ordering, retention and export mapping belong in a
separately reviewed contract before an OTel integration claim.

## Publication assurance policy

Publication/recovery uses precise reviewed safety and conditional liveness
specification, implementation correspondence and existing bounded tests.
[TD-001](../technical-designs/TD-001-publication-recovery.md) owns states, transitions, properties
and assumptions. A separately reviewed bounded analysis may increase assurance;
it cannot establish a production writer fence or native snapshot retention.

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

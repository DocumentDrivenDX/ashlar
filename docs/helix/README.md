# Ashlar project documentation

Ashlar defines and queries domain-independent property graphs on Unity Catalog
managed Delta tables. UMF owns reusable schema interpretation, validation and DDL;
Ashlar owns admitted graph bindings, source ingestion and immutable publications.
Native singleton reads work through the publication resolver independently of Fabric.

## Start here

- [Runnable setup, ingest and query example](../../examples/end-to-end/README.md)
- [Toolkit components and local commands](../../src/ashlar/README.md)
- [Execution plan and evidence](04-build/end-to-end-plan.md)
- [Current release closure sequence](04-build/release-closure-20261101.md)
- [Product requirements](01-frame/prd.md) and [consumer conformance strategy](03-test/consumer-conformance-plan.md)
- [Physical tables](02-design/contracts/CONTRACT-003-delta-graph-tables.md),
  [publication boundary](02-design/contracts/CONTRACT-001-publication-boundary.md),
  [consumer reads](02-design/contracts/CONTRACT-002-consumer-read-boundary.md) and
  [publication resolver](02-design/contracts/CONTRACT-004-publication-resolver.md)

## Implementation status

The candidate selects UMF SDK 0.10.1 / Python Core 0.10.5, Weft 0.4.56,
Access 0.1.0.dev8 and published Truss 0.0.52. The six-role `.4-xxhash64`
carrier has bounded local SDK publication, retained manifest and native
Spark 4.2 / Delta 4.4.1 singleton, grouped-query and page-read evidence
([SDK result](04-build/evidence/current-upstream-release-20261101/g03-sdk0101-r3-manifest-continuation/r5-completion.md),
[Weft result](04-build/evidence/current-upstream-release-20261101/g03-weft-native-consumer/README.md)).

A fresh local-development Truss 0.0.52 source has public acceptance, consumer
and seven-operation seed evidence. Its first available page is a catalog/revision
transaction, followed by the seed; the full ordered Delta publication and
protected ACK path remains open. A bounded serverless Jobs task helper and
Weft `.4.1` compiler route are on main, but no paid `client_dev` trial has run.
The managed A/B source-prefix owner, live source/ACK custody and native pin
composition remain open ([release closure](04-build/release-closure-20261101.md),
[A1 source-owner gap](04-build/evidence/a1-managed-source-prefix-owner-gap-20261101.md)).

The owner held the 1B-node/5B-edge scale trial after the measured sample
projected more than 15 minutes at 80% of observed throughput. The stable GitHub
release has not been tagged or published. Claims remain limited to their exact
source, runtime, model and evidence.

## Requirements navigation

| Requirement | Feature | User story |
| --- | --- | --- |
| FR-1: UMF graph profile | [FEAT-001](01-frame/features/FEAT-001-umf-graph-profile.md) | [US-001](01-frame/user-stories/US-001-review-graph-mapping.md) |
| FR-2: Schema package | [FEAT-002](01-frame/features/FEAT-002-databricks-schema-package.md) | [US-002](01-frame/user-stories/US-002-validate-schema-package.md) |
| FR-3: Graph queries | [FEAT-003](01-frame/features/FEAT-003-graph-query-path.md) | [US-003](01-frame/user-stories/US-003-query-typed-graph.md) |
| FR-4: Incremental publication | [FEAT-004](01-frame/features/FEAT-004-incremental-gold-publication.md) | [US-004](01-frame/user-stories/US-004-trust-replayed-publication.md) |

[Discovery and scope](00-discover/research.md) and
[potential-consumer input](00-discover/hot-store-publication-input.md) retain their
source context. Desired behavior belongs in requirements, contracts and test plans;
implementation results belong in build evidence. The
[pre-cleanup documentation archive](04-build/evidence/documentation-history-20261009/docs/helix/README.original.txt)
preserves the earlier status history verbatim. HELIX uses the installed catalog;
no methodology catalog is copied into this repository.

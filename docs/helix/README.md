# Ashlar project documentation

Ashlar defines and queries domain-independent property graphs on Unity Catalog
managed Delta tables. UMF owns reusable schema interpretation, validation and DDL;
Ashlar owns admitted graph bindings, source ingestion and immutable publications.
Native singleton reads work through the publication resolver independently of Fabric.

## Start here

- [Runnable setup, ingest and query example](../../examples/end-to-end/README.md)
- [Toolkit components and local commands](../../src/ashlar/README.md)
- [Execution plan and evidence](04-build/end-to-end-plan.md)
- [Product requirements](01-frame/prd.md) and [consumer conformance strategy](03-test/consumer-conformance-plan.md)
- [Physical tables](02-design/contracts/CONTRACT-003-delta-graph-tables.md),
  [publication boundary](02-design/contracts/CONTRACT-001-publication-boundary.md),
  [consumer reads](02-design/contracts/CONTRACT-002-consumer-read-boundary.md) and
  [publication resolver](02-design/contracts/CONTRACT-004-publication-resolver.md)

## Implementation status

A limited local development profile has exercised configured JSONL publication,
update/delete and history, resolver singleton reads, guarded Weft queries and
committed replay. Original UMF 0.8 commerce also has bounded local Truss 0.0.22
publication and query evidence. These receipts qualify only their recorded
source, model, engine versions and authority; they do not establish current Truss
0.0.49 acceptance/feed/ACK, broad model support, managed Unity Catalog or
production authority ([configured publication](04-build/evidence/native-configured-publication-complete-20261008.json),
[Truss 0.0.22 lifecycle](04-build/evidence/truss22-commerce-lifecycle-20261013/README.md)).

Current dependency pins include Weft 0.4.53 and UMF core 0.10.1/access
0.1.0.dev7. Focused checks passed for one tagged Weft page fixture and the UMF
macOS arm64 install/core path; Linux assets were byte-verified but not run.
These are package and compiler checks, not emitted-SQL execution or native,
Databricks, or Linux runtime qualification ([Weft adoption](04-build/evidence/current-upstream-release-20261023/weft053-ashlar-adoption-20261023/README.md),
[UMF adoption](04-build/evidence/current-upstream-release-20261023/umf-0.10.1-ashlar-adoption-20261024/README.md)).

Truss 0.0.49 R12 passed one effect-free preflight against its selected Ashlar
source and current runtime pins. Its native lifecycle remains unrun; owner
authorization and an execution-approval file are absent. The result establishes
preparation readiness only, with no migration, source/feed/publication/ACK, or
production-authority qualification ([R12 packet and limits](04-build/evidence/current-upstream-release-20261023/truss049-r12-current-selected-source-lifecycle-20261024/README.md)).

Local Spark 4.2.0/Delta 4.4.1 candidate checks include bounded Weft query and
SDK paging; a separate native one-table Delta check accepted a candidate DDL
shape. Managed Unity Catalog and the full Truss lifecycle remain unqualified.
The synthetic 1B-node/assumed-5B-edge trial remains held because its latest
write-only projection exceeds the 15-minute owner-review gate. Fabric activation
is pending current source/ACK, capacity, path and cleanup admission. See the
[toolkit delivery plan](04-build/toolkit-delivery-plan.md) for ordered work and
acceptance criteria; detailed run results remain under [build evidence](04-build/).

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

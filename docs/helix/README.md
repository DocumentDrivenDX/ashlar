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
source, model, engine versions and authority; they do not establish selected Truss
0.0.50 acceptance/feed/ACK, broad model support, managed Unity Catalog or
production authority ([configured publication](04-build/evidence/native-configured-publication-complete-20261008.json),
[Truss 0.0.22 lifecycle](04-build/evidence/truss22-commerce-lifecycle-20261013/README.md)).

Current dependency pins include Weft 0.4.53 and UMF core 0.10.1/access
0.1.0.dev7. Focused checks passed for one tagged Weft page fixture and the UMF
macOS arm64 install/core path; Linux assets were byte-verified but not run.
These are package and compiler checks, not emitted-SQL execution or native,
Databricks, or Linux runtime qualification ([Weft adoption](04-build/evidence/current-upstream-release-20261023/weft053-ashlar-adoption-20261023/README.md),
[UMF adoption](04-build/evidence/current-upstream-release-20261023/umf-0.10.1-ashlar-adoption-20261024/README.md)).

The current Truss pin is 0.0.50. Its official wheel, `RECORD`, `[local,native]`
dependency resolution, and feed/receipt/migration import surfaces pass focused
checks with current UMF core 0.10.1/access 0.1.0.dev7, Weft 0.4.53, and psycopg
3.3.6. Versioned .50 source admission and feed-binding checks pass. The
selected-source R14 packet is now preflight-ready, while native execution
remains held pending exact owner approval. No .50 migration,
source/feed/publication/ACK, or native action is qualified
([.50 package qualification](04-build/evidence/current-upstream-release-20261024/truss050-package-cohort-qualification-20261024/README.md),
[source admission](04-build/evidence/current-upstream-release-20261024/truss050-source-admission-20261024/README.md),
[R14 selected-source preflight](04-build/evidence/current-upstream-release-20261024/truss050-r14-selected-source-lifecycle-20261024/README.md)).

Local Spark 4.2.0/Delta 4.4.1 candidate checks include bounded Weft query and
SDK paging; a separate native one-table Delta check accepted a candidate DDL
shape. One bounded local GraphFrames 0.12.3 action over persisted 5-node/4-edge
Delta fixtures passed with independent result and owned cleanup checks
([R14 result](04-build/evidence/canonical-graphframes-holder-candidate-20261023/r05-native-action-r13/README.md)).
The current-core configured-local `.3` path also completed a reviewed native
initialize, three publication batches and exact replay. Its selected Weft
0.4.53 verifier and private gate/SDK selection controls are focused-tested, but
live alias-bound continuation, native singleton/query execution, and the
separate `.4-xxhash64` profile remain open
([R02 result](04-build/evidence/current-upstream-release-20261024/r02-native-prepublication-20261024/README.md)).
Managed Unity Catalog and the full Truss lifecycle remain unqualified.
Managed A1 also remains unadmitted: policy use and cluster-creation entitlement
are unproven, source-holder custody is open, and no conservative total-cost
bound has been established. No A1 schema or paid compute was created or started
([A1 permission admission](04-build/evidence/current-upstream-release-20261023/managed-a1-permission-admission-20261024.md),
[A1 trial constraints](04-build/evidence/current-upstream-release-20261023/managed-a1-correctness-trial-constraints.md)).
The synthetic 1B-node/assumed-5B-edge trial remains held because its latest
write-only projection exceeds the 15-minute owner-review gate. An opt-in
concurrent table-write candidate is implemented but has no native performance
result ([physical limits](04-build/evidence/r05-materialization-physical-limits-20261024.md),
[candidate packet](04-build/evidence/r05-full-delta4-scale-packet-20261023/README.md)).
Fabric activation is pending current source/ACK, capacity, path and cleanup
admission. See the
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

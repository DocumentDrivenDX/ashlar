# Ashlar project documentation

Ashlar defines and queries domain-independent property graphs on Unity Catalog
managed Delta tables. UMF owns reusable schema interpretation, validation and DDL;
Ashlar owns admitted graph bindings, source ingestion and immutable publications.
Native singleton reads work through the publication resolver independently of Fabric.

## Start here

- [Runnable setup, ingest and query example](../../examples/end-to-end/README.md)
- [Toolkit components and local commands](../../src/ashlar/README.md)
- [Execution plan and evidence](04-build/end-to-end-plan.md)
- [0.1.0 release notes](05-deploy/release-notes.md)
- [Current release closure sequence](04-build/release-closure-20261101.md)
- [Product requirements](01-frame/prd.md) and [consumer conformance strategy](03-test/consumer-conformance-plan.md)
- [Physical tables](02-design/contracts/CONTRACT-003-delta-graph-tables.md),
  [publication boundary](02-design/contracts/CONTRACT-001-publication-boundary.md),
  [consumer reads](02-design/contracts/CONTRACT-002-consumer-read-boundary.md) and
  [publication resolver](02-design/contracts/CONTRACT-004-publication-resolver.md)

## Implementation status

The current local qualification cohort is Truss 0.0.55, UMF SDK 0.10.2 /
Python Core 0.10.6, Weft 0.4.56, Access 0.1.0.dev8 and declared UMF
model 0.10.0. The selected managed serverless qualification passed for its
documented profile. Ashlar `0.1.0` is published as an immutable GitHub release;
see the publication closeout.

The composed R61 local-development run used Spark 4.2.0 / Delta 4.4.1 and the
`.4-xxhash64` carrier. It committed a metadata genesis, seven-operation seed
and two-record continuation, matched their protected Truss checkpoints, and
produced a canonical six-node/three-edge owner export. A separately bounded
GraphFrames action matched that retained export. The closeout preserves the
original full-worker GraphFrames adapter failure and the later retained-bytes
pass; the native Weft singleton and grouped values were observed, but the full
worker did not persist a durable Weft result hash. See the R61 closeout.

A separate local managed-current recovery reopened the exact attempt14 pending
A1 page, independently compared all four native Delta role bags against the
source oracle, then reconciled one protected A1 ACK. It performed no source
group replay or Delta write. This recovery records a source-hold discontinuity
and uses local path-based Delta reads; it does not establish Databricks or
Unity Catalog execution. See the attempt14 recovery closeout.

The selected `aidev-cus` managed serverless target passed the R25 A1
continuation: 14 native rows matched the exact table UUID/version vector,
predictive optimization was enabled at all three checks, and the protected
Truss ACK matched. R25 reused the matched R23 A0 settlement and committed A1
seed without replay after recording the source-hold gap; source, pin, schema,
table, and Volume cleanup completed. See the managed closeout,
raw qualification receipt,
and R23 A0 / RC10 source-probe evidence.

The original workspace's catalog inventory did not include `client_dev`; the
selected `aidev-cus` workspace exposes it to the current principal. The
dedicated managed classic Spark Jobs attempt was refused by the workspace
backend before cluster creation because the organization has no associated
worker environments; no classic workload ran. Fabric integration is
unfinished, and the 1B-node / 5B-edge scale trial remains held. The current
release closure records the completed managed scope and local release sequence.

## Requirements navigation

| Requirement | Feature | User story |
| --- | --- | --- |
| FR-1: UMF graph profile | [FEAT-001](01-frame/features/FEAT-001-umf-graph-profile.md) | [US-001](01-frame/user-stories/US-001-review-graph-mapping.md) |
| FR-2: Schema package | [FEAT-002](01-frame/features/FEAT-002-databricks-schema-package.md) | [US-002](01-frame/user-stories/US-002-validate-schema-package.md) |
| FR-3: Graph queries | [FEAT-003](01-frame/features/FEAT-003-graph-query-path.md) | [US-003](01-frame/user-stories/US-003-query-typed-graph.md) |
| FR-4: Incremental publication | [FEAT-004](01-frame/features/FEAT-004-incremental-gold-publication.md) | [US-004](01-frame/user-stories/US-004-trust-replayed-publication.md) |

Discovery and scope and
potential-consumer input retain their
source context. Desired behavior belongs in requirements, contracts and test plans;
implementation results belong in build evidence. The
pre-cleanup documentation archive
preserves the earlier status history verbatim. HELIX uses the installed catalog;
no methodology catalog is copied into this repository.

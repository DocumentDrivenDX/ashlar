# Ashlar project documentation

Ashlar defines and queries domain-independent property graphs on Unity Catalog
managed Delta tables. UMF owns reusable schema interpretation, validation and DDL;
Ashlar owns admitted graph bindings, source ingestion and immutable publications.
Native singleton reads work through the publication resolver independently of Fabric.

## Start here

- [Runnable setup, ingest and query example](../../examples/end-to-end/README.md)
- [Toolkit components and local commands](../../src/ashlar/README.md)
- [Execution plan and evidence](04-build/end-to-end-plan.md)
- [Current release closure sequence](04-build/release-closure-20261029.md)
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
0.0.51 acceptance/feed/ACK, broad model support, managed Unity Catalog or
production authority ([configured publication](04-build/evidence/native-configured-publication-complete-20261008.json),
[Truss 0.0.22 lifecycle](04-build/evidence/truss22-commerce-lifecycle-20261013/README.md)).

The candidate pins Weft 0.4.56, UMF SDK 0.10.0, Python Core 0.10.4
(semantic Core 0.10.0) and Access 0.1.0.dev8. Focused installed macOS checks
passed for compiler responses, managed page-target admission, and the SDK
Delta 4 bridge's six-role MiniRacer parity. The local wheel and sdist contain
the pinned bridge resources. Linux native execution and managed publication
remain open ([upstream adoption](04-build/evidence/current-upstream-release-20261031/README.md),
[runtime integration](04-build/evidence/current-upstream-release-20261031/sdk010-delta4-runtime-integration/README.md)).

The candidate `truss-local` extra selects Truss 0.0.51 with its `[native]`
dependencies. R16 stopped during cold-backup restore after 305.595 seconds of
preparation, with its approval spent. R17 consumed its separate approval but
refused at the frozen selected-tree check before any native effect; its approval
is also spent. The original selected and cold-backup paths are now empty, while
the exact R16 retained copies and their preservation receipts remain available
as candidate inputs. R18 binds those copies in a fresh full-lifecycle proposal
with passing focused effect-free checks, but has no owner approval and has not
run. The Truss lifecycle remains unqualified; selected-source acceptance and
feed/ACK evidence remain open ([R16 failure](04-build/evidence/current-upstream-release-20261024/truss051-r16-native-candidate-20261029/r16-failure-evidence.json),
[R17 failure](04-build/evidence/current-upstream-release-20261024/truss051-r17-native-candidate-20261029/r17-pre-effect-failure-20261031.md),
[R18 candidate](04-build/evidence/current-upstream-release-20261024/truss051-r18-native-candidate-20261031/README.md)).

Local Spark 4.2.0/Delta 4.4.1 candidate checks include bounded Weft query and
SDK paging; a separate native one-table Delta check accepted a candidate DDL
shape. One bounded local GraphFrames 0.12.3 action over persisted 5-node/4-edge
Delta fixtures passed with independent result and owned cleanup checks
([R14 result](04-build/evidence/canonical-graphframes-holder-candidate-20261023/r05-native-action-r13/README.md)).
The current-core configured-local `.3` path completed a reviewed native
initialize, three publication batches and exact replay, followed by one
reviewed ordinal-1 live-alias continuation using Weft 0.4.53. That continuation
matched its independent source oracle for the singleton, original and positive
Backend03 grouped queries, and Backend02 page plus continuation. This is narrow
local `.3` evidence only: it does not qualify `.4-xxhash64`, protected ACK,
cloud execution, or production source custody
([prepublication result](04-build/evidence/current-upstream-release-20261024/r02-native-prepublication-20261024/README.md),
[continuation result](04-build/evidence/current-upstream-release-20261024/r02-native-execution-packet-20261024/native-continuation-result-427287c7f6564985b1a947bea049158a.json)).
A separate local `.4-xxhash64` run completed six-role initialization only; no
publication, query, or ACK was exercised ([initialization evidence](04-build/evidence/current-upstream-release-20261024/configured-local-delta4-init-20261024/README.md)).
Managed Unity Catalog and the full Truss lifecycle remain unqualified.
A bounded probe confirmed create/read/delete authority for one empty owned
schema under `client_dev`; that schema was deleted and verified absent. Managed
A1 remains held: Jobs creation and execution authority, exact runtime/access
mode, live source/ACK custody, prior spend and a bounded total cost remain
unproven. The two-node shape and cost figures are candidates, not admission;
no paid compute was created or started ([A1 admission packet](04-build/evidence/managed-a1-jobs-admission-packet-20261029.md),
[schema probe](04-build/evidence/current-upstream-release-20261023/managed-a1-schema-authority-probe-20261024.md),
[A1 trial constraints](04-build/evidence/current-upstream-release-20261023/managed-a1-correctness-trial-constraints.md)).
The owner decision for the synthetic 1B-node/assumed-5B-edge trial is
“Hold and redesign first.” The 45M/225M sample projects 28.206 minutes for
setup, writes and one combined row/value plus endpoint-formula scan at 80% of
measured throughput; the end-to-end estimate is 28.206 minutes + X, with the
additional scan, lookups, graph action, startup, checks and cleanup unmeasured.
No full run is authorized ([R05 redesign napkin](04-build/evidence/r05-redesign-napkin-math-20261029.md),
[R05 packet](04-build/evidence/r05-one-shot-full-scale-run-packet-20261029.md)).
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

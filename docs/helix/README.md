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

The small configured JSONL example has actual managed-Delta
[publication, update/delete and history evidence](04-build/evidence/native-configured-publication-complete-20261008.json),
[resolver singleton evidence](04-build/evidence/native-configured-singleton-20261008.json),
[guarded Weft string/presence query evidence](04-build/evidence/native-configured-guarded-weft-query-20261008.json)
and [committed replay evidence](04-build/evidence/native-configured-replay-20261008.json).
These qualify the recorded development source, model, native versions and authority
profile; they do not prove real Truss acceptance/feed/ACK or production authorization.
Original UMF 0.8 commerce has actual Truss 0.0.22
[catalog/feed/publication and lifecycle evidence](04-build/evidence/truss22-commerce-lifecycle-20261013/README.md),
[partial-return, fulfillment and refund passes on a retained publication](04-build/evidence/truss22-commerce-original-three-queries-20261014/README.md)
and a [settlement pass on the evolved publication](04-build/evidence/truss22-commerce-indexed-settlement-20261014-c/README.md).
These use local Spark/Delta and indexed Weft 0.4.2; broader models, runtimes,
managed Unity Catalog and production authority require separate evidence. A blocked external engine or Truss source
affects its own lane.

The current configured-local profile is a macOS arm64/Python 3.12 candidate on
Spark 4.2.0, Delta 4.4.1 and JDK 21. Its bounded evidence includes grouped
two-hop counts, a negative HAVING case, SDK pages with a 2+1 continuation, and
a close-time runtime-custody refusal. The compiler wheel is a task-local
candidate, not a public Weft release; this does not qualify managed Unity
Catalog, production authority, or Truss acceptance/feed/ACK. See the
[current-upstream evidence packet](04-build/evidence/current-upstream-release-20261021/local-current/current-native-query-and-sdk-pages-20261021.json)
for the exact tuple, report hashes and receipt-cap qualification.

The separate `.4` local synthetic carrier samples measured 74.19 MB at
1M/5M rows and 681.44 MB at 10M/50M rows. Four exact hash-plus-full-key probes
per sample showed hash/source/type filters pushed into Parquet scans; the SQL
included exact IDs, but truncated plans did not verify ID pushdown or file
pruning. Marginal bytes extrapolate to about 67.48 GB at 1B/5B rows for this
fixture only. A 4-core write-only projection at 80% of the 10M/50M measured
rate is 56.73 minutes, so the full run is held for owner review. See the
[`.4` sample evidence](04-build/evidence/current-upstream-release-20261021/local-current/canonical-carrier-xxhash64-sample-20261021.json).

[Layout experiments](02-design/spikes/SPIKE-001-table-layout.md) retain existing
local and native performance measurements. Metrics guide tuning; they do not
change the managed-Delta architecture. Functional checks remain small; explicitly
requested scale trials use dedicated compute and approved resource/cost limits.
Graph-tool exports and shape checks alone do not qualify actual
Fabric, PuppyGraph or GraphFrames execution over Ashlar publications.

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

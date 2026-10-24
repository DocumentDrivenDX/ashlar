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

Ashlar now pins public Weft 0.4.53. Its released macOS wheel compiled one
tagged Ashlar page fixture with a byte-identical response to 0.4.52; emitted
SQL and native execution remain separate gates ([focused adoption
receipt](04-build/evidence/current-upstream-release-20261023/weft053-ashlar-adoption-20261023/README.md)).
Ashlar pins UMF access 0.1.0.dev7 and the published UMF core 0.10.1
macOS arm64/Linux x86_64 assets. Both asset bytes were verified; the fresh
isolated install and focused core checks ran on macOS only. This does not
qualify Linux runtime or Databricks compatibility, nor native Delta behavior
([0.10.1 adoption receipt](04-build/evidence/current-upstream-release-20261023/umf-0.10.1-ashlar-adoption-20261024/README.md),
[access receipt](04-build/evidence/current-upstream-release-20261023/umf-0.5.5-ashlar-access-adoption/README.md),
[managed qualification limits](04-build/evidence/current-upstream-release-20261023/managed-a1-correctness-trial-constraints.md)).
Truss 0.0.49 R5 is frozen to Ashlar source `0fce3b6c` and passed its permitted
effect-free supervisor/worker preflight with process inventory, SysV cleanup and
exact approval-payload parity ready. This is interim current-core readiness for
UMF core 0.10.0/access 0.1.0.dev7; no PostgreSQL or Spark process, migration,
seed, source/feed/publication/ACK effect, execution approval or native qualification
is claimed. The exact-payload approval proposal remains unapproved ([R5 readiness
and receipts](04-build/evidence/current-upstream-release-20261023/truss049-r5-readiness-20261023/README.md)). R4 supplies the reused runtime provenance; R3 and the failed R2
attempt remain historical evidence ([R4 readiness](04-build/evidence/current-upstream-release-20261023/truss049-r4-readiness-20261023/README.md),
[R3 status](04-build/evidence/current-upstream-release-20261023/truss049-r3-readiness-20261023/README.md),
[R2 failure](04-build/evidence/current-upstream-release-20261023/truss049-selected32ec-head0506fb7-failure/README.md)).
An effect-free cohort-refresh proposal binds the selected Ashlar source revision and the public
Weft 0.4.53 wheel. A fresh CPython 3.12.14 runtime now passes offline wheel,
installed RECORD, import-origin and dependency checks; this is package provenance
only. No current-cohort supervisor preflight or source/consumer custody recheck
has run, so R5's Weft 0.4.52 runtime and preflight remain historical ([refresh
proposal](04-build/evidence/current-upstream-release-20261023/truss049-r5-current-cohort-refresh-20261023/README.md),
[current-cohort runtime receipt](04-build/evidence/current-upstream-release-20261023/truss049-current-cohort-runtime-20261023/README.md)).
The R6 worker and supervisor were statically bound to the `6986e832` source
snapshot. Ashlar main then moved the UMF core URL to the latest published
release with identical wheel bytes. R6 is now a frozen record, not a current
lifecycle packet; its live preflight and lifecycle have not run ([R6 readiness](04-build/evidence/current-upstream-release-20261023/truss049-current-cohort-readiness-20261023/README.md)).
R7’s static packet check passed against its archived Ashlar source `7c4006e8` and published UMF 0.10.1 runtime/bridge. The private root is 737,552,060 bytes, below the 1-GiB cap. Its one effect-free supervisor preflight attempt exited 1 before admission gates because the pinned runtime receipt omits `runtime.venv_isolated`; a full contract review found the worker also expects a `passed:` `install.pip_check` prefix while the receipt records `No broken requirements found.` No preflight result or native start marker was produced. Sol’s bounded host inventory found no Java, Spark or R7 process. A live interpreter probe confirmed venv isolation; an R8 correction proposal is recorded in the assessment. The packet remains held for corrected, freshly bound runtime evidence; current-source judgment remains a release-qualification gate ([R7 packet and assessment](04-build/evidence/current-upstream-release-20261023/truss049-r7-readiness-20261023/README.md)).

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
The Truss 0.0.48 r3 attempt failed before source acceptance because its candidate
venv lacked the embedded `py_mini_racer` payload. The r4 attempt committed the
original UMF 0.10 source at catalog revision 1. Recovery attempt r5-06 then
configured one public module-access grant and verified the accepted source
byte-for-byte, its revision-1 report, and the complete current catalog view with
11 definitions. This bounded result covers the local macOS arm64, Python 3.11.17,
PostgreSQL 18.6 profile only; consumer-profile admission, Truss feed/lifecycle,
publication, destination, and ACK remain unqualified. It does not establish the
release's declared Linux support on macOS or a broader Ashlar support pass. See the
[0.0.48 evidence](04-build/evidence/current-upstream-release-20261022/truss-0.0.48-adoption-preparation.json).

The first local `.4` lifecycle campaign stopped at its first UMF table DDL:
OSS Delta 4.4.1 rejected the authored `delta.parquet.compression.codec` property
as an unknown configuration. The registered feed consumer remains at the genesis
checkpoint; no genesis publication or ACK, seed, or continuation occurred. UMF PR #47 is merged at `259bf1e` (tree `204cfba`), and public `umf-v0.5.6` is published. The 0.10.1 macOS and Linux wheel bytes match public release metadata; Ashlar's fresh install and focused compatibility check are macOS-only ([UMF asset adoption](04-build/evidence/current-upstream-release-20261023/umf-0.10.1-ashlar-adoption-20261024/README.md)). The merged source includes the versioned direct Delta-path realization. Ashlar's embedded bridge has now been rebuilt from the public tag. Seven of 33 UMF source inputs changed from the pre-merge candidate; all six Ashlar role semantics pass focused offline checks ([source bridge rebind](04-build/evidence/current-upstream-release-20261023/umf-0.5.6-local-delta4-bridge/README.md)). One native local Delta 4 create/write/read accepted the earlier candidate DDL shape with an owned path; it did not run the full bridge or Truss
lifecycle ([one-table receipt](04-build/evidence/current-upstream-release-20261023/umf-0.5.6-delta4-one-table-acceptance-20261023/README.md)). See the [bridge rebind evidence](04-build/evidence/current-upstream-release-20261023/umf-0.5.6-local-delta4-bridge/README.md),
[SDK package-install receipt](04-build/evidence/current-upstream-release-20261023/umf-0.5.6-sdk-package-install/README.md)
and the [historical 0.5.4 candidate receipt](04-build/evidence/current-upstream-release-20261022/local-delta4-umf-realization-candidate.json).

The current configured-local profile is a macOS arm64/Python 3.12 candidate on
Spark 4.2.0, Delta 4.4.1 and JDK 21. Its bounded evidence includes grouped
two-hop counts, a negative HAVING case, SDK pages with a 2+1 continuation, and
a close-time runtime-custody refusal. The compiler wheel is a task-local
candidate, not a public Weft release; this does not qualify managed Unity
Catalog, production authority, or Truss acceptance/feed/ACK. See the
[current-upstream evidence packet](04-build/evidence/current-upstream-release-20261021/local-current/current-native-query-and-sdk-pages-20261021.json)
for the exact tuple, report hashes and receipt-cap qualification.

The `.4` publisher now stages numeric identity and endpoint fields as JSON
integers parsed directly into BIGINT, while retaining opaque custody and payload
strings; last-change selection also avoids rescanning the full fresh page for
each touched key. Focused tests and a small Spark parser check passed. See the
[bounded staging evidence](04-build/evidence/current-upstream-release-20261021/local-current/xxhash64-typed-json-staging-20261022.json);
it does not qualify Delta writes or managed Unity Catalog publication.

A separate pinned local compatibility smoke executed one bounded GraphFrames
one-hop query over a frozen synthetic Delta fixture (three nodes, two parallel
edges) and matched both complete result rows, including their distinct edge
IDs. Its first attempt failed closed at protocol admission and is retained in
the [smoke evidence](04-build/evidence/current-upstream-release-20261021/local-current/pinned-delta-graphframes-smoke-20261021.json);
the corrected attempt records Delta 4.4.1's observed legacy protocol features.
This does not exercise an Ashlar publication or qualify full-table graph export,
scale, or production use.

The canonical `.4` GraphFrames holder and action-verifier code has been reviewed
and landed, but the persisted-carrier native action remains pending. The offline
candidate checks carrier metadata and exposes exact-version lazy frame handles;
it does not establish retained-file/exclusive-use custody or authorize an action.
See the [candidate and action-readiness evidence](04-build/evidence/canonical-graphframes-holder-candidate-20261023/README.md).

The `.4` synthetic carrier samples reached 45M nodes/225M edges on local[18]
in 73.82 seconds, writing 3.06 GB of active ZSTD Parquet. At 80% of those
measured per-table rates, the historical 1B/5B write projection was 18.05 minutes;
the later completed 225M ZSTD edge insert in attempt-02 revises the write-only
projection to 16.50 minutes at 80%, still above the 15-minute owner-review gate.
The SNAPPY candidate was stopped at its polled output threshold before INSERT
completion, with no valid timing comparison. The held trial remains held. The
68.05 GB size calculation is fixture arithmetic, not a
production capacity estimate. This larger run improved throughput over local[4]
but below ideal 4.5x core scaling, especially for edges. Its redundant
`source_position=id` and decimal `source_delivery_id` are synthetic; actual
accepted-feed storage costs remain unknown. Four point probes showed hash,
source and type pushdown; truncated plans did not verify ID pushdown or file
pruning. A static Delta-log stats check found that the current sequential-ID
files admit one candidate file for the full tuple probes while hash-only admits
all 18; it does not measure runtime files read or prove clustering. See the
[file-statistics analysis](04-build/evidence/current-upstream-release-20261021/local-current/canonical-carrier-file-statistics-candidates-20261021.json),
[18-core calibration](04-build/evidence/current-upstream-release-20261021/local-current/canonical-carrier-xxhash64-18core-calibration-20261021.json)
and [earlier samples](04-build/evidence/current-upstream-release-20261021/local-current/canonical-carrier-xxhash64-sample-20261021.json).

A bounded local comparison of the pinned 225M-edge insert removed regular
Parquet statistics for `source_delivery_id` while preserving the selected
lookup-key statistics and the exact point result. It saved 238,615 Parquet
bytes in this pair. The observed 32.11s baseline and 25.80s candidate insert
times are one unprofiled sequential pair, not a general speed claim; the 80%
row-linear 1B/5B gate estimate remains above 15 minutes with node time unchanged.
The candidate is not promoted to source configuration. See the
[completed Parquet-statistics pair](04-build/evidence/parquet-field-statistics-paired-20261023/README.md)
for execution scope, limitations, and receipt hashes.

A follow-up local hash-range comparison completed cleanly. Sorting by the full
identity tuple within each hash range reduced the random-hash layout from 7.68 GB
to 4.25 GB and from 68.56 to 58.24 write seconds; Spark reported one file read
for each of four exact point probes. The original hash-only sort sample's writes
and probes completed, but its process exited 1 during final receipt projection;
that partial status is retained in the [comparison evidence](04-build/evidence/current-upstream-release-20261021/local-current/canonical-carrier-hash-range-materialization-20261022.json).
The identity-sort layout still took 49% longer and stored 39% more active bytes
than the 38.98-second/3.06-GB sequential-identity baseline, so neither hash-range
layout is the default. Its 80%-throughput write-only arithmetic for 1B/5B is
26.96 minutes and remains held. The successful comparison and its synthetic
value limits are in the [identity-sort evidence](04-build/evidence/current-upstream-release-20261021/local-current/canonical-carrier-identity-sort-materialization-20261022.json).
The R05 reassessment records the latest ZSTD 1B/5B write-only estimate at
16.50 minutes, based on attempt-02's completed 25.962255-second 225M-edge
insert; its supervisor receipt retains process-group uncertainty. Earlier
estimates were 18.05 minutes and a nonselected statistics sensitivity at 16.425
minutes. The SNAPPY candidate exceeded its polled output threshold without
completing INSERT, so it supplies no timing result. These estimates exceed the
15-minute owner-review gate and do not authorize the held scale trial
([R05 budget and gates](04-build/evidence/r05-end-to-end-budget-design-20261023/README.md)).

[Layout experiments](02-design/spikes/SPIKE-001-table-layout.md) retain existing
local and native performance measurements. Metrics guide tuning; they do not
change the managed-Delta architecture. Functional checks remain small; explicitly
requested scale trials use dedicated compute and approved resource/cost limits.
Graph-tool exports and shape checks alone do not qualify actual
Fabric, PuppyGraph or GraphFrames execution over Ashlar publications.
Fabric R1/R2 activation and interrupted-replacement evidence is still held:
the [fresh functional plan](04-build/evidence/commerce-fabric-fresh-functional-plan-20261023/README.md)
requires current source/ACK, capacity, path and cleanup admission before launch.

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

---
ddx:
  id: SPIKE-001
  type: spike
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: FEAT-002
    kind: informed_by
  - id: FEAT-003
    kind: informed_by
  - id: FEAT-004
    kind: informed_by
---

# SPIKE-001: Delta graph table layout

## Goal and Baseline

Find a concrete table layout for 1 billion nodes and more edges that keeps native
Databricks singleton lookup fast, retains Truss's property-oriented meaning, and
maps to PuppyGraph, GraphFrames and Microsoft Fabric Graph. The owner selected
Fabric and authorized reasonable provisional budgets on 2026-10-05.

Compare generic JSON-text bags, shared promoted scalar columns, and per-type /
per-relationship scalar serving tables. Truss ADR-002 is source evidence for
canonical shape, not a warehouse performance baseline. Its 2× hand-designed SQL
ratio does not become Ashlar's absolute lookup target.

## Provisional Performance Gates

Targets below are experiment defaults, not measured support or production SLAs.
They apply at a recorded warehouse size and include request/queue/network timing
where labeled. Budget owners must review hardware/cost and workload before
acceptance. Use 1B nodes / 5B edges as the planning scenario, plus 2B/10B edge
sensitivity runs; 5B is a chosen planning default, not supplied consumer evidence.

| Shape | Provisional target | Bounds and measurement |
| --- | --- | --- |
| Native singleton, warm available SQL warehouse | p95 ≤100 ms, p99 ≤250 ms engine; p95 ≤250 ms caller-to-result | Identity predicate; one row; result cache disabled; disk cache reported |
| Native singleton, cold data on running warehouse | p95 ≤1 s caller-to-result | Random identities; startup/resume measured separately |
| Filtered list / one-hop | p95 ≤500 ms caller-to-result | 100 rows; selective start/filter; incomplete marker on cap |
| Fixed two-hop | p95 ≤2 s caller-to-result | 100 outputs; explicit edge/path multiplicity; ≤100k candidate-edge work gate |
| Grouped count | p95 ≤3 s caller-to-result | Scoped to declared selective cohort; full 1B-node scans are batch analytics |
| Native publication freshness | p95 ≤60 s after complete feed boundary | Provisional 10k changed entities/s, burst 100k/s; source transport included separately |
| Concurrent native reads | Start at 32 readers, sweep 1/8/32/128 | Report throughput, queue, tail latency and hourly cost |

Never present a result LIMIT as a bound on scanned or joined work. High-degree
starts must execute under an actual resource gate or return incomplete. Deferred
full-scan aggregates require explicit asynchronous/batch treatment, not a false
interactive promise. No unselected dollar budget is claimed; cost is a required
report and owner acceptance input.

## Concrete Table Candidate

[CONTRACT-003](../contracts/CONTRACT-003-delta-graph-tables.md) and
[Delta DDL](SPIKE-001-table-layout/sql/delta-candidate.sql) define the surface.

| Layer | Shape | Physical candidate |
| --- | --- | --- |
| Canonical objects | Source/type/id, logical key, exact property-ID bag, retained map, version/origin | Liquid cluster source_system/type_id/id; singleton uses all three |
| Canonical edges | Relationship/id, typed source/target, property/retained bags | Liquid cluster relationship/source/target; explicit edge IDs |
| Journal/tombstones | Property-level changes and deletion evidence | Progress/entity clustering; retention explicit |
| Manifest | Exact Delta table-version vector plus source-progress vector | Append descriptor after validation; fixed-version native reads |
| Typed serving nodes/edges | Identity keys, selected scalar properties, presence flags and exact carriers | Per-type identity/filter layout; per-relationship endpoint layout |
| Optional reverse adjacency | Narrow target/source/edge identity rows | Target clustering; measure duplication/refresh cost |

### Native Layout Variants

L: unpartitioned liquid-clustered tables. Z: separate tables partitioned by type
or relationship only when each partition is sufficiently large, Z-ordered within
partitions by identity/endpoints. Test unpartitioned Z-order too where sparse types
make partitions ineffective. Never partition by individual node IDs. Candidate
L2 reduces object keys to type/id for a single authority; compare source/type/id
when multiple authorities share the canonical table. Candidate A adds reverse
adjacency; it must preserve edge identity, multiplicity and publication version.

Databricks states liquid clustering cannot be combined with partitioning/ZORDER,
requires statistics on cluster keys and upgrades Delta reader/writer protocols.
These are separate experiments; no portable-reader support is inferred.
[Clustering guidance](https://docs.databricks.com/aws/en/tables/clustering),
[data skipping](https://docs.databricks.com/aws/en/tables/data-skipping),
[partition guidance](https://docs.databricks.com/aws/en/tables/partitions).

## Truss Alignment and Explicit Differences

Evidence read: local Truss ADR-002 and checkout 522edced2570360e1da5ea23ce561d7a2b43ec35.
The potential-consumer input references a newer Truss feed Contract absent from
this checkout; its exact wire/transaction semantics remain unverified.

Retain catalog property/type IDs, original object/edge IDs scoped by source,
typed endpoints, canonical object/edge property maps, retained unknown content
and per-property history. Keep logical primary keys distinct from internal IDs.
Replace PostgreSQL JSONB with exact delivered JSON text; replace FK/index
mechanisms with evidenced publisher checks and columnar layout. Typed serving
projections are rebuildable and do not replace the generic canonical model.
Whole-entity synthetic events are a test profile, not a claim that Truss emits them.

## Local Screening Execution

Executed with local DuckDB CLI 1.5.6, four threads, 2GB memory cap, deterministic
three-type graph, 512-character repeated property padding, two relationships,
parallel paths and one high-degree hub. Two scales: 30k nodes / 90k edges and
300k nodes / 900k edges. Warm in-process tables; one discarded warmup plus seven
recorded samples per shape/layout. No client transport or concurrent readers.

[Runner](SPIKE-001-table-layout/run.py), [generated SQL](SPIKE-001-table-layout/sql/),
[raw summary](SPIKE-001-table-layout/out/screening.json) and retained JSON query
profiles make the screening reproducible. All 25 layout/query comparisons at
both scales have identical result digests for the equivalent query shape.

The local screen queries in-memory native DuckDB tables, not Parquet files,
Delta tables or a Spark warehouse. Parquet exports measure compressed footprint
only. Sorting is not proof of Delta file pruning, Z-order or liquid clustering.
Seven-sample p95 is effectively a maximum and is descriptive only; use at least
200 requests per native configuration and ≥1000 for p99 evaluation.

Adapter examples and their independent small-fixture shape checks are retained in
[adapters](SPIKE-001-table-layout/adapters/). The check recovers original property
maps, detects vertex/edge identity collisions and missing endpoints, retains
isolated A0, and confirms four paths/two distinct targets. It does not execute
PuppyGraph, GraphFrames or Fabric. `check_adapters.py` is the reproducible check.

### Observed Results at 300k Nodes / 900k Edges

| Query | Bag median ms | Shared promoted median ms | Typed median ms |
| --- | --- | --- | --- |
| lookup | 0.956 | 0.624 | 0.583 |
| list | 18.506 | 0.827 | 0.716 |
| count | 19.414 | 1.177 | 1.109 |
| one_hop | 0.808 | 0.693 | 0.728 |
| two_hop | 2.324 | 2.392 | 2.389 |
| reverse | 2.593 | 2.597 | 2.58 |
| hub_bounded | 0.85 | 1.889 | 1.009 |
| hub_count | 3.984 | 4.313 | 3.9 |

Promoted shared list/count was approximately 22×/16× faster than JSON extraction
on this synthetic screen. Typed and shared-promoted two-hop medians were nearly
equal; no broad advantage for canonical per-type tables follows. Reverse
adjacency had noisy tails and provides no sound local p95 selection evidence.
The repeated payload compresses unrealistically well; compressed bytes are not
extrapolated to 1B nodes.

## Native Execution Evidence

The owner authorized dbw-aidev-cus on 2026-10-05. The active aidev-cus profile
accessed its existing data-gateway 2X-Small serverless Photon SQL warehouse.
[Native evidence](native-layout-evidence.md) records statement IDs, runtime,
uncached metrics, isolated schemas and limitations. The actual seven-table DDL
passed, exact JSON carriers survived native storage, version-pinned reads retained
old state after update, and validation detected a Delta-accepted duplicate.

The first partitioned CTAS rejected partition column type_id in the data-skipping
statistics list. The corrected partitioned variant collects id statistics while
type_id supplies partition pruning. This failure and recovery are retained in
native statement evidence. No other schemas/data or warehouse settings changed.

## Native Spike and Stop Conditions

[Databricks notebook](SPIKE-001-table-layout/databricks_spike.py) creates disposable
candidate tables and runs differential singleton/list/count/reverse/two-hop
queries. The notebook is prepared and syntax-checked. The equivalent native SQL runner
was executed against the SQL warehouse; notebook execution remains untested. It requires an explicit
existing sandbox catalog/schema; it does not provision infrastructure or select
a cloud/runtime. Spark driver timing is not SQL warehouse caller latency.

Run scale phases 10k/type smoke → 1M/type → 10M/type → 100M nodes → 1B nodes,
with edges 5× nodes and observed skew/property-width/type-count distributions.
The supplied notebook starts with a simplified 3× edge-ratio fixture; the 5×
production-planning phase requires extending the generator before scale acceptance.
The completed 3M-node run remained mostly one-file scans with 100% IO-cache
reads. Before any larger-scale admission, introduce high-entropy payloads at
0.5/2/8 KB, variable type counts and enough files that the identity predicate can
prove pruning. Record default/adaptive file sizing and clustering optimization
cost; do not infer benefit from OPTIMIZE completion alone.
Capture cold/warm random lookups, negative lookups, concurrent reader sweeps,
result-cache disablement, disk cache state, queue/startup time, bytes/files
scanned versus table size, shuffle/spill, compaction cost and publication churn.
Require all variants to match the independent corpus before comparing latency.

Stop a configuration when it violates exactness/integrity, exceeds the resource
cap or offers no pruning at scale. Advance scale only with a reviewed cost cap
and target budget. Accept a layout only when native gates, interruption/recovery
and consumer mappings have evidence. None of those gates is satisfied by this
local screening. Native SQL execution is now available and bounded tests ran on dbw-aidev-cus.
No external graph engine was run; native evidence does not prove 1B-node admission.

## Consumer Mapping Gates

PuppyGraph: map typed node/edge scalar tables using independent IDs and endpoint
keys. Its current docs expose Delta/Unity Catalog connectivity and row filters;
verify the selected binary against exact Delta features, fixed-publication reads
and delegated policy. A documented mapping is not an executed connector.
[PuppyGraph model](https://docs.puppygraph.com/modeling/),
[Delta connector](https://docs.puppygraph.com/connecting/connecting-to-delta-lake/).

GraphFrames: build vertices from all node tables with unique id and edges with
src/dst plus edge identity/properties. Never infer vertices solely from edges,
which would exclude isolated nodes. Pin a common publication and exact Spark/
GraphFrames versions before motif/identity tests.
[Creation guide](https://graphframes.io/04-user-guide/01-creating-graphframes.html).

Fabric: materialize bounded scalar projections into supported OneLake sources;
treat refresh as a separate engine publication. Current documentation permits
roughly 2B nodes+edges total and excludes nested Delta map/struct ingestion;
therefore the owner’s 1B nodes plus more edges does not qualify for full graph
support. Exact decimals and unsupported/oversized properties stay in the canonical
source with a named projection residual. Profile strings against the published
65,535-byte property limit. Never silently split cross-scope edges or claim
federated traversal between projection scopes.
[Limits](https://learn.microsoft.com/en-us/fabric/graph/limitations),
[refresh](https://learn.microsoft.com/en-us/fabric/graph/manage-data).

All source documents above were inspected on 2026-10-05 as live documentation,
not pinned runtime release evidence. External engine profiles, delegated access control and source-position
correspondence remain UNTESTED. Bounded native SQL measurements are recorded
separately and do not admit billion-node performance.

## Recommendation

Proceed with generic canonical objects/edges and selective scalar serving
projections, as proposed in ADR-001. Keep singleton lookup native. Review bounded native Delta results and close the failed singleton latency
gate before accepting production clustering, partitioning or Z-order settings. Resolve exact UMF bindings after its capabilities merge.

## Incremental native screening (2026-10-05)

[Ingest evidence](native-ingest-evidence.md) measures corrected 10k/100k/600k object batches, exact property-string journal recovery and serving parity on a 1M-object seed. Publication windows are 20.274/25.605/40.578 seconds, excluding staging. [Eight-client evidence](native-concurrency-evidence.md) retains old fixed-version values after newer writes, but the load overlaps staging rather than canonical/serving MERGEs. Engine singleton p95 is 366ms and fails the gate. A CLI timeout was recovered by the existing successful statement handle, never by resubmission. Sustained arrivals, end-to-end freshness, failure-safe publisher, edges under ingest, high-file-count pruning, cold I/O and billion-node/cost admission remain open. Prior invalid-journal run a1 is timing only, excluded from conformance.

## Persistent-client and file-pruning screening (2026-10-05)

[Latency controls](native-latency-floor-evidence.md) remove per-call CLI process creation using official SDK 0.102.0. No-table and one-row Delta controls still fail both warm latency gates; observed execution minima were 151ms and 165ms. [Multi-file pruning](native-pruning-evidence.md) proves native identity pruning across 32-file liquid and 64-file diagnostic bucket layouts with exact carrier parity. Smaller liquid files reduce bytes read by about 8.2x, but do not materially improve warm engine p95 (278ms versus 276ms). No production file-size/bucket winner is selected. Cold I/O, cross-type/source distributions, edge ingest, continuous freshness and billion-node/cost admission remain open.

## Cold I/O and scheduled arrival evidence (2026-10-05)

[Remote-I/O-qualified reads](native-cold-read-evidence.md) observe 1.33–1.45s caller tails in small cold subsets, failing the provisional 1s budget. [Scheduled publication streams](native-stream-evidence.md) retain source arrival clocks and include staging: serial freshness reaches 118s; parallel independent writes with range pruning improve it to 74s but still accumulate backlog and fail <=60s. Every tested object batch passes canonical/serving/journal value checks. Native cold-read, continuous-ingest and billion-node admission remain open; do not equate previous staging-excluded 600k batch timing with sustained freshness.

## SQL-driver and catalog-commit probes (2026-10-05)

[SQL-driver controls](native-driver-evidence.md) achieve uncached warm engine p95 76–96ms on the bounded 1M-object fixtures; a transport/query-shape matrix confirms this is not solely a UUID artifact. Caller p95 279–336ms remains above 250ms. Native parameters preserve exact returned carriers but do not remove the caller gap. [Catalog-commit probe](native-atomic-evidence.md) proves tiny two-table commit and intentional rollback, preserving exact stored text. No full publisher/scale/external-reader claim is made, and no workspace preview flag changed. Repeat cold/concurrent tests through the selected driver and test complete catalog-commit publication/version recovery before revising production design.

## Driver cold reads and atomic stream (2026-10-05)

[Driver cold evidence](native-driver-cold-evidence.md) supports 16MiB as a singleton candidate: 27 remote-I/O-qualified requests have 698ms caller p95, while the 128MiB subset reaches 1218ms. [Atomic stream](native-atomic-stream-evidence.md) applies and publishes all six batches but stops at its backlog bound after final freshness reaches 131s. Duplicate validation and in-window synthetic generation are attributed explicitly. [Catalog-managed lookups](native-catalog-lookup-evidence.md) are slower in this candidate comparison, so catalog commits are not selected as a singleton latency optimization. Warm caller, ongoing ingest, metadata scale, edges and cost admission remain open.

## Bounded next scale stage

[Completed 10M-node evidence](native-10m-scale-evidence.md) proves exact canonical carrier and identity parity in 419 files (6.44GB). Mixed-I/O singleton p95 is 295ms engine and 522ms caller, with 11 remote-reading requests among 50 measurements; median one file read and 417–418 pruned. The result fails the overall latency gates; the subsequent repeated warm control below separates cache effects before attributing the regression to scale. It does not admit ingest or billion-node/edge scale.

The repeated-key control is now complete: two later passes have zero remote bytes and zero result-cache hits, with engine p95 93–94ms and caller p95 331–337ms. The engine gate passes in that bounded workload; the caller gate still fails. An I/O-cached prime pass reaches 115ms engine p95, so first-pass overhead is not solely remote I/O. A six-batch prebuilt-producer ingest control keeps full duplicated carriers and transaction validation, includes staging/version discovery/publication in freshness, and excludes producer payload generation plus redundant outside validation. Its completed results are recorded below.

[Prebuilt-producer results](native-prebuilt-ingest-evidence.md) are complete:
all six transaction checks and publications pass, but final oldest freshness
reaches 128s. Later batches still take 25–29s versus ten-second arrivals.
Producer generation and redundant outside validation do not close the gap.
Compare explicit narrow projections with canonical residual retrieval next;
preserve the property journal and exact canonical carriers throughout.

## Narrow projection intervention

[Narrow-serving evidence](native-narrow-serving-evidence.md) validates all six
published snapshot hydrations (600k changed rows total) and exact singleton
carriers. Active serving files shrink about 90x to 8MB, while the property journal
and complete canonical bags remain available. Freshness still reaches 105s and
misses the 60s target. The variant remains experimental in CONTRACT-003;
staging/publication overlap and a version-pinned scalar projection over canonical
state are the next throughput comparisons. Full canonical-schema integration,
edge loads, caller latency, cost and billion-node admission remain unproved.

## Staging overlap intervention

[Pipeline evidence](native-pipeline-ingest-evidence.md) preserves all transaction
checks and six immutable snapshot hydrations while overlapping staging on one
independent driver session. Final oldest freshness improves to 76s but still
fails the 60s target. Later publication takes 18–19s, with median atomic apply
15.92s alone exceeding the ten-second arrival interval. Next compare a pinned
scalar projection over canonical state and test edge identity/adjacency layouts.
No existing general-purpose clusters were listed for an in-region caller test;
that evidence and larger-stage spend constraints remain pending.

## Pinned scalar projection intervention

[Canonical-only publication evidence](native-pinned-projection-evidence.md)
removes the physical serving write and proves all six complete snapshot and
scalar projection checks. Oldest freshness ends at 57.5s, within 60s for this
short schedule. The growing 31s queue and 13.3s median atomic apply still prevent
sustained-throughput admission at ten-second arrivals. Next compare larger
publication batches without excluding their accumulation window, and exercise
edge identity versus adjacency layouts. External-engine compatibility and
query-time scalar costs remain unproved for this variant.

## 200k batching intervention

[Batching evidence](native-200k-batching-evidence.md) preserves the 10k/s rate
with 200k changes every 20s, including the accumulation window. Six batches
apply 1.2M changes across 600k repeatedly updated objects, with exact prior-state
and journal checks. All snapshot hydrations pass; oldest freshness stays within
43.8–44.9s and queue delay remains near four seconds after startup. This is
promising two-minute evidence with little headroom, not sustained/burst or
billion-node admission. Integrate full contract columns, representative edges,
longer arrivals and concurrent reads next. Billing usage remains missing on a
bounded recheck; no zero-cost inference or larger-stage approval follows.

## Full schema integration

[Full object/journal/manifest evidence](native-full-contract-ingest-evidence.md)
uses CONTRACT-003 columns and keys, 16MiB object files and experimental catalog
commits. Six repeated-update batches apply 1.2M changes with all snapshot checks,
manifest descriptor checks and journal-key uniqueness passing. Oldest freshness
stays below 53s, but queue delay ends near 13s; sustained throughput and headroom
remain unproved. Native edge layout comparisons, concurrent publication reads,
longer arrivals, cost and billion-node admission remain required.

## Edge identity versus adjacency

[Native edge comparison](native-edge-layout-evidence.md) proves full canonical
carrier parity, typed endpoint resolution and 200k retained parallel endpoint
pairs across 1M edges. ID clustering reads one of 18 files for singletons,
versus all nine endpoint-clustered files, reducing median bytes about 17.6x.
Engine p95 is 79ms versus 93ms, while caller latency fails. Narrow adjacency
preserves identical outgoing edge IDs in 6.7MB but has only one file, so scale
pruning is unproved. Carry ID-first canonical edges plus selective narrow
adjacency forward as a candidate; edge ingest, hubs, reverse/two-hop, external
readers, costs and billion-edge admission remain open.

## Concurrent reads during full-schema publication

[Two-reader evidence](native-publication-reader-evidence.md) preserves all
572 exact old-snapshot reads and six later snapshot hydrations, while oldest
freshness remains below 54s in the bounded schedule. Read latency fails:
376 no-remote reads overlapping atomic writes have 151ms engine p95 and 552ms
caller p95. Idle warm-key passes do not admit the combined workload. Compare
ordinary Delta plus manifest publication against catalog-managed commits next;
retain exact pinned snapshots and explicit partial-write failure semantics.

## Ordinary Delta comparison

[Ordinary publication evidence](native-ordinary-publication-evidence.md) retains
all exact checks and six snapshot hydrations, but no-remote readers overlapping
the apply interval still reach 145ms engine/490ms caller p95. Oldest freshness
ends at 59.8s with approximately 19s queued. The comparison does not select
ordinary writes as a latency fix; partial-write recovery is separately unproved.
Test burst and edge publication next, retaining existing compute bounds and
explicitly leaving read/write isolation and in-region latency unmeasured.

## Provisional burst test

[Burst evidence](native-burst-evidence.md) applies 1M changes accumulated over
ten seconds (explicit provisional duration for 100k/s) using the full schema,
with all preservation and snapshot checks passing. Newest freshness is 60.6s
and oldest 70.6s, so this scenario fails the 60s target even without concurrent
readers. The duration is not owner-approved and one sample is not population
p95. Edge publication and bounded compute interventions remain next; the short
10k/s pass does not admit burst performance.

## Edge property publication

[Edge ingest evidence](native-edge-ingest-evidence.md) updates 200k independent
edge identities with full canonical/journal/manifest columns, preserving exact
bags, retained content, typed endpoints and pinned adjacency hydration. Oldest
freshness is 45.4s including a 20s accumulation window. This single batch uses
fixed endpoints and does not prove adjacency mutation cost or steady combined
node/edge rates. A complete graph descriptor must include the fixed node-table
version as well; the edge-only result does not admit a full graph release.

## Explicit graph vector and traversal

[Graph-vector evidence](native-graph-vector-evidence.md) now pins node, edge,
adjacency and journal table versions, verifies typed endpoint existence and
adjacency parity, and retains 200k parallel pairs plus 9.8M isolated vertices.
It closes the fixed-node reference gap for this synthetic snapshot, not source
transaction/recovery or engine compatibility.

[Fixed-vector traversal](native-fixed-vector-traversal-evidence.md) preserves
all edge/path multiplicity and observes two-hop caller p95 704–744ms. Narrow
forward/reverse list tails pass bounded 500ms screening. Two-hop work admission
remains unproved: median rows scanned exceed 2M despite only 25 path outputs.
Implement explicit expansion-budget rejection and test degree skew next; output
cardinality cannot replace the candidate-edge work gate.

## Traversal guard and degree skew

[Budget evidence](native-traversal-budget-evidence.md) rejects a 20,001-edge
root with 120,006 multiplicity-preserving candidate expansions before path
fetch. A regular root retains all 25 paths, and an isolated existing node returns
empty results. Degree and adjacency are pinned to exact versions. This is an
explicit experimental logical-work definition, not a physical scan cap or
production admission; the regular request takes 2.14s. Optimize coordinator
round trips and prove derived-degree publication and physical I/O separately.

[Server-side script comparison](native-server-guard-evidence.md) preserves all
regular, hub, isolate and missing-vertex outcomes, but measured regular caller
p95 is 2.57s. It does not close latency admission. Parent execution includes
internal script processing; compare a fused relational admission query plus
admitted traversal next without changing the logical work definition.

`native_scale_10m.py` tests the actual CONTRACT-003 canonical object columns and source/type/id liquid keys at 10M nodes, with a 16MiB file-size target. It reuses a fixed 1M-property payload pool ten times; this is stated explicitly rather than presented as independent per-node entropy. Existing warehouse only, 180s session statement timeout and 15min preparation wall bound; expected active object data approximately 6–8GB. Exact row/identity counts, logical keys, full stored-carrier equality and warm composite lookups are required. No 1B or 5B-edge admission is implied. The larger-stage cloud-spend ceiling remains pending.

Fused relational admission plus admitted traversal now passes the bounded 2-second caller screen: regular p95 1,526.93 ms across twenty measured roots; hub, isolate, and missing controls all pass. See [fused guard evidence](native-fused-guard-evidence.md). This is a static logical expansion guard, not a physical scan cap or maintained degree publication. Singleton, concurrent ingest, burst, and billion-scale gates remain open.

### Scattered updates across the 10M canonical table

[Scattered-ingest evidence](native-scattered-ingest-evidence.md) fails freshness at 110.96s oldest / 90.96s newest for one 200k-change batch with 20s accumulation. All changed rows and all 9.8M untouched full-carrier rows pass exact preservation checks. The aggregate atomic block is 84.62s, but intermediate MERGE history is only 6.75s with 419 deletion vectors and eight new files. Next combine repeated validation scans while preserving correctness; do not attribute aggregate duration to data-file rewriting or admit scattered ingest from prior concentrated batches.

### Fused validation on scattered updates

[Fused validation evidence](native-fused-validation-evidence.md) preserves full prior/current/journal checks, explicitly detects missing rows, and passes eleven adversarial guard controls. Oldest freshness improves sequentially to 93.46s but still fails 60s; MERGE is 5.03s versus 67.51s atomic block. Post-run adaptive plans show shuffled outer joins. Next test broadcast matching with total **and distinct typed-key** cardinality checks so eliminating outer joins cannot hide missing/duplicate rows. No sustained or billion-scale admission follows.

### Broadcast matching with distinct identity cardinality

[Broadcast evidence](native-broadcast-validation-evidence.md) passes one scattered 200k-change freshness screen at **45.55s**, with exact changed/untouched checks and twelve adversarial guard controls. Atomic wall is 19.40s; cache residency also rose to 94%, so the sequential speedup is not exclusively attributed to joins. Processing is 25.54s versus 20s interarrival, leaving sustained capacity unproved. Next fixed-schedule overlapping staging / modestly larger batches; then concurrent singleton reads and burst. Aggregate broad scans still prevent billion-scale inference.

### Scheduled 300k/30s scattered publications

[Scheduled broadcast evidence](native-scheduled-broadcast-evidence.md) runs four fixed-clock batches at modeled 10k/s with independent staging. Oldest freshness is 59.30, 60.24, 59.22 and 61.74s: **two miss 60s** despite little queue. All published changed/journal checks and final 8.8M untouched full-carrier rows pass. Next 250k/25s at the same rate, then concurrent singleton readers; neither this bounded schedule nor high-cache 10M scans admit the full scale gate.

### 250k schedule and singleton after ingest

[Combined evidence](native-250k-and-post-ingest-evidence.md): all four 250k/25s batches miss 60s, reaching 65.86s with 15.65s queue. Preservation passes. Maintenance versions 13–15 occur outside this harness's submitted records; initiator/policy is not established. Post-ingest pinned singleton warm p95 is **117ms engine / 391.8ms caller**, reading 65–67 files instead of the clean fixture's one/two. Next measure reclustering cost and its effect on the same lookup workload; account for maintenance before claiming ingest and read targets together.

### Reclustering restores pruning with material maintenance cost

[Reclustering evidence](native-recluster-evidence.md): explicit incremental OPTIMIZE takes 69.05s; the identical key/projection read drops from 65–67 files to 2–4. No-remote engine p95 is 83–84ms, caller 367–371ms still fails. All 51 returned carriers match pre-maintenance records; this is not exhaustive maintenance parity. Next test write organization or partition/bucket candidates and account for maintenance alongside ingest; the warm engine pass does not establish the full read, freshness or scale goal.

### Physical replacement with ID-only clustering

[Insert-clustering evidence](native-insert-clustering-evidence.md): one atomic physical DELETE/INSERT batch preserves update journal semantics and full identities/carriers, with 54.30s oldest freshness. Immediate reads remain at 3–5 files, but engine p95 91–107ms is inconsistent and caller 391–438ms fails. Processing 34.29s exceeds 20s interarrival. This single-source/type experiment does not establish a generic ID-only layout. Next composite physical lookup-key candidate retaining exact native identity, mixed-source/type correctness, repeated publications and read/maintenance accounting.

### Composite physical lookup key with overlapping native IDs

[Composite baseline](native-composite-lookup-evidence.md) preserves 10M typed identities across five sources/two types with only 1M native IDs. One explicitly derived pruning column clusters the full tuple; exact native predicates remain mandatory, including a forced-collision control. All 10M hashes/carriers validate. Singleton reads one file; cached engine p95 81–92ms passes, caller 367–382ms fails. This 14-column candidate has separate experimental DDL and does not replace normative CONTRACT-003. Next multi-domain scattered publications and concurrent reads, with hash/journal integrity and maintenance measured.

### Composite publication and concurrent pinned lookup

[Integrated evidence](native-composite-publication-evidence.md): 200k mixed-domain changes publish at 57.24s with full preservation and 218 exact old-vector reads. No-remote atomic-overlap p95 is **133ms engine / 450ms caller**, failing both targets despite one-file reads. Immediate post-write lookups remain at two files, repeat engine 83–90ms passes but caller fails. One batch does not admit sustained rate. Next inspect existing running compute for contention isolation, then repeated publications and edge identity/adjacency; new resources remain subject to the pending cost bound.

### Mixed-domain canonical edge identity and narrow adjacency

[Edge evidence](native-composite-edge-evidence.md) validates 10M typed edge identities, all endpoints against fixed nodes, exact carriers and all 2M parallel pairs. Hash-clustered edge point reads one file and cached engine p95 75–79ms passes; caller fails. Narrow adjacency preserves identical five-edge outgoing results and is faster than canonical scanning. Only one warehouse runs; isolation needs new resource bounds. Next edge publication with pinned graph vectors/readers, then structural adjacency/degree maintenance; no static or billion-scale inference.

### Composite edge property publication

[Publication evidence](native-composite-edge-publication-evidence.md) preserves all 200k changed and 9.8M untouched edge rows, exact journal origins, descriptor readback and 213 pinned reader carriers. Oldest freshness 53.66s passes one batch; processing 33.65s exceeds its 20s interval. No-remote overlapping reads reach p95 123ms engine / 446ms caller, failing both gates. Post-write engine 75–85ms passes with two-file pruning; caller 362–401ms fails. Next structural adjacency/degree maintenance, with full-scale and sustained admission still open.

### Structural preparation and degree arithmetic

[Preparation evidence](native-structural-preparation-evidence.md) establishes a complete 2M-group degree baseline for 10M edges and valid disjoint stages for 20 deletions/20,001 hub insertions. All endpoints resolve; each deleted edge has one retained parallel counterpart. Independent projected hub work is 120,003 after deletion adjustments, exceeding the 100k logical budget. Structural publication and maintained degree/admission checks are next; this arithmetic does not prove runtime latency or a physical scan cap.

### Structural atomic application

[Structural evidence](native-structural-publication-evidence.md) records canonical/adjacency/degree/journal/tombstone atomic application in 15.05s, full parity for 10,019,981 edges, exact 17-field surviving/inserted carriers and zero unresolved endpoints. Maintained hub work matches 120,003. An incomplete revision descriptor requires the separately verified r3-2 correction; original evidence is retained. This is one synthetic batch, not rate admission. Next actual budget refusal, path/parallel controls and pinned old snapshots, then concurrency/recovery.

### Structural read controls

[Read evidence](native-structural-read-evidence.md) verifies independently reconstructed paths: affected root 20 paths, unchanged root 25, old snapshot 25, and every deleted edge's retained parallel counterpart. Application admission refuses the hub at 120,003 without path fetch; ordinary/isolated controls pass. Single admission samples are hundreds of milliseconds and do not admit latency or physical work limits. Next structural rollback/recovery and concurrent pinned reads.

### Structural rollback fault

[Fault evidence](native-structural-rollback-evidence.md) verifies explicit SIGNAL rollback after six catalog-managed table writes: unchanged versions and affected contents, absent receipt and unchanged external descriptor. An initial attempt including the ordinary manifest was rejected and separately confirmed unchanged. Server-side rollback passes; response-loss, fencing and descriptor recovery remain open.

### Receipt descriptor gap recovery

[Recovery evidence](native-receipt-recovery-evidence.md) commits one native property update with a durable vector receipt, closes the session before descriptor publication, then independently validates and publishes from exact pinned versions. Two sequential recovery passes yield one descriptor and zero data replays. Planned versions rely on exclusive writer, and this does not prove network-loss or concurrent/fenced recovery. Next concurrent read and fencing controls.

### Concurrent recovered full-row singleton reads

[Concurrent evidence](native-recovered-concurrent-read-evidence.md) preserves all 102 full 17-column old/new carriers across two paced clients. Cached engine p95 78/82ms passes, caller 525/373ms fails, at one/two files with no remote or result-cache reads. Favorable one-identity workload does not admit broader concurrency or scale. Next fencing, repeated rate/maintenance and in-region/isolation latency work under resource bounds.

### Publisher fence contention primitive

[Fence evidence](native-publisher-fence-evidence.md) yields one winner and one terminal row-conflict loser from a synchronized two-session epoch race; one receipt exists. Revoked stale holder fails without changing versions. This is a private primitive, not graph/manifest integration. Next conditional fence writes guarding recovered publication and duplicate contenders, with ingest overhead measured.

### Fenced duplicate descriptor recovery

[Fenced recovery evidence](native-fenced-recovery-evidence.md) combines an actual conditional fence write with private catalog-managed manifest publication. One racing recovery succeeds, one conflicts, and one exact descriptor remains; sequential recovery leaves its version unchanged and stale epochs cannot publish. Single successful caller sample 3.741s must be budgeted, not treated as negligible overhead. Graph-data fencing and integrated repeated throughput remain next.

### Fenced graph-data integration

[Integrated fence evidence](native-fenced-graph-evidence.md) rejects stale data writes and guards the active property/journal/receipt transaction plus separate descriptor transaction with actual conditional fence writes. Pinned old carrier stays exact during the descriptor gap; complete new descriptor validates. One-event clock is 6.70s data / 12.55s through descriptor, not rate admission. Next repeated multi-domain ingest and concurrent reads with all overhead included.

### Four fenced 200k/20s publications

[Scheduled evidence](native-fenced-scheduled-evidence.md) fails the rate/freshness screen: processing 30.61–33.64s, queue grows to 37.19s, oldest freshness 50.62/63.69/77.18/90.83s. Stages prebuilt and no concurrent readers make this favorable, not full ingest admission. All 800k journals, 9,219,981 untouched 17-field carriers and descriptors validate separately. Next reduce redundant integrated validation/metadata work without weakening guards and measure post-write multi-key lookup behavior; new resources require pending bounds.

### Post-schedule multi-key singleton behavior

[Read evidence](native-scheduled-post-read-evidence.md) preserves independent ten-column carriers at canonical version 10 without OPTIMIZE. Median six files; engine p95 98/103/95ms, caller 448/389/379ms. Caller fails all phases and engine misses one. Clock attribution shows post-change checks are significant, but atomic apply plus publication already exceeds arrival budget. Next controlled validation/batch tuning and counted maintenance.

### Matched post-change validation hint

[Hint evidence](native-validation-hint-evidence.md) preserves identical 200k/zero-mismatch results across 16 pinned checks, alternating ordering. Caller median falls from 8.34s to 2.27s with staged-side broadcast. Small cached comparison does not prove integrated throughput. Next three 300k/30s fenced batches with all checks retained and counted.

### Three hinted 300k/30s fenced batches

[300k evidence](native-fenced-300k-evidence.md) misses every freshness observation (64.68/65.34/68.57s), with processing 30.66–34.66s versus 30s arrivals. All 900k journals, 9,119,981 untouched 17-field carriers and three descriptors validate. The hinted separate post-check still costs 3.1–6.6s. Next transactional fused post-state validation retaining exact invariants, then integrated measurement; no scale/concurrency admission.

### Fused run exposed physical version allocation failure

[Version mismatch](native-fused-version-mismatch-evidence.md): first data transaction succeeds at edge 14/journal 15, but receipt predicts journal 12. Three intervening OPTIMIZE versions have unverified initiator. Actual-version guard stops descriptor publication. All 300k changed post/journal predicates verify at actual snapshots; no complete run or rate admission. Next durable commit identity and maintenance-aware version resolution, then corrected immutable publication, without replaying committed data.

### Content marker version resolution prototype

[Marker evidence](native-marker-resolution-evidence.md) resolves actual current/journal commits 3/4 across metadata-only intervening versions, differing from prior+one and latest. Durable receipt/session restart and exact contents pass. No later logical writes and complete retained history are prerequisites; a production barrier remains unimplemented. Next resolve and verify the already committed r17 batch without data replay.

### Committed r17 recovery without replay

[Resolution evidence](native-r17-resolution-evidence.md) resolves actual edge 14/journal 15 from fixture-unique source markers, verifies all 300k changed journal/carrier fields and 9,719,981 untouched 17-field carriers, then publishes corrected immutable r17-1-resolved under the fence. Original invalid receipt remains and no data replays. Manual pending-batch sequencing is not production barrier enforcement; original rate clock remains failed. Next enforce barrier and test row-commit metadata resolution before another integrated throughput trial.

### Strict native row-commit resolution

[Row metadata evidence](native-row-commit-resolution-evidence.md) resolves edge 14/journal 15 with strict non-null complete 300k-row counts and one distinct metadata version each. Independent history/content resolution agrees. Initial caller samples 579/472ms support testing this cheaper path, not a percentile or rate claim. Actual maintenance preservation and pending-publication barrier are next; native row IDs never replace Truss identity.

### Row tracking across actual reclustering

[Maintenance evidence](native-row-tracking-maintenance-evidence.md) verifies actual removed/added files and exact preservation of all 10k native rows, row IDs and commit versions across OPTIMIZE FULL after a clustering change. Selected runtime case supports native resolution, not general lifecycle/external compatibility admission. Next atomic pending-batch barrier, then integrated ingest/read clock.

### Pending-publication barrier protocol

[Barrier evidence](native-pending-barrier-evidence.md) persists pending state through restart, rejects a later data batch unchanged, rolls back descriptor insertion and barrier clear together on fault, then allows a next batch only after valid publication. Old pinned snapshot remains exact. Cooperative private protocol only; full graph/races/permissions not integrated. Next repeated graph batches with strict actual row-commit resolution and counted barrier overhead.

### Graph barrier with actual commit vectors

[Integrated evidence](native-barrier-graph-evidence.md) publishes two 300k batches without guessed versions and clears pending atomically. All 600k changed/journal fields, 9,419,981 untouched carriers, origins and descriptors validate. Freshness 75.58/87.82s fails, processing 45.57/42.24s exceeds 30s arrivals. Resolution costs 1.14s; atomic data dominates at 36.3–39.6s. Next actual plan/layout evidence and counted post-write maintenance/read behavior.

### Fused post-validation join plans

[Plan comparison](native-post-plan-evidence.md) preserves exact results; two broadcasts caller 6.62/5.09s beats forced journal sort-merge 11.02/8.19s. Initial plans show the latter loses full Photon execution. Both report missing canonical/journal optimizer statistics. Retain the existing hint and test targeted stats with maintenance cost measured; committed-read samples are not integrated transaction admission.

[Targeted statistics trial](native-optimizer-stats-evidence.md) costs 2.643s maintenance and preserves all four 300k exact validations. Before 4.096/4.471s and after 4.250/4.043s overlap; no demonstrated improvement. Read-only inspection confirms current-catalog statistics and full current-plan recognition, but matched pinned plans still report canonical/journal statistics missing. Keep exact verification and the existing hint. Next post-ingest singleton pruning and counted reclustering; no ingest, caller or full-scale gate is admitted.

[Post-ingest edge reclustering](native-edge-recluster-evidence.md) compares full 17-field plus hidden-metadata reads at versions 16/19. One incremental OPTIMIZE costs 54.977s and makes three commits; median files read improve six to two despite active file count increasing 344 to 388. Final warm engine p95 98ms passes, caller 444ms fails; post-rewrite prime includes remote I/O. All 51 sampled rows preserve exact carriers and tracked identities. Next exhaustive maintenance parity and a bounded partition/Z-order comparison; no sustained, caller or billion-scale admission.

Follow-up exhaustive maintenance verification passes all 10,019,981 rows across versions 16/19: zero missing rows, zero complete-carrier or hidden-metadata differences and unique typed keys at both versions. See the updated [maintenance evidence](native-edge-recluster-evidence.md). Partition/Z-order comparison remains next; preservation does not close performance gates.

[Four-bucket partition/Z-order build](native-partition-zorder-evidence.md) passes exhaustive 17-field and derived-bucket parity for 10,019,981 edges, ending at separate candidate version 1 with 312 files/5.12GB. Copy 41.766s and Z-order 52.831s are explicit setup/maintenance costs. Native keys remain authoritative; four fixture partitions do not establish a full-scale optimum. Next matched full-projection singleton and incremental ingest comparisons.

Matched alternating full-field singleton comparison passes all keys on both layouts. Final warm engine p95 is 84ms on each, caller 380ms partition / 393ms LC still fails. Partition median files one versus LC two; prime cache conditions differ and no causal winner is established. See updated [partition evidence](native-partition-zorder-evidence.md). Next paired scattered ingest and counted maintenance with complete journal/publication semantics.

Paired ingest preparation preserves the full 2,820,023-row journal multiset and all 17 prior fields for the same 300k staged entities on each layout. Journal copy costs 19.784s; stage preparation 16.524s outside the future arrival clock. No apply/publication or rate pass is claimed. See updated [partition evidence](native-partition-zorder-evidence.md); guarded apply and counted maintenance remain next.

[Paired guarded publications](native-partition-ingest-evidence.md) complete with actual versions LC edge20/journal18 and partition edge2/journal1. Processing 50.449s / 29.914s yields assumed 30s-window oldest freshness 80.449s / 59.914s. Independent checks pass 300k changed/journal records and 9,719,981 untouched full carriers per layout, unique typed keys, buckets, vectors and cleared barriers. One sequential batch and distinct physical histories do not establish sustained throughput or causal layout superiority. Next inherited history/descriptor metadata follow-up, post-write reads and counted maintenance.

Inherited-history verification now passes both complete 2,820,023-row multisets and all publication/receipt metadata. Untouched-key post-ingest control passes complete carrier checks and warm engine p95 77ms LC / 80ms partition, caller 374ms / 366ms still fails. Coverage finds zero updated IDs among reused keys; changed-key latency is unmeasured and is next before counted maintenance. See updated [paired evidence](native-partition-ingest-evidence.md).

Deliberate changed-key comparison now covers 51 updated identities across all ten native domains, preserving all canonical fields with independent new payload/hash calculation. Final no-remote warm engine p95 103ms LC / 113ms partition fails, caller 412ms / 415ms also fails. Median files three / two. Thus the untouched-key pass does not establish post-ingest admission. Next counted maintenance and matched changed-key follow-up; see updated [paired evidence](native-partition-ingest-evidence.md).

[Counted maintenance](native-partition-maintenance-evidence.md) takes partition Z-order 59.145s (5.45GB removed) versus incremental LC 16.257s (68MB removed), ending at experimental edge versions3/21. Cleanup scopes differ; no continuous policy winner is proved. All sampled 17-field carriers match. Final warm engine p95 LC78ms/partition86ms passes, caller378ms/385ms fails. Existing descriptors still pin edge20/partition2. Next exhaustive hidden-metadata preservation and guarded equivalent-vector publication, then integrated maintenance/read/ingest evidence.

Maintenance follow-up passes exhaustive 10,019,981-row field/hidden-metadata parity on each layout and unique typed keys at both versions, including partition buckets. Equivalent guarded descriptors now pin edge21/journal18 LC and edge3/journal1 partition, retaining progress/revisions and leaving barriers clear. See updated [maintenance evidence](native-partition-maintenance-evidence.md). Next repeated arrivals with maintenance inside the publication clock and fixed-vector readers; no performance gate is promoted.

Two common disjoint 300k-entity stages from LC21 are prepared for the maintenance-inclusive comparison, with unique typed keys/ordinals, uniform 2KB new properties and independently checked payloads in every native domain. Native IDs 240001–270000 / 270001–300000. Preparation is outside the future clock; two 30s arrivals per layout can expose queue growth but cannot establish sustained admission on a pass. Actual integrated apply/maintenance/publication and readers remain next.

[Maintenance-inclusive arrivals](native-maintained-schedule-evidence.md) complete both two-batch clocks, including maintenance and exact post checks before publication. LC freshness104.334/149.854s and partition131.806/191.657s all fail with growing queues. Actual overlap readers preserve every old 17-field carrier but fail engine/caller p95 (LC209ms/1377ms, partition307ms/952ms). Guarded changed records pass at LC23/19 then26/20 and partition5/2 then7/3. Next exhaustive final untouched/history/metadata/barrier verification, then change the tested policy/layout; no combined or full-scale admission.

Final independent verification now passes both schedules: 9,419,981 untouched complete carriers, 600k changed/journal records at their own and final vectors, unique typed identities, exact 3,120,023-row inherited journal multisets, all descriptors/receipts and clear barriers. See updated [maintained evidence](native-maintained-schedule-evidence.md). Next a separate 64MiB hash-clustered wide copy, capturing remote first-touch reads before exhaustive parity warms it. Fewer files versus larger singleton I/O is a hypothesis to test, not an improvement claim; the 16MiB combined failures remain authoritative.

[64MiB wide copy](native-lc64-evidence.md) at version0 preserves all 10,019,981 canonical rows and unique native identities. Active files452→128, median lookup files4→1; actual median/max file bytes46,318,196/70,779,781. Remote first-touch caller p95 921ms passes bounded running-compute screen; warm engine86ms passes, caller357ms fails. Compile p95 remains similar (16MiB174ms/64MiB177ms), so planning improvement is unproved. Copy52.161s, distinct physical lineage, no journal/publication or ingest admission yet. Next matched guarded ingest rather than redundant reads.

[Matched LC64/16 ingest](native-lc64-ingest-evidence.md) completes one common300k guarded batch per layout. Atomic33.977s/46.862s,processing39.853s/52.719s,assumed30s-window freshness69.853s/82.719s both fail. LC64 publishes edge1/journal1,LC16 edge27/journal21; independent300k changed/journal and9,719,981 untouched full-carrier checks,unique identities,actual vectors and cleared barriers pass. Inherited3,720,023-row history is exact before apply; post-apply history/metadata readback is next. Then exact conditional MERGE with negative rollback evidence may fuse prior checking into DML; no omission, speedup or gate admission is assumed.

Post-apply inherited3,720,023-row history and full descriptor/receipt metadata now pass both layouts. [Conditional MERGE controls](native-conditional-merge-evidence.md) pass one clean exact update and16 native rollback cases, preserving canonical hidden identities and all four table states on rejection. Full prior matching moves into a null-safe17-field matched condition, backed by marker absence plus complete post/origin guards. Broader epoch/publisher-marker qualification and full300k timing remain next; no performance or production semantic admission.

Epoch-isolation follow-up and three separate publisher-ID controls now pass on native tiny tables. Older epoch history and unrelated older publisher batches sharing producer positions remain exact; current publisher marker reuse on staged/outside rows rolls back all four table states. Derived `apply_batch_id` is experimental physical metadata, not an approved native schema addition or producer checkpoint guarantee. See updated [conditional evidence](native-conditional-merge-evidence.md). Next full300k timing with epoch-qualified journal selectors, separate publisher identity and all exact guards; ingest, caller, sustained/burst and billion-scale gates remain open.

Conditional apply preparation now passes300k disjoint prior carriers with unique typed identities/ordinals and independent2KB payloads across ten domains. LC64 canonical/journal remain at1/1. Stage creation17.318s is setup outside the future clock. [Evidence](native-conditional-merge-evidence.md); full conditional apply and publication timing remain next, with no performance admission.

[Conditional full300k apply](native-conditional-merge-evidence.md) completes in31.968s with actual canonical/journal3/3. Assumed30s-window freshness61.968s still fails. Independent exact changed/journal,9,719,981 untouched carriers,unique keys,all hidden row identities,inherited journal multiset,descriptor vector and clear barrier checks pass. Full metadata readback and post-update singleton performance remain next; no sustained or full-scale admission.

Conditional publication metadata readback now passes. Deliberate updated-key reads at edge3 preserve all17 fields and independently calculated payloads; final warm engine p9591ms passes,caller363.25ms fails. Median17 files,zero remote bytes/result-cache hits. [Evidence](native-conditional-merge-evidence.md). Next quantify fragmentation/count incremental maintenance, then include it with reads in the publication clock; no combined/full-scale admission.

[Conditional-state maintenance](native-conditional-merge-evidence.md) takes33.993s,version3→5,files148→136,DV marks600k→0. It also rewrites inherited history,so not an isolated300k cost. Same51 carriers pass;medianlookupfiles17→2,finalengine93ms passes,caller420.89ms fails. Separate cost accounting oldest95.961s fails;newversion5 remains unpublished. Next exhaustive18-field/hidden-metadata parity,then revise the tested policy/caller path rather than repeating a failing schedule.

Exhaustive version3/5 maintenance parity now passes10,019,981 complete18-field rows including publisher ID and hidden row/commit metadata, with unique typed keys at both snapshots. Version5 remains unpublished. Final server p95328ms alone exceeds caller250ms,so network-only changes cannot make this measured workload pass. Next a materially revised query/execution/maintenance policy; no unchanged bound-parameter repetition or full-scale inference. [Evidence](native-conditional-merge-evidence.md).

Paired exact-key versus hash-candidate reads at verified5 pass full carriers but fail caller gates:final344.06ms candidate/368.04ms exact. Candidate modestly lowers compilation158ms vs173ms p95 while reading3medianfiles vs2. All result-cache countszero;forced collision/refusal controls unproved. [Evidence](native-conditional-merge-evidence.md). No contract promotion;retain exact native SQL. Next materially different execution/transport path under resource bounds rather than repeat this failed shape.

Native execution discovery identifies Lakehouse Real-Time Beta as a separate read-only Delta/UC serving hypothesis. Fresh warehouse inventory contains only data-gateway,so no existing RT resource can be used. [Scoped plan and primary references](native-conditional-merge-evidence.md) require pinned-snapshot/protocol/cache controls,quoted cost and resource bound before creation. No workspace preview/compute change or latency admission. Ordinary SQL retains ingest/maintenance; UMF remains deferred.

RT requests are prepared for51 exact full17-field identities pinned5 with recorded expected rows/digest,not submitted. Browser eligibility check reaches sign-in;localSDK/connectorlackRTtype/kerneloption. DirectSEAtransport can avoid that localclient gap,but workspace eligibility,costbound and exact snapshot/cache controls remain prerequisite. [Evidence](native-conditional-merge-evidence.md). No new resource or preview changed.

Proposed ashlar-delta/0.2 DDL now has native execution evidence for12tables in private schema `client_dev.ashlar_layout_v02_20261006_r65`:canonical hashes/separate publisher IDs,property journal,tombstone,manifest,typed examples,forward/reverse adjacency,degree summary,fence and receipt. Native degree CHECK rejected;direction/count invariants remain explicit publisher checks. Confirmed9completedtables and resumed only3missingCREATEs;raw `out/native/ashlar_layout_v02_ddl_20261006_r65/` and `_r65_resume/`. Empty tables/DDL only;no scale,source semantics,publication enforcement or external-reader claim. [DDL](SPIKE-001-table-layout/sql/delta-layout-v02.sql). Next tiny full-profile publish/read/conformance fixture and actual engine mapping adapters,not latency-gate loops.


### Native 0.2 publication fixture — 2026-10-06

The [r66 result](SPIKE-001-table-layout/out/native/ashlar_layout_v02_fixture_20261006_r66/summary.json) passes on `dbw-aidev-cus`, using the existing warehouse and private `client_dev.ashlar_layout_v02_20261006_r65` schema. Three nodes and three edges cover repeated numeric IDs across types, parallel edges, a self-loop and an isolate. Checks cover independently recomputed identity hashes, exact property/retained JSON text, six unique whole-entity journal origins, endpoint resolution and complete declared degree coverage. Read-only negative predicates identify a dangling endpoint and invalid degree direction/count; they are not transaction rollback tests.

The immutable publication pins object/edge version 1, journal version 2, tombstone version 0, and forward/reverse adjacency and degree version 1. A later unpublished object mutation does not change the pinned read. Consumers must use the manifest vector, never assume latest tables form a publication. Coordination rows are illustrative; the receipt digest is a placeholder. This fixture does not establish fencing, recovery races, cross-table atomicity, property-level missing/null journal semantics, producer adaptation, external-engine execution, latency or billion-node capacity.

Next reconcile adapter examples to the 0.2 identities, exact carriers and actual version vector. Validate isolated vertices, parallel edge identity and scoped endpoint closure in each export. Keep engine execution and refresh behavior as separate evidence obligations; defer UMF bindings.


The [r67 publication-scoped export](SPIKE-001-table-layout/out/native/ashlar_layout_v02_export_20261006_r67/summary.json) passes native read/Python conformance for all three nodes and edges from the actual r66 manifest, including exact carriers, parallel edge identities, self-loop, isolate and endpoint closure. Decimal-string native IDs avoid JSON numeric coercion. No external-engine support is established. The proposed GraphFrames helper now requires explicit projection versions instead of implicit latest-table reads. Next qualify per-engine release materialization and refresh against these same invariants.


### Native release and scale planning — 2026-10-06

[r68](SPIKE-001-table-layout/out/native/ashlar_layout_v02_release_20261006_r68/summary.json) materializes four Delta release tables from pinned canonical versions and verifies every exported field. Projection versions are all 0, independently recovered from actual histories. External engines remain unexecuted; no engine activation or permission-enforced immutability claim.

[Capacity sensitivity](SPIKE-001-table-layout/out/capacity-planning-v02.json) separates assumptions from measurements. At 1B nodes/5B edges, assumed mean stored carrier sizes of 512–2,048 bytes imply 3.072–12.288 TB canonical storage and about 45,777–183,106 target-sized 64MiB files. Two 96-byte adjacency copies add 0.960 TB before other overhead. A hypothetical one 1KiB journal event per changed entity at continuous 10k/s adds 26.542 TB over 30 days; property-event fanout multiplies it. These are decimal bytes, arithmetic estimates only, excluding retained versions, compaction overlap and serving exports. Repeated-payload compression is not a sizing baseline. This makes history retention and optional projection coverage material design inputs before a larger-scale run.

The selected architecture remains canonical typed identity plus exact JSON carriers, hash-clustered native singleton reads with full identity predicates, separately clustered optional adjacency/degree tables, and actual-version publication manifests. The physical tuning candidate is 64MiB liquid clustering; it is not a proved universal file-size or billion-node performance winner. The owner’s fixed Unity Catalog Delta direction supersedes historical architecture admission gates, while measured latency/freshness shortfalls and full-scale unknowns remain recorded.


### Property-journal carrier evidence — 2026-10-06

[r69](SPIKE-001-table-layout/out/native/ashlar_property_journal_v02_20261006_r69/summary.json) passes five synthetic property events in a separate native Delta table at version 1. Exhaustive reads preserve absence versus explicit JSON null, integer `9007199254740993`, decimal/exponent token `1.2300e+04` and timestamp text with `+05:30`. Origin uniqueness and present/token consistency pass. `entity_version=5` is an explicit fixture boundary marker, not a maximum derived from property versions. This verifies storage only: state reconstruction, native feed ordering, transactional publication and as-of support remain unproved. CONTRACT-003 now enumerates source-profile evidence needed before a Truss adapter claim; the inspected checkout lacks the referenced feed CONTRACT-006. No existing publication table was modified.


The later local-branch audit found Truss draft CONTRACT-006/CONTRACT-002 at `spec/change-feed-and-groups`, `6d87fce`. This supersedes the missing-source-spec observation above. The draft supplies safe `(xid,seq)` watermark ordering and record-version scope, but also reveals a preservation gap: Ashlar 0.2 journal omits origin and additional revision/provenance/reservation record content. CONTRACT-003 now records the mapping limits and an unexecuted supplemental raw source-record DDL candidate. Neither the draft nor the candidate proves real transport/producer support. No Truss branch or worktree was changed.


[r71 supplemental source-envelope evidence](SPIKE-001-table-layout/out/native/ashlar_source_record_20261006_r71_resume/summary.json) passes five kinds in a private table at version 1, including exact nested reservation key text, unknown origin/record content, revision bytes and an unsigned-range tuple cursor carried as strings. r70 is explicitly failed preservation evidence due to SQL literal escape loss. r71's initial VALUES expression was rejected without inserts; the recovery verified empty version 0 and used INSERT SELECT byte decoding. No native Truss transport, watermark, atomic retry or current-state reconstruction claim is made. The supplementary carrier remains outside ashlar-delta/0.2 pending a versioned source profile.


[r72](SPIKE-001-table-layout/out/native/ashlar_source_record_replay_20261006_r72/summary.json) now establishes actual single-table atomic raw-record replay/refusal. Identical replay preserves every field; a byte-different payload with valid digest plus a new row fails the entire MERGE, and exhaustive post-failure comparison retains all five rows and excludes the sixth. No concurrency, cross-table publication, native delivery identity or source watermark support claim. ADR-001 now carries the supplementary source-retention decision proposal and separates this statement evidence from production fencing requirements.


Proposed ashlar-delta/0.3 now incorporates the raw source-record table and native cursor/direct origin references on current/history/tombstone rows. Scalar positions are nullable only under qualified tuple-source profiles. Thirteen active CREATE definitions and four reference extensions are structurally checked; full native DDL and semantic reference validation are pending. Prior 0.2 publications remain unchanged. The candidate package identifies this distinction explicitly.


[r73 0.3 DDL](SPIKE-001-table-layout/out/native/ashlar_layout_v03_ddl_20261006_r73_resume/summary.json) now passes all 13 native CREATEs and verifies four pairs of STRING cursor/origin-reference fields. No 0.2 migration. A rejected schema-selection statement was recovered by confirming the newly created schema empty and using explicit table qualification. Semantic reference validation remains next; DDL success does not establish source compatibility or new publisher performance.


[r74](SPIKE-001-table-layout/out/native/ashlar_layout_v03_references_20261006_r74/summary.json) completes one synthetic 0.3 reference-bound publication with seven raw records and actual versions for eight tables. Missing delivery ID, cursor mismatch and cross-epoch current-row controls are detected before any manifest exists; restored rows publish with original tuple progress. This is not transaction rollback or crash recovery evidence. Canonical carriers came from the pinned 0.2 fixture; raw envelopes omit some structural fields despite their synthetic full-carrier label, so no raw reconstruction/native feed completeness claim is made. Producer cursor grammar and native transport remain unqualified.

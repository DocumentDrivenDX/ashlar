---
ddx:
  id: CONTRACT-003
  type: contract
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
  - id: ADR-001
    kind: informed_by
---

# Contract: Delta graph tables

**Contract ID**: CONTRACT-003
**Type**: schema
**Version**: proposed ashlar-delta/0.3 (0.2 evidence preserved)
**Status**: Draft

## Purpose

Define actual warehouse tables close to Truss's accepted generic object/edge
layout, with rebuildable scalar-column projections for graph tools and filtered
analytics. Singleton lookup MUST be native Databricks SQL over Delta; no Fabric
Graph dependency is permitted for that path.

**Owner architecture direction (2026-10-06):** Canonical state resides in Unity
Catalog managed Delta tables. Latency is an operational measurement, not an
architecture acceptance gate; preservation, identity and publication correctness
remain contract requirements.

## Scope and Boundaries

The [0.3 DDL](../spikes/SPIKE-001-table-layout/sql/delta-layout-v03.sql) is the proposed revised surface; all 13 CREATEs and new cursor/reference column checks now pass in private schema `client_dev.ashlar_layout_v03_20261006_r73`. The [0.2 DDL](../spikes/SPIKE-001-table-layout/sql/delta-layout-v02.sql) remains the preserved executed baseline. All12 CREATEs and table descriptions now pass on the existing
dbw-aidev-cus warehouse in private schema `client_dev.ashlar_layout_v02_20261006_r65`.
Degree CHECK constraints were rejected and removed; the publisher must validate
direction and nonnegative counts. This is DDL evidence, not end-to-end correctness.
The preserved
[0.1 baseline](../spikes/SPIKE-001-table-layout/sql/delta-candidate.sql) has all seven
table CREATE statements passed on dbw-aidev-cus SQL channel 2026.38
in an isolated synthetic schema; see the native spike evidence. Physical-layout
settings are experiment candidates, not approved compatibility claims. UMF
binding and source catalog schemas remain deferred. Truss's catalog IDs and
source-local object/edge IDs are retained without renumbering.

## Current qualification summary (non-normative, through r691)

The latest [complete seventh publication](../spikes/SPIKE-001-table-layout/seventh-publication-disposition-r602.md)
selects N6/E10/R7/J7/A8/T7 for the named private tables in its immutable vector.
These labels are role-local versions, not a global revision. Node physical head8
is distinct from selected node6. The synthetic bootstrap and seven disjoint100k
batches preserve exact current carriers, raw/unknown content, property-level
history, typed endpoints and tombstones. Final selected current is8M nodes/
39.93M live edges. Edge physical head12 is distinct from selected edge10 after
the bounded maintenance experiment. Production source authority, canonical constraint enforcement
and cross-role writer fencing remain unqualified. This does not approve the draft.

Retain unpartitioned identity-hash liquid clustering and the64MiB initial file
target as the physical default candidate, with exact full native key predicates.
The [matched full-guard pilot](../spikes/SPIKE-001-table-layout/file-target-merge-disposition-r580.md)
and [post-ingest reads](../spikes/SPIKE-001-table-layout/file-target-post-disposition-r583.md)
do not justify promoting256MiB: on a2.5M-row slice it reduces file count32 to16
and lowers one MERGE engine observation7.232s to5.553s, but adds21.108s maintenance
engine/1.504GB writes and nearly doubles changed-key read bytes. Changed-key
repeat p95 is64:94ms engine/398ms caller versus256:111ms/421ms. All complete
carriers/CDF/typed keys pass. These small ordered cached cohorts cannot establish
a universal file-size winner or transfer their timing to the40M-edge graph.
Actual emitted files, deletion vectors and maintenance cost remain measured
policy inputs; targetFileSize is not a maximum or promised file geometry.

Latest complete ready-input processing is213.056s; its current MERGE takes
70.787s caller/69.648s engine and reads35.775GB. Both exceed the provisional60s
freshness budget before source preparation or arrival is accounted for. The
seventh publisher integrates complete concurrent closing checks, all15 uncached
post-commit validations, closed commit intervals and exact manifest readback.
Its151 native statements have final telemetry. This is one synthetic observation,
not producer-arrival p95, sustained10k/s or100k/s burst admission. Metadata
observations remain distinct from a production writer fence.

The [sparse maintenance experiment](../spikes/SPIKE-001-table-layout/sparse-maintenance-disposition-r612.md)
rewrites ten broad-overlap files into six in21.722s, with two same-SID OPTIMIZE
commits11/12. Every removed/added full20field multiset matches bidirectionally;
common file path/size/livecount/extrema remain unchanged. [Complete common-file preservation](../spikes/SPIKE-001-table-layout/common-preservation-disposition-r628.md)
now checks all39.48M common-file live carriers; combined with450k rewritten-file
carriers, all39.93M live snapshot contents match, including deletion-vector
filtering. Independent raw Delta-log action custody and producer fencing remain
separate unproved obligations. Existing E10
publication remains pinned; no maintenance manifest or source progress advances.
The complete maintenance/preservation experiment takes51.970s/4.615GB read/
0.363GB write, separately from17.435s baseline preparation.

[Matched E10/E12 reads](../spikes/SPIKE-001-table-layout/sparse-read-disposition-r616.md)
include identities inside and outside the selected range. All32 full-record or
deletion-absence responses pass. Repeated E12 engine p95 is96ms and caller374ms;
median files11.5→2.5 and aggregate read bytes fall73.8%. This tiny serial cached-data
cohort passes the scoped engine target but misses caller250ms; it proves no
service-wide warm/cold/concurrency gate. Retain overlap-triggered maintenance as
a policy candidate with explicit cost and preservation admission, rather than
blind range sweeps or a promised per-batch60s cleanup. Current physical heads
must be reconciled independently of selected publication pins before new writes.
Controlled cold/concurrent service and actual1B-node/5B-edge admission remain open.
Unity Catalog Delta stays selected independently of those misses.


[Broader maintained read evidence](../spikes/SPIKE-001-table-layout/maintained-concurrent-disposition-r629.md)
checks64 full-carrier/absence responses with four synchronized readers and all16
native request windows overlapping. Repeated caller423ms/engine135ms misses both
targets. The [same32-key binding comparison](../spikes/SPIKE-001-table-layout/binding-disposition-r633.md)
also misses them on one client: parameters511ms/110ms, literals489ms/108ms.
Read bytes are identical and compilation shows no reproducible winner. Keep bound
parameters. The earlier8-key engine96ms pass is narrowly scoped and does not
qualify the larger cohort, sustained service, cold data or caller250ms.

[All-six-role active capacity sensitivity](../spikes/SPIKE-001-table-layout/out/full-role-capacity-r634.json)
uses115.765GB measured active bytes across nodes/currentedges/raw/journal/forward/
tombstones. Freezing this synthetic history mix projects14.495TB active roles at
1B nodes/5B edges, excluding retained versions, staged inputs, logs, failed-work
storage and additional projections. This is arithmetic, not native admission,
pricing or a production producer history profile. Doubling the current graph
models231.530GB active roles, before validation/retention/peak storage. Scale
writes still require measured full-role generation and explicit phase bounds.

The [independent append extents](../spikes/SPIKE-001-table-layout/edge-growth-disposition-r670.md)
now preserve 8M new nodes and 8M new edges with their complete raw records,
property events and forward adjacency. The pinned old/new node union contains
16M distinct typed identities; all 16M new-edge endpoint references resolve.
These private extents preserve historical IDs using a disjoint allocation profile,
rather than regenerating old carriers with a larger node-count parameter.
They leave the selected publication unchanged and do not establish one 16M-node
serving table or the complete 80M-edge target. [Observed-head preflight](../spikes/SPIKE-001-table-layout/next-edge-preflight-disposition-r691.md)
reconciles seven background maintenance commits through edge13/raw13/journal14/
adjacency12 with full-field parity. The two unchanged adjacency blocks reuse
explicitly matched cached results; all changed-head digest blocks were uncached.
This is content preservation evidence, not a production writer fence or retention
proof. The next 8M-edge growth stage has a completed independent local oracle
and combined preflight/growth bounds of 85GB reads, 35GB writes and zero spill.
Its execution must be audited before any larger-scale or throughput claim.

[File geometry](../spikes/SPIKE-001-table-layout/file-overlap-disposition-r488.md)
and [throughput envelope](../spikes/SPIKE-001-table-layout/throughput-envelope-disposition-r489.md)
remain conditional capacity models, not executed billion-scale qualification.
Account for nodes, current edges, independent raw/journal/tombstone roles,
projections, staged inputs, logs, retained snapshots and failed work; edge-only
storage or cached singleton bytes are not complete scale/capacity admission.

Graph-tool claims remain scoped: local GraphFrames0.12.3/Spark3.5.3/Delta3.2.1
execution does not prove direct UC reader3/writer7 support; the local PuppyGraph
1.13 mapping does not prove canonical UC feature or billion-scale support.
Fabric uses bounded projections within its recorded2B-element limit, not the
full6B-element planning graph. Exact canonical values, typed identities and
explicit projection residuals remain mandatory. UMF binding stays deferred.
Older evidence below retains its historical versions and original scope.

## Consolidated physical candidate after complete mixed bootstrap and changes

This remains proposed ashlar-delta/0.3 under the owner-selected Unity Catalog
Delta architecture. The following dispositions consolidate executed evidence;
they do not approve a production source profile or reopen the architecture.

| Role | Physical candidate | Qualified limit |
| --- | --- | --- |
| object_current / edge_current | Shared generic BIGINT identities, exact STRING bags/retained content, typed endpoints; unpartitioned hash liquid clustering and explicit hash+full-key predicates | Complete8M-node/40M-edge synthetic bootstrap and seven disjoint100k change publications; latest selected current8M/39.94M. CTAS spike constraints are separately scoped. Full scattered-range subdivision is rejected by r457 cost evidence; singleton/cold/service/billion gates remain open. |
| source_record | Independent exact envelope/digest and string cursor; feed/epoch/delivery identity with origin-oriented LC and statistics | Every mixed bootstrap/change origin retained. Raw storage is additional to current, not replaceable by parsed current fields. |
| property_journal | Independent exact old/new tokens, presence flags and direct origin/event ordinal | Full DDL's position-oriented clustering remains proposed. Mixed experiment uses source-delivery LC; nullable scalar position and real producer access pattern require a separately measured origin-layout decision. Do not silently replace source meaning. |
| tombstone | Typed deletion identity, direct entity_version and source origin, with lifecycle version cross-checked against qualified journal/source evidence | Current0.3 DDL declares entity_version BIGINT NOT NULL; r73 native CREATE succeeded. Small r232 CTAS tombstone sample omitted this field and does not qualify the complete canonical schema. Latest-deletion/version fencing and resurrection prevention still require a source-profile proof; a tombstone row alone does not establish them. No TTL. |
| adjacency / degree / typed projections | Optional rebuildable narrow roles; include exact canonical versions in publication | Mixed forward adjacency preserves edge identity and applies updates/deletions. Reverse/degree and external engine runtime limits remain separately qualified. |
| publication_manifest | Immutable exact version vector and validated source progress, revision map and preservation report | Match metadata before publication, not after repair. Physical heads may advance independently; readers pin published versions and refuse expired snapshots. Real fencing/ACK/recovery remains unqualified. |

Implementation MUST name INSERT target columns or explicitly verify source/target
column-order compatibility before appending events. The r231 failed journal cast
left raw1 and other roles0 with an empty private descriptor; partial role commits
are not a complete publication. Schema-revision metadata MUST be validated
against the selected source/profile revisions before advancing the descriptor.
The corrected mixed-r234 descriptor is retained alongside the original empty-map
mixed-r232 evidence; its repaired clock is not substituted for a clean service
measurement.

The complete mixed bootstrap is4,096nodes/20,480edges with24,576rawrecords,
98,304propertyevents and20,480adjacency rows. Independent all-row field digests
and direct smaller differentials preserve exact values. A mixed2,048-edge batch
updates1,843/deletes205, retains26,624rawrecords/102,741events/205tombstones and
passes final identities/typed endpoints. These are synthetic bootstrap/change
profiles, not real Truss semantics or producer authority. Evidence:
[bootstrap](../spikes/SPIKE-001-table-layout/out/native/ashlar_mixed_materialize_r226/summary.json),
[apply](../spikes/SPIKE-001-table-layout/out/native/ashlar_mixed_apply_r232/summary.json),
[corrected descriptor](../spikes/SPIKE-001-table-layout/out/native/ashlar_mixed_descriptor_r234/summary.json).

The complete intermediate synthetic graph now has scoped native evidence:
8M nodes,40M edges,48M raw records,192M bootstrap property events and40M forward
adjacency rows. The [r274 audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_scale_edges_r274/audited-summary.json) checks every known field in bounded groups,
unique edge identities and both typed endpoint tuples. Qualified Delta versions
are object6/edge8/raw15/journal17/forward8, bound to native UUIDs and statement
receipts. The final8M-edge iteration took1914.135s including667.863s local oracle
preparation, read187.721GB and wrote18.896GB with zero reported spill. Sequential
active metadata totals113.297GB across2087files; it is not a retained physical
inventory. Two4M generation commits are an experimental scheduling choice, not
an isolated causal performance comparison. Growth CTAS tables omit some full
canonical constraints and do not establish the proposed64MiB target, real Truss
producer authority, publication freshness, singleton/cold/concurrency targets
or1B-node/5B-edge admission. The100k-change input/publisher test remains separate.

The subsequent [r281 publication audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_mixed_publish_r281/audited-summary.json)
applies90k updates/10k deletes from four exact pinned input roles. Final state is
8M nodes,39.99M edges/forward rows,48.1M raw records,192,216,667 journal events
and10k versioned tombstones. Full inherited/change field digests preserve
multiplicity; global identities, deletions and typed endpoints pass. The descriptor
contains exact native UUID/version inputs, actual revision map and canonical
recorded_at. This is synthetic profile qualification, not a real producer fence.
Apply-only took49.209s; ready-input processing through full validation/descriptor
readback took922.120s, failing60s freshness. Whole run1229.906s includes301.023s
clone/reference preparation. Earlier357.215s transfer,92.411s input staging and
44.096s predecessor qualification are separately reported preparation costs.
The run read372.920GB/wrote354.291MB with zero reported spill. Current merge
updates90k/deletes10k without copying unaffected rows, adds11files/72.709MB and
603deletion vectors; forward adds6files/0.9998MB and6deletion vectors. Active
shallow-clone metadata113.651GB/2107files is not retained physical inventory.
The [r284 sensitivity](../spikes/SPIKE-001-table-layout/out/publication-capacity-r284.json)
shows even apply-only service exceeds the10s modeled arrival interval at10k/s.
No sustained throughput, service p95, cold/concurrent singleton or billion admission.

The [r282 post-ingest native singleton cohort](../spikes/SPIKE-001-table-layout/out/native/ashlar_growth_pruning_r282/audited-summary.json)
passes64 exact full-carrier/absence queries on the39.99M-edge published snapshot.
This deliberately stratified warm cohort has4deleted/12updated/16unchanged keys,
repeated twice. Combined engine p95 is119ms/caller430.197ms, compilation177ms,
read189.352MB/12files; both warm latency targets miss. No remote data reads were
reported. Separate pass caller p95 values430.197/503.759ms show the small sample
is not robust service-tail evidence.614livefiles total32.038GB, including11files
with hash spans above99% of the domain. Scattered changes, deletion vectors and
wide-span emitted files motivate a separately bounded maintenance comparison,
not a claim that cleanup will meet caller latency. No controlled cold or concurrent
service result follows from the warmed scan. The [incremental-custody candidate](../spikes/SPIKE-001-table-layout/incremental-custody-cdf-candidate.md)
keeps full changed-image and inherited snapshot proof requirements explicit;
native CDF qualification is separate from source semantic history and publisher
freshness. Unity Catalog Delta remains selected independent of benchmark misses.

Complete role accounting, including exact history duplication, is required for
scale admission. The mixed8M/40M bootstrap sensitivity is115.681GB; the140GB
proposal excludes unresolved staging, changes, retained versions, failed work
and physical metadata reserves. A measured one-batch sensitivity adds353.406MB
per100k changes and raw+journal2.420TB/day at10k/s if widths/mix remain constant.
These are arithmetic, not sustained growth predictions or a retention policy.
Do not omit roles, expire source/history or infer source ACK to fit the ceiling.
[Reproducible measured role costs](../spikes/SPIKE-001-table-layout/out/mixed-role-costs-r235.json).

Original100ms engine/250ms caller, controlled-cold1s,60s at sustained10k/s plus
100k/s recovery and1B-node/5B-edge objectives remain open. Narrow engine95/96ms
passes do not close caller or service gates. Unity Catalog Delta remains selected;
PuppyGraph/GraphFrames/Fabric support stays within the separately recorded
versions/subsets/projection bounds. UMF binding remains deferred.

## Normative Surface

| Table | Logical key | Meaning and rules |
| --- | --- | --- |
| object_current | source_system, type_id, id | Canonical current object; isolated objects are valid |
| edge_current | source_system, rel_type_id, id | Canonical directed edge with independent identity and typed endpoints |
| property_journal | Source-profile origin: feed/epoch/delivery ID/event ordinal for native records; explicit legacy scalar profile separately | Property-level evidence, missing/null flags, revision and direct raw origin reference |
| source_record | source_feed, source_epoch, delivery_id | Complete exact transport envelope, cursor, kind and digest; preserves uninterpreted content |
| tombstone | source_system, entity_kind, type_id, id | Highest accepted deletion version; no TTL inferred |
| publication_manifest | publication_id | One immutable descriptor of all exact table versions and source progress |
| adjacency_forward / adjacency_reverse | source_system, rel_type_id, edge_id | Optional narrow endpoint access; independent edge identity retained |
| degree_summary | source_system, type_id, id, rel_type_id, direction | Optional independent-edge counts at the same structural boundary |
| publisher_fence | stream | Optional coordination state; publisher epoch/sequence distinct from producer origins |
| apply_receipt | apply_batch_id | Optional durable batch proof; uniqueness enforced by publisher |
| node_type_a | node_key | Example typed serving projection; not canonical |
| edge_ab | edge_key | Example typed edge projection preserving parallel edges |

IDs are signed 64-bit source-local integers in this initial Truss-compatible
profile. A source requiring another identity representation MUST use an explicit
profile/adapter; no hash or numeric narrowing is implicit. `logical_key_json`
retains the producer's exact logical primary-key tuple separately from Truss's
internal ID. The `(source_system,type_id,id)` tuple is physical source identity,
not an assertion that UMF logical identity is a generated surrogate.

All `*_json` fields are STRING containing validated exact JSON text. Property
maps use stable producer catalog property IDs as text keys. Missing member is
absent; JSON null is explicit null. `retained_json` holds unknown extension/data
content, with no silent rebinding. Exact parser and renderer are required:
JavaScript number decoding, decimal rounding and time-zone normalization are
not implicit. Native JSONB author lexical representation is retained as delivered;
this Contract does not promise recovery of a lexeme already normalized by Truss.

Edge source/target IDs resolve in the same source_system in this profile. A
cross-source relationship requires a profile revision carrying endpoint authority
explicitly. Endpoints MUST resolve by both id and type, never id alone. Catalog
relationship endpoint restrictions MUST be validated before publication. The
same endpoint pair MAY have distinct edge IDs; a producer's multiplicity rule
may reject that but MUST NOT collapse identities during projection.

`property_journal` preserves property-level source events; null property_id is
reserved for entity lifecycle events. `old_present/new_present` distinguish
absent values from explicit JSON null. Raw source event/time text MUST be retained
where interpretation is unsupported. Version scope and source transaction
boundaries are adapter inputs requiring proof; the scalar fixture entity_version
MUST NOT be inferred by taking a maximum across unrelated property versions.
Whole-entity fixture batches in CONTRACT-001 remain a synthetic profile. A Truss
property-feed adapter reconstructs accepted object/edge state at a completed
source boundary while retaining the original journal. Its native feed semantics
and missing ordering guarantees require explicit reporting before adoption.

### Graph key projection

For ASCII source-system s, numeric type t and numeric id i, node key is
`N` + decimal character count of s + `:` + s + `:` + decimal t + `:` + decimal i.
Edge key substitutes `E` and relationship type. The length prefix prevents
source-name delimiter collisions; integer components use canonical signed
base-10 without leading zeros. This encoding is injective in the selected
profile; it is not a truncated hash. src/dst are node keys of typed endpoints.
Non-ASCII source names require an explicit encoding revision, not locale coercion.

Serving tables keep exact props/retained text plus explicitly selected scalar
columns and boolean presence flags. Conversion failure MUST block projection or
produce a named residual; it MUST NOT convert unsupported values to null silently.
No generic decimal-to-DOUBLE conversion is allowed. Graph exports may use exact
text plus a separate evidenced analytic cast. Native typed columns remain bounded
by their declared precision/range. JSON strings are preservation carriers, not
proof that every graph consumer can interpret their contents.

### Publication and lookup

Publication MUST write/validate each table, capture its Delta version, then append
one complete manifest row. Only one fenced publisher may commit a manifest for
a stream; competing publishers require a later coordination design. Readers
resolve one manifest and time-travel every table to its recorded literal version.
Unmanifested newer table writes MUST remain invisible to boundary reads.
Retention MUST protect every referenced version; expired versions return
RECOVERY_REQUIRED. No cross-table atomicity claim follows from independent MERGEs.

Independent physical write lanes may leave committed versions before manifest
validation. A validation refusal MUST retain the previous manifest and MUST NOT
advance source progress; unmanifested versions require explicit state inspection
before recovery, not blind mutation replay. The
[r156 failed-lane control](../spikes/SPIKE-001-table-layout/out/native/ashlar_failure_r156/audited-summary.json)
rejects 1,000 raw payloads with self-consistent digests but changed exact bytes,
while current/history writes succeed. No new manifest is submitted, and the old
manifest plus all 1,000 old carriers remain exact. This bounded synthetic slice
qualifies validation refusal after successful writes only; process-crash recovery,
transport ambiguity, durable receipts and real producer fencing remain open.

Native lookup filters object_current by source_system, type_id and id at the
manifest's fixed version. Clustering/data skipping is the candidate optimization;
uniqueness and referential integrity require publication checks. Ordinary NOT NULL
columns do not enforce semantic keys, type rules or multi-table integrity.

Graph tools that cannot pin Delta versions MUST read immutable export tables from
one completed publication, retaining its descriptor. A mutable table/view pointer
swap is not assumed atomic across graph sources. Export materialization, engine
refresh and retained snapshots are measured separately; copying 1B nodes for every
microbatch is not an acceptable default. Frequent native publications and less
frequent bounded graph releases are separate policies.

## Precedence and Compatibility

CONTRACT-001 governs accepted state; CONTRACT-002 governs read outcomes. This
Contract maps them physically while preserving source evidence. Catalog additions
must not force canonical per-type columns; derived projection columns may evolve
through a versioned projection with explicit rebuild and loss report.

Delta clustering feature/protocol compatibility MUST be verified with each reader.
Liquid and partitioned/Z-ordered layouts are separate candidates. Type/id columns
must have statistics; key columns are never high-cardinality partition directories.
No PostgreSQL index, FK or JSONB capability is presumed to exist on Delta.

The edge physical tuning candidate adds entity_version and apply_batch_id to
file statistics alongside lookup_hash/source_system/rel_type_id/id. The logical
identity and table columns remain unchanged. Native r130 statistics controls,
r131 paired MERGEs and r133 canonical publication evidence qualify this setting
for the scoped 100k property-update fixture over 20M edges. Update-only MERGE
eligibility in ON is a separately tested tuning option; it does not establish
producer fencing or insertion semantics. Existing tables require an explicit
statistics backfill, whose measured setup cost is outside the publication clock;
older publication snapshots retain their original statistics. The reference DDL
now reflects the tested edge settings, but its revised complete 13-table package
has not been re-executed. Singleton latency, sustained ingest, freshness and
billion-scale qualification remain open. See the
[recorded native tuning evidence](../spikes/SPIKE-001-table-layout/entropy-scale-status.md).

The journal physical tuning candidate includes apply_batch_id in file statistics
alongside feed/epoch/source-position/id, without partitioning or changing its
logical columns. [r100 native evidence](../spikes/SPIKE-001-table-layout/out/native/ashlar_journal_batch_statistics_20261007_r100/audited-summary.json)
preserves all 900k journal rows and unchanged file sizes/counts and protocol.
After explicit statistics backfill, paired version-pinned checks report 26 rather
than 100 file reads and prune 74 file reads; byte volume and latency remain similar.
The old publication vectors retain the old statistics snapshot. This is a
pruning candidate, not a freshness guarantee or billion-scale qualification.

## Error Semantics

| Condition | Outcome | Recovery |
| --- | --- | --- |
| Duplicate identity, bad endpoint or broken type restriction | Validation refusal | Correct source/projection before manifest |
| Exact cast fails or meaning is unsupported | Projection refusal/residual | Preserve exact input; select explicit mapping |
| Manifest table version expired | RECOVERY_REQUIRED | Rebuild consistent publication |
| Unknown Delta reader feature | Compatibility refusal | Compatible export profile |
| Native singleton returns several rows | Integrity failure | Do not choose an arbitrary row |
| External graph release exceeds target limits | Unsupported projection | Bound scope or select another engine |

## Examples

Truss source `pilot`, TypeA catalog id 1, object id 123 maps to node key
`N5:pilot:1:123`. AB relationship id 7, edge id 800 maps to `E5:pilot:7:800`.
Singleton lookup uses source_system='pilot', type_id=1, id=123 on Delta directly.
The same edge's src/dst carry the endpoint node keys; no graph import renumbers it.

## Experimental narrow serving variant

The normative candidate DDL still duplicates full bags in serving tables.
`native_stream_narrow.py` tests an alternative with only source/type/id,
entity version, and two promoted scalar properties in the serving fixture.
Exact property-ID maps and all retained content remain in canonical state;
property 103 is deliberately unpromoted. A consumer requesting complete content
MUST resolve canonical state using the same immutable publication table-version
vector, typed identity, and matching entity version. A scalar-only external
graph export MUST declare residual properties and retained content unavailable
in that export and provide an explicit canonical retrieval path where supported.
No automatic completeness claim follows from the scalar graph projection.

The fixture has one fixed source/type and non-null scalar values. It does not
prove generic missing/null/cast policy, source reconstruction, edge mappings,
or publication recovery. Transaction checks validate canonical exact text,
scalar promotion, journal old/new values and version parity; post-run checks
hydrate every published version and compare all changed rows to exact producer
carriers. This variant remains experimental until performance, fidelity and
external-consumer evidence justify updating the normative DDL and ADR.

The subsequent `native_stream_projection.py` control removes the physical
serving copy entirely. Its scalar graph surface is a SELECT over canonical
`VERSION AS OF` from the immutable manifest, with injective node keys and
explicit residual declarations. The manifest records only actual table
versions; it MUST NOT invent a serving-table version. Complete reads use that
same canonical version. This control tests the ingest cost of maintaining
serving state separately; scalar parsing cost, connector ability to consume
the pinned projection, policy and full-scale query performance remain unproved.

## Validation Checklist

- [x] Delta DDL executes in the recorded dbw-aidev-cus synthetic sandbox.
- [ ] Duplicate/endpoint/exact-value checks and interrupted publication proven.
- [ ] Record native lookup latency and pruning with workload/compute/cache scope; tuning objectives do not gate the UC Delta architecture.
- [ ] Each engine proves mapping, protocol, identity, policy and progress limits.


## Physical design refinement to carry into the next DDL revision

The logical current-state tables remain generic Truss-compatible carriers.
Canonical identity-oriented clustering uses an optional publisher-computed
`lookup_hash` with an explicitly versioned exact-tuple encoding and validation.
It cannot replace native key predicates or identity checks. Preserve the existing
DDL as the executed baseline until the revised profile is authored and verified;
experimental hash/publisher columns are not silently normative additions.

Add narrow adjacency keyed by source authority, relationship and independent
edge ID, retaining source/target type and ID. Indexing layouts for forward and
reverse traversal may differ; parallel endpoint pairs never collapse. Degree
summaries are derived from that same published structural state and declare what
counts they represent. A logical expansion cap is not a physical scan bound.

Publisher batch IDs, fencing epochs and durable receipts are coordination
metadata. They must not overwrite or manufacture producer source-feed positions
or event ordinals. The property journal remains source evidence, with exact
missing/null and old/new token semantics. Catalog-managed atomic transactions are
an optional implementation profile with explicit protocol/reader qualifications;
the immutable manifest remains the consumer boundary in either implementation.

Graph mapping adapters consume selected typed scalar projections with complete
native identity and explicit residual/cast declarations. Their physical copies
and refresh cadence are optional workload decisions, not an automatic copy of
all canonical bags for every microbatch. Preserve an exact canonical retrieval
path where the selected consumer supports it. UMF vocabulary/source binding stays
deferred while these Ashlar storage and read contracts progress.


### Revision 0.2 field and projection rules

`lookup_hash` is required derived metadata in both current tables. Compute it as
lower-case SHA-256 hex of UTF-8 compact JSON with named members in exact order:
`source_system,type_id,id` for objects; `source_system,rel_type_id,id` for edges.
The publisher validates this value; readers still filter the full native tuple.
The encoding must have adapter differential evidence before a non-SQL producer
uses it. Hash equality does not establish native identity.

Nullable `apply_batch_id` in current/journal tables permits imported rows while
keeping coordination distinct from producer history. A selected apply protocol
must issue and validate unique batch IDs and retain durable receipts. The fence
and receipt DDL does not itself enforce race/replay protection or cross-table
atomicity. Catalog-managed transactions require a separate explicit participant
feature profile and compatibility qualification.

Adjacency logical identity is the canonical edge tuple; structural versions are
publisher projection revisions, not replacements for property event versions.
Property-only updates need not rewrite unchanged adjacency. Degree counts retain
parallel edges; self-loops count once per direction. Missing summary rows mean
zero only under declared complete relationship/direction coverage. Every derived
table that a request consumes must appear in its immutable publication vector.

Optional typed tables are examples, not an obligation to duplicate every type or
bag. Reader support and workload evidence decide projection materialization;
revised DDL execution does not establish any external graph-engine support.


Native 0.2 evidence: the [small publication fixture](../spikes/SPIKE-001-table-layout/out/native/ashlar_layout_v02_fixture_20261006_r66/summary.json) passes typed identity/hash checks, lifecycle history, endpoint/degree checks and manifest-pinned reads across an unpublished write. This is synthetic three-node/three-edge evidence. Coordination records are illustrative (including a placeholder receipt digest); no producer, recovery-race, cross-table atomicity, external-engine or scale support is promoted.


### Property-feed qualification boundary — 2026-10-06

The inspected Truss checkout at `522edce` contains accepted ADR-002 D7/D8: current object rows are canonical, per-property old/new values plus schema revision and origin are journaled in the engine transaction, and bypass SQL is not journaled. Stable property IDs depend on catalog identity. That checked-out branch does not contain the discovery input's referenced CONTRACT-006. A subsequent branch audit found draft CONTRACT-006 and CONTRACT-002 at `spec/change-feed-and-groups`, commit `6d87fce`; the qualification below supersedes the initial missing-contract finding. Draft source contracts are specification evidence, not implementation proof.

Before claiming a Truss feed adapter, require a versioned source profile specifying: (1) transaction-complete boundary and bootstrap snapshot/offset handshake; (2) origin uniqueness across feeds/epochs and exact replay/conflict rules; (3) the native version scope for each property and lifecycle event; (4) revision ordering and stable property identity; (5) current-state reconstruction or transaction-consistent full carriers, including row-home properties and retained data; (6) endpoint creation/deletion ordering; and (7) retention/checkpoint recovery guarantees. Do not fill these fields from whole-entity synthetic fixtures.

For the existing journal representation, absent means `present=false` and SQL NULL token; explicit JSON null means `present=true` and the exact text `null`. A present value requires a non-NULL JSON token. Invalid flag/token combinations must be refused before publication. Preserve producer tokens as delivered, including decimal/exponent and timezone text. Every accepted origin appears once in the pinned journal; retries with conflicting content at an existing origin are refused. Journal reconstruction requires the qualified source ordering profile and complete retained history. Truss as-of behavior cannot be advertised from mere storage parity. Retention is currently unspecified: no purge is authorized by these experiments.

The [r69 native carrier test](../spikes/SPIKE-001-table-layout/out/native/ashlar_property_journal_v02_20261006_r69/summary.json) verifies these missing/null and exact token representations for five synthetic events. It supplies no native producer ordering or reconstruction evidence.


### Truss draft feed reconciliation — 2026-10-06

Read Truss CONTRACT-006 and CONTRACT-002 at local branch `spec/change-feed-and-groups`, `6d87fce`. Their draft layout 0.2 specifies `(xid, seq)` ordering strictly below the source safe watermark; `seq` or statement time alone is not a valid checkpoint. `ver` is the record version after the change; all property rows of one change share entity/version/xid. This resolves the version-scope question at the draft-spec level, while implementation and transport conformance remain unproved.

The source carries `change`, `revision`, `source` and `reservation` records. A change includes exact old/new JSONB text, revision and origin (including unknown host keys). Create/delete envelopes include props and retained values. Retain/rebind/transform semantics require separate application rules. Revision documents have exact bytes and content hashes; native source/null distinctions require an explicit transport encoding. The 0.2 Ashlar journal alone cannot preserve origin, all source record kinds or complete revision documents. Do not advertise a lossless Truss adapter from current fixture results.

Propose the supplemental [source_record carrier](../spikes/SPIKE-001-table-layout/sql/source-record-candidate.sql), native-executed with synthetic envelopes (r71) and outside the 0.2 profile. Store every delivered record's complete exact transport envelope and cursor, including unknown fields/kinds, before interpreting it. Encode xid/seq as canonical decimal strings inside cursor JSON; do not flatten `(xid,seq)` into existing `source_position` or a publication sequence. Journal/current projections remain rebuildable only under a qualified source profile and retention policy. The exact envelope encoding, digest definition and delivery-ID derivation must be versioned before this table is adopted.

A release progress entry for this source must expose the original tuple cursor plus safe watermark and source-profile version. Source acknowledgement follows durable publication and recovery state; lost acknowledgement can replay safely. Never advance the checkpoint into a partially consumed source transaction or silently discard provenance/reservation records. Unknown source operations may be preserved without interpretation, but publication of current state requiring their meaning must stop with an explicit unsupported-operation residual. This separates lossless retention from supported interpretation.

The draft feed requires retained history past every registered consumer position unless that consumer is explicitly removed; bootstrap from a consistent snapshot and cursor remains to be designed and tested. A source ahead of the safe watermark is reported as not yet publishable, separately from publishable backlog. Transport freshness is measured from source evidence; it cannot be inferred from fixture statement timing. UMF document interpretation and bindings remain deferred; raw revision content preservation is independent of that work.


The supplemental [r71 native source-envelope test](../spikes/SPIKE-001-table-layout/out/native/ashlar_source_record_20261006_r71_resume/summary.json) now passes exact payload/cursor and digest checks for all four draft record kinds plus one unknown kind. It preserves unknown origin content, numeric JSON tokens, base64 document bytes checked against their content digest, and xid text beyond signed BIGINT range. Native source IDs/cursor delivery rules are not implemented; fixture delivery IDs and presence encoding are synthetic.

The first run r70 failed because SQL string-literal parsing removed nested JSON backslashes. That result remains rejected. Corrected ingestion uses UTF-8 bytes via `decode(unhex(...),'UTF-8')` inside SELECT expressions; the attempted inline VALUES form was terminally rejected before any rows were written. The recovery verified the existing table empty at version 0, then appended once. General ingestion must use bound parameters or a verified byte-preserving transport, never interpolated unverified SQL literals. Read-only duplicate/conflict classification does not prove atomic idempotent append.


[r72 single-table replay controls](../spikes/SPIKE-001-table-layout/out/native/ashlar_source_record_replay_20261006_r72/summary.json) pass: identical five-record retry leaves every field unchanged; one conflicting payload accompanied by a new sixth identity causes terminal SOURCE_RECORD_CONFLICT and preserves all five existing rows while refusing the new row. Stage identities and payload digests are independently checked before MERGE. This proves one statement's atomic refusal under a serialized fixture, not multi-writer uniqueness or the full publication protocol. Received time and apply batch metadata are preserved on retry; they are ingestion metadata rather than native payload identity.


The [Truss mapping candidate](../spikes/SPIKE-001-table-layout/truss-feed-mapping-candidate.md) now distinguishes source tuple cursors from scalar fixture positions, proposes injective delivery IDs with their qualifications, and specifies transaction-complete batching and acknowledgement ordering. The journal alone lacks all graph structural/current carrier fields, and provenance/reservations lack journal-style cursors. A source-profile revision plus direct journal/raw-record references and transaction-consistent source extraction is required before claiming native Truss ingestion. Existing fixture results do not close these gaps.


### Proposed 0.3 cursor/reference extension

The versioned DDL incorporates source_record and adds source_cursor_json/source_delivery_id to object_current, edge_current, property_journal and tombstone. The reference resolves the exact raw row under the same source_feed/source_epoch namespace. Cursor JSON preserves native components as strings; cursor identity is the qualified profile's tuple, not JSON member ordering. Native-source publication requires non-null valid cursors and resolvable references. Include the source_record table's actual version in every publication consuming these references. Do not flatten a source cursor into scalar source_position.

Scalar source_position becomes nullable for native tuple profiles; it remains required under the explicit legacy whole-entity fixture profile. Native journal origins are keyed by feed/epoch/delivery ID plus stable event ordinal within that envelope. The producer profile declares the ordinal mapping and validates uniqueness. Current/tombstone references identify their last accepted causal record; reconstructing full current carriers still requires a consistent extraction boundary and complete structural data. These fields preserve provenance; they do not prove a reconstructing adapter.

This is an additive draft schema change plus relaxed scalar nullability, not an in-place migration. Existing 0.2 publications/tests retain their original profile/version vectors. 0.3 native DDL execution now passes; synthetic semantic reference controls now pass; end-to-end producer/publisher tests remain pending. Exact cursor format and producer encoding remain source-profile qualifications.


[0.3 native DDL evidence](../spikes/SPIKE-001-table-layout/out/native/ashlar_layout_v03_ddl_20261006_r73_resume/summary.json) verifies the complete 13-table set and cursor/reference STRING columns on all four origin-bearing tables. The first schema-selection statement was terminally rejected after CREATE SCHEMA; recovery verified the schema empty and qualified each table explicitly. No 0.2 table was migrated. This is empty-table DDL evidence, not referential enforcement or end-to-end publication proof.


[r74 0.3 reference/publication fixture](../spikes/SPIKE-001-table-layout/out/native/ashlar_layout_v03_references_20261006_r74/summary.json) passes origin references on current objects/edges, lifecycle journal and a tombstone, plus a publication vector including raw source and structural projections. Actual mutations introduce an absent delivery ID, mismatched cursor and wrong epoch; each is detected while no manifest exists, then restored before publication. Actual committed versions and tuple progress are read back exactly. This is validator evidence under a serialized synthetic publisher, not transactional refusal or crash recovery.

The fixture's `synthetic-full-carrier/1` label refers to canonical rows supplied directly from a pinned fixture; its raw envelopes include bags/identities but omit some structural fields. It therefore does not prove raw-record reconstruction or complete native transport content. Cursor comparison is exercised on known synthetic xid/seq strings; canonical cursor grammar/range validation and untrusted source encoding remain producer-profile obligations. Existing full-carrier, endpoint and degree parity evidence applies to 0.2 and must not be promoted to a broader native source claim.


Newer uncommitted Truss working drafts were located after the committed-branch review; the [mapping reconciliation](../spikes/SPIKE-001-table-layout/truss-feed-mapping-candidate.md#newer-working-draft-reconciliation) and hashed source index supersede older side-record identity/checkpoint assumptions. Complete manifests, trusted registration/generation and verifier-observed committed application are mandatory qualifications for that draft worker profile. The 0.3 carrier schema retains bytes but existing fence/receipt/publication tests do not establish those authorities. No executable producer implementation was found in the scoped inventory.


The [portable native singleton builder](../spikes/SPIKE-001-table-layout/adapters/native-read.ts) and [r75 evidence](../spikes/SPIKE-001-table-layout/out/native/ashlar_native_read_builder_20261006_r75/summary.json) exercise pinned version, SQL hash and full native tuple predicates using named SDK parameters. Four small reads prove query/carrier correctness only, not performance or scale. Identity strings are validated within the signed-int64 profile before casting; values are not interpolated into SQL.


[r76 native adjacency evidence](../spikes/SPIKE-001-table-layout/out/native/ashlar_native_adjacency_builder_20261006_r76/summary.json) verifies bounded parameterized forward/reverse keyset reads at the published structural versions, preserving parallel edge identity and self-loops. Continuation ordering is `(rel_type_id,edge_id)` within a fixed source/type/id endpoint, direction and publication. Cursor context custody remains caller-service work, not a property of the SQL builder.


### Publication recovery requirements for the 0.3 layout

These are proposed protocol requirements; the executed 0.3 DDL remains unchanged.
A publication profile MUST identify its writer authority and the mechanism that
prevents stale owners from committing. A fence row read before a separate write
is insufficient. Until that mechanism has evidence, the implementation profile
is restricted to one externally serialized publisher with no automatic takeover.

A durable application receipt MUST bind the batch ID to exact staged content,
source profile and complete input boundary, previous publication, and validated
output table-version vector. The existing apply_receipt columns do not contain
that complete binding. A future schema revision or qualified supplemental record
is required; sequence and payload_digest alone MUST NOT imply publication success.
Digest encoding and canonical comparison are profile-versioned requirements.

| Durable state observed after interruption | Required recovery outcome |
| --- | --- |
| Raw capture only; no output receipt or manifest | Resume validation from the retained complete input; source progress remains at the prior publication |
| Some output commits; no complete output receipt | Keep the prior manifest visible; inspect actual commits and retained stage before repair; never assume a timeout means rollback |
| Complete validated output receipt; no manifest | Revalidate authority, predecessor and exact versions, then publish the same batch once; do not reapply its producer events |
| Manifest exists; acknowledgement absent | Verify manifest/receipt/input binding, return the same publication and retry acknowledgement only |
| Existing batch ID with different input, predecessor or output vector | Refuse conflict and retain evidence; no replacement manifest |
| Missing pinned version, expired input evidence or uncertain writer authority | Block recovery and source acknowledgement; require reconciliation |

Readers MUST resolve a single immutable manifest and consume only its recorded
versions. Internal latest-state writes are not a consumer publication. Every
publication ID MUST identify one immutable descriptor; duplicate descriptors
must be rejected or verified byte/semantic equivalent under the selected profile.
The ordinary manifest DDL provides no uniqueness enforcement. A source adapter
MUST acknowledge only its qualified complete boundary after durable publication
and required trusted downstream proof; an Ashlar receipt cannot manufacture the
Truss worker profile's host-observed authority.

Retention MUST protect every version referenced by an active publication or
registered reader and the raw input/receipt evidence needed for replay. No safe
expiry horizon has been selected. Maintenance creates a new version and may be
published only after preservation validation; it does not silently update an
existing descriptor. These rules require interruption, lost-acknowledgement and
stale-owner tests before runtime support is claimed. Existing r72/r74 tests cover
single-table refusal and prepublication reference detection respectively; neither
executes this recovery protocol.


[r78 serialized recovery evidence](../spikes/SPIKE-001-table-layout/out/native/ashlar_serialized_recovery_20261006_r78/summary.json) passes a three-object native Delta test with an isolated supplemental receipt binding. One row changes exact property text; independent full-row comparison preserves every other field and both untouched rows. After intentional interruption following output commit and durable binding, a fresh client reads that binding and publishes the recorded output version without reapplying data. Repeated publication simulates lost acknowledgement and preserves both descriptors and the output version; old pinned rows remain unchanged. This is one-table serialized recovery with a synthetic retained stage, not a native producer acknowledgement, concurrent fence, ambiguous submission recovery, graph-wide publication or production receipt implementation. The executed 0.3 DDL is unchanged.


### Projection invalidation and unchanged version reuse

A publication MUST validate derived rows against its canonical structural state.
A property-only canonical change MAY reuse previously published adjacency and
degree versions when endpoint, independent-edge identity and declared coverage
are unchanged. A structural_version is a projection revision; it MUST NOT be
compared to an unrelated canonical property entity_version as proof of freshness.
The validation report MUST identify which projections changed, which versions
were reused, and their direction/relationship coverage.

| Accepted change | Required invalidation |
| --- | --- |
| Property/retained content only | Canonical carrier and applicable typed property projections; structural projections may remain unchanged |
| Edge create/delete or endpoint/type change | Affected forward/reverse adjacency and degree counts within declared coverage; typed edge release when included |
| Node create/delete | Included typed node projection; incident-edge closure checked in final state; node deletion cannot leave surviving edges |
| Revision changes scalar mapping or type inclusion | Affected typed projection rebuilt or explicitly unavailable until validated; canonical/source meaning remains preserved |

Only the selected source profile can decide whether an operation is interpretable
and which native events it creates. This matrix does not define Truss retain,
rebind, transform or recreation semantics. Unknown structural effects MUST block
publication rather than justify reuse. Missing coverage MUST be reported as
unavailable; it MUST NOT silently become an empty adjacency or zero degree result.


[r79 partial-output recovery evidence](../spikes/SPIKE-001-table-layout/out/native/ashlar_partial_recovery_20261006_r79_resume/summary.json) passes a three-edge endpoint change under a serialized synthetic publisher. An edge-only commit leaves forward adjacency mismatched (two symmetric EXCEPT ALL differences); the old descriptor and both pinned snapshots remain unchanged. A fresh client reads durable intent, repairs adjacency, validates exact carrier/structural parity and target closure, records actual output versions and publishes them without reapplying canonical edges. All untouched fields and independent edge identities survive. The initial stage CREATE was terminally rejected for unsupported SELECT-star REPLACE syntax; explicit-column recovery verified the three existing tables and version-zero copies before proceeding. Both logs are retained. This fixture excludes property journal/raw-feed updates, reverse/degree coverage, source event/version interpretation, concurrent fencing and production source acknowledgement; it does not prove the complete graph publisher.


### Read-plan locator validation

Before issuing a multi-table read, the executor MUST require a committed version
for every table that the selected plan consumes, including hydration and structural
validation dependencies. It MUST reject malformed versions and identifiers, retain
one publication identity, and protect admitted versions from later caller mutation.
Unused optional projections need not exist, but their absence cannot become zero
results for a plan that requires them. The plan inventory is an executor input;
complete inventory selection and publication authority require separate validation.

The [publication locator spike](../spikes/SPIKE-001-table-layout/adapters/publication-locator.ts)
checks an already decoded locator and creates a frozen defensive copy. Local checks
exercise thirteen refusals, missing consumed tables, invalid unconsumed entries,
inherited versions, caller mutation and preservation of the generated singleton
pin. Browser-target bundling passes; real browser execution is untested. The caller
must reject duplicate JSON descriptor members during decoding and establish trusted
descriptor custody, semantic projection validity and policy. This helper proves
none of those properties; the native-read builder also remains usable independently
and does not automatically invoke whole-plan validation.


[r81 varied native fidelity](../spikes/SPIKE-001-table-layout/out/native/ashlar_varied_fidelity_20261006_r81/summary.json) loads the declared-shape corpus into three private tables with the 0.3 object, edge and raw-record definitions using bound STRING parameters and explicit native casts. Exhaustive all-column comparisons pass for 64 objects, 192 edges and 256 raw records at actual version 1. BIGINT transport strings include signed extremes; timestamp columns compare instants while JSON time tokens remain exact text. Eight property shapes, variable opaque lengths, Unicode/escaping, decimal/exponent/negative-zero tokens and nested retained content survive. Native SQL hash derivation matches all local canonical hashes, raw references/cursors/digests resolve, and both typed endpoint directions close. These complete synthetic envelopes avoid r74's omitted structural fields; no native source reconstruction, journal history, transaction completeness, publication recovery or graph-engine/scale claim follows.


### Proposed durable receipt surface

The separate [publication-receipt candidate DDL](../spikes/SPIKE-001-table-layout/sql/publication-receipt-candidate.sql)
proposes ashlar-publication-receipt/0.1. It does not revise the executed 0.3 table
set. Its logical key is `(stream,apply_batch_id,phase)` with phase `intent` or
`complete`; the publisher MUST enforce uniqueness and phase semantics. Records
are immutable. Record both phases as append-only evidence, rather than replacing
an incomplete record and losing the recovery intent.

The exact UTF-8 receipt_json is the binding envelope; receipt_digest is its
lower-case SHA-256. It MUST contain the fields below. profile_version, stream,
apply_batch_id and phase MUST agree with their table columns. recorded_at is
operational metadata outside replay identity. Native source IDs, cursor components
and worker generations retain qualified text representations within their original
boundary/authority envelopes. Do not narrow them to the old BIGINT fence column.

| Envelope field | Intent | Complete | Validation |
| --- | --- | --- | --- |
| profile_version, stream, apply_batch_id, phase | Required | Required | Exact supported profile and table-column agreement |
| predecessor_publication_id | Required, nullable only for explicit bootstrap | Same as intent | Predecessor descriptor must exist unless a separately qualified bootstrap boundary authorizes its absence |
| authority_context | Required | Required | Complete selected authority context, including generation/registration where applicable; stored context is not trusted proof |
| source_boundaries | Required | Same exact boundaries as intent | Versioned complete boundary records for every input feed/epoch; a highest tuple alone is insufficient |
| input_table_versions | Required | Same as intent | Qualified retained stage/raw table names and actual committed versions, with complete input membership and byte-digest definition |
| input_digest | Required | Same as intent | Digest of the qualified retained input representation; profile must define membership, ordering and encoding before use |
| output_table_versions | Absent | Required | Every consumed canonical/history/raw/derived table at actual validated versions, including reused unchanged versions |
| validation_report | Absent | Required | Checks and declared projection coverage, exact input/output binding, source-profile and validator version; no unsupported capability promoted |

A complete record MUST bind to the existing intent and its original input,
predecessor and authority context. The recovery protocol MUST independently verify
whether the authority remains valid; it cannot reuse an expired registration just
because the bytes match. Receipt uniqueness, stale-writer refusal and atomic
manifest installation remain protocol obligations outside this ordinary DDL.

An identical key retry MUST preserve the original record after comparing exact
receipt bytes and their digest. A byte-different envelope at the same key is
RECEIPT_CONFLICT even if its JSON objects appear semantically equivalent; retain
producer input and stop publication. This profile deliberately makes byte encoding
part of replay identity. A caller needing reordered equivalent JSON must reuse
original retained bytes or select a different explicitly qualified profile.

Recovery from intent alone inspects actual output commits and repairs missing
work without guessing a vector. Recovery from a complete receipt revalidates its
input/output binding and installs one immutable manifest. A manifest reference
must identify the complete receipt by stream, batch, digest and actual receipt
version in its validation report; a mutable latest lookup cannot establish custody.
Lost acknowledgement returns the same manifest and never appends new source
history. The source's trusted downstream-application proof remains independently
required where its worker profile demands it.

This proposed envelope addresses the missing receipt bindings identified by r78
and r79. Those tests use earlier isolated fixture bindings; they do not execute
this table, complete-source membership validator or native receipt/manifest
installation. No expiry or production authority mechanism is selected.


[r82 receipt carrier evidence](../spikes/SPIKE-001-table-layout/out/native/ashlar_receipt_20261006_r82/summary.json) now executes the proposed seven-column DDL in an isolated native table. Two fixture intent/complete records preserve all fields, exact envelope/digest and unsigned-range text. Identical replay leaves them unchanged; a byte-different valid-digest complete record produces terminal RECEIPT_CONFLICT with complete target parity. Both phases are appended together for storage testing, not a staged publication protocol. Input digest/completeness claims are explicit fixture placeholders. Native source membership, phase linkage, authority, concurrent uniqueness and receipt-bound manifest installation remain unproved. The original candidate DDL remains preserved with its pre-execution comment; this evidence supersedes that historical execution status only within the stated carrier scope.


### Engine release protocol qualification

[r83 read-only release inventory](../spikes/SPIKE-001-table-layout/out/native/ashlar_release_protocol_20261006_r83/summary.json) verifies all four r68 typed release tables remain at version 0. Each advertises minReaderVersion 3, minWriterVersion 7 and features appendOnly, deletionVectors, invariants and v2Checkpoint. Scalar columns and unclustered DDL therefore do not imply a lowest-protocol export. Feature metadata alone does not prove immutable access or successful external ingestion.

A release adapter MUST record the actual Delta protocol/features, reader/runtime
version, catalog/storage access route and publication binding. It MUST establish
that the selected reader consumes the exact release before promoting support. A
failed or unsupported feature negotiation blocks that engine's activation while
native canonical reads remain available. If a separate compatible materialization
is required, select and verify its protocol after creation, compare every field
and identity, and bind its own actual versions; no canonical downgrade is implied.
Feature removal, storage export and access provisioning are separate actions.

The [PuppyGraph Delta connection documentation](https://docs.puppygraph.com/connecting/connecting-to-delta-lake/)
requires access to both metastore and storage and describes Unity Catalog/Azure
credential vending configuration. The inspected page does not supply a complete
reader-feature support matrix for these releases; connector version/runtime tests
remain necessary. [Fabric Graph limitations](https://learn.microsoft.com/en-us/fabric/graph/limitations)
identify OneLake/Mirrored Database sources, scalar ingestion types and unsupported
nested map/struct types; this does not qualify these Azure Unity Catalog release
locations as Fabric input. Fabric requires the separately scoped OneLake release.
GraphFrames qualification likewise requires the selected Spark Delta reader and
actual pinned DataFrames, beyond the syntax-checked adapter. Source documents were
inspected 2026-10-06; no engine session or import was run.


[r84 hash encoding differential](../spikes/SPIKE-001-table-layout/out/native/ashlar_hash_encoding_20261006_r84/summary.json) verifies exact ordered JSON text and SHA256 parity between Python and native SQL for 64 node/edge tuples: eight ASCII source cases including quotes, backslashes, newline/tab/NUL/DEL and SQL-looking text, with zero, values above 2^53 and signed-int64 extremes. Python uses ensure_ascii=False; default ASCII escaping of DEL changes the bytes and must not be substituted. Encoding numeric IDs as JSON strings also changes the hash. This qualifies the tested representative encoder cases, not every possible implementation or identity source. Full native tuple predicates remain mandatory even with a correct hash.


Merged Truss draft layout0.2 at d3dcdde specifies shared object/edge ID allocation
and unique relationship/source/target edges. See the [source-profile qualification](../spikes/SPIKE-001-table-layout/truss-feed-mapping-candidate.md#merged-upstream-layout-profile-qualification).
A source adapter claiming that profile must preserve its allocation/multiplicity
rules; generic repeated-ID/parallel-edge fixtures demonstrate Ashlar carrier
capacity only. Incompatible producer duplicates must be refused, never collapsed.
The newer complete-feed worker draft remains separately unqualified.


[Local GraphFrames integration](../spikes/SPIKE-001-table-layout/out/graphframes-local-20261006.json)
executes the version-pinned v0.2 mapping with Spark 3.5.3, Delta Lake 3.2.1 and
GraphFrames 0.12.3. Exact carriers, isolated vertices, parallel-edge multiplicity,
self-loops, two-hop motifs and exclusion of an unpublished append pass on three
vertices/three edges rematerialized from the checksum-pinned native export.
This is local Delta integration evidence, not native Unity Catalog protocol
qualification, a legal Truss parallel-edge fixture, or performance/scale evidence.
The canonical UC architecture and independently qualified serving-release design
remain unchanged.


[24M-carrier physical comparison](../spikes/SPIKE-001-table-layout/scale-comparison.md)
passes full-field native/local parity on 4M objects/20M edges, native typed
endpoint closure and a separately corrected 200k property-ID-map update. Initial
update-wrapper semantics were invalid and are explicitly excluded from profile
qualification. Corrected native version-2 reads have 50 final uncached serial and
50 four-client records. Warm latency budgets fail; native DV amplification is
substantially lower than OSS copying. No source/raw/history/publication protocol,
Truss producer legality, external reader, cold-data or billion-scale qualification
follows. The observed FULL maintenance invocation made no data/version change.


[Higher-entropy 24M-element iteration](../spikes/SPIKE-001-table-layout/entropy-scale-status.md)
passes local full-field parity, typed closure, shared-allocation/disjoint ranges
and unique relationship endpoint pairs on 40.77GB. The same existing GraphFrames
adapter executes 4M vertices/20M edges and 100M two-hop paths with complete
version-0 release bindings. This strengthens local mapping/algorithm evidence,
not direct UC feature or actual Truss producer qualification. Native higher-entropy
validation is still in flight; generation alone cannot qualify it. Raw/history,
publication freshness, fenced recovery, other engines and 1B/5B remain open.


The [native higher-entropy audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_entropy_20261006_r86/audited-summary.json)
now completes at 40.71GB / 576 files and 4M objects/20M edges. Unique-ID cardinality
plus full byte-exact field comparison and missing/extra-row checks pass, as do
typed closure, disjoint shared-allocation ranges and unique endpoint pairs.
Both current versions are 0. This proves the generated storage surface, not
actual Truss source/catalog authority, production NOT NULL enforcement, full
publication, native singleton at this width or billion-scale operation.

[r89 large incremental slice](../spikes/SPIKE-001-table-layout/out/native/ashlar_entropy_publication_20261006_r89/audited-summary.json) validates 200k scattered property 107 additions over 20M higher-entropy edges: full all-field baseline/output parity, exact old-property lexical preservation, 200k complete synthetic raw envelopes/digests/origin references and 200k absent-to-true journal rows. Actual-version descriptor readback passes, with no tombstones or structural changes. Baseline raw origins remain unqualified, explicitly in the descriptor; this is a serialized synthetic changed-origin slice, not a native source or production publisher profile. Audit-heavy arrival-to-verified-manifest is 988 s, not sustained/burst freshness admission. [r90 post-update reads](../spikes/SPIKE-001-table-layout/out/native/ashlar_entropy_post_reads_20261006_r90/audited-summary.json) touches 17 files at p95 after the merge versus 1 before it; provisional performance comparisons remain unsatisfied and guide tuning under the fixed UC Delta architecture.

[r94 large-token slice](../spikes/SPIKE-001-table-layout/out/native/ashlar_entropy_wide_journal_20261006_r94/audited-summary.json) preserves 100k intended native carriers and exact property 105 old/new journal tokens (about 6.2 KB each), with 639 MB incremental journal bytes. Verified synthetic publication takes 78 s; this is hot-set evidence, not scheduled-rate or graph-wide scattered admission. Baseline raw origins and real source/writer authority remain unqualified.

[r95 independent wire oracle](../spikes/SPIKE-001-table-layout/out/native/ashlar_wire_timestamp_20261006_r95/summary.json) supersedes complete-wire wording for r89/r94: default synthetic to_json truncates derived published_at by 307/870 µs respectively. Native Delta carrier/journal timestamps and property/retained/cursor strings remain exact. Payload comparison against the same encoder masked the loss. Historical payloads remain unchanged and do not prove full all-field reconstruction. The explicit UTC-microsecond encoder passes 100k independent instant roundtrips and sub-millisecond/pre-epoch controls; future publication fixtures gate that independent equality. Compact ASCII token qualification does not extend to arbitrary escaped/pretty native tokens.

### PuppyGraph runtime identity and activation requirements

[PuppyGraph 1.13.0 local evidence](../spikes/SPIKE-001-table-layout/out/puppygraph-local-r105.json)
qualifies the four typed carrier mappings through DuckDB and Bolt, with exact
scalar parity and isolate/self-loop/parallel multiplicity controls. It does not
qualify direct UC Delta protocols, scale, latency or atomic release activation.
Graph identity columns were not exposed as ordinary properties in the initial
model. An adapter MUST retain independently queryable identity carriers (the
executed model uses `carrier_id` and `carrier_key`) and compare them against
the release source. Carrier equality alone MUST NOT replace graph endpoint and
identity checks. No JSON-number parsing or floating-point identity conversion
is permitted. These aliases belong to the serving adapter, not canonical tables.

An in-place catalog/model change was rejected by the actual engine; a clean
disposable engine accepted the corrected model. A production adapter MUST
qualify its activation and rollback mechanism rather than infer atomic switching
from successful schema upload. Readers MUST remain bound to one validated release
while its successor is built and checked. Retained releases and any dual-engine
overlap enter capacity estimates; expiry requires proof that no reader pins them.

[The direct UC prerequisite probe](../spikes/SPIKE-001-table-layout/out/puppygraph-uc-prerequisite.json)
rejects READ credential vending because external data access is disabled on the
current metastore. External runtime access requires separately authorized access
configuration and reader authentication; native Databricks singleton queries
continue to use canonical Delta without that dependency.

### Adjacency ordering qualification boundary

[r112 native relationship-first pages](../spikes/SPIKE-001-table-layout/out/native/ashlar_hub_contract_pages_r112/audited-summary.json)
preserve `(rel_type_id,edge_id)` continuation exactly on the synthetic20M-edge
hub graph. Both eight-file layouts scan all8files at each tested relationship
cursor. Earlier edge-ID-only8/4/1file pruning is not evidence for the contractual
order. Keep relationship-first semantics and native edge identities; source/type/
endpoint clustering and optional relationship clustering must be evaluated on
that query shape. The seven-column fixture omits structural_version and does not
qualify the complete reference adjacency DDL or published structural coverage.
No canonical identity-layout change, cursor-order change or billion-scale
admission follows. The proposed reference DDL remains unchanged.

[r113 full-shape forward adjacency](../spikes/SPIKE-001-table-layout/out/native/ashlar_hub_contract_pages_r113/audited-summary.json)
adds20M-row eight-column parity, including a constant synthetic structural version,
and36 exact contractual page checks. Relationship-range writes with the proposed
forward clustering prune8/4/1files at successive relationship cursors; the
edge-range control stays at8files. This supports matching writer file statistics
to `(rel_type_id,edge_id)` order, not a universal partition count or batch width.
Keep reference DDL and canonical identity clustering unchanged. The explicit
range-batch mechanism, exceptionally low entropy, different control column count,
CTAS constraints, unbound synthetic structural revision and noisy caller timings
limit the evidence. Automatic maintenance, scattered updates, reverse access
and real source/publication authority still require qualification.


### Full20M optional bucket-copy preservation evidence

[r167–r169 audited parity](../spikes/SPIKE-001-table-layout/out/native/ashlar_bucket_parity_r169/audited-summary.json) proves all20 logical fields of canonical E23 equal all20M rows in the owned64-bucket table at version4. Every text field uses UTF8 binary equality; null-safe typed comparison covers the other fields. Global nonnull uniqueness and exact disjoint joined counts establish complete membership, with25 native changed-value refusals and two membership/duplicate counterexamples. This closes the initial copy's wide-value obligation for that fixed snapshot. It does not replace canonical hash liquid clustering, qualify the CTAS constraint surface as canonical DDL, select a billion-scale bucket count, prove later maintenance/update preservation, or improve any measured singleton/ingest gate. The owned copy remains unpublished and retained for the next bounded physical comparison; Truss-compatible logical identities and maps remain unchanged, with UMF deferred.


### Full-size bucket/ZORDER comparison remains experimental

[r170–r175 comparison](../spikes/SPIKE-001-table-layout/out/native/ashlar_bucket_full_reads_r175/comparison-summary.json) now measures the full20M owned bucket copy before and after actual partition-local ZORDER:528 files become448, point-read file p95 becomes2→1, and28 no-remote post-repeat reads have engine p9599ms/caller416.783ms. The all30 post-repeat sample has engine151ms and2 remote reads. Caller latency still misses the provisional250ms target, and cache/load differences prevent a causal partitioning claim. A33.156GB physical rewrite is a distinct maintenance cost; its native read telemetry is incomplete. Initial copy4 is exhaustively value-equal to E23; wide parity at rewritten8 remains pending. Retain canonical hash liquid clustering and the close-to-Truss logical surface;64 buckets and64MiB are experiment settings, not a1B/5B selection.


### Full20M update cost favors retaining the canonical LC candidate

[r176/r177](../spikes/SPIKE-001-table-layout/out/native/ashlar_bucket_update_r176/audited-summary.json) applies one same-length wide100k property update: LC clone MERGE4.110s/326.515MB/0 copied rows, maintained bucket MERGE37.018s/5.254GB/2,987,212 unchanged copied rows plus381DVs. Both changed-carrier20-field checks and20M global IDs pass;0 inserts/deletes. Differing file histories prevent a partition-only causal claim, but the measured write amplification keeps bucket64 experimental and canonical hash LC unchanged. The75GB/4GB local cost guard stopped after77.071GB reads/6.244GB writes, before point reads; no native failure or replay occurred. Final bucket-wide preservation (including preceding ZORDER) remains required; copied rows cannot be justified by unchanged physical-file custody. Single-MERGE cost is not publisher/sustained-rate admission, and no UMF binding is added.


### Final preservation evidence r178–r180

The final bucket version9 now passes full20M/20-field exact equality against E23 plus intended stage0 after ZORDER and the100k update, including copied unchanged rows. The LC clone0/MERGE1 passes19.9M unchanged identity/file/row-position custody, combined with prior exact100k changed-output checks. The bucket wide validation exceeded its140GB read plan at177.558GB; the preserved controller stop is a cost failure, while all four native comparisons passed. LC custody used4.273GB reads. No SQL was replayed, no canonical publication changed, and no latency, sustained-rate or billion-scale admission follows. Canonical hash LC remains proposed and bucket64 experimental. Evidence: [bucket audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_bucket_final_parity_r178/audited-summary.json), [LC custody](../spikes/SPIKE-001-table-layout/out/native/ashlar_bucket_lc_custody_r179/summary.json).


### Range-aligned hot-output candidate r187–r188

A fresh owned20M LC clone applies the same100k wide stage with a6-range lookup_hash input hint, preserving all20 changed fields,19.9M unchanged physical custody and20M unique identities. Actual six hot-file ranges narrow to16–18% each; same20-key operational read p95 improves from8files/403.803MB/179ms engine/433.354ms caller to3files/139.231MB/104ms/365.700ms. MERGE writes326.548MB,0 copied rows, caller6.903s; whole run stays within budget. Canonical publication is unchanged. This supports a range-aligned writer tuning candidate, not a production range count, randomized causal SLO claim or revised architecture. Both warm gates, integrated sustained/burst publication, cold and1B/5B remain unproved. Evidence: [owned native audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_lc_range_update_r187/audited-summary.json). Qualify final file shape rather than assuming a source hint controls every writer path; UMF binding remains deferred.


### Integrated range-input qualification limit r191–r194

A resumed isolated100k publisher passes exact raw/wire/origin/current20-field/journal/20M structural and identity checks plus19.9M unchanged physical custody; its owned manifest is verified, canonical publication unchanged. Measured processing59.883s and modeled record-age p9569.386s fail the60s freshness target for this finite controller run; no sustained-rate admission follows. The6-range MERGE source hint produces16 nearly global hot-file ranges in the integrated path, unlike standalone r187. Thus the hint is not a reproducible writer-output contract and the earlier singleton improvement cannot be advertised for this publication. Canonical UC Delta/hash LC remains selected/proposed; range shaping remains experimental pending writer-behavior qualification. Evidence: [publication audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_queue_resume_r192/audited-summary.json), [actual output shape](../spikes/SPIKE-001-table-layout/out/native/ashlar_publication_shape_r194/summary.json). UMF binding and full performance/scale gates remain open.


### Writer isolation evidence r195–r196

Two new sequential E23 shallow clones apply the prior eight-file and fresh four-file100k inputs, both producing6 disjoint narrow live hash ranges. Exact20-field changed values,19.9M unchanged physical custody and20M unique IDs pass; cross-stage native identities/endpoints/hashes match. Four source files alone do not explain the integrated16 broad files, but concurrency causation and writer determinism remain unproved. Canonical UC Delta/hash LC remains selected/proposed and range hints experimental. Next compare isolated current emission after raw/journal writes, preserving the full validation/manifest barrier and measuring its freshness cost. Evidence: [writer isolation audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_lc_writer_isolation_r195/audited-summary.json). No singleton, sustained, cold or1B/5B qualification transfers from this write-only comparison; UMF binding remains deferred.


### Serial-current scheduling tradeoff r197–r200

On fresh owned clones with the same immutable input, raw/journal writes precede current MERGE with0 native statement-window overlap. Actual output is6 narrow hot-file ranges; all raw/wire/origin/current20-field/journal/20M structural and identity plus19.9M physical custody checks and manifest readback pass. Measured controller62.468s gives modeled record-age p9571.973s, worse than69.386s for three overlapping writes. Keep isolated current emission as an experimental optional tuning profile, not a required freshness path or guaranteed writer layout. Canonical UC Delta/hash LC remains selected/proposed; semantic publication validation stays mandatory and no standalone singleton result transfers without measurement. Evidence: [publication audit](../spikes/SPIKE-001-table-layout/out/native/ashlar_queue_serial_r197/audited-summary.json), [native schedule comparison](../spikes/SPIKE-001-table-layout/out/native/ashlar_queue_serial_r197/schedule-comparison.json), [actual shape](../spikes/SPIKE-001-table-layout/out/native/ashlar_publication_shape_r199/summary.json). Full performance/scale and UMF obligations remain open/deferred.


### Matched published-snapshot reads r201–r203

Two20-key warm passes at verified private publication1 show serial-current engine p9595/96ms (scoped100ms screen passes), caller348.750/340.264ms (250ms fails),3files/142.939MB; overlap129/101ms,416.120/410.620ms,18files/414.851MB. All80 complete20-field results match immutable stage0. Combine with publisher age p9571.973s serial/69.386s overlap: both60s targets fail, so optional serialization is a measured read/freshness tradeoff, not a complete solution. Background Predictive Optimization advanced physical heads; reader verifies original version1 MERGE IDs and unchanged manifest vectors, then reads exact VERSION AS OF1. Never require publication==latest head or silently use a newer head. Evidence: [matched reads](../spikes/SPIKE-001-table-layout/out/native/ashlar_publication_reads_r202/summary.json). No cold, service, sustained/burst or1B/5B admission follows; UC Delta/hash LC remains selected/proposed and UMF deferred.

## JSONL streaming adapter development profile

Proposed ashlar-jsonl-transactions/0.1 admits exact-byte raw source batches,
separately from event interpretation, current/history apply and publication.
The binary stream contains begin, zero or more event lines and commit, each
with exactly one LF-terminated UTF8 JSON object. Reject duplicate JSON members,
nonfinite numbers, incomplete lines and unknown/nested control. Unknown event
payload members are retained verbatim and are not executable graph semantics.

Begin is exactly kind=begin and nonempty opaque batch_id. Event requires
kind=event and a nonempty delivery_id unique within that transaction; arbitrary
additional payload is retained. Commit is exactly kind=commit, matching batch_id,
integer record_count and records_sha256. The digest is SHA256 over the ordered
event lines, each preceded by its exact uint64 big-endian byte length; original
LF bytes are included. Matching count/digest proves membership for this explicit
source profile, not producer authority, source retention or safe Truss watermark.
No per-event checkpoint is admitted. Empty transactions have the SHA256 empty
input digest and a zero count; they still require explicit begin/commit custody.

SourceBatch contains profile, feed, epoch, batch_id, cursor_before, cursor_after,
ordered SourceRecords, exact begin/commit bytes and records_sha256. Each record
contains delivery_id, exact line bytes/digest and its ending byte offset as
canonical nonnegative decimal text. Feed/epoch and positioned cursor are trusted
host inputs. Different files must not be assigned the same immutable feed/epoch
without original producer/replay correspondence. Resume requires positioning at
the previously published complete boundary and retaining the original source;
a claimed cursor alone is not custody proof. Never infer XID or graph version
from a byte offset.

The reader defaults to 1000 records and 1MiB total transaction bytes, including
control lines. It refuses before yielding an over-limit/partial batch. Hosts
bound line reads before allocation; the supplied stdin CLI reads at most 1MiB+1
bytes at a time. Prior completed batches may be yielded before a later malformed
transaction; that later failure never admits progress into its partial contents.
Exact replay has the same namespace/batch/delivery/cursor/raw/digest values. The
durable consumer must reject same-key changed bytes and preserve original raw
custody, not merely parsed payloads.

No acknowledgement API is supplied in this reader. Durable staging, source
fencing, schema/operation interpretation, apply validation and immutable complete
publication must precede checkpoint/acknowledgement. Unknown operations retain
raw custody but block dependent current-state publication. This development
profile is distinct from Truss's native transactional feed and PostgreSQL outbox;
their bootstrap, authority, transaction/version and recovery contracts remain
required. The full end-to-end goal includes all three source paths.

## Durable raw batch staging boundary

Proposed source-batch-stage/0.1 is a supplemental raw transaction custody table,
not a replacement for source_record/current/history or a completed publication.
Its nine nonnull STRING columns are source_profile, feed, epoch, batch_id,
cursor_before, cursor_after, records_digest, batch_json and batch_digest. The
logical key is (feed, epoch, batch_id). batch_json is the deterministic stage
encoding of the full SourceBatch, including original begin/commit/event bytes
in base64; batch_digest is SHA256 over its UTF8 text, checked natively. Unknown
source payloads remain byte-exact. Ordered event membership digest remains
separate from this stage encoding digest.

DeltaBatchStage consumes the exact complete JSONL batch, verifies bounded raw
source reconstruction and all supplied metadata, then requires an injected
authorized exclusive-writer context through native UUID verification, parameter-
bound MERGE and exact full readback. No permissive writer policy is supplied.
An identical original row is a replay; any changed field under the same batch
key raises SOURCE_BATCH_CONFLICT without replacing original custody. Missing,
ambiguous, mismatched readback or replacement UUID refuses a staging receipt.
A submission/observation exception is unresolved or failed native work requiring
original statement-handle/custody recovery; never blindly submit a replacement.

StagePolicy.writer must hold authorization and exclusion/fencing for every
admitted writer path until verification and retain recovery custody on unknown
outcome. Its context manager yields None; a returned false/success token is
not policy completion. This port does not itself install native authority,
grant restrictions or concurrency uniqueness. The development runner uses
administrative private-table writes plus local cooperating-process file locking;
remote/adversarial bypass and native source fencing remain unqualified.

The immutable StagedBatch result records table/UUID, source namespace/batch ID,
batch digest and candidate cursor_after. It is only raw-stage custody, with no
accepted schema, current/history apply, source acknowledgement or checkpoint.
Per-delivery identity/version conflicts across different raw batches still
require the downstream source_record/apply admission; raw batch membership
does not establish those invariants. Publication must validate complete source
and effect membership, schemas, durable replay and pins before progress.

Small native evidence on 2026-10-08 stores one two-event synthetic batch in
client_dev.ashlar_layout_v03_20261006_r73.source_batch_stage_20261008, checks
identical replay and changed valid source conflict, verifies table UUID/full
row parity, and independently decodes the native stored base64 to reproduce
the original full source transaction. No checkpoint or published graph changed.
The native receipts are under out/native/source_stage_20261008. Thirty-four
focused local checks include refusal before native effects on denied or
nonconforming policy, table replacement and forged batch metadata.

## Whole-entity apply planning boundary

The proposed pure planner consumes an explicitly admitted whole-entity source
profile, not Truss's per-property change feed. EntityKey is the complete
(source, object-or-edge kind, type_id, id); IDs use the selected signed64 range.
EntityState contains exact entity version, schema revision, property/retained
JSON object text and, for an edge, two same-source typed object endpoint keys.
Change adds feed, epoch, original delivery ID/digest and create/replace/delete.
Native source normalization must verify original raw custody and the exact
version/carrier semantics before supplying these values; taking a maximum
property position or guessing a whole-entity version is forbidden.

ApplyState retains current, immutable original history keyed by entity/version,
tombstones and original delivery claims keyed by feed/epoch/delivery. A trusted
complete prior state and explicit schema_policy are mandatory. That policy must
authorize and validate the exact revision, carrier/operation, identity and
relationship constraints for every change, including replay; success returns
None. The pure planner installs no schema/constraint/authority and supplies no
permissive policy. Test schema accepted-1 and digest a*64 are synthetic independent
planner inputs, not actual accepted Truss revision or source custody evidence.

Identical original delivery replay changes no state. Changed original delivery,
entity/version assigned to a different original delivery or non-increasing
version refuses. Create cannot overwrite or resurrect an existing identity;
replace/delete require a live entity. Delete must carry exact prior property,
retained and endpoint meaning with its new version/revision; it removes current
but preserves the original delete in history/tombstones. Every accepted distinct
change remains in original history. A later source lifecycle allowing identity
resurrection needs an explicitly different admitted profile.

Complete boundary validation requires every remaining edge endpoint to resolve
to the exact live typed object. Endpoint creation later in the same transaction
is permitted; a node deletion leaving an incident edge refuses. Parallel edge
IDs and isolated nodes remain distinct. The planner copies prior containers and
returns immutable defensive mappings only after the entire transaction passes;
a refusal leaves prior state unchanged.

This is current/history/tombstone planning, not native table apply, property_journal
derivation, immutable manifest publication or checkpoint advancement. Persisting
its effects requires serialized native state revalidation, retained delivery and
version claims, per-property/native source obligations where applicable, complete
all-table parity and the publication contract. Source acknowledgement still
follows a completed durable publication. Truss-native reconstruction, other
value/key/relationship profiles and full end-to-end native execution remain
required rather than being replaced by these synthetic whole-entity checks.

## Explicit whole-entity JSONL and native apply experiment

changes_from_batch composes complete raw transaction validation with an explicit
ashlar-whole-entity/0.1 event profile. Required fields are kind=event, delivery_id,
source_profile, source_system, entity_kind, type_id, id, entity_version,
schema_revision, operation, props_json and retained_json. Edge events additionally
require exactly two endpoints, each with type_id and id. Identity/version values
are canonical signed64 decimal strings; nontext values, aliases, leading zeroes
and narrowing refuse. Versions are source-authored whole-entity versions. No
property-feed maximum, display-name mapping or hidden source default is used.
Unknown or missing executable members block this profile while original raw
source custody remains available separately. Exact JSON property/retained text
is passed unchanged to the mandatory admitted apply planner.

The graph-source.jsonl fixture carries two complete transactions and nine source
events: three objects (including an isolated node), two parallel edges, one
object replacement and three deletes. It uses synthetic fixture-schema-1 with
explicit property IDs 23/24; this is not accepted Truss/UMF schema evidence. The
replacement retains JSON null distinctly and a retained integer token beyond
the signed64 range. No semantic support for that token is inferred
from storing exact text.

The bounded native experiment creates fresh private canonical0.3 object_current,
edge_current and tombstone tables plus a whole_source_history carrier. It
materializes only touched fixture identities with keyed MERGE DELETE then insert;
this is intentionally unpublished intermediate state, not an atomic multi-table
write, durable replay/recovery protocol or a reusable production applier.
History retains each original event's full base64 bytes and source digest. The
fixed synthetic admission policy validates only its declared fixture revision/
source; it is not native schema acceptance or broad constraint enforcement.
Proposed publication clock values in current rows do not constitute publication.

Independent expected native inventories pass after both transactions: initial
three nodes/two distinct edges, final two nodes/no edges, three exact tombstones
and nine original history records. Surviving object lookup hashes agree with
native SHA256/to_json encoding; final edge set is empty, so that final query
does not qualify edge encoding. Original first DELETE failed terminally because
Delta rejects multi-column IN; the resumed run verified all four tables empty
and used keyed MERGE DELETE, preserving original receipts.

Evidence is out/native/graph_apply_20261008. No manifest/checkpoint changed,
no accepted Truss schema advanced, and no native caller fencing/retention/replay
or property_journal/Truss feed reconstruction is qualified. Forty-two focused
local checks cover explicit source profile/version admission and complete
apply planning. The full toolkit still requires reusable native recovery,
immutable publication, Truss acceptance/feed and the other source bindings.

## PostgreSQL transactional outbox source profile

Proposed ashlar-postgresql-outbox/0.1 is a separate additional source, not a Truss
native mutation/journal/feed substitute. Fresh private source DDL is
sql/ashlar-outbox/01-postgresql.sql. One selected namespace has a singleton
signed64 nonnegative head and immutable-by-admitted-writer batch rows keyed by
positive position with exact unique opaque batch_id, UTF8 payload text and
SHA256 byte digest. Payload is a 1-byte to1MiB opaque complete JSONL group. Native
append preserves bytes; the consumer independently verifies the contained
begin/event/commit count/digest and identity before admitting a source batch.
Opaque storage alone is not graph or schema interpretation.

The SECURITY DEFINER append function fixes its search path and locks the head
through the actual caller transaction. Exact existing batch bytes return the
original position without allocation; different bytes raise OUTBOX_BATCH_CONFLICT.
Fresh allocation inserts payload/digest and advances head in that same outer
transaction. The returned scalar is pending until actual commit; rollback
restores both payload/head. Position exhaustion refuses. The selected namespace
serializes admission/commit order on that head; cross-host blocking, fairness,
resource and lost-commit qualification remain separate. No scale/SLO follows
from the small sequential test.

PUBLIC has no schema/table/function access. The NOLOGIN writer role has only
namespace usage and append execution; direct batch/head DML is denied. The
NOLOGIN reader has only namespace usage and source-table SELECT, no append.
Native owner/admin authority remains separate; this development installation
does not provision application login/credential or remote service authority.
It changes no Truss table, catalog revision, mutation or feed registration.

PostgresOutbox reads a committed head then its bounded ordered original prefix.
Default page is10 groups, maximum32; each original payload is at most1MiB.
Head/cursors are canonical decimal strings in signed64 range, never host floats.
Missing, duplicate, unordered or corrupt original groups and an ahead-of-head
checkpoint refuse. Hashes and full original JSONL group membership/identity are
verified before the complete page is returned. Exact original byte payloads
and contained unknown content survive read/parse; unbound executable meaning
remains the downstream admission boundary.

OutboxTransaction carries profile, registered feed/epoch, previous, native
position, payload_digest and the original contained SourceBatch. Native
PostgreSQL sequence positions are distinct from that contained JSONL's local
byte offsets. Publish/acknowledge only the outer source cursor under an explicit
registered adapter; passing its inner SourceBatch to a JSONL publisher does not
admit PostgreSQL progress. Source connection/schema identity and immutable
feed/epoch registration are trusted host obligations; arbitrary caller strings
do not establish producer authority. Retain committed rows beyond every
registered consumer boundary. No cleanup, acknowledgement or new epoch reset
is supplied by this reader.

Application producers must couple source append to their own actual write
transaction and implement original lost-commit recovery; this profile is not
automatic capture of arbitrary SQL changes. The native test covers two committed
groups/seven events, a rolled-back pending group, exact replay/conflict, ordinary
role restrictions and read pagination with original bytes. It does not qualify
external application write/effect correspondence or native Truss capture.
Evidence: out/native/outbox_20261008 on isolated PostgreSQL17.9. Fifty-four local
checks pass. Durable source-context/epoch registration and publication/checkpoint
wiring remain required for this additional source in the full toolkit goal.

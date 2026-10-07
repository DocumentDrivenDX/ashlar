# Higher-entropy scale and GraphFrames iteration

## Maintained snapshot under publication load

[Audited r103](out/native/ashlar_maintained_contention_20261007_r103/audited-summary.json)
repeats the one-reader/parallel-append method against maintained candidate15.
No descriptor binds that candidate; original r101 remains pinned13. Independent
r101 stage0 expectations qualify the 30-key candidate reads. New publication
r103 binds current16/raw12/journal14/nodes0/tombstones0 after all affected-carrier,
raw, journal and independent timestamp gates pass. All 248 candidate/new-release
singleton reads return exact full20 carriers, with final uncached metrics.

Processing takes 63.94 s; the append pair occupies 23.97 s with 23.84 s caller
overlap. Complete pre-staged input readiness to manifest is 63.94 s; oldest
modeled arrival freshness is 73.94 s. This one batch is not a sustained/burst
test or a freshness distribution. New values and increased history prevent an
isolated causal comparison with r101's 64.19 s sample.

| Cohort | n | Engine p95 | Caller p95 | File-read p95 | Remote queries |
| --- | ---: | ---: | ---: | ---: | ---: |
| idle candidate15 | 30 | 113 ms | 394 ms | 3 | 0 |
| wholly during publisher | 130 | 833 ms | 1,096 ms | 3 | 16 |
| no-remote during-publisher subset | 114 | 495 ms | 784 ms | 3 | 0 |
| within append-overlap subset | 54 | 505 ms | 775 ms | 3 | 1 |
| post candidate15 | 30 | 106 ms | 485 ms | 3 | 0 |
| post new publication16 | 30 | 110 ms | 374 ms | 15 | 0 |

The table omits 26 input-wait and two boundary-straddling reads; subsets overlap
parent cohorts. One closed-loop client cycles the same 30 large-token keys;
these are observed small-cohort p95s, not graph-wide or cold-data SLAs. The
maintained snapshot retains pruning under contention, but loaded latency remains
high even without remote bytes. The new update snapshot again reads 15 files
at p95. Maintenance alone does not qualify a shared compute read/write policy.
Consider resource separation/admission and update-file policy as distinct tuning
questions; no resource resize/provision or attributable dollar claim follows.

These repeated hot-set measurements now establish concrete limits of the tested
shared resource. Next work should prioritize actual graph-engine/catalog and
producer integration, and justified scale/resource bounds, rather than keep
repeating an isolated hot-set timing variant. Unity Catalog Delta remains the
selected architecture; all performance targets and 1B/5B admission stay open.

## Updated-key maintenance comparison

[Audited r102](out/native/ashlar_maintenance_reads_20261007_r102/audited-summary.json)
executes one routine OPTIMIZE in 16.88 s after r101. The same statement ID owns
both commits: version14 replaces 16 update files / 326,996,710 bytes with four
files / 326,814,770 bytes; version15 records zero files/bytes changed and 58 ms
conflict detection. No deletion vectors are removed. Post-operation version15
is captured rather than assuming one commit per statement. Older removed files
remain in retention accounting; this is not a net storage-saving claim.

Global 20M-row identity/distinct-ID counts and the 100k entity-version9 count
pass. All 20 fields of every affected 100k carrier match immutable intended
stage0 with exact UTF-8 text. This is not a fresh all-field comparison of all
20M rows. All 150 singleton reads over the same 30 large-token keys pass exact
full-carrier expectations, with final uncached metrics and no remote reads.

| Alternating pair | Snapshot | Engine p95 | Caller p95 | File-read p95 |
| --- | --- | ---: | ---: | ---: |
| 1 | published13 | 114 ms | 496 ms | 15 |
| 1 | maintained15 | 122 ms | 420 ms | 3 |
| 2 | published13 | 108 ms | 485 ms | 15 |
| 2 | maintained15 | 116 ms | 358 ms | 3 |

Before maintenance, the same 30-key cohort measures 115/378 ms engine/caller
p95 and 15 file reads. The maintained snapshot improves pruning and paired
caller samples, but engine p95 is slightly higher in both pairs. Small quiet
hot-set cohorts do not establish a general latency gain, loaded-read SLA or
billion-scale qualification. The 100/250 ms provisional warm comparisons remain
unmet. No concurrent publisher is active in this experiment.

The existing r101 manifest remains pinned13, so it does not gain maintained
snapshot15 automatically. No new descriptor is installed. Maintenance adds
17 s and retained rewrite bytes to resource planning; this result does not
justify mandatory cleanup after every batch or close publication freshness.

## Parallel appends and singleton contention

[Audited r101](out/native/ashlar_parallel_publication_20261007_r101/audited-summary.json)
publishes one new 100k-member large-token batch against the 20M-edge fixture,
using two independent persistent connections for raw and journal appends and
one bounded serial singleton reader. Input preparation remains outside the
publisher clock; the complete pre-staged input is released at 10 s. All previous
affected-carrier, lexical patch, raw/digest/cursor/independent timestamp,
envelope metadata and journal multiset gates pass. MERGE updates exactly 100k
rows with zero copied, inserted or deleted rows. The verified manifest binds
current13/raw11/journal13/nodes0/tombstones0.

Publisher processing is 64.19 s, complete-input-to-verified-manifest 64.19 s,
and oldest modeled record freshness 74.19 s. Raw/journal callers overlap for
25.09 s; the pair occupies 25.82 s wall time. Individual append callers are
25.09/25.82 s and engine times 24.37/25.22 s. Earlier serial r99 appends were
about 12–13 s each. This is not a controlled isolated parallel/serial speedup
comparison: r101 also has a reader, newer file statistics and more retained
history. Observed parallelism does not improve this full-path sample or qualify
10k/s sustained, 100k/s burst or 60 s freshness p95.

| Read cohort | n | Engine p95 | Caller p95 | Remote-read queries |
| --- | ---: | ---: | ---: | ---: |
| idle, old publication12 | 30 | 137 ms | 422 ms | 0 |
| wholly during publisher, old publication12 | 129 | 637 ms | 921 ms | 13 |
| during publisher, no-remote subset | 116 | 497 ms | 798 ms | 0 |
| wholly within append overlap, subset | 51 | 635 ms | 921 ms | 2 |
| afterward, old publication12 | 30 | 116 ms | 502 ms | 1 |
| afterward, new publication13 | 30 | 110 ms | 414 ms | 0 |

All 246 singleton reads return the exact expected 20-field carriers from
independently staged inputs. The table omits 25 input-wait reads and two
boundary-straddling reads; subsets overlap their parent cohort. All exact final
metrics are uncached. The same 30 large-token hot-set keys are cycled by one
closed-loop reader; these nearest-rank p95 samples are not graph-wide, steady,
four-client or cold-data SLAs. Old controls intentionally remain pinned to
publication12 while canonical version13 is applied and installed. Separate
[new-publication checks](out/native/ashlar_parallel_publication_20261007_r101/new-publication-reads/summary.json)
validate the newly published values at version13.

Reported file-read p95 is 15 in these cohorts, against unmaintained update-file
snapshots. Read latency rises under shared work even in the no-remote subset.
Do not assume overlapping writes create additional warehouse capacity or that
idle read results represent publication contention. Keep writer concurrency a
resource-policy choice; measure native incremental maintenance and subsequent
pruning before selecting a read/maintenance policy. The observed-after-run
warehouse remains serverless PRO 2X-Small with one min/max cluster; no resize or
provision operation was performed. Attributable dollars, real producer authority,
full baseline origins and billion-scale admission remain unqualified.

## Journal batch file statistics

[Audited r100](out/native/ashlar_journal_batch_statistics_20261007_r100/audited-summary.json)
adds apply_batch_id to the journal's existing feed/epoch/source-position/id file
statistics, then explicitly recomputes Delta statistics. The property change
takes 1.08 s and backfill 6.35 s. Journal versions 10→11→12 record SET TBLPROPERTIES
and COMPUTE STATS, without new publication. The 43 active files / 4,476,500,707
bytes, clustering, reader/writer versions and table features remain unchanged.
Full bidirectional EXCEPT ALL on all 900k rows and all columns passes in 84.20 s;
this is experimental preservation accounting, not a required per-batch scan.

| Paired snapshot | Caller | Engine | Read bytes | File reads | Pruned file reads |
| --- | ---: | ---: | ---: | ---: | ---: |
| old version10, pair1 | 6.64 s | 5.73 s | 2.63 GB | 100 | 0 |
| new version12, pair1 | 6.76 s | 5.89 s | 2.60 GB | 26 | 74 |
| old version10, pair2 | 6.47 s | 5.61 s | 2.63 GB | 100 | 0 |
| new version12, pair2 | 6.22 s | 5.39 s | 2.74 GB | 26 | 74 |

Counts aggregate repeated scans in the multiset query, not unique physical
files. All exact final metrics are uncached; remote-read bytes vary, including
271 MB on the last new-version query. Both snapshots contain the same journal
rows. Pruning improves, but bytes and latency do not improve materially within
these two alternating pairs. No publication-freshness or billion-scale gain is
claimed. Existing manifests still pin the old journal versions; future publisher
vectors must capture actual new versions to bind the backfilled statistics.

The proposed 0.3 DDL now includes this supplementary journal statistic, retaining
logical columns and clustering. Existing tables need an explicit backfill;
configuration alone is not retroactive. [Official ANALYZE semantics](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-aux-analyze-compute-statistics)
distinguish Delta file statistics from optimizer statistics. This experiment does
not add a separate optimizer-statistics sweep, re-cluster/partition the table or
prove pruning after arbitrary mixed-batch compaction. Publisher throughput and
read contention remain separate measurements.

## Actual filtered publication follow-up

[Audited r99](out/native/ashlar_filtered_publication_20261007_r99/audited-summary.json)
completes three new causal 100k-member hot-set publications. Actual vectors bind
current 10/11/12 and raw/journal 8/9/10, with nodes/tombstones 0. Every batch passes
all 20 affected carrier fields, lexical old/new property preservation, exact
journal multiset equality, raw payload/digest/cursor and independent timestamp
checks. Added outer-envelope schema revision, batch, record kind and non-null
received-time checks pass. Each MERGE updates exactly 100k rows with zero copied,
inserted or deleted rows. All exact statement metrics are final and uncached.

The first batch stopped on a terminal metadata-column compilation error before
canonical apply: the added predicate used record_type/captured_at instead of
record_kind/received_at. Recovery corrects that read and reuses completed steps
only after exact SQL equality. Audit verifies one successful execution of every
stage CREATE, raw append, journal append, MERGE and manifest append. No writes
were repeated. Its 89 s processing interval includes the recovery gap; schedule
offsets are reconstructed from the original wall-clock epoch, so no clean
freshness distribution is claimed.

The uninterrupted second/third batch processing intervals are **61.62/66.47 s**.
Batch-scoped raw parity takes 6.00/6.08 s; raw/journal appends each take about
12–13 s. These are actual full-path samples, not a p95 or sustained admission.
Compared with r96, transport changes from REST to a persistent uncached session,
raw parity is batch-scoped, extra metadata gates are added and retained history
has grown. Do not attribute all timing differences to one predicate.

Journal parity takes 7.01/7.55/12.42 s, with 68/86/100 files read and zero files
pruned. The observed journal has 43 active files / 4,476,500,707 bytes after the
run; configured file statistics cover source_feed/source_epoch/source_position/id,
omitting apply_batch_id. A batch statistic is the next bounded physical-layout
candidate; faster last-batch validation is not proved. This is distinct from
changing the logical journal or promising pruning after mixed-batch compaction.

The warehouse is observed after the run as unchanged serverless PRO 2X-Small,
one min/max cluster, ten-minute auto-stop. No resize/provision operation was
performed and attributable dollars remain unqualified. The persistent session
sets and reads back a 180-second statement execution timeout and disabled result
caching; [session timeout semantics](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/parameters/statement_timeout)
bound execution rather than all controller/queue wall time. Input preparation,
real source completeness/fencing/acknowledgement, reader contention, sustained
10k/s or 100k/s burst admission and 1B/5B operation remain unqualified.

## Validation tuning after the scheduled run

[r97](out/native/ashlar_raw_validation_20261007_r97/persistent-uncached/audited-summary.json)
shows no useful latency benefit from combining raw payload parity and membership:
separate caller totals 5.67/6.24 s, combined 5.78/6.16 s, with higher combined
engine time in both pairs. Exact negative controls pass. Cached REST repeats
are retained and excluded; the comparison uses a persistent session with caching
disabled and final exact-query metrics.

[r98](out/native/ashlar_raw_batch_filter_20261007_r98/audited-summary.json)
isolates the batch predicate. Unfiltered/filtered caller timings are 15.14/5.18 s
and 6.49/4.81 s; read bytes decrease from 3.64–4.03 GB to 1.33 GB. All reads are
uncached query executions over warm data. No new publication or freshness claim
follows. [Design implications and authority prerequisites](incremental-publication-design.md)
retain complete batch membership and global origin uniqueness requirements.

## Finite scheduled publication follow-up

[Audited r96 schedule](out/native/ashlar_scheduled_publication_20261006_r96/version-aware-preflight/audited-summary.json)
completes three causal updates of the same 100k-member opaque hot set against
the existing 20M-edge native table. All 20 affected carrier fields, lexical
property patches, full raw bytes/digests/origins, independent UTC-microsecond
timestamp instants, journal multiset equality and global identity counts pass.
All exact statement metrics are final and uncached. Manifests bind actual
current versions 7/8/9, raw versions 5/6/7 and journal versions 5/6/7.

| Batch | Modeled input window | Queue wait | Processing | Complete input to verified manifest |
| --- | --- | ---: | ---: | ---: |
| r96-b1 | 0–10 s, 10k changes/s | 0.005 s | 83.80 s | 83.81 s |
| r96-b2 | 10–20 s, 10k changes/s | 74.95 s | 77.56 s | 152.51 s |
| r96-b3 | 20–21 s, 100k changes/s | 152.63 s | 80.48 s | 233.11 s |

Uniformly modeled per-record arrival freshness over 300k changes is p50 157.51 s,
p95 233.96 s and maximum 234.11 s. These are a finite modeled distribution,
not measured producer arrivals or sustained-rate admission. The controller
actually gates complete, pre-staged immutable batches at the window ends;
input generation/extraction/network cost is excluded. No concurrent reader load,
real writer fence, producer completeness or acknowledgement is exercised.
The serialized path on unchanged 2X-Small compute accumulates backlog and misses
the provisional 60 s freshness comparison even under this favorable scope.
Unity Catalog Delta remains selected; optimize publisher work and measure
resource/batch choices rather than treat this result as an architecture veto.

Before admission, recorded background OPTIMIZE commits advanced current 3→6
and raw/journal 2→4. Two read-only preflights stopped before writes; their records
are retained. The corrected harness checks maintenance lineage and captures
actual versions. Service-account identity does not establish the scheduling
mechanism. Current MERGEs each update exactly 100k rows with zero unchanged rows
copied, inserts or deletes; maintenance, file retention and wide journal bytes
remain part of physical accounting. No attributable dollar cost or billion-scale
claim follows.

The earlier 24M-carrier test compressed current rows to about 100 bytes. This
iteration retains 4M objects / 20M edges and eight semantic property shapes, but
uses 64–128 distinct SHA-derived blocks in opaque values. Repeated-block
compression is removed. This is a declared storage sensitivity, not a measured
consumer distribution or a billion-scale support claim.

## Completed local evidence

[Local result](out/entropy-local-20261006/summary.json) passes full-field
bidirectional multiset equality on all 24M rows, both typed endpoint closures,
unique relationship/source/target pairs and disjoint object/edge ID ranges.
The shared-allocation and endpoint rules mirror the inspected Truss storage
specification. Real Truss feed, catalog authority and worker behavior are not
implemented or qualified by this fixture. All vertices have degree five in each
direction; hubs/skew and lifecycle changes are absent.

The 100k-row calibration measured 1,691.40 Parquet bytes per row, average property
text 3,127.40 bytes and maximum 8,275 bytes. It projected 40.59GB current data and
162.37GB temporary/retention allowance, below a 500GB admission bound. Actual
current data is **40,770,243,854 bytes**:

| Local table | Rows | Files | Bytes | Write | Full-field validation |
| --- | ---: | ---: | ---: | ---: | ---: |
| object_current | 4M | 1,024 | 6,775,253,234 | 50.17 s | 73.35 s |
| edge_current | 20M | 1,024 | 33,994,990,620 | 305.47 s | 639.05 s |

Spark 3.5.3 / Delta 3.2.1 / Java17, local[8], 32GiB driver, UTC; no directory
partitions. Files are hash-range-distributed and sorted, Parquet Zstd. The 1,024
files/table are an explicit physical test setting, not the chosen native liquid
clustering algorithm or a universal optimal file count. Validation cost is part
of experimental accounting and cannot be omitted from a future publication SLA.

## Completed GraphFrames integration

[GraphFrames result](out/graphframes-scale-20261006/summary.json) uses the existing
`create_v02_release_graph` adapter over materialized version-0 releases. Spark
3.5.3 / Delta 3.2.1 / GraphFrames 0.12.3 executes the real API at 4M vertices / 20M
edges. Every vertex has in-degree/out-degree 5, and the two-hop motif returns
**100M paths**, preserving independent path multiplicity. Adapter closure and
identity checks pass; no entire graph is collected into Python. Recorded degree
operations take 4.67s/4.04s, motif count 26.11s; these individual local executions
are not latency distributions or an SLA.

Node/edge release materialization takes 18.60s/83.46s, retaining property and
unknown-content text in the projections. Other source metadata remains in pinned
canonical tables. The first process then failed serializing its GraphFrame object
for the result log. It was terminal before read-only resume: existing Delta
locations were registered and read at version 0, without repeating release writes.
[Materialization/recovery record](out/graphframes-scale-20261006/materialization.json)
keeps those timings separate from the resumed checks.

This qualifies the tested local DataFrame mapping and algorithms at 24M elements.
It does not qualify direct UC reader3/writer7 features, native access permissions,
PuppyGraph, Fabric, degree-skewed graph workloads, full source publication or
1B-node/5B-edge operation. Release copies enter storage and refresh accounting.

## Native higher-entropy phase

`entropy_native.py` completes the same dataset on existing 2X-Small single-cluster
serverless `data-gateway`, without resize/new compute. [Audited native results](out/native/ashlar_entropy_20261006_r86/audited-summary.json)
pass exact all-field comparisons and typed endpoint/shared-allocation/unique-pair
checks. Both final table versions are 0; no cancellation was requested.

| Native table | Files | Bytes | Write caller | Full comparison caller |
| --- | ---: | ---: | ---: | ---: |
| object_current | 64 | 6,764,430,923 | 91.08 s | 90.58 s |
| edge_current | 512 | 33,947,307,001 | 652.85 s | 729.45 s |

Total active current bytes are 40,711,737,924. Final history gives write engine
90.458s/652.276s and full-comparison engine 89.914s/728.340s. Node mean files are
about 105.69MB and edge mean 66.30MB; the 64MiB target is not a hard maximum.
Reader3/writer7 includes clustering, deletionVectors, rowTracking and v2Checkpoint.
The 576-file native result is a substantial metadata step beyond the 21-file
compressed baseline, but not the tens-of-thousands-of-files billion case.
No latency/steady-publication result is inferred from these static writes.

Native parity first proves exact count and unique native-ID cardinality, then
performs one full outer expected-row comparison over every carrier field. Text
comparisons encode UTF8 and compare hex bytes; missing/extra IDs are explicit.
This fixture has globally unique native IDs. This join is not a generic Ashlar
identity rule. [Negative controls](out/native/ashlar_entropy_parity_20261006/summary.json)
verify zero differences for equal rows and detection of one lexical property
change and one missing row. Expected opaque data is generated once, avoiding the
local check's two expensive expected-data shuffles without weakening field scope.

## Transport and resource controls

[Paired read evidence](out/native/ashlar_parameter_reads_20261006_r87/summary.json)
compares 50 literal-hash reads with 50 fixed-SQL native named-parameter reads on
the existing r85 version 2 table. All 50 paired full-carrier results agree; all
query metrics are final and result caching is off. Native binding preserves
signed-int64 extremes and an exact value above 2^53. Driver 4.3.0 uses the
[documented native parameter support](https://docs.databricks.com/aws/en/dev-tools/python-sql-connector).
Default harness behavior stays unchanged; parameter and comment control are
explicit options.

Binding does not materially improve measured performance. Zero-remote-byte
cohorts contain 39 literal / 36 parameter reads: engine p95 235/239ms and caller
p95 590/666ms. Positive-remote-byte cohorts contain 11/14 reads: caller p95
5,833/1,391ms. These are observed cohorts, not controlled full warm/cold workload
admission. Overall caller p95 is 1,350/1,304ms; neither changes UC architecture
or meets the provisional comparison budgets.

Separate REST SET commands do not establish persistent session settings.
`Client(cancel_after=900, observation_timeout=960)` requests cancellation once
against the known statement handle after its deadline, then observes that same
handle. It never resubmits the write. [Cancellation controls](out/cancel-controls-20261006.json)
verify canceled, successful-race and default-no-cancel transitions with a mock
API; they do not prove server rollback. A native phase hitting the cancellation
bound stops further growth even if a committed success races the request.
Inspect table history and result integrity before any recovery.

[Compute inventory](out/native/ashlar_entropy_20261006_r86/compute.json) records
serverless PRO, 2X-Small, min/max clusters1 and 10-minute idle stop. No shared
settings were changed. [Billing inventory](out/native/ashlar_entropy_20261006_r86/billing/summary.json)
returns 28.926475 DBU for shared warehouse-day activity, through 18:00 UTC—before
these runs. It is lagged and not experiment-attributed cost. No dollar or zero-cost
claim follows. Record final storage, engine time, metering coverage and retained
copies when the native phase is complete.

## Remaining full-goal requirements

Actual 1B/5B scale, sustained and burst publication, raw source and property
history amplification, metadata planning at tens of thousands of files, skewed
reads, recoverable fenced publisher authority and other engines remain open.
No data expires and no budget is silently revised by these experiments. This
iteration materially raises physical bytes and tests a real graph engine, while
preserving the full objective and the fixed Unity Catalog architecture.

## Native full-carrier singleton measurements

[r88 audited results](out/native/ashlar_entropy_reads_20261006_r88/audited-summary.json)
pin edge_current version 0: 20M edges / 33.95 GB in 512 files. All 150
full-carrier reads return the expected native source/type/id; exact full-field
baseline parity is established separately by r86. Four persistent driver 4.3.0
sessions disable result caching; all exact query IDs have final history metrics
and zero result-cache hits. Each read touches exactly one file.

| Cohort (50 reads each) | Engine p95 | Caller p95 | Reads with remote bytes |
| --- | ---: | ---: | ---: |
| First touch | 619 ms | 866 ms | 44 |
| Repeated same keys | 124 ms | 397 ms | 4 |
| Four clients, repeated keys | 156 ms | 455 ms | 0 |

File bytes read have p95 84.43 MB. The configured 64 MiB target is not a
maximum. Four-client fully local reads miss provisional 100 ms engine / 250 ms
caller comparisons; repeated reads include four small remote reads and are
not an entirely warm cohort. First touch is not a controlled cold-data test.
These measurements tune the owner-selected Unity Catalog Delta architecture.
They do not admit 1B/5B scale or demonstrate sustained publication freshness.
Next material work is a large incremental publication slice including raw
input, property journal, tombstones and the validated publication manifest,
followed by skew/file-count and steady/burst workload evidence.

## Large incremental publication and post-update reads

[r89 audited publication](out/native/ashlar_entropy_publication_20261006_r89/audited-summary.json)
adds property 107=true to 200,000 scattered edges. The immutable stage changes
only property text, entity version, explicit new origin/batch references and
publish time. All 20M current rows pass exact per-field comparison against the
baseline plus intended changes, including UTF-8 lexical comparison of string
carriers. A separate 200k comparison proves old property 107 absent and preserves
every prior property token. Both checks pass with zero mismatches.

The declared synthetic complete-edge wire contains all 20 field names, with
JSON carrier text stored as strings. 200k raw envelopes/digests/reference tuples
and 200k exact journal rows pass; property history represents absent-to-true with
old_present=false, old_json=NULL, new_present=true, new_json='true'. There are no
deletions and the tombstone table is empty. The descriptor reads back exactly
with actual versions: objects 0, edges 1, raw 1, journal 1, tombstones 0. Changed-row
origins qualify; **baseline raw origins remain unavailable and unqualified**.
No production source checkpoint, acknowledgement, receipt/fence authority or
full graph-origin support follows from this manifest. Endpoints remain unchanged;
this phase deploys no adjacency/degree or typed property projections.

| Phase | Caller time |
| --- | ---: |
| Immutable stage | 5.67 s |
| Raw capture | 6.78 s |
| Journal append | 2.77 s |
| Canonical MERGE | 61.37 s |
| Exhaustive 20M-row output audit | 642.29 s |
| 200k prior-property lexical check | 247.26 s |
| Manifest installation | 2.36 s |

Arrival to verified manifest is **988.04 s**, including staging, checks, version
inventory and readback. This is one audit-heavy batch, not p95 or sustained/burst
admission. No audit time is subtracted to declare freshness satisfied. The
[incremental validation design candidate](incremental-publication-design.md)
requires qualified source completeness, writer authority and predecessor coverage
before replacing exhaustive checks in an operational path.

MERGE engine operation time is 59.726 s: 9.617 s scanning and 50.045 s rewriting.
It updates 200k, inserts/deletes 0, copies 0 unchanged rows, removes 0 baseline files,
adds 16 files / 332,461,375bytes and 512 deletion vectors. Active edge storage is
34,279,768,376bytes in 528 files. The additional active current/raw/journal/stage/
manifest total is 1,018,536,638bytes; it excludes logs, sidecars, retained old files
and later maintenance. Compute remains existing single-cluster 2X-Small serverless
PRO; shared billing dollars remain unqualified. Every statement is terminal,
metrics are final, and none reached the cancellation bound or hit result cache.

[r90 audited post-update reads](out/native/ashlar_entropy_post_reads_20261006_r90/audited-summary.json)
passes 200 identity/version checks with session result caching disabled and final
metrics for every exact query ID. The first three cohorts repeat r88's keys.
The fourth explicitly selects 50 updated rows and checks version 1/property 107.
No maintenance precedes these reads.

| Cohort (50 each) | Engine p95 | Caller p95 | Files p95 | Bytes p95 | Remote-read count |
| --- | ---: | ---: | ---: | ---: | ---: |
| First touch at version 1 | 231 ms | 475 ms | 17 | 415.0 MB | 3 |
| Same keys repeated | 114 ms | 402 ms | 17 | 419.5 MB | 0 |
| Four clients | 248 ms | 522 ms | 17 | 419.5 MB | 0 |
| Explicit changed rows | 103 ms | 343 ms | 17 | 421.0 MB | 0 |

Baseline four-client reads touched one file (84.4 MB p95), with engine/caller p95
156/455 ms. The updated fixed-key workload touches up to 17 files and about five
times the bytes; its four-client engine/caller p95 grows to 248/522 ms. Neither
version meets both provisional warm comparisons. First touch is not controlled
cold data; serial changed rows are not a concurrent workload. Next test targeted
clustering maintenance and its cost, then qualify incremental batch validation
and scheduled arrivals. Keep file-count/skew and 1B/5B admission open.

## Routine maintenance and affected-row validation

[r91 maintenance](out/native/ashlar_entropy_maintenance_20261006_r91/audited-summary.json)
commits edge version 2 in 12.75 s caller time. Routine OPTIMIZE replaces 16 update
files/332,461,375bytes with 4 files/332,650,126bytes; it removes no deletion
vectors and leaves the 512 baseline files in place. Active current storage is
34,279,957,127bytes in 516 files. Removed files remain retained for version 1;
maintenance adds 333 MB of retained physical files, not just the 189 KB active-byte
difference. Exact all-field/full-membership comparison over 20M rows passes with
zero mismatches (326.26s audit). No phase hits the cancellation bound.

[r92 maintained reads](out/native/ashlar_entropy_maintained_reads_20261006_r92/audited-summary.json)
passes 200 full-carrier identity/version checks with uncached final metrics.
The same mixed-key and explicit updated-key cohorts are used; this is version 2
physical evidence. Existing manifest r89 remains pinned to version 1 and does
not automatically benefit from maintenance.

| Cohort (50 each) | Engine p95 | Caller p95 | Files p95 | Bytes p95 | Remote reads |
| --- | ---: | ---: | ---: | ---: | ---: |
| First touch | 169ms | 465ms | 2 | 156.9MB | 4 |
| Same keys repeated | 103ms | 335ms | 2 | 156.9MB | 0 |
| Four clients | 161ms | 463ms | 2 | 156.9MB | 0 |
| Explicit changed rows | 437ms | 668ms | 2 | 175.6MB | 3 |

Pruning improves from 17 files to 2 and four-client p95 from 248/522 ms to 161/463 ms
engine/caller. Provisional warm comparisons remain unsatisfied. Explicit changed
rows include 3 remote reads and cannot be called a fully warm cohort; first touch
is not controlled cold data. Retained deletion vectors are not automatically a
reason to rewrite all baseline files. One observed 13-second operation does not
establish a per-batch maintenance policy or billion-scale metadata cost.

[r93 affected-row validation](out/native/ashlar_entropy_changed_validation_20261006_r93/audited-summary.json)
compares all 20 canonical fields for 200k retained intended/current rows in 8.98 s,
with exact UTF-8 text equality and missing/extra membership detection. A broadcast
of narrow native source/relationship/id keys filters baseline rows before the
wide property join; exact prior-property preservation takes 28.64 s versus 247.26 s
in r89. The [physical plan](out/native/ashlar_entropy_changed_validation_20261006_r93/plan/physical-plan.txt)
shows a PhotonBroadcastHashJoin on those three keys, followed by the affected
wide-row join. Negative controls reject explicit null as absence and malformed
JSON; a byte-different valid JSON stage token produces exactly one mismatch.
All final query metrics are uncached.

This times checks on a stored synthetic batch, not new arrivals, sustained ingest
or publication freshness. It supplements the exhaustive audit and does not
qualify source completeness, predecessor origins, acknowledgement or concurrent
writer authority. Next measure an actual batch with affected-row checks, including
large old/new property tokens in the journal: r89's common boolean 107 event
strongly understates that history-width case. File-count/skew, sustained/burst
load, external readers and 1B/5B admission remain open.

## Large old/new journal publication and serializer qualification

[r94 measured slice](out/native/ashlar_entropy_wide_journal_20261006_r94/audited-summary.json)
changes property 105 on 100,000 existing opaque-value edges. The qualified compact
ASCII-hex fixture has quoted old/new tokens 4,162–8,258bytes, mean 6,209.93bytes.
Native keys, endpoints, retained data and every other property token remain
unchanged; exact intended patch and all 20 affected output fields pass. Raw
reference/digest checks and 100k complete journal row comparisons pass. Global
current cardinality/distinct IDs remain 20M, with 100k entity-version 2 rows.
This is an already updated hot set, not uniformly scattered new keys.

| Phase | Caller time |
| --- | ---: |
| Stage construction, including new value generation | 14.38s |
| Independent intended carrier patch | 7.09s |
| Raw capture | 7.35s |
| Large-token journal append | 6.66s |
| Raw reference/body comparison | 7.30s |
| Exact journal comparison | 6.77s |
| Canonical MERGE | 9.19s |
| Exact affected output comparison | 5.58s |
| Manifest installation | 1.28s |

Arrival to verified descriptor is 78.26 s, including generation, staging, input/
output checks, counts, actual-version inventory and readback. One batch misses
the 60 s comparison; it is not p95, 10k/s sustained or 100k/s burst admission. No
construction/validation time is subtracted. MERGE operation time is 8.110 s,
updates 100k, copies/inserts/deletes 0, adds 16 files/327,001,551bytes and 4 deletion
vectors. Earlier scattered 200k updates added 512 vectors and took 61.37 s caller.
That comparison has different membership/value changes and establishes a
distribution sensitivity, not an isolated throughput scaling law.

Incremental physical files total 2,285,219,689bytes: stage 663,687,208, new current
327,001,551, raw 655,463,391 and journal 639,067,539. Logs, sidecars, descriptor
and older retained files are excluded. The large-token journal adds about 639 MB,
compared with 2.54 MB for the earlier 200k common boolean events. History width
and retention belong in admission budgets; sparse boolean events do not qualify
large-value change costs. No new/resized compute is used; shared dollars remain
unqualified. New synthetic descriptor r94 pins objects 0/current 3/raw 2/journal 2/
tombstones 0 and retains r89. Baseline raw origins and real writer/source authority
remain unqualified. There are no structural/projection changes.

**Wire precision correction:** an independent timestamp oracle found that the
default serializer truncates derived published_at to milliseconds. All 100k r94
raw envelopes lose 870 µs versus their stage value; all 200k r89 envelopes lose 307 µs.
The prior expected-payload comparison used the same encoder and masked this
loss. Full native Delta carrier/journal timestamps remain intact, as do property,
retained and cursor text. Historical raw payloads/descriptors are retained and
MUST NOT substantiate complete all-field wire reconstruction. This supersedes
earlier complete-envelope wording within that field/codec scope.

[Corrected encoder oracle](out/native/ashlar_wire_timestamp_20261006_r95/summary.json)
passes independent instant equality on all 100k retained stage rows, plus
sub-millisecond/pre-epoch controls and exact large-integer/decimal bag text.
[wire_json.py](wire_json.py) specifies UTC and six fractional digits. Future
harnesses use it and independently compare wire/stage timestamp instants; this
is a read-only correction proof, not repaired historical inputs or a new batch
freshness result. The old payloads are not rewritten under their original IDs.

[Post-publication token qualification](out/native/ashlar_entropy_wide_journal_20261006_r94/token-qualification/summary.json)
proves every old JSON token appears byte-for-byte in its prior carrier and every
new token differs. Escaped/pretty source syntax is explicitly outside this
compact ASCII fixture; negative controls detect it rather than normalize it.
The first VALUES negative query was terminally rejected for computed literals;
SELECT UNION ALL correction passes, with the failed read retained. No generic
native-source token extraction or full producer reconstruction follows. Next
measure repeated scheduled publications under the corrected encoder and full
raw/history cost, then qualify file-count/skew and larger resource bounds.

## r103 latency component audit

[Offline exact-query decomposition](out/contention-components-r103.json) uses
saved final uncached query histories; no benchmark queries were rerun. Quiet
new-publication reads have component p95s of 156 ms compilation, 110 ms execution,
264 ms total server duration and 114 ms caller-minus-server duration. During
publication, the 114 reads with no remote bytes have 231/495/667/128 ms for those
components respectively. Component percentiles describe different queries and
MUST NOT be added. Caller-minus-server includes connector, network and client
scheduling; queue-end minus server start also includes initialization.

The live read-only compute inventory on 2026-10-07 found exactly one warehouse
(`data-gateway`, 2439e1f2e37ac563, serverless 2X-Small, min/max one cluster,
10-minute auto-stop, running) and no classic clusters. No resize or provisioning
was performed. A true reader/publisher compute-isolation comparison needs another
resource. Before that, compare stable parameterized singleton SQL against the
existing literal-query path on identical keys and pinned versions, with result
cache disabled and exact all-field checks. Separate compilation, execution and
caller durations. This may reduce compilation overhead but is not presumed to
fix execution contention or admit the provisional 250 ms caller target.

## r106 stable-query comparison

[Final native audit](out/native/ashlar_parameterized_reads_r106/audited-summary.json)
passes 120 exact all20-field reads at published edge version16. Two balanced
rounds compare the same 30 keys on one persistent connection, without comments
for either mode. All final queries are uncached and read no remote bytes.
Literal/parameterized p95 respectively: caller367.30/379.23ms, execution106/112ms,
compilation147/155ms, server total256/266ms; both touch15files at p95.
Parameterized SQL has one text versus30literal texts. It provides no measured
latency improvement here, so retain it for safe query construction without
claiming plan-cache gains. Quiet hot-set results do not qualify contention, cold
data, sustained ingest or billion-scale admission. No writes or compute changes.
The next performance comparison should isolate compute resources.

## High-degree narrow adjacency sensitivity (r108)

[Native final audit](out/native/ashlar_hub_adjacency_r108/audited-summary.json)
creates a separate seven-column20M-edge synthetic adjacency table at version0.
One million edges are redirected to typed pilot32/other160 hubs with degrees
900004/100004; all other endpoints retain their baseline values. Hub relationship
IDs form a distinct synthetic range by destination type. Full EXCEPT ALL projection
parity,20M unique edge IDs, typed endpoint closure against4M objects and20M unique
relationship/typed-endpoint pairs pass. Canonical tables were not modified.
This altered graph is a layout sensitivity fixture, not a serving projection of
a published canonical graph or an actual Truss relation vocabulary.

Endpoint liquid clustering `(source_system,source_type,source_id)` produces8files
and240160289bytes. Five uncached warm repetitions each give:

| Hub | Degree | Count caller/engine p95 ms | 100-row page caller/engine p95 ms | Files p95 |
| --- | ---: | ---: | ---: | ---: |
| pilot32 | 900004 | 406/140 | 458/191 | 1 |
| other160 | 100004 | 346/108 | 333/96 | 1 |

All query histories are final, uncached and report zero remote bytes. Exact first
page IDs, relationship IDs and typed destinations match an independent arithmetic
oracle. Small five-sample p95s are observed maxima, not service percentiles.
Pages read37.4/5.1MB at p95 despite returning100rows; source clustering does not
prove edge-ID ordering or constant page cost. No later/deep pages, reverse layout,
degree cache, updates, concurrent ingest or billion-scale behavior is admitted.
The table's actual reader3/writer7 features still need external qualification.

## Deep keyset and empty-tail sensitivity (r109)

[Read-only exact-query audit](out/native/ashlar_hub_deep_pages_r109/audited-summary.json)
passes44 arithmetic-oracle page/tail/end queries over pinned adjacency version0.
Cursors4M/4.4M/4.8M return100 exact increasing edges; cursor5M returns the four
remaining baseline edges with exact relation/destination types and IDs. A cursor
beyond the last edge returns empty. Five repetitions per page and two per empty
end are all final uncached, with no remote reads.

Pilot pages always read37.4MB/onefile regardless of cursor; other pages read5.1MB.
Empty ends still read21.3/3.0MB. Engine page maxima across the five-sample groups
are160–198ms pilot and86–99ms other. Caller observations include a1.09s pilot
maximum despite186ms engine execution; no caller stability or general SLO follows.
Endpoint clustering locates a hub but does not demonstrate within-hub edge-ID
pruning. Next compare explicit edge-ID clustering/order under a hub that spans
multiple files, with write/maintenance costs recorded. Preserve canonical
identity clustering and keep any adjacency change rebuildable and publication
pinned. These results do not qualify ingest, external engines or1B/5B.

## Paired edge-ID clustering attempt (r110)

[Native exact audit](out/native/ashlar_hub_ordering_r110/audited-summary.json)
passes full independent20M-row parity for two identical all-hub graphs, typed
closure, unique edge IDs and unique relationship/typed-endpoint pairs, plus48
exact shallow/midpoint/deep/empty-end page queries. Degrees are18M/2M.

The intended multi-file case was NOT achieved: endpoint-only clustering produces
one41.2MBfile, endpoint-plus-edge-ID produces one77.3MBfile despite a64MiB
target setting. Actual output, not configured target bytes or row count, determines
qualification. Highly repeated endpoint/relation columns compressed unusually
well. All final queries are uncached, with zero remote bytes. Nonempty pages
read41.5MB versus77.9MB at every cursor; observed three-sample engine maxima
are291–328ms versus329–354ms. Adding edge ID shows no measured benefit here.
Both layouts skip all files for a cursor beyond the global maximum.

Liquid clustering across four columns does not imply a lexicographic source/edge
sort. This result does not settle within-hub multi-file pruning; the next fixture
must verify that a hub physically occupies multiple files before running that
comparison, using declared entropy or controlled writer layout and recording
additional bytes/cost. Canonical tables, selected UC architecture and provisional
SLOs remain unchanged; no billion-scale or ingestion admission follows.

## Actual eight-file hub comparison (r111)

[Final native audit](out/native/ashlar_hub_ordering_r111/audited-summary.json)
materializes the same20M all-hub graph in eight2.5M-row append ranges per table,
with automatic compaction/optimized writes disabled. Metadata confirms each hub
occupies8files before timing. Full EXCEPT ALL parity against the qualified pinned
r110 baseline, typed closure,20M unique IDs/relation-endpoint pairs and48 exact
page/end arithmetic checks pass. Each target is pinned to observed version8.

Both declared clustering layouts prune8/4/1/0files at cursors4M/14M/22M/24M.
The chosen append ranges provide disjoint edge-ID bounds; this is not evidence
that adding edge ID to liquid clustering caused the pruning. Three-sample maxima
for deepest nonempty pages are83–88ms engine and322–341ms caller. Shallow pages
still scan8files; ordering plus LIMIT does not prove early file termination.
All histories are final uncached with zero remote bytes.

Endpoint-only physical size is76188bytes versus60689808bytes with the extra
clustering key. This extreme periodic fixture compression excludes production
capacity/entropy extrapolation, regardless of the passing20M-row correctness
checks. Additional key clustering offers no clear timing benefit in this run.
Write evidence records sixteen actual appends; no OPTIMIZE or scattered update
has demonstrated retention of these range boundaries. Keep canonical identity
clustering unchanged. A narrow adjacency layout can exploit edge-range file
statistics, but maintenance costs and robustness under realistic updates need
qualification before selecting a range or bucketing policy. No source-rate,
external-engine or1B/5Badmission follows.

## Proposed contract ordering differential (r112)

[Native final audit](out/native/ashlar_hub_contract_pages_r112/audited-summary.json)
passes36 exact pages in CONTRACT-003's `(rel_type_id,edge_id)` order at three
relationship/edge cursors, over both pinned eight-file layouts. Independent
arithmetic enumeration verifies cross-relationship continuation, all identities
and typed destinations. All final queries are uncached with no remote reads.

Every nonempty contract-order page reads8files; endpoint-only consumes76188bytes
and the added-edge-ID layout66826386bytes. Three-sample engine maxima span
131–168ms and caller369–470ms. The earlier edge-ID-only range pruning does NOT
qualify the proposed relationship-first cursor. Keep the cursor semantics; next
compare the reference DDL's relationship-oriented clustering or deliberate
relationship-range writer layout, preserving exact independent edge identities
and measuring costs. Do not infer a policy from this highly periodic fixture.

## Full-shape relationship-range candidate (r113)

[Final native audit](out/native/ashlar_hub_contract_pages_r113/audited-summary.json)
passes20M full eight-column adjacency parity against the qualified baseline plus
constant structural_version1,20M unique edges, and36 independently enumerated
relationship-first page comparisons. Eight append batches each cover20relationship
IDs, with reference forward clustering `(source_system,source_type,source_id,
rel_type_id)` and declared statistics on identity/endpoints/edge ID. Metadata
confirms8files per hub; no canonical tables or reference DDL were changed.

The candidate reads8/4/1files at relationship cursors9005/9085/9155, versus8/8/8
for the edge-range control. Corresponding candidate byte maxima are97968/45814/
11087 versus76188 at every control cursor. All histories are final uncached and
report no remote bytes. Caller three-sample maxima remain360–513ms; less scanning
is not proof of stable latency. The candidate carries an additional constant
structural-version column, so file bytes are not a strictly schema-identical
comparison. This very low-entropy synthetic graph excludes production capacity
extrapolation. CTAS does not qualify NOT NULL enforcement or publication custody.

Retain the proposed relationship-first cursor and forward adjacency clustering.
Prefer relationship-oriented file statistics to edge-only range claims when
planning its writer/maintenance policy. Explicit batches here establish the
bounds; neither automatic clustering nor arbitrary scattered updates have been
shown to preserve them. Do not prescribe20relationships/file, disable compaction
universally, or infer reverse/ingest/billion-scale performance from this fixture.

## Singleton RPC boundary measurement (r114)

[Final audit](out/native/ashlar_singleton_rpc_r114/audited-summary.json)
passes30 exact full20-field E16 singleton reads against the independent r103
oracle, using one persistent connection on the existing warehouse. All final
histories are uncached, with15files at p95 and no remote reads. Warm p95 is
111ms engine,162ms compilation,280ms total server and385.05ms caller.
Component percentiles must not be added. This quiet sample does not establish
cold, concurrent or billion-scale performance, and exceeds provisional budgets.

Instance-scoped, method-only instrumentation records exactly30 ExecuteStatement
RPCs, with no status polling or fetch RPCs during measured reads. Per-query
caller time minus its measured RPC time has p95 0.613ms. This sample therefore
provides no evidence for fixing a client polling delay; network/server RPC time
requires separate attribution. No RPC arguments or credentials were recorded.
Current Thrift backend source fingerprints match installed RECORD entries;
package metadata reports4.3.0. These fingerprints qualify this run only and do
not retroactively identify historical connector source or prove upstream release
identity. Keep UC Delta architecture selected and singleton tuning empirical.

Next physical-layout qualification should exercise relationship-range adjacency
statistics under scattered updates and OPTIMIZE, retaining the proposed cursor
order and independent full-row oracles. The4M-node/20M-edge local and native
large-data evidence remains the tested scale;1B/5B admission is still open.

## Scattered adjacency changes and maintenance (r115)

[Final audit](out/native/ashlar_hub_maintenance_r115/audited-summary.json)
passes full20M eight-column parity and unique edge identities at clone v0,
updated v1 and optimized v4, plus54 exact full-shape cursor pages. The isolated
shallow clone starts from r113 v8; a deterministic hash selects100117edges
(0.50%) for source-hub moves and structural_version2. Expected state is projected
from the immutable original, independent of the mutated clone. This is synthetic
structural churn, not a real producer or canonical publication test.

Baseline page file counts8/4/1 become9/5/2 after update: one new mixed-range file
is admitted at every cursor. Byte maxima rise from97968/45814/11087 to
2389009/1524907/881219. The explicit OPTIMIZE produces one2,163,234-byte file,
read at every cursor. [Delta lineage](out/native/ashlar_hub_maintenance_r115/delta-history.json)
records clone0, update1, then three OPTIMIZE commits2–4 (one data rewrite and
a final no-op), not three issued OPTIMIZE commands. Update adds8deletion vectors;
maintenance removes8vectors and replaces9files with1. The baseline physical
files are shared across both hubs: per-hub counts must not be summed.

All54 final pages are uncached with no remote bytes. Three-sample caller maxima
span360–408ms baseline,369–440ms updated and414–561ms optimized; engine maxima
span87–138ms,116–171ms and163–294ms respectively. This is not a robust p95 or
a controlled causal timing estimate. Actual caller update cost4.094s and
OPTIMIZE15.554s exclude producer/publication work; they cannot establish ingest
freshness. Clone costs7.092s, with no additional compute provisioned; shared
warehouse dollar attribution remains unknown.

Retain the relationship-first cursor and reference clustering, but treat explicit
append range boundaries as temporary. Plan maintenance around measured bytes and
query selectivity as well as active file count; this fixture contradicts a claim
that fewer files necessarily improve adjacency latency. Its extremely periodic
low-entropy input cannot justify capacity, physical bucket counts, reverse access
or1B/5Badmission. Next test adjacency with realistic endpoint/relationship entropy
and bounded skew before making maintenance thresholds normative.

## Higher-entropy adjacency screen (r116)

[Final audit](out/native/ashlar_hub_entropy_r116/audited-summary.json) passes
20M full eight-column parity, unique edge IDs, unique typed relationship-endpoint
pairs, native N0 typed closure, and36 exact relationship-first pages. Destination
IDs use an affine bijection on4M IDs (coprime multiplier104729); target type and
source custody follow valid native node identities. Hash-distributed32relationship
buckets per hop and random hash write order break the previous periodic encoding.
Eight20-relationship appends are a controlled intervention, not a universal policy.
Projection oracles use the native hash semantics; no independent Python hash
oracle or actual producer distribution is claimed.

Built v8 has8files/137620838bytes, compared with r113's97968bytes for its different
periodic graph. This is observed higher entropy, not an isolated compression
causality estimate. OPTIMIZE leaves v11 with3files/137425359bytes. Contract-order
page reads change8/4/1→3/3/1files and byte maxima139690725/69947979/17337453→
138186734/138186734/51367696. Reported query read bytes differ from table physical
bytes and must not be treated as identical storage metrics. All histories are
final uncached and report no remote bytes.

Three-sample caller maxima span371–439ms built and409–493ms optimized; engine
99–165ms and140–223ms. These descriptive samples do not establish p95, concurrency
or cold-data SLOs. Eight actual appends total34.091s caller, create2.053s and
OPTIMIZE11.312s, excluding preservation/closure/oracle checks; no ingest freshness
claim follows. [Delta lineage](out/native/ashlar_hub_entropy_r116/delta-history.json)
retains all exact versions and maintenance metrics. Existing compute only;
shared warehouse dollars remain unattributed.

This reinforces the need to tune adjacency file size separately from canonical
wide-row singleton storage. A next bounded comparison should use the same exact
fixture with smaller target files and preserved contract clustering, accounting
for actual bytes scanned, update amplification and maintenance costs. Retain the
logical eight-column shape and relationship-first cursor; do not prescribe
artificial20-relationship buckets or infer automatic range maintenance. The two
hubs remain an extreme skew screen, not representative production degrees,
reverse access, publication custody or1B/5Badmission.

## Matched adjacency file-target comparison (r117)

[Final audit](out/native/ashlar_hub_filesize_r117/audited-summary.json) passes
full20M eight-column parity and36 exact contract-order pages on independent
shallow clones of r116 v8. Both retain identical relationship-oriented clustering
and run explicit OPTIMIZE FULL after setting16/64MiB targets. Actual snapshots
are16-target v3:8files/137620838bytes;64-target v4:3files/137425304bytes.
Captured individual file sizes and relationship bounds permit direct inspection.

Important asymmetry: [16-target lineage](out/native/ashlar_hub_filesize_r117/delta-history-16.json)
records no data rewrite; the existing append boundaries are retained.
[64-target lineage](out/native/ashlar_hub_filesize_r117/delta-history-64.json)
rewrites8files into3. Therefore this does not prove16MiB maintenance creates
relationship ranges from fragmented updates. It qualifies the observed no-op
versus coalescing behavior for identical source data, not a universal rewrite
policy or target-size guarantee.

Page file counts are8/4/1 versus3/3/1; byte maxima139690725/69947979/17337453
versus138186734/138186734/51367696. All36 histories are final uncached with no
remote bytes. Three-sample caller maxima span361–486ms for16 and400–505ms for64;
engine98–180ms versus138–228ms. Balanced alternating reads limit ordering bias,
but three samples are descriptive and cannot establish p95 or causal certainty.
Actual maintenance caller costs6.388s versus10.045s; no-op versus rewrite matters
to that comparison. Clone costs4.933/3.791s and target-setting1.381/1.220s.
Existing compute only; shared-warehouse dollar attribution remains unknown.

Carry16MiB as a bounded forward-adjacency candidate, independently of64MiB
canonical wide-row tuning. Next qualify scattered changes plus maintenance on
this exact higher-entropy graph before selecting a default. Retain all identity,
endpoint and relationship-first cursor semantics. Do not change reverse defaults,
add forced relationship partition directories, or infer production ingestion,
cold/concurrent latency or1B/5Badmission.

## Typed-closure correction to r115 (r118 follow-up)

[Correction evidence](out/native/ashlar_hub_closure_r118/correction.json) records
100117 dangling targets in r115 v4 against native N0. Moving source custody
without moving the destination left the selected edges outside the node source
profile. The r115 full-row parity and cursor measurements remain historical
physical observations, but its changed graph FAILS typed closure and cannot
qualify graph-preserving structural maintenance. The missing post-change closure
check was a harness gap. Check closure and unique relationship/endpoint pairs at
every changed and maintained snapshot; matching an intended projection alone is
insufficient.

The first corrected attempt r118 failed before updates with native
CANNOT_SHALLOW_CLONE_NESTED. Preserve its [terminal rejection](out/native/ashlar_hub_maintenance_r118/failure.json);
continue using original r116 v8 as the clone source in r119. No failed write was
blindly retried.

## Valid scattered endpoint changes on16MiB candidate (r119)

[Final audit](out/native/ashlar_hub_maintenance_r119/audited-summary.json) passes
full20M eight-column parity, unique edge IDs and relationship/endpoint pairs,
typed target closure and54 exact cursor pages at baseline v1, updated v2 and
maintained v10. Clone source is r116 v8;16MiB target is explicitly applied.
100117 hash-selected destinations advance10 IDs modulo4M, preserving source
custody and resolving valid target types. Changed relationships use disjoint
synthetic labels+1000 and structural_version2. This catalog-free fixture does not
qualify real producer relationship authority or source schema evolution.

For the large pilot hub, page file counts8/4/1→11/7/4→8/5/1 and byte maxima
139690725/69947979/17337453→142171344/71616650/18397163→
121163312/62011781/19280345. The smaller other hub ends in one file, reporting
13434301 read bytes at all tested cursors. Per-hub file counts must not be summed
when files are shared. Unlike r117's no-op16MiB maintenance, this run performs
actual reclustering; [exact lineage](out/native/ashlar_hub_maintenance_r119/delta-history.json)
retains every commit from the single explicit OPTIMIZE FULL operation.

All54 pages are final uncached and report no remote bytes. Three-sample caller
maxima span361–450ms baseline,411–482ms updated and386–434ms maintained;
engine97–181ms,133–187ms and93–156ms. They are descriptive, not p95 admission.
Actual update caller cost2.656s, FULL maintenance29.291s, clone3.792s; these
exclude source capture/journal/publication and correctness queries. No added
compute was provisioned; shared-warehouse dollars remain unattributed.

Retain16MiB as the forward-adjacency candidate: it preserves useful deep-page
pruning through this bounded valid-change/maintenance test, without proving all
original append boundaries survive. Separate maintenance from every publication
batch;29seconds is material against the60second freshness comparison. Next
connect its incremental maintenance cost to the complete publication path before
promoting physical defaults. Reverse layout, production degree distribution,
real source custody, external-engine activation and1B/5Badmission remain open.

## Canonical forward projection and structural reuse (r120)

[Final audit](out/native/ashlar_canonical_adjacency_r120/audited-summary.json)
reads actual immutable r101-b1/r103-b1 descriptors and verifies their canonical
edge anchorsE13/E16. Full20M structural EXCEPT ALL equality passes in6.946s
caller: independent identity, relationship, source custody and typed endpoints
are unchanged. This qualifies a structural reuse decision for these snapshots,
not interpretation of unknown producer events. Structural revision1 is a
projection revision, independent of canonical property entity_version.

A new canonical-derived forward candidate is explicitly created with all eight
NOT NULL columns, reference source/relationship clustering, declared statistics,
Zstd and16MiB target. Actual build8.699s (CREATE1.296s) yields v1 with16files
and206001802physical bytes. Full20M parity, global independent edge identities,
unique typed relationship-endpoint pairs and both endpoint closure checks pass.
Canonical tables and existing descriptors are read-only throughout.

Thirty exact full-shape pages at ten lexicographically first endpoint keys
(three repetitions) are final uncached with no remote bytes. Engine p95=74ms,
caller=299.38ms, files=1 and read bytes=2372865. These sparse neighborhoods are
not canonical singleton queries, representative degrees, cold reads or general
adjacency SLO evidence. The candidate is a faithful projection of the pinned
synthetic canonical graph rather than an altered hub fixture.

[Reviewable vector](out/native/ashlar_canonical_adjacency_r120/publication-candidate.json)
adds this exact adjacency version to r103's existing canonical/raw/journal/node/
tombstone anchors and explicitly marks reverse/degree/typed-property coverage
unavailable. It is NOT activated, inserted into an immutable manifest, or proof
of full publication/fencing/acknowledgement. Existing compute only; shared
warehouse dollars are unattributed.

Apply CONTRACT-003's property-only invalidation rule: a publisher may reuse a
qualified unchanged structural projection rather than rebuild/compact it for
every property batch. This full scan is a spike oracle, not a prescribed
per-batch billion-scale validator. A real complete-boundary source and changed
structural-set evidence are still required before incremental reuse is trusted.
Next measure a complete property publication with the candidate structural
version reused and explicitly included in a new descriptor; do not add the
r11929second structural maintenance cost to unrelated property-only batches.

## Property publication with canonical adjacency reuse (r121)

[Final publication audit](out/native/ashlar_isolation_r121/audited-summary.json)
records one actual100k property apply, exact raw capture, property journal,
canonical MERGE and immutable descriptor r121-b1. The descriptor pins E17/N0/
R13/J15/T0 and canonical forward adjacency r120 v1. Before publication, all20M
structural rows are compared exactly against the reused adjacency; no rebuild
or maintenance is issued. All20 affected carrier fields, exact ASCII token patch,
raw payload/digest/origin/UTC instant and property journal checks pass.

Ready-input→verified-descriptor=64.615s; modeled oldest-record freshness=74.615s
for a uniform10second/10k-per-second admission window. Actual input was prepared
outside the clock (13.092s stage CREATE), not delivered by a real producer. One
sample cannot establish publication p95 or sustained rate and misses even the
provisional60second per-sample comparison. No100k/s burst was run here.
Parallel raw/journal append wall=22.529s, canonical apply7.752s, full structural
reuse oracle6.021s and descriptor INSERT1.141s. Component timings must not be
added as though all phases were sequential.

Thirty exact20-field fresh singleton reads are final uncached, remote bytes0,
p95 engine110ms/caller361.48ms, files15. Both provisional warm targets remain
missed. The recorded preparing scope accidentally inherited a two-reader string
from the isolation harness; the final audit corrects it: one serialized publisher
and subsequent fresh reads, with no concurrent readers or new compute. No
real completeness, concurrent fence or acknowledgement is qualified. Baseline
legacy origins/timestamp representation remain explicitly unqualified.

Next reduce validation critical-path cost using evidence, not by weakening
preservation: investigate independent validation lanes and a declared changed-set
structural check, retaining exhaustive post-run oracles and predecessor/output
lineage guards. The full20M scan here is an experiment oracle, not a viable
universal per-batch billion-scale validation policy. Costs remain shared-warehouse
caller timings without attributable dollar billing.

[Post-publication integrity audit](out/native/ashlar_isolation_r121/integrity-custody/audited-summary.json)
passes logical-key/immutable-file/physical-row-index custody for all19.9M
untouched carriers in12.644s, with19.9M count and matching pinned predecessor/
output DBAPI schemas for canonical/raw/journal. This relies on Delta immutable
file semantics and stable schemas, not an independent re-read of all unchanged
payload bytes. The attempted wide untouched EXCEPT ALL timed out at180seconds
and is retained as [failed evidence](out/native/ashlar_isolation_r121/integrity/failure.json).
It proves no payload equality. Changed100k20-field equality is separately proven.

Complete inherited raw-record and property-journal EXCEPT ALL comparisons pass
in117.127s and110.623s respectively. All successful follow-up histories are final
uncached. These post-publication checks are excluded from recorded freshness;
do not claim descriptor activation waited for them. The cost reinforces keeping
exhaustive payload qualification bounded rather than prescribing it per batch.

Current native canonical anchors advance to E17/R13/J15 with node/tombstone0
and forward adjacency1 in r121-b1. The pending r107 isolation runner's old
E16/R12/J14 maintenance-only preflight is now stale and must be adapted to this
new predecessor before live execution. Its historical passing preflight and
controller unit evidence remain preserved; no isolated warehouse was provisioned.

## Exact validation lane scheduling (r122)

[Final audit](out/native/ashlar_validation_lanes_r122/audited-summary.json) passes
eight identical pinned100k raw/journal validation checks from r121 in two
balanced serial/parallel rounds. Separate worker-owned connections use existing
compute; no writes, source changes or publisher activation occur. All measured
queries are final uncached. Raw checks retain exact payload bytes, digest, origin
and UTC instant; journal checks retain full-row EXCEPT ALL.

Serial query caller sums are14.141/11.055s; parallel query interval spans
10.163/10.003s, with actual overlaps9.645/8.777s. Parallel individual caller sums
are19.808/18.781s: contention increases total query time while modestly reducing
the critical interval. Whole-lane walls19.074/14.208s serial versus11.817/11.530s
parallel include connection setup/close and history retrieval; do not count all
of that difference as a publication saving with pre-established clients.
Two rounds are descriptive, not p95 or a stable causal estimate.

This supports a bounded publisher scheduling candidate, not a freshness pass.
The replay excludes simultaneous append/MERGE, real producer arrivals and
cold/concurrent singleton workloads. Existing compute only; summed query time
is not attributable DBU/dollar billing. Next measure publication with independent
validation lanes, retaining durable raw capture before apply, final descriptor
after every required check, and explicit partial-commit recovery constraints.
Do not remove integrity checks to manufacture a passing60second result.

## Publication with parallel exact validation (r123)

[Final audit](out/native/ashlar_isolation_r123/audited-summary.json) records one
actual100k property publication after r121-b1. Raw/journal appends remain durable
before validation/apply; both exact validation lanes finish before canonical
MERGE. Full affected20-field parity, exact raw wire/digest/origin/UTC instant,
full property journal, global identities and complete20M structural adjacency
parity pass before descriptor r123-b1. Its actual vector is E18/N0/R14/J16/T0/
forward r120 v1; no adjacency rebuild or maintenance occurs.

Ready-input→verified-descriptor62.943s, modeled oldest freshness72.943s under
the same10second uniform admission window. Preparing actual input outside this
clock takes17.352s. Parallel append wall23.612s and validation pair10.612s;
canonical apply7.152s and structural-reuse oracle6.476s. All required publisher
checks are retained. This single sample is1.671s below r121's ready-input time;
changed data and workload noise prevent a strict causal improvement claim. It
still misses60seconds and does not establish sustained10k/s,100k/s burst or
publication p95. The repeated modeled clock is not a real producer arrival trace.

Thirty full20-field singleton checks after publication are final uncached with
no remote bytes; p95 engine102ms/caller351.76ms, files15. Both warm targets
remain missed. No concurrent reader load or additional compute. This run does
not repeat r121's expensive inherited-history/untouched-carrier post-audit; its
validation scope is explicitly affected full carriers, captured/journal slices,
identity counts, lineage and complete structural projection.

Keep parallel validation as a measured candidate, not a sufficient throughput
solution. Further work should focus on validator dependency boundaries and
changed-set structural proof, with exact source/predecessor/apply guards and
independent exhaustive qualification. Baseline source authority, concurrent
fencing, acknowledgement, external graph activation and1B/5Badmission remain
unqualified. Existing shared-warehouse timing does not attribute dollar costs.
The pending isolated-reader runner must use the new E18/R14/J16 predecessor.

## Changed-set structural differential and guard (r124/r125)

[Corrected final audit](out/native/ashlar_changed_structure_r125/audited-summary.json)
passes six exact100k structural comparisons between fixed trusted stage0, E17
predecessor and E18 output. Nine read-only corruptions are rejected: each of the
seven structural fields, deletion and duplicate insertion. Membership comes from
the immutable original stage, not mutated candidate rows. All final histories
are uncached. Stage checks3.352–3.530s and output checks5.499–5.837s do not show
a large timing gain over r123's6.476s full20M structural scan. Do not add a
redundant3.5second precheck and describe it as a saving.

[Initial failed control](out/native/ashlar_changed_structure_r124/failure.json)
is retained: the duplicate UNION ALL operand was incorrectly grouped with
EXCEPT ALL. r125 explicitly wraps candidate relations and gets exactly one
difference for insertion/deletion controls. Native queries in r124 succeeded,
but the harness assertion failed; r124 is not a qualified validator.

A separately tested finite lineage gate passes the recorded r123 MERGE and
rejects altered query ID/read version/operation/version, source/update counts,
inserts/deletes/copied rows and duplicate commits. It requires continuous
versions, exactly one known MERGE, original predecessor readVersion, exact
member count, and no unsupported mutations. Actual query history matches the
recorded owned apply text; its SHA256 is recorded. Connector-rendered operation
parameters are not valid JSON, so the guard does NOT guess escaping or claim
general predicate/source analysis.

The changed-set differential alone cannot detect outside-membership mutations.
Replacing full-graph validation requires a qualified immutable adjacency baseline,
fixed complete source membership, exact owned source/apply SQL, complete lineage
and metrics, no concurrent publisher gap, and both before/after structural proofs.
Unknowns fail closed. Existing20-field intended/output checks already contain
structural columns: investigate composing those proofs with these guards rather
than introducing redundant scans. No publisher substitution or activation was
run here; production source authority/fencing and1B/5Badmission remain open.
Two local unit tests include actual evidence and ten rejection subcases; all
pass. All native tests are read-only on existing compute; no attributable dollar
billing or whole-publication freshness claim follows.

## Composed structural reuse proof (r126; no native workload)

[Recorded result](out/composed-structure-r126.json) composes r121's full20M
adjacency baseline at E17 with r123's intended full-carrier check, exact output
check, unique100k input membership, global identities and closed owned MERGE
lineage E17→E18. Reviewed exact query hashes are pinned in a digest-bound fixture
profile; native history query text, query IDs, terminal success and result arrays
are checked. The baseline and before/after checks collectively imply unchanged
structural rows for this finite closed apply, agreeing with r123's independent
full20M structural oracle. No extra Databricks workload or publication occurred.

Three local unit tests pass: actual proof, all six missing-query cases, and five
altered SQL/result/history/profile/lineage cases. Prior lineage guard tests
remain separately applicable. Failed native operation-parameter JSON is not
interpreted; matching actual owned query text avoids guessing its meaning.

This is a fixed reviewed evidence profile, NOT a generic SQL verifier or a
production publisher admission implementation. The profile is deliberately
pinned to E17/E18, original stage0,100k members and adjacency1; it cannot be
reused by merely changing versions or query digests. Generalization requires
owned safe query templates, trusted complete membership, a qualified immutable
adjacency baseline and source meaning, closed apply/lineage and a concurrent
publisher boundary. Unknown source effects or missing proof must fail closed.
Outside-membership mutations are not covered by changed-set comparison alone.

The existing intended/output20-field checks already preserve structural fields,
so avoid adding redundant changed-set scans when composing their proof. A
future bounded integrated publisher experiment must retain an independent
post-run full structural oracle and report it outside the publication clock.
No new timing saving,60second pass, p95, real producer authority, external graph
activation or1B/5Badmission follows from this local composition.

## Owned property query templates and reuse composition (r127; local only)

[Recorded qualification](out/property-template-r127.json) replaces ad hoc query
copying with typed owned constructors for the bounded synthetic ASCII property105
profile. Intended change, MERGE, exact output, membership and global identity
queries reproduce all five successful r123 native statements byte-for-byte,
including the actual native history text. Source/type/ID and both typed endpoints
remain unchanged in the intended/output20-field checks; UTF8 text comparisons
preserve lexical carrier/retained values. Table names and synthetic tokens reject
unsupported syntax; versions, entity versions and cursor xid reject booleans,
floats, overflow and stale outputs. New batch identity is distinct from predecessor.

The composed proof is derived from owned templates rather than per-fixture query
hash substitution. It binds an exact prior full structural baseline, immutable
stage0, successful matching native queries, unique membership, full intended/
output checks and closed MERGE lineage. Proof mode is explicitly serialized
synthetic-property105; production and other source profiles fail closed.
Unknown source semantics, real completeness/fencing and outside-membership
changes without a closed owned apply are not qualified. Caller evidence must
come from trusted native capture; this does not authenticate arbitrary supplied
history dictionaries. Structural revision is independent of entity_version.

Five local tests pass: native equivalence,14 invalid parameter/output cases,
actual composition and12 missing/altered boundary/check cases. No Databricks
queries, writes or new compute run in this iteration. The recorded next r128
input/apply SQL is compiled only; stage creation, durable raw/journal capture,
actual output version/lineage, validation and post-run full structural oracle
remain execution obligations. Do not infer a performance saving from local
proof composition. Warm/freshness, real producer, external engines and1B/5B
requirements remain open.

## Integrated owned-query structural reuse publication (r128)

[Final audit](out/native/ashlar_isolation_r128/audited-summary.json) records one
actual100k property publication using owned intended/apply/output/identity
queries and the composed structural reuse proof. Its inherited baseline is
r123's full20M E18↔forward1 parity, bound to exact native query text/ID. Immutable
stage0 membership, all20 intended/output fields and closed owned MERGE lineage
prove reuse before descriptor activation. Raw/journal capture and exact validation
remain required and finish before canonical apply. No structural fields change.
Actual r128-b1 vector: E19/N0/R15/J17/T0/forward r120 v1.

Ready-input→verified-descriptor60.874s, modeled oldest freshness70.874s under the
10second uniform admission window. Actual input preparation13.820s is excluded
from that clock; this is not a real producer arrival trace. Parallel appends
24.486s, parallel validations12.674s, canonical apply7.095s, composed proof with
native history retrieval0.709s. Compared with r123's62.943s processing, the
single-sample difference2.069s is not a strict causal or p95 estimate. Even
ready-input latency remains above60seconds; oldest-record comparison also fails.

The independent full20M structural EXCEPT ALL oracle runs after descriptor
verification and passes in6.427s, explicitly outside the publication clock.
Do not claim activation gated on that oracle. It corroborates the composed
proof's decision for this closed synthetic apply. Thirty exact20-field singleton
checks are final uncached, remote bytes0, p95 engine115ms/caller394.57ms and
files15. Both warm gates remain missed. No concurrent reader load, new compute
or repeated expensive inherited-history post-audit was run.

This qualifies owned-query proof composition in the bounded serialized synthetic
publisher, not production completeness/fencing, general source semantics or
billion-scale admission. All20 affected carriers, exact raw/journal slices,
global identities, owned apply lineage and independent structural parity pass;
untouched full-carrier payload equality is not independently rerun here.
Shared warehouse dollar attribution is unknown. Current anchors advance to
E19/R15/J17; future execution must use the actual r128 predecessor.

Further physical tuning should target actual canonical write/file behavior and
raw/journal append costs rather than more copies of redundant validators.
Keep the16MiB narrow forward candidate distinct from canonical hash clustering
and64MiB wide-row tuning. Performance comparisons, real source authority,
external activation, reverse/degree coverage and1B/5Badmission remain open.


Eligibility-statistics screen (r129/r130): [audited native measurements](out/native/ashlar_write_pruning_r130/audited-summary.json)
compare three balanced eligible-key joins against canonical E19 and an isolated
shallow clone with additional entity_version/apply_batch_id skipping statistics.
Each returns exactly100k keys. Total query files fall535→19, bytes416–447MB→13.4MB,
and engine time491–635ms→243–294ms; result cache is disabled and remote bytes0.
These totals include the stage and are not target-only file counts. The full20M
key/filepath/row-index comparison passes, assuming stable logical schema and
Delta immutable files; no fresh wide-payload comparison or payload rewrite.
Statistics recomputation costs23.694s caller time and reads1.351GB, outside any
publication clock. Canonical tables and publication remain unchanged.

Read-only r129 metadata confirms configured Zstd on edge_current, source_record
and property_journal; this does not independently verify historical file codecs.
Edge_current has532 files/34.280GB, source_record74/8.217GB, journal72/7.672GB.
The SQL warehouse/Unity Catalog targetFileSize setting controls OPTIMIZE;
Databricks' documented clustering-on-write operation list does not include
MERGE. Sources: [file-size controls](https://learn.microsoft.com/en-us/azure/databricks/tables/tune-file-size)
and [liquid clustering](https://learn.microsoft.com/en-us/azure/databricks/tables/clustering),
consulted2026-10-06. Do not infer MERGE write sizing from the64MiB setting.

Next: qualify an actual update-only MERGE with eligibility predicates in ON and
these statistics, including exact full-carrier and unchanged-membership checks,
before changing the owned publisher templates or canonical settings. This SELECT
screen does not qualify actual MERGE latency, mixed-revision production admission,
publication p95, singleton SLOs or billion-scale performance. UC Delta remains the
architecture; performance measurements inform tuning, not an architecture veto.


Actual update-only MERGE pruning: [r131 audit](out/native/ashlar_merge_pruning_r131/audited-summary.json)
compares two original canonical E18 shallow clones using the same100k r128 stage.
Control retains eligibility in WHEN MATCHED; candidate adds prior-version/batch
statistics and moves eligibility into ON, with no INSERT clause. Total query
reads554→38 files,2.023GB→358.444MB; engine6.814→4.184s and caller7.461→4.575s.
The candidate's one-time statistics recomputation costs22.267s caller/1.351GB read,
separate from MERGE. Counts include stage reads, not exclusively target files.
All measured queries are final and result-uncached; candidate MERGE remote reads
1249B, control0; no spill. This is one sequential pair, changing both statistics
and predicate placement, not a factorial attribution or p95 estimate.

Both MERGEs update exactly100k rows with zero insert/delete/copied rows. Exact
20-field stage/output comparison passes; global20M IDs and100k new-version rows
pass. All19.9M untouched logical keys/filepaths/row indices are identical under
stable schema and Delta immutable-file assumptions; no fresh wide untouched
payload comparison. Canonical E19/publication remain unchanged. Intended checks
still read7.512GB in both paths, so pruning does not eliminate validation costs.

Next: qualify mixed eligible/ineligible/unmatched input semantics on a bounded
native control before adapting owned templates and the integrated publisher.
Account separately for initial statistics maintenance and ongoing write stats.
No production admission, publication p95, read SLO or billion-scale qualification
is established by this pair. Performance targets remain open.


Mixed eligibility control: [r132 audited native evidence](out/native/ashlar_merge_semantics_r132/audited-summary.json)
passes five exact checks comparing both update-only predicate placements across
8 unique inputs/7 existing rows. Only eligible id1 updates; stale version, wrong
predecessor, null eligibility/hash, mismatched compound identity and unmatched
ID sharing another hash remain untouched. Full20-field expected output and paired
parity pass. Synthetic nullable fields exercise SQL three-valued logic beyond
canonical NOT NULL constraints; this is not multiwriter/fencing admission.

Owned PropertyApply now accepts explicit eligibility_placement='on', with the
historical 'matched' default preserved. The new option reproduces the r131 native
MERGE byte-for-byte and therefore binds through the existing exact-SQL proof
interface. Six template/proof unit tests pass, including historical equivalence,
new native-query equivalence and invalid placement rejection. No canonical write
or new publication occurred. Next integrate the opt-in template and statistics
into the publisher, measuring maintenance separately and preserving raw/journal,
exact carrier and structural proof requirements. All performance gates remain
unproven.


Integrated pruned publication: [r133 audited evidence](out/native/ashlar_isolation_r133/audited-summary.json)
adds eligibility statistics to canonical edge_current (maintenance E19→E21), then
publishes100k changed entities with owned ON eligibility and explicit adjacency
reuse. Descriptor r133-b1 pins E22/N0/R16/J18/T0/forward1. Statistics setup23.179s,
stage preparation7.281s and full20M baseline6.249s occur before the publisher clock
and are recorded separately; no claim that setup is free or needed every batch.
Exact20-field intended/output carriers, raw bytes/digests/UTC instant, property
journal, global IDs and composed owned lineage proof pass before publication.
Independent full20M structural parity passes afterward6.221s outside that clock.

Ready-input→verified descriptor52.952s; oldest modeled arrival62.952s for the10s
uniform10k/s admission window. This misses the60s oldest-record target and is a
single synthetic batch, not publication p95 or sustained10k/s/burst admission.
The observed ready-input interval improves from r12860.874s, without isolating
all timing causes across runs. Parallel raw/journal append23.997s and validation
10.905s remain major costs. Actual MERGE4.230s caller/3.943s engine,40 total read
files/359.506MB,16 output files/327.002MB,100k updated rows. New writes retain
configured skipping statistics; no statistics recompute runs between apply and
output validation. Initial maintenance and steady-write costs must stay distinct.

Thirty exact fresh20-field singleton reads: warm p95 engine107ms/caller372.50ms,
15 files and no remote reads, final result-uncached. Both singleton gates remain
missed. No untouched wide-payload comparison, real producer completeness/fencing,
concurrent read admission, sustained arrival throughput, cold-data p95 or1B/5B
qualification follows from this experiment. UC Delta remains chosen architecture.
Next investigate measured raw/journal append cost and throughput; shrinking an
admission batch alone would not prove sustained10k/s when service time exceeds
its arrival window. Preserve exact record and property-history requirements.


Stored-carrier append isolation: [r134 native audit](out/native/ashlar_append_layout_r134/audited-summary.json)
replays the same100k captured r133 raw and journal rows from immutable R16/J18 into
four scratch tables, both Zstd. Raw3-key/journal4-key liquid-clustered parallel
pair8.791s versus unclustered4.665s. Raw caller8.790→4.663s, journal7.859→3.943s;
engine8.333→4.381s and7.350→3.510s respectively. Same source-read bytes in each
comparison: raw685.206MB/79 files, journal641.312MB/5 files. No spill, all measured
queries final and result-uncached. Clustered outputs6 files each versus8 each
unclustered. Total four-output bytes2,589,507,841; no resource resize/new compute.

Complete symmetric UTF8-byte-normalized comparisons pass for every raw/journal
field and both layouts,100k rows each. Exact-check caller costs19.618/17.462s
clustered and19.935/17.154s unclustered, separate from append intervals. Only one
sequential pair per layout; empty scratch tables are not the growing production
history layout. Already-serialized input excludes source serialization. The
observed8.791s clustered replay versus23.997s r133 capture pair motivates isolating
encoding/input-layout work, not assigning the entire difference to serialization.
Do not infer steady throughput, publication p95, or billions of history rows.

[Cleanup evidence](out/native/ashlar_append_layout_r134/cleanup/summary.json)
records identity-checked drops of all four owned scratch tables. UC retention
still applies; no immediate storage reclamation/VACUUM claim. Canonical E22,
R16/J18 and descriptor r133-b1 remain unchanged. Next measure exact serialization
cost separately and test deferred clustering against history-read pruning and
maintenance costs before changing canonical raw/journal layout. Native singleton,
publication freshness/rate, concurrency and1B/5B admission gates remain open.


Serialization screen: [r135 final native audit](out/native/ashlar_serialization_r135/audited-summary.json)
forces consumption of every full wire byte using100k count/byte-length/digest
aggregates in three balanced pairs. Encoding stage0 engine877–1182ms versus
stored R16 payload591–626ms; caller1126–1491ms versus822–864ms. All aggregates
agree; independent exact UTF8 lexical comparison passes4.698s caller, not merely
digest equality. Result cache disabled, remote bytes0. Encoding reads4 files/
665.733MB; stored bytes79 files/672.444MB, so different input layout prevents exact
CPU attribution. These timings do not explain the full23.997s r133 capture pair;
pre-encoding is not yet justified as the primary fix. No new data/table writes.

[Read-only write plans](out/native/ashlar_write_plans_r136/summary.json) capture
raw serialization INSERT, journal INSERT and stored raw replay through EXPLAIN
only. All three expose AppendDataExecV1 command wrappers and report unsupported
Photon wrappers; internal sort/shuffle/write stages are absent. Do not infer
that all runtime tasks fall back or attribute latency from this incomplete plan.
All three EXPLAIN histories final; no INSERT execution occurred.

Qualification correction for r134: inspection of its saved DESCRIBE DETAIL
shows clustered scratch tables include rowTracking, while unclustered scratch
tables do not. Both retain deletionVectors/v2Checkpoint/Zstd. Therefore the
8.791→4.665s pair changes a feature bundle, not clustering alone. Historical
exact raw/journal preservation remains valid; performance attribution to liquid
clustering alone is unqualified. Next replay with row tracking held constant,
then investigate constraints/growing-table versus empty-table write costs and
history-read pruning before altering canonical layout. E22/R16/J18/r133-b1 remain
unchanged. No performance gate or billion-scale admission is established here.

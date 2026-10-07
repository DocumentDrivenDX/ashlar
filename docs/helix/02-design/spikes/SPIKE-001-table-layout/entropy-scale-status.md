# Higher-entropy scale and GraphFrames iteration

## r167–r169: full20M exact copy preservation proved

[Audited parity](out/native/ashlar_bucket_parity_r169/audited-summary.json) proves all20 logical edge fields across the complete20M-row owned bucket copy at version4 equal canonical E23. All11 text columns use null-safe UTF8 binary equality, including property/retained JSON and raw cursor text; numeric and timestamp columns use native typed equality. Four disjoint hash ranges cover4,999,762/5,003,588/4,999,471/4,997,179 edges with zero mismatches. Both snapshots independently have20M nonnull globally unique IDs. The first range uses a full outer join; the remaining inner joins require the exact expected joined count, which together with uniqueness proves exhaustive one-to-one membership, while all logical identity components are still compared. This is direct field equality, not sampled or digest-only parity.

The four comparisons take95.086/95.119/90.552/90.434 caller seconds. Total validation/recovery/audit reads89.987GB, remote writes0 and spill0 remain within the100GB read admission bound. All native IDs are terminal FINISHED with finalized uncached metrics. One unchanged and25 changed native constant controls pass, including NFC/NFD byte differences, JSON whitespace, retained/cursor integers above2^53, null and a timestamp1us change; two additional membership/duplicate counterexamples show why table counts alone are insufficient.

The r167/r168 controllers stopped after successful results while waiting for the last query's metric-final flag; no successful comparison was replayed. Installed connector source shows fetchall retains the active result until the next execute or cursor close. Explicit public cursor close/new cursor on the same session in r169 allowed final metrics to be inspected before further admission. Native result_fetch telemetry was5,875/61,659ms on the retained-result runs and302/303ms with explicit closure. This supports the lifecycle explanation but is not randomized causal isolation or caller fetch latency, and gives no singleton-SLO improvement claim. Preserve the stopped summaries and same-ID recovery evidence.

This resolves the initial full-wide-copy obligation specifically for owned UUID99831c85-f437-4e6f-8b8b-eaec9ab300d2/version4 and E23. The candidate is retained for the next physical comparison; r139-b1 publication is unchanged. It does not prove a later maintenance/update version, alter canonical DDL/constraints, select64 as a1B/5B bucket count, or admit source authority/fencing, external engines, cold reads or sustained freshness. Next: bound and run partition-local ZORDER, then matched update/singleton comparisons. UMF binding and existing graph-engine limits remain unchanged.

## r170–r175: full20M bucket maintenance and point-read comparison

[Comparison evidence](out/native/ashlar_bucket_full_reads_r175/comparison-summary.json) records four alternating30-key/60-query cohorts on canonical E23 and the owned full20M bucket copy. Every read returns the full20 carriers exactly. Before maintenance, bucket4 initial/repeated all-cohort caller p95 is818.284/493.483ms and engine p95 is593/232ms, with28/5 remote queries and2 files at p95. Canonical E23 has18 files at p95 and a different DV/hot-file history; this is an operational comparison, not causal isolation of partitioning.

Four partition-local ZORDER commands actually rewrite all528 input files/33,157,013,686 compressed bytes into448 files/33,156,068,142 bytes at owned8. Each16-bucket group takes75.903/77.901/79.137/79.839s. The64MiB best-effort target yields roughly74MB mean output files, not guaranteed64MiB files. Native commit IDs, before/after versions and64-partition coverage are audited in [maintenance evidence](out/native/ashlar_bucket_zorder_r172/audited-summary.json). The planned36GB new-write limit holds; the measured table footprint justified recording the100GB revised cumulative failed-build/copy/maintenance planning ceiling before mutation. OPTIMIZE history reports zero read bytes here despite rewriting33GB; input file footprint is recorded separately, and actual physical maintenance reads/read amplification are not independently measured. The reported0.939GB reads are check queries, not total maintenance IO.

After ZORDER, bucket8 initial/repeated all-cohort caller p95 is783.702/416.783ms and engine p95 is539/151ms, with29/2 remote queries and1 file/77.75MB at p95. Applying the same no-remote subset definition to every phase gives before-repeat n25 engine107/caller358.030ms and after-repeat n28 engine99/caller416.783ms. This is a scoped warm-engine observation near the100ms target; the caller250ms target remains missed. Remote samples also contain cached bytes, so neither initial phase proves a controlled cold-data gate. Sequential cache/load and canonical compilation variation prevent attributing timing differences solely to ZORDER. All240 queries are exact with finalized uncached native metrics; combined reader-cohort reads56.606GB, writes0 and spill0.

Input4 has full20M20-field parity. Output8 has20M global IDs/bucket integrity and120 exact after-maintenance point reads, but its exhaustive wide-value preservation obligation remains open. The candidate is retained unpublished at the same UUID for matched100k high-entropy updates and bounded full-wide final-state validation. Canonical r139-b1 pins remain unchanged; no DDL choice, source authority, graph-engine activation, sustained/burst publication or1B/5B admission changes.

## r176/r177: full20M wide update shows material bucket write amplification

[Audited updates](out/native/ashlar_bucket_update_r176/audited-summary.json) apply the same100k stage of same-length high-entropy property105 replacements to an owned shallow LC clone of E23 and the maintained full20M bucket8. Both complete intended/output20-field checks pass, including UTF8 text, exact retained/cursor data and typed endpoints. Both final snapshots have20M globally unique IDs; each native MERGE updates exactly100k with0 inserted/deleted. Stage length drift is0 and canonical r139-b1 still pins E23 and its original vector. LC clone is retained at1, bucket at9 and stage at0, unpublished.

LC MERGE takes4.110s and adds326,515,247 bytes, removes16 hot files/adds6 and copies0 unchanged rows. Bucket MERGE takes37.018s and adds5,253,739,757 bytes, copies2,987,212 unchanged rows, removes67 files/adds69 and creates381 deletion vectors. Its output contains3,087,212 rows. This is measured mixed DV/copy behavior despite DV being enabled, not a guarantee of changed-row-only writes or an attributed runtime mechanism. File histories and consolidation differ, so these are operational layout results, not an isolated causal bucket-count experiment.

Validation/staging remain separate costs: the bucket pre-apply complete-carrier check alone takes40.728s and reports33.807GB reads. The total completed run/audit reports77.071GB reads,6.244GB writes and0 spill, exceeding the75GB/4GB admission plan after the bucket update/checks. The local guard stopped before any point-read phase; every submitted native query succeeded and finalized, and no write was replayed. The cap was not an in-flight physical IO limit. Preserve the stopped checkpoint and read-only native-ID/UUID/version audit rather than relabel it a passing budget run.

This strengthens the case for retaining canonical hash liquid clustering while the bucket alternative remains experimental. Do not infer publisher freshness or sustained10k/s from either MERGE: no raw/history/manifest cycle ran. Next validate LC19.9M immutable custody and the complete final bucket values against E23 plus stage0. Whole19.9M physical-row custody cannot hold for bucket9 because nearly3M unchanged rows were physically copied; it needs exhaustive value-level proof, also covering the preceding ZORDER. Caller/cold/scale/source/fencing/engine limits remain open; UMF is deferred.

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


Matched-row-tracking append comparison: [r137 native audit](out/native/ashlar_append_layout_r137/audited-summary.json)
explicitly enables row tracking on both Zstd layouts and preflights tracking plus
cluster keys before writing. Same immutable100k R16/J18 inputs, one sequential
parallel pair each: clustered8.743s versus unclustered4.379s. Raw caller8.741→4.378s,
journal7.438→3.999s; engine8.262→4.087s and6.842→3.490s. Same source read bytes
per role, no spill, final result-uncached. Outputs6 versus8 files per role; full
symmetric UTF8-exact raw/journal comparison passes100k rows per output. This
removes the r134 tracking mismatch, but remains an empty-table replay, not a
production growing-table or sustained-rate estimate.

[Twenty exact full-row reads](out/native/ashlar_append_reads_r138/audited-summary.json)
on those same tables use five SHA-ranked immutable keys per role, alternating
layouts. Clustered reads1 file/89–123MB; unclustered8 files, raw657.716MB and
journal640.251MB. Raw engine123–150ms clustered versus118–139ms unclustered;
journal120–388ms versus113–131ms. Small remote reads occur on three clustered
journal reads (100/48,939/11,379B); all other read remote bytes0. Cached storage
and a journal latency outlier preclude a cold-read or general latency benefit
claim. These are history/raw lookups, not canonical singleton SLO evidence.
Replay source ordering and100k-row scale limit extrapolation. The pruning loss
makes removing clustering globally premature despite faster scratch append.

[Cleanup](out/native/ashlar_append_layout_r137/cleanup/summary.json) drops all four
identity-checked owned scratch tables; platform retention applies, no VACUUM or
immediate physical reclamation claim. Canonical E22/R16/J18/r133-b1 unchanged.
Retain raw/journal clustering in the candidate layout. Next test materialized
exact write inputs with preparation counted inside publication timing: stored
replay is faster than direct computed capture, but the net materialization cost
must be measured rather than moved outside the clock. Preserve exact carriers,
history and origin checks. All native singleton/freshness/rate/concurrency and
1B/5B admission gates remain open.


Materialized-write publication: [r139 final audit](out/native/ashlar_isolation_r139/audited-summary.json)
publishes100k synthetic property changes using immutable raw/journal CTAS inputs,
with materialization counted inside the publication clock. Descriptor r139-b1
pins E23/N0/R17/J19/T0/forward1. Initial eligibility statistics reused; prepared
source stage7.319s and full20M structural baseline6.969s before that clock remain
recorded separately. Exact20-field affected carriers, lexical patch, raw bytes/
digests/origins/UTC instant, property journal/global IDs and owned structural
reuse proof pass before publication; independent full20M parity passes6.235s
after publication, outside the clock. Canonical raw/journal clustering retained.

Materialization pair10.123s plus append8.351s =18.474s versus r133 direct pair
23.997s, but validation16.124s versus10.905s erases the phase saving. Complete
ready-input interval53.452s versus52.952s; oldest modeled arrival63.452s, still
above60s. One batch/nonmatched runtime variability does not establish a causal
regression, p95, sustained10k/s or100k/s burst. Do not promote this materialization
candidate based on faster append alone; it adds intermediate storage/I/O without
an observed complete-clock benefit. Keep direct pruned publication as the current
execution candidate, retaining this experiment for reproduction.

Thirty exact full20-field fresh reads: p95 engine156ms/caller410.65ms,15 files;
five queries have remote reads. This is a mixed-cache post-publication sample,
not a wholly warm or controlled cold-data distribution. Targets remain unmet or
unproven; no billion-scale admission. [Cleanup](out/native/ashlar_isolation_r139/write-input-cleanup/summary.json)
drops two owned100k intermediates after confirming version0, membership and
absence from the published version vector. Retention applies; no VACUUM/immediate
physical reclamation claim. Next address scattered post-MERGE files for native
singleton reads and investigate bounded physical maintenance/input layout rather
than repeatedly adding capture stages. Preserve property history and exact values.


Incremental singleton maintenance: [r140 final audit](out/native/ashlar_singleton_optimize_r140/audited-summary.json)
uses an isolated original E23 shallow clone (before0/after2). One OPTIMIZE command
creates a rewrite and a no-op OPTIMIZE commit; retain both in r141 delta history.
It removes16 files/326.999747MB and adds4/326.482281MB, total532→520 files;
caller13.340s/engine11.741s,658.854MB read. No FULL rewrite, source E23 unchanged.
Thirty SHA-ranked affected full20-field reads before: engine p95124ms/caller
422.70ms,18 files/414.852MB, remote0. Immediately after:574ms/832.02ms,3 files/
160.379MB, five remote queries. This mixed-cache result cannot establish a warm
latency benefit or controlled cold-data admission.

Full20M keyed SHA256 wire comparison times out180s after96.497GB read, and remains
failed evidence. It is not retried or counted as passing preservation. [r141
proof](out/native/ashlar_singleton_custody_r141/summary.json) instead passes exact
UTF8-normalized equality for all100k rewritten20-field carriers and19.9M untouched
key/filepath/row-index custody, plus20M globally unique IDs. Custody relies on
stable schema and immutable Delta-file semantics, not fresh wide-payload equality.
Bounded diagnostics must not imply the failed full digest passed.

[Warm repeat r142](out/native/ashlar_singleton_warm_r142/audited-summary.json)
checks the same30 exact keys after rewritten-carrier verification warmed files:
p95 engine101ms/caller363.10ms,3 files/163.869MB, remote0, final result-uncached.
Both agreed warm gates still missed; this is one finite repeat, no general
concurrency or billion admission. Maintenance improves file/byte pruning but
adds13.340s work and initially introduces cold rewritten files; no integrated
publication freshness claim. Treat it as a tuning candidate, not an admitted
operational schedule. Next isolate query compile/client overhead and assess
post-MERGE write distribution before adding maintenance to every publication.
[Cleanup](out/native/ashlar_singleton_optimize_r140/cleanup/summary.json) drops
only the identity-verified owned clone; source/publication r139-b1 remains pinned
E23/N0/R17/J19/T0/forward1. Platform retention applies; no VACUUM claim. All full
goal performance/scale requirements remain open.


Persistent-client/compile floor: [r143 final audit](out/native/ashlar_client_floor_r143/audited-summary.json)
passes30 exact SELECT1 queries on one connection, result cache disabled, final
histories. Caller p95154.13ms; compile29ms, engine19ms, server total54ms. Paired
caller-minus-server p95106.13ms. Historic optimized r142 full-carrier singleton:
caller363.10ms, compile145ms, engine101ms, server259ms; paired caller-minus-server
111.56ms and caller-minus-execution257.12ms. Residuals computed per query before
ranking; never add or subtract separately ranked p95s. Outside-server residual
includes transport/client and metric-boundary differences, not just network;
caller-minus-execution is diagnostic, not a zero-engine latency prediction.

This small control shows client/compile work must be addressed alongside file
pruning; it does not qualify singleton performance. SELECT1 differs in plan and
payload from20-field Delta queries. Existing workstation→Central US endpoint
measurement is the recorded caller context; a service colocated with the warehouse
is an unmeasured deployment alternative, not an inferred faster result or a
changed250ms target. Preserve current gates while recording caller placement,
compilation and parameterized-query behavior as explicit tuning dimensions.
No new tables, writes, compute, publication or scale admission. E23/R17/J19 and
r139-b1 unchanged. Next test bounded query/plan choices with exact identity and
publication pinning retained, and post-MERGE distribution controls; do not treat
a scalar query or relaxed pinning as the native singleton architecture.


Typed parameter partial experiment: [r144 final audit](out/native/ashlar_typed_reads_r144/audited-summary.json)
retains E23 snapshot/hash/source/Rel/id predicates and full20-field carriers,
alternating decimal-text CAST versus Python integer parameters. Only12 pairs
complete (24 exact carrier matches), all result-uncached/remote0. Descriptive
completed-query nearest-rank p95/max: CAST caller469.78ms/engine126ms/compile254ms;
typed366.23ms/116ms/155ms,18 files each. These exclude the stalled25th request,
so cannot be used as latency admission or a completed30-key comparison. No
signed64 boundary or general connector precision admission; retain current casts.

Server-only pending cast-12 query01f1c1fb-304e-1e2b-bb03-de1d807c805f was FINISHED/
final in239ms while the client stayed stalled for several minutes. A separate
history observation also initially waited; basic workspace HTTP reachability303
was insufficient to establish authenticated SQL health. [Termination evidence](out/native/ashlar_typed_reads_r144/termination.json)
records stopping only identity-verified owned Python PID8866 after the authoritative
server terminal observation (exec session79617 exit143). No blind restart, duplicate
SQL, native write or assertion of the undelivered result. Exact culprit within
client/gateway/response handling remains unknown; this is not slow Delta execution.

This adds a transport reliability requirement to the next bounded client experiment:
finite request wall timeout, durable request correlation/server-query recovery,
and no re-execution solely on observation/response timeout. Current query builder
and agreed gates unchanged. No candidate is promoted from this incomplete sample.
E23/N0/R17/J19/T0/forward1 and r139-b1 unchanged; no scratch data or new compute.
All full goal performance/scale obligations remain open. Continue physical write
and native-read tuning while preserving caller-layer failures in end-to-end evidence.


Bounded correlated reader: [r145 final audit](out/native/ashlar_bounded_reads_r145/audited-summary.json)
runs an owned read-only subprocess with60s total worker wall bound (including
connect/auth),10s configured sockets, one connector request attempt,10s retry
budget/no redirects and15s SQL statement timeout. Unique run/label SQL correlation
and parameters are persisted before submission; no generic timeout replay. Thirty
exact E23 pinned20-field reads pass in14.044s total worker time, all final result-
uncached/remote0. Caller p95397.28ms/engine159ms/compile144ms,18 files. Gates still
missed; comments/control settings and runtime sample differ from previous runs,
so no causal overhead comparison or transport fault admission.

[Same-query status recovery](out/native/ashlar_bounded_reads_r145/recovery/summary.json)
uses history GET only to recover the previously undelivered r144 server ID as
FINISHED/final, without resubmission. Status recovery does not recover result
bytes or assert the lost carrier. The caller must fail closed on ambiguous request
correlation; new request labels identify pending work when a handle is unavailable.
Whole-worker timeout covers reads only, not subsequent SDK history audits.

[Offline timeout field control](out/native/ashlar_bounded_reads_r145/timeout-field-control.json)
confirms installed HTTP timeout setter10000ms propagates to10s pool timeout. Base
and subclass share class name THttpClient/private field; an initial separate-field
concern was resolved, not a discovered timeout bug. Installed source fingerprints
are recorded; controls are internal/version-specific, not general API guarantees.
No induced socket/auth/trickle-response fault was tested. The observed healthy run
therefore verifies acceptance/configuration and exact reads, not all timeout paths.
No table writes/new compute/publication; canonical E23/R17/J19/r139-b1 unchanged.
Next use bounded correlated reads for post-MERGE distribution experiments. Keep
native singleton, publication/rate, concurrency, cold-data and1B/5B gates open.


Post-MERGE source-distribution control: [r146 final audit](out/native/ashlar_merge_pruning_r146/audited-summary.json)
compares two original E22 clones, same100k stage r139 and inherited eligibility
statistics/ON predicates. Candidate adds REPARTITION_BY_RANGE(16,lookup_hash) to
the source SELECT only; no assertion that the writer preserves source ordering.
Both native MERGEs pass exact20-field affected values,100k updates/zero insert,
delete,copied rows,20M global identity and19.9M untouched immutable row custody.
No canonical or publication write. Custody assumes stable schema/immutable files.

Control MERGE caller4.021s/engine3.568s; hinted6.680s/6.127s. Actual changed-data
file groups6 versus16 (100k live rows each). Min/max hash spans: control0.490–
0.99996 of256-bit hash space, candidate0.99901–0.99996; this is measured range
coverage, not proof of internal sort ordering or hint preservation. All candidate
ranges remain nearly full-space and overlap, so source hint does not deliver the
intended selective file ranges in this experiment. Do not promote it to owned
publisher templates.

Two separately bounded correlated read children each pass30 exact pinned carriers
within60s process limits. Control p95 engine109ms/caller386.69ms,8 files; candidate
105ms/390.18ms,18 files, no remote reads, final result-uncached. Both gates missed;
one sequential pair cannot establish timing causality or general read distributions.
Full exact output validation3.542/3.453s and untouched custody13.024/13.083s are
separate diagnostic costs, not publication measurements. No source/burst/sustained
rate or billion-scale admission. [Cleanup](out/native/ashlar_merge_pruning_r146/cleanup/summary.json)
checks recorded latest MERGE statement identity/version and drops only both owned
clones. Canonical E23/R17/J19/r139-b1 unchanged; platform retention/no VACUUM.
Next compare physical key/maintenance choices with explicit ingest/read tradeoffs;
source partition hints alone did not solve post-MERGE file overlap. Retain the
chosen UC Delta architecture and current agreed gates; all full-goal obligations
remain open.


### r147 physical key screen and candidate DDL reconciliation

[r147 native audit](out/native/ashlar_key_screen_r147/audited-summary.json) compares
lookup_hash clustering with (source_system,id) on the same 100k complete carriers
from stage r139. Exact symmetric 20-field/UTF-8 carrier equality and unique IDs
pass for both tables. Both OPTIMIZE FULL commands were no-ops (version 0 retained,
zero rewritten bytes); the 16 MiB target did not cause a demonstrated rebuild.
Actual hash/source-id layouts contain 16/14 files and 335.655/335.319 MB.

Thirty alternating exact singleton reads per layout prune one file each. Hash
p95 engine/caller is 103/338.45 ms; source-id is 135/366.79 ms with two remote-read
queries versus zero for hash. Result cache is disabled. Neither comparison admits
the latency targets. This 100k screen does not establish full-scale source/type
diversity, incremental write behavior or billion-scale performance; retain hash
as the candidate. Both owned tables were identity/version checked and dropped;
canonical E23/R17/J19 and the r139-b1 publication remain unchanged.

The candidate DDL now includes edge entity_version/apply_batch_id statistics,
reflecting r130/r131/r133 evidence. CONTRACT-003 records the bounded evidence,
backfill cost, update-only eligibility scope and remaining obligations. Logical
schema and the 64 MiB reference target remain unchanged. The revised complete
13-table DDL has not been re-executed. Next resolve the publication maintenance
policy and service-side reader deployment using the existing large fixture;
retain UC Delta as selected architecture and keep ingest/read metrics explicit.


### r148 publication capacity and maintenance policy

The [reproducible model](out/publication-capacity-r148.json) derives a serialized
capacity sensitivity of 1,889 changed entities/s from the one r133 100k publication
(52.947 s processing). Holding this sample constant at modeled 10k/s arrivals
increases queue wait by 42.947 s per batch; uniform first-batch record-age p95 is
62.447 s. No sustained throughput, actual arrivals or service p95 is established.
A first 100k/s burst batch appears under 60 s in this model but subsequent queue
growth invalidates burst admission. Separate maintenance adds cost rather than
closing this capacity gap; its warmed reads still miss the agreed targets.

The [draft maintenance policy](publication-maintenance-policy.md) preserves pinned
publication vectors, distinguishes cleanup from source progress and requires
qualification before a maintenance-only manifest can be exposed. ADR-001 links
the policy and qualifies revised-DDL execution scope. Next run a bounded three-batch
queue diagnostic on isolated outputs, targeting publisher service before more
clustering screens. This iteration runs arithmetic locally and performs no native
write or compute provisioning. Full goal remains active and unproven.


### r149 native three-batch queue diagnostic

[Final native audit](out/native/ashlar_queue_r149/audited-summary.json) executes
three serialized 100k complete synthetic property batches on isolated E23/R17/J19
shallow clones, existing warehouse 2439e1f2e37ac563, runtime
19.8.x-aarch64-photon-scala2.13. Releases are modeled at 10/20/30 s (10k/s), not
real source arrivals. No readers, maintenance, resizing or new compute.

| Batch | Processing seconds | Queue wait seconds | Modeled record-age p95 seconds |
| --- | --- | --- | --- |
| 1 | 50.139 | 0.0001 | 59.639 |
| 2 | 54.654 | 40.507 | 104.661 |
| 3 | 49.974 | 85.567 | 145.041 |

Combined 300k uniform modeled per-record freshness p95 is 144.041 s. The oldest
record in batch 1 is 60.139 s old. Distinguish these quantiles from service p95,
which three samples do not establish. Input drained at 165.541 s after controller
epoch, 135.541 s after final modeled input completion. First-batch near-pass does
not admit sustained 10k/s or 100k/s burst; queue growth is actually observed under
the modeled release schedule, not just the prior fixed-cost arithmetic.

Exact20 affected fields and token changes, exact raw bytes/digests/origin/UTC
instant and property journal pass per batch. All20M unique IDs and owned structural
reuse proofs pass; independent full20M structural parity passes after the clock.
Final19.9M untouched physical custody passes under stable schema/immutable-file
assumptions, not fresh wide-payload equality or unknown-writer evidence. The second
and third batches count a full structural baseline scan inside processing; batch1
baseline is before the epoch. This extra proof cost is explicit, not attributed to
layout degradation. Prepared input is outside clock; real source staging remains
unmeasured. Source/history appends and validation are parallel separate lanes,
while logical publication is serialized. Carrier entropy remains synthetic.

Append pairs take13.696/10.670/10.596 s; raw/journal validation pairs take
18.358/19.221/11.707 s; MERGE caller3.953/3.767/3.540 s. Costs across preparation,
publication and final audit:122.642 GB recorded read_bytes and6.856 GB remote
writes (decimal). These counters are not distinct storage footprint or billing.
No whole-graph wide digest scan was repeated. Each statement is bounded180 s;
controller stops admitting SQL after600 s, not a guaranteed global process kill;
connector socket/retry bounds and durable submission correlation remain explicit.

[Cleanup](out/native/ashlar_queue_r149/cleanup/summary.json) UUID/version-checks
and drops all seven owned tables. Canonical E23/R17/J19/r139-b1 remains unchanged;
no VACUUM. The evidence prioritizes reducing publisher validation/capture service
cost with equivalent preservation proof over further singleton key changes.
Next compare one bounded precomputed-wire validation path against exact wire
checks, counting materialization inside publication; do not promote it without
measured end-to-end benefit and an explicit proof-strength comparison. The prior
r139 materialization regression remains relevant failed-candidate evidence.
UC Delta remains selected; sustained/burst/concurrent/cold/billion gates stay open.


### r150 exact wire witness screen: rejected

[Final native audit](out/native/ashlar_wire_validation_r150/audited-summary.json)
compares three alternating exact raw-wire validation pairs using immutable stage
r139 v0 and published source_record v17/r139-b1. An isolated100k-row Zstd wire
witness preserves complete20-field-plus-old_json UTC/microsecond encoding; full
symmetric exact witness comparison passes, as do100k unique delivery memberships
and all six exact raw UTF8/digest/origin validations. No canonical, raw, journal,
MERGE or publication write occurs; only one temporary witness is created.

Control caller5.220/5.030/6.216 s; stored5.436/4.844/5.107 s. Engine control
4.750/4.579/5.207 s; stored4.911/4.227/4.651 s. Control91files versus stored95;
control all remote0, first stored19.902MB remote, other stored remote0. Actual
witness8files673.355MB/reader3/writer7; source-stage and witness physical layouts
are different. Do not attribute timing differences to serialization alone.

Materialization7.473 s plus stored validation yields a hypothetical12.317–12.909 s
single-use cost versus5.030–6.216 s direct checks. Independent exact witness
verification12.774 s raises that sensitivity to25.091–25.683 s. These sums are
separate samples, not a measured publication. Digest does not replace exact raw
UTF8 comparison. No demonstrated benefit: reject this validation witness path;
retain the earlier r139 whole-publication materialization regression as evidence.

Total recorded reads11.515GB/remote writes0.673GB, not billable amounts or distinct
storage footprint. Each statement90 s; socket/retry bounded and submissions
correlated. [Cleanup](out/native/ashlar_wire_validation_r150/cleanup/summary.json)
UUID/version-checks and drops the witness; canonical E23/R17/J19/r139-b1 unchanged,
no VACUUM. All native metric IDs are final and result-uncached; three cached-data
pairs do not admit service/freshness/concurrency/billion targets.

Next screen explicit source-feed/epoch bounds in the raw validation query using
its existing source_feed/source_epoch/delivery_id statistics. Such bounds must be
proved from immutable stage membership and retain missing-row and exact-origin
refusals. This targets raw-file pruning without copying complete wire payloads or
weakening preservation proof. If it fails, do not keep adding per-batch copies.
UC Delta remains selected and the full performance goal remains active.


### r151 explicit raw origin bounds: pruning candidate

[Final native audit](out/native/ashlar_origin_validation_r151/audited-summary.json)
compares three alternating100k exact raw validations over stage r139 v0/source
records v17. A stage-origin GROUP BY proves one non-null feed/epoch and100k rows;
static feed/epoch filters supplement the existing apply_batch_id predicate.
Because every stage row has that exact origin and the join already requires it,
these filters preserve the logical match set. They do not replace delivery-ID,
missing-row, wire UTF8/digest/origin/UTC instant validation or global raw-membership
checks. Six exact comparisons pass. Two deliberately incorrect static origin
bounds each produce100k failures, confirming that missing rows are not suppressed.

Control caller6.148/4.885/5.408 s versus bounded4.919/4.470/5.095 s; engine
5.631/4.567/4.738 versus4.440/4.083/4.480 s. File reads91→12 in every pair;
read bytes1.353→1.323GB (decimal), all remote0/result-uncached. File-count gains
are much larger than byte savings; no assumption that payload serialization or
publication bottlenecks are solved. Three cached-data pairs do not establish
service p95, sustained rate, cold reads or timing causality. Stage-origin proof
cost is separate and must count inside any integrated publication comparison.

Read-only existing warehouse2439e1f2e37ac563; no materialization, mutations,
maintenance or compute changes. Canonical anchor E23/r139-b1 remains unchanged.
All phases plus refusal controls record8.057GB reads/zero remote writes, not
billing. Statements90 s, refusal controls30 s, bounded connector retries/socket
and durable correlation. Initial audit encountered finalization lag for two
FINISHED refusal-control IDs; [finalization note](out/native/ashlar_origin_validation_r151/audit-finalization-note.json)
records same-ID history refresh without SQL replay. Final histories all final.

The [executed bounded query](out/native/ashlar_origin_validation_r151/bounded-query.sql)
is a scoped owned candidate, not a generic consumer SQL API. Next incorporate
origin-proof validation into an owned query builder, rejecting unverified,
non-singleton or null origins, then count origin proof and exact validation in
one isolated publisher comparison. Preserve multi-origin fallback and real source
semantics; never assume all future batches share one epoch. This is the first
useful raw-validation pruning change in this sequence, but no agreed full gate is
admitted. UC Delta remains selected and the full goal remains active.


### r152 owned origin guard and integrated publication

The owned [raw validation helper](raw_validation_queries.py) consumes the exact
successful immutable stage0 origin query and requires one nonempty feed/epoch,
exact expected count and a native query ID. [Guard evidence](out/raw-validation-guard-check.json)
matches the executed r151 SQL after equivalent literal spelling and rejects11
adversarial query/version/result/count/null/multi-origin/different-stage cases.
The helper is inside a trusted controller; its records/dataclass are not an
untrusted authentication boundary or generic producer fencing. Unsupported
origin grouping is refused by this scoped helper; retain the prior unbounded-by-
origin exact validation as an explicit multi-origin fallback in future integration.

[Integrated native audit](out/native/ashlar_queue_r152/audited-summary.json) runs
one100k synthetic property publication on isolated E23/R17/J19 shallow clones,
existing warehouse2439e1f2e37ac563/runtime19.8.x-aarch64-photon-scala2.13.
Modeled10k/s release window is10 s; no actual source/rate or simultaneous readers.
Origin proof costs0.337 s inside the clock. Processing50.378 s, complete-input
queue wait0.00027 s, modeled record-age p95 59.878 s and oldest age60.378 s.
The first r149 batch took50.139 s: this single sequential comparison does not
show an end-to-end gain or establish a service/freshness p95 distribution.
A one-batch modeled near-pass cannot admit sustained10k/s or100k/s bursts.

Raw exact validation caller13.652 s/engine13.111,5files/1.474GB; journal symmetric
validation17.749/16.999 s,10files/2.885GB, keeps the parallel validation critical
path at17.753 s. MERGE4.182/3.775 s,40files/359.516MB. Reduced raw file scans do
not solve the journal validation cost. Exact20 affected fields/token patch,
raw origin/digest/UTF8/UTC instant and exact property journal pass, alongside20M
unique IDs and owned structural-reuse proof. Independent full20M structural
oracle and19.9M untouched physical custody pass after the clock under stable
schema/immutable-file assumptions. No fresh wide-payload equality claim for all
untouched rows, real producer completeness/fencing/ACK or billion admission.

Preparation/publication/final audit totals44.998GB recorded reads/2.285GB remote
writes, not billing or distinct storage footprint. Controller admits SQL for at
most600 s; per-statement180 s/socket/retry bounds remain explicit. Final history
IDs are all final. [Cleanup](out/native/ashlar_queue_r152/cleanup/summary.json)
UUID/version-checks and removes all five owned tables; canonical E23/R17/J19 and
r139-b1 remain unchanged. No new compute or VACUUM.

Keep guarded origin bounds as a scoped pruning option, with no claimed throughput
improvement. Next compare a single-pass exact journal comparison against the
symmetric EXCEPT query, preserving all columns, UTF8 equality, missing/null flags,
row multiplicity and membership refusals. Measure its own duplicate-key checks;
no digest substitution or moved validation clock. UC Delta remains selected and
all full-goal sustained/read/concurrent/cold/billion gates remain open.


### r153 exact journal multiset aggregation: rejected for latency

[Native audit](out/native/ashlar_journal_validation_r153/audited-summary.json)
compares three alternating100k journal validation pairs over immutable stage
r139 v0 and property journal v19/r139-b1. Candidate unions normalized expected
and actual rows with signed multiplicities, groups by all21 fields and refuses
nonzero balances. Every text field uses hex UTF8 bytes; timestamps, booleans,
numerics and nulls remain native. No digest or approximate equality substitutes
for full values. Zero imbalance means exact multiset equality within this bounded
200k-row profile; nonzero result counts differ from EXCEPT counts but both refuse.

Both paths pass exact100k equality. Eleven tiny typed native controls are refused
by both: duplicate/missing row, old-value null, missing flag, Unicode lexical
normalization difference, new value, native cursor, source epoch, microsecond
instant, event ordinal and property ID. Expected stage membership/uniqueness and
source authority remain separate publisher responsibilities; equal duplicated
expected/actual bags alone would not prove those invariants. This helper is owned
synthetic query code, not a generic SQL/authentication or source-contract API.

Control caller7.156/6.346/6.653 s versus candidate9.283/9.416/9.071 s; engine
6.076/5.560/5.894 versus8.504/8.727/8.403 s. Files24→12; reads2.602GB→
1.301–1.305GB; spill0 throughout. Remote bytes control0/0/40,076; candidate
18.409MB/0/3.927MB, so not strict all-warm paired admission. All result caches
disabled, final native IDs and balanced order. Do not infer internal CPU/shuffle
causality without a physical profile or extrapolate these pairs to service p95.
The lower scan volume does not produce a latency benefit here. Reject candidate
for publisher promotion; preserve the existing symmetric exact validator.

Existing warehouse2439e1f2e37ac563, same published synthetic fixture; no tables,
new compute, maintenance or publication writes. All comparison/refusal phases
record11.717GB reads/zero remote writes, not billing. Statements90 s and durable
submission/socket/retry limits remain explicit. No cleanup needed. A local syntax
error was corrected before any SQL submission; the native run was not replayed.

Next screen a full outer comparison keyed by unique journal delivery/property/
event identity, counting uniqueness proof and all21 exact fields. Multiplicity
refusal must survive; otherwise retain symmetric EXCEPT. Bound it to the same
read-only100k corpus before another integrated publication. The r15250 s service
and r149 queue-growth results remain unmet sustained capacity evidence; optimizing
scan counts alone does not meet the full goal. UC Delta remains selected.


### r154 keyed exact journal comparison: rejected

[Native audit](out/native/ashlar_journal_validation_r154/audited-summary.json)
compares three alternating100k journal pairs over stage r139 v0 and property
journal v19/r139-b1. Candidate full-outer joins feed/epoch/delivery/property/event
keys, compares all21 columns with UTF8-normalized texts and includes per-side
window counts for duplicate refusal. This is an owned unique-event synthetic
profile; it does not select native Truss event identity. The source-profile key
and authority requirements of CONTRACT-003 remain unqualified for real feeds.

All exact equality checks and11 typed native corruption refusals pass on both
paths. Candidate also refuses equal duplicated expected/actual bags: intentionally
stricter than multiset equality because this profile requires unique events.
Missing-row detection uses the non-null window count, not nullable value columns.
Nulls, missing flags, Unicode lexical differences, native cursor/origin, exact
microseconds, event ordinal/property identity and changed values stay distinguishable.

Control caller6.618/6.673/6.589 s versus candidate11.389/11.497/11.999 s; engine
5.941/5.924/5.870 versus10.635/10.658/11.318 s. Files24→12, reads2.602–2.606GB→
1.301–1.305GB, spill0. Remote bytes control0/0/18.443MB versus candidate1.969MB/
0/3.961MB. Balanced order, final native IDs, result cache disabled; this is not
strict all-warm service-p95 or causal internal-profile evidence. The included
uniqueness checks halve scans but do not reduce latency. Reject publisher
promotion and retain symmetric exact EXCEPT validation.

Read-only existing warehouse2439e1f2e37ac563; no resources/tables or publication
changed. All phases11.719GB recorded reads/zero remote writes, not billing.
Statement90 s and bounded submission/socket/retry controls. No cleanup needed.
Preserve both r153/r154 failed alternatives rather than claiming fewer scans are
faster or changing the exact preservation obligation.

Next test three independent physical write lanes: current apply, raw append and
journal append, followed by the same complete exact checks and manifest barrier.
Only one logical publisher may own the fixture; consumers remain pinned to the
old manifest until all new vector members pass. A failed lane may leave unpublished
physical versions and requires inspection/recovery, never progress advancement or
blind mutation replay. Do not advertise native multi-table transactions or real
fencing from this experiment. Count all checks, history collection and manifest
confirmation in publication time. Keep the isolated100k/current20M bound and
existing compute; this addresses elapsed service without weakening validation.
The full goal and all sustained/read/concurrent/cold/billion gates remain open.


### r155 independent write overlap: bounded candidate, modest improvement

[Native audit](out/native/ashlar_queue_r155/audited-summary.json) runs one100k
property publication over isolated E23/R17/J19 shallow clones on existing
warehouse2439e1f2e37ac563/runtime19.8.x-aarch64-photon-scala2.13. Current MERGE,
raw append and journal append run through three separate client lanes; logical
publisher remains serialized. Exact pre-apply intent precedes all writes, and
all existing raw/history/current/identity/structural checks precede the manifest.
No native multi-table transaction, real feed fencing or recovery admission.

Processing48.761 s, modeled10 s input window, oldest record58.763 s and uniform
record-age p95 58.263 s. The prior r152 serial-apply publication took50.378 s;
this is one sequential comparison with different cache/load conditions and an
extra observer, not a causal win or measured freshness/service p95 distribution.
At100k batches, holding this sample constant still implies only about2,051
changed entities/s; sustained10k/s and100k/s burst requirements remain unproved.
Do not substitute first-batch latency for queue/rate admission.

Overlapped write barrier15.161 s: raw15.156, journal11.690 and MERGE7.826 s caller.
Prior serial-apply MERGE4.182 s: overlap also slows individual operations. Exact
raw/journal validation barrier17.541 s remains dominant; the saved clock time is
modest. Full20 affected fields/UTF8 exact values/raw digest and origin/native
cursor/property journal/20M IDs and owned structural proof pass. Independent
full20M structural parity and19.9M unchanged physical custody pass after the
publication clock under stable schema/immutable-file assumptions.

A [read-only observer](out/native/ashlar_queue_r155/observer/summary.json) completes
before manifest submission and sees manifest count0,100k applied current rows and
100k rows retained at old pinned version0. This proves one unpublished physical
window and old snapshot retention, not full consumer concurrency or a failed-
lane recovery protocol. The final audit includes an explicit scope correction to
its inherited no-concurrent-reader wording; actual observer IDs/costs are retained.
All saved native IDs are final. Total preparation/publication/observer/final audit
48.941GB recorded reads/2.286GB remote writes, not distinct storage or billing.
Controller600 s admission deadline/per-statement180 s and connector submission
bounds remain explicit; no new compute/resize or whole-graph payload digest.

[Cleanup](out/native/ashlar_queue_r155/cleanup/summary.json) UUID/version-checks and
drops all five owned tables; canonical E23/R17/J19/r139-b1 remains unchanged.
Keep write overlap as an experimental option, not an admitted publisher default.
Next prove failed-lane manifest refusal on a much smaller isolated fixture, then
consider overlap of independent validation lanes without weakening any checks.
Do not repeat the20M custody scan for a tiny protocol control. Resource/rate,
singleton, cold/concurrent reads, external-runtime and1B/5B obligations stay open;
UC Delta architecture remains selected.


### r156 failed raw-lane publication refusal

[Final native audit](out/native/ashlar_failure_r156/audited-summary.json) uses a
1,000-edge property slice with isolated current/raw/journal/manifest/stage tables,
existing warehouse2439e1f2e37ac563. Three separate clients complete native MERGE,
raw append and journal append. Raw records deliberately append one whitespace byte
to each full wire payload and store the matching SHA256 of that corrupt value.
JSON parsing and digest consistency alone therefore cannot establish exact source
bytes. Exact origin-bounded raw validation reports all1,000 mismatches.

Current20-field output and property journal equality pass. Controller validation
refuses the new manifest before any submission; saved records contain no
publish-new statement. The complete old manifest remains byte-for-byte equal,
and all1,000 old pinned carriers match immutable stage r139 v0 with UTF8-normalized
strings. New physical rows stay unmanifested. CONTRACT-003 now links this evidence
and explicitly distinguishes validation refusal from crash/ambiguous transport,
durable receipt, recovery and producer-fence qualification.

This is a protocol control, not a performance screen, full graph or endpoint-
closure/billion admission. Existing preserved typed endpoints/current carriers
are used; no20M structural/custody scan is repeated. Source subset extraction and
old snapshot verification still read original large files: recorded reads18.007GB,
remote writes26.948MB across preparation/control/audit, not billing or distinct
storage footprint. Initial local syntax correction preceded all SQL; native
mutations were not replayed. Statements60 s, final old-snapshot check30 s,
connector socket/retry and durable correlation retained; all native IDs final.

Unpublished versions were recorded/inspected, then [cleanup](out/native/ashlar_failure_r156/cleanup/summary.json)
UUID/version-checks and drops all five owned tables. This is cleanup, not durable
recovery. Canonical E23/R17/J19/r139-b1 stays unchanged; no new compute or VACUUM.
Next test overlap of exact current/output validation with raw/journal validation
on the existing100k fixture, keeping all publication checks and costs inside the
barrier. Full throughput/singleton/cold/concurrent/external-runtime/1B/5B targets
remain open, and UC Delta architecture remains selected.


### r157 three validation lanes: scheduling tuning plateau

[Native final audit](out/native/ashlar_queue_r157/audited-summary.json) runs one
100k synthetic property publication on isolated E23/R17/J19 clones and existing
warehouse2439e1f2e37ac563/runtime19.8.x-aarch64-photon-scala2.13. Three write lanes
are followed by three exact validation lanes: current global identities/output,
raw full-wire checks and symmetric property journal checks. All native IDs, owned
query/lineage proofs and manifest checks are finalized inside publication timing;
no checks are omitted or deferred from the existing publisher barrier.

Processing47.981 s; uniform10 s modeled input window gives oldest57.983 s and
record-age p95 57.483 s. Previous r15548.761 s, r15250.378 s. These are separate
single batches, not a controlled sustained/service-p95 comparison or proof of
10k/s capacity. They show a scheduling plateau rather than the required roughly
fivefold service improvement. Raw validation20.840 s, journal19.396 s, current
identities4.247 s and exact output7.884 s; three-lane barrier20.845 s. Additional
concurrency hides work but also slows individual checks. No causal CPU/shuffle
claim without further profiling. Do not promote this variation as a default.

All20 affected fields/UTF8 values, exact raw origin/digest/UTC instant/native
cursor and property journal,20M global IDs and owned structural reuse pass.
Independent20M structural parity and19.9M unchanged physical custody pass after
timing, under stable schema/immutable Delta-file assumptions, not fresh wide
payload comparison for untouched rows or real feed authority/fencing/ACK. No
reader workload/controlled-cold/external runtime/billion admission. Preparation,
publication/final audit totals48.733GB recorded reads/2.286GB remote writes,
not billing. Controller600 s SQL-admission deadline/per-statement180 s and bounded
connector submission remain explicit. [Cleanup](out/native/ashlar_queue_r157/cleanup/summary.json)
UUID/version-checks and drops all five owned tables; canonical E23/R17/J19/r139-b1
unchanged; no new compute/resize/VACUUM.

Stop this sequence of small scheduling/validator substitutions. Next return to
physical design: screen a fixed64-bucket hash partition plus within-bucket Z-order
against the current hash-clustered candidate. Historical four-bucket partition
publication evidence motivates the comparison but uses a different10M fixture;
it cannot establish a current20M winner or a universal billion-scale bucket count.
First use100k complete existing carriers to verify bucket derivation, native DDL,
exact values and directory pruning with actual file sizes; this is a prerequisite
pilot, not scale admission. Keep logical Truss identity/typed endpoints unchanged
and treat bucket as derived physical metadata only. Count copy/Z-order/write and
point-read costs separately, preserve source proofs and reject bucket/hash drift.
Advance to the existing20M corpus only after the pilot and a justified byte/time
bound; avoid another unbounded whole-graph wire-digest query. UC Delta remains
selected and the full agreed performance/correctness goal stays active.


### r158 native64-bucket physical layout pilot

[Native audit](out/native/ashlar_bucket_screen_r158/audited-summary.json) copies
100k complete immutable stage r139 v0 carriers into hash-LC and64-bucket partition
alternatives, same Zstd/64MiB configured target/identity+eligibility statistics.
Partition metadata is first60 SHA256 bits modulo64 (safe signed64 conversion),
not logical identity. Every point lookup still filters full hash and native
source/relationship/id tuple. The [experimental DDL](sql/bucket64-layout-candidate.sql)
records the shape; selected canonical DDL remains hash-clustered.

Full20-field UTF8-normalized carrier equality,100k unique IDs, bucket/hash drift,
64 bucket membership and lowercase64-hex shape pass. Native boundary vectors
zero/max60-bit/leading-prefix values match Python modulo. Bucket populations
1,474–1,653. Both OPTIMIZE LC and partition ZORDER commands are no-ops with zero
rewritten bytes; both remain v0. This pilot establishes directory pruning, not
actual Z-order/file-resizing effectiveness. CTAS LC11.146 s versus partition6.814 s;
no claim that isolated copy timings predict producer ingest or maintenance.

Thirty alternating exact full-carrier lookups per layout: LC p95engine120ms/
caller394.16ms, partition93ms/381.61ms. Both read1file; p95bytes92.655MB versus
5.606MB; remote0/result-uncached. Data were warmed by full equality scans, so these
are not cold-data admission. Partition passes the engine screen only; both caller
gates fail. Actual LC4files335.574MB versus partition64files336.286MB. Configured
64MiB target did not constrain actual copy file sizes or make OPTIMIZE rewrite.

Both reader3/writer7 but feature bundles differ: LC clustering/domainMetadata/
rowTracking plus common DV/v2Checkpoint; partition lacks rowTracking in this
pilot. This is an explicit whole-layout comparison, not causal isolation of
partitioning alone or external-reader compatibility. Match feature settings in
future ingestion controls. Extra physical bucket also requires a bucket-aware
apply source/assignment policy; historical20-column UPDATE SET * helpers must
not silently target this21-column table without adaptation and drift checks.

All pilot/control phases6.733GB reads/0.672GB remote writes, not billing. Existing
warehouse2439e1f2e37ac563, statements90 s/bucket controls15 s, bounded connector
correlation/retries. [Cleanup](out/native/ashlar_bucket_screen_r158/cleanup/summary.json)
UUID/version-checks and drops both owned tables; canonical E23/R17/J19/r139-b1
unchanged; no new compute, resize or VACUUM. No full20M/billion, incremental MERGE,
publisher freshness, reader concurrency or graph-engine admission.

Next validate a bucket-aware update template and matching native feature settings
on the100k fixture before full20M copy. Preserve all carrier/origin/projection
proofs; then bound larger copy/maintenance/parity bytes and statement/controller
costs explicitly.64 is a pilot bucket count, not a chosen1B/5B partition strategy.
The smaller point-read bytes justify continuing this physical comparison, while
hash-LC remains the canonical candidate and all full-goal obligations stay open.


### r159 matched rowTracking and bucket-aware updates

[Native audit](out/native/ashlar_bucket_screen_r159/audited-summary.json) creates
two100k full-carrier layouts with rowTracking explicitly enabled, common DV/
Zstd/64MiB target/identity+eligibility statistics. LC retains its inherent
clustering/domainMetadata features. [Owned bucket apply](bucket_apply_queries.py)
derives source bucket from exact hash, matches bucket plus full native tuple and
prior entity-version/batch eligibility, then assigns all20 logical fields
explicitly. Bucket stays derived physical metadata. [Helper guard](out/bucket-apply-guard-check.json)
matches executed SQL byte-for-byte and refuses unsupported matched placement.
This remains trusted synthetic update-only code, not generic inserts or fencing.

Initial100k exact20-field UTF8 copies/unique IDs pass. Both100k property MERGEs
pass full intended/output values and post-update global membership; bucket drift
and lowercase hash shape checks pass. Both native details confirm rowTracking
true; zero/max60-bit and leading-prefix native bucket vectors match Python.
One stage0 generates explicit64-hex-character property105 replacement values,
much shorter than previous4–8KiB opaque values. Exact semantic replacement is
intentional, but this is compact-update correctness and physical screening,
not wide producer ingest or an equal-payload comparison with r158/r157.

LC MERGE caller3.019 s versus partition5.605 s. Thirty alternating exact point
reads: LC p95engine103ms/caller365.51ms/9.656MB; partition76ms/367.39ms/0.166MB,
1file each/remote0/result-uncached. Both caller gates fail; compact payloads and
prior full validation warm data, so no cold/full-scale or sustained admission.
Different layouts remain different physical bundles; no causal directory-only
or billing inference. No optimization/Z-order is run in this update pass.

The original harness completed all60 reads then stopped because canonical head
was25 instead of23. [Read-only recovery](bucket_updates_recover_r159.py) records
contiguous OPTIMIZE24/25 lineage and verifies old r139-b1 still pinsE23/R17/J19,
with20M pinned edges readable. No MERGE/copy or read workload was replayed. This
experiment performs no canonical mutation; external maintenance changed its head,
and its resource interference is an additional timing confound. Distinguish
current physical head25 from the immutable published snapshot23. Do not silently
repoint the manifest or assert the head stayed unchanged.

Pilot/preparation/control/lineage audit records5.592GB reads/1.030GB remote writes,
not billing or distinct storage footprint. Existing warehouse2439e1f2e37ac563;
statements90 s/lineage30 s/bucket controls15 s and bounded connector submissions.
All saved native IDs final. [Cleanup](out/native/ashlar_bucket_screen_r159/cleanup/summary.json)
UUID/version-checks and removes the two owned tables and stage; no VACUUM/new
compute/resize. PublishedE23/R17/J19/r139-b1 remains intact.

The update-path prerequisite is now evidenced, but retain hash-LC as canonical
candidate. Next compare equal-size wide property replacements and retain the
same rowTracking/eligibility/bucket checks before a full20M copy. Observe actual
canonical head/lineage and source snapshot rather than hard-coding unchanged
physical head; keep explicit published versions.64 buckets remain experimental,
not the chosen1B/5B layout. All full-goal obligations remain open.


### r160 same-length wide bucket updates

[Native audit](out/native/ashlar_bucket_screen_r160/audited-summary.json) repeats
100k full-carrier LC/64-bucket updates with high-entropy property105 replacements
matching every old length; native length-drift count0. Both rowTracking-enabled
layouts receive the same immutable input. Initial copies, full intended/output
20 fields/UTF8 and global unique membership pass; bucket/hash/shape/boundary
checks pass. This removes the compact-value caveat from r159 while retaining an
isolated hot-set scope, not a20M corpus or producer publication/rate claim.

MERGE caller LC3.585 s/partition6.609 s. Thirty alternating exact lookup p95:
LCengine105ms/caller355.15ms,6files/326.928MB; partition81ms/350.51ms,
1file/5.471MB. Remote0/result-uncached, data warmed by exact output validation.
Both caller gates fail. Partition passes this engine screen only. One sequential
MERGE pair cannot establish causal/general write performance or service p95.
Different inherent LC/partition features remain explicit despite matching
rowTracking/DV/compression/statistics. No OPTIMIZE/Z-order in this pass.

Post-update LC6files326.711MB versus partition64files327.464MB. Copy callers
10.857/6.040 s are setup costs. All pilot/control phases18.475GB reads/1.990GB
remote writes, not billing or distinct storage footprint. Existing warehouse
2439e1f2e37ac563; statements90 s/controls15 s, bounded durable submissions.
All saved native query metrics final. Head is observed independently of the
manifest; r139-b1 still pinsE23, source stage139 v0 stays immutable. No canonical
mutation, compute change or VACUUM. [Cleanup](out/native/ashlar_bucket_screen_r160/cleanup/summary.json)
UUID/version-checks and drops both layouts plus stage.

The [20M comparison plan](bucket-scale-plan.md) now states row/write/read/time
bounds and explicit initial wide-copy proof limits. Next execute that physical
comparison on the existing20M fixture within those bounds. It measures file
counts, lookup/ingest tradeoffs and maintenance, not billion admission; full20M
wide-copy parity remains separate if only scoped checks pass. No chosen64-bucket
production layout or successful full performance gate is inferred. Hash-LC stays
canonical candidate; UC Delta stays selected and the full goal remains active.


### r161: full 20M bucket build reached the bounded timeout

The existing 2XSmall warehouse attempted a full 20-million-edge, 20-logical-field copy of canonical E23 into the 64-bucket physical candidate, with row tracking, Zstd, 64MiB target files and the same eligibility statistics. The source was the pinned 34.28GB/532-file snapshot, independent of the observed canonical head. The CTAS terminated with a native 180-second timeout (query `01f1c206-2c29-1449-a308-ac76ec5e0126`, final FAILED). Native metrics report 20M rows read, 34.784GB read, 29.014GB remote read, 16.961GB remote write work and zero spill. These failed-write metrics are not committed table size or ingest throughput.

No bucket table was registered. The shallow clone was UUID/version verified and dropped; no stage was created. No ZORDER, full structural parity, updates or singleton comparison ran. The source-copy wide-value obligation remains unproved. Uncommitted storage cleanup was not independently proven; no VACUUM or write retry was issued. Canonical publication r139-b1 still pins E23. Evidence is in `out/native/ashlar_bucket_screen_r161/{query-history,failed-summary}.json` and its inspection/cleanup subdirectories. The intended worker's unexecuted success-summary qualification inherited the 100k pilot scope; it produced no summary, and this terminal failure record defines r161's actual scope.

UC managed Delta remains selected. This bounded failure measures build cost on the existing small warehouse, not a reason to change architecture or a billion-scale result. Next: resume design around the canonical hash-clustered tables and explicitly separate optional bucket build/maintenance costs; a future full bucket comparison needs a longer controlled build window or resumable owned staging before read results can be compared.


### r164–r166: full20M owned bucket copy completed in four commits

The hash-range build succeeded on the unchanged existing 2XSmall warehouse. Four immutable E23 ranges produced4,999,762/5,003,588/4,999,471/4,997,179 rows in50.060/52.974/55.814/57.196 caller seconds. Owned table `client_dev.ashlar_entropy_20261006_r86.bucket_part_r164`, UUID `99831c85-f437-4e6f-8b8b-eaec9ab300d2`, is retained at version4:528 files/33,157,013,686 bytes. Each of the four exact native append IDs matches its Delta WRITE history and output row count; versions0–4 contain only the empty CREATE plus those appends. Reader3/writer7 with rowTracking/DV, Zstd,64MiB target and identity/eligibility statistics are recorded in the saved detail.

Full20M count and global distinct-ID cardinality pass; all derived buckets match the first60 SHA256 bits modulo64, valid lowercase64-hex hashes pass, and64 bucket counts sum to20M. Total build/recovery/audit reads38.199GB and remote writes33.157GB fit the45GB/40GB admission plan. The previous terminal failed r161 CTAS write work16.961GB remains separate evidence: combined historical write work50.118GB before any maintenance. No spill/latency/ingest conclusion is inferred merely from these totals; exact per-query metrics are retained. Canonical r139-b1 publication still pins E23/R17/J19 and its other original tables.

Two controller defects are preserved: r164 compared a nested result row to a flat expected list after a successful first append; r165 lacked the columns local and stopped before submitting another append. r166 checked the owned UUID, native version1 and exact expected count before appending only the remaining three ranges. No successful write was replayed. This is commit-inspection evidence for the owned experiment, not durable real-producer recovery/fencing admission.

The complete20-column SELECT plus derived bucket has now been executed at20M scale, but full20-field byte-exact parity is still required. No ZORDER, update comparison, singleton measurement, cold/service/rate or1B/5B admission follows. Keep this owned version for the next bounded exact-value validation and matched physical comparison; cleanup requires the same UUID and latest-version checks. Evidence: `out/native/ashlar_bucket_build_r166/audited-summary.json`, with r164/r165 failures and same-ID histories alongside it.


### r178–r180: final 20M preservation closed; validation cost exceeded

The final owned bucket table at version9 passes four complete ranges totaling20M unique edges with zero differences across all20 logical columns against canonical E23 plus the immutable100k intended stage0. Text comparisons use UTF-8 binary values; other fields use null-safe native equality. This closes the full-value obligation after ZORDER and the r176 update, including the2,987,212 copied unchanged rows. All original native statements succeeded and their recorded histories are final. The audit preserves the original controller failure and corrects its generic “remaining ranges unproved” wording without replaying SQL.

The wide proof consumed177.558GB reads, exceeding its140GB admission budget; remote writes and spill were zero. The cost guard ran after the four successful ranges, so it was not an in-flight spending cap. Further expensive work is stopped; this is a validation planning failure, not a failed value comparison. Evidence: `out/native/ashlar_bucket_final_parity_r178/audited-summary.json`.

The separate LC custody check passes for all19.9M unchanged native identities, immutable file paths and row positions across owned clone0/MERGE1, with20M unique IDs and4.273GB reads, zero writes/spill. Combined with the r177 exact100k changed-carrier checks and closed two-version lineage, this establishes preservation for this LC update; it is not a fresh20M wide payload scan. Evidence: `out/native/ashlar_bucket_lc_custody_r179/summary.json`.

Canonical hash liquid clustering remains the physical proposal; bucket64 remains experimental because measured update write amplification is material. UC Delta remains selected. No new canonical publication, producer authority, sustained ingest, cold/service latency or1B/5B admission is established. Next: a small matched post-update singleton comparison on these retained versions, then consolidate the ingest/maintenance design using the observed write amplification; do not repeat the wide oracle.


### r181–r182: post-update singleton comparison

All60 complete-carrier lookups pass against immutable stage0,30 identical SHA-ranked updated keys per layout, alternating query order and disabling result caching. LC clone1 p95 is179ms engine/433.354ms caller,8files/403.803MB; bucket9 is228ms/487.591ms,2files/160.850MB. Remote reads occur in1/30 LC and9/30 bucket queries; this is neither a controlled cold test nor a fully warm cohort. Both observed distributions miss the provisional100ms engine/250ms caller targets. Different physical histories and sequential samples preclude a causal partition claim. Fewer bucket files did not imply lower observed latency.

All native query IDs are successful and final. The worker's post-query cost assertion stopped after15.753GB reads against15GB; zero writes/spill. The audit reconstructs completed comparisons without replay and preserves the cost failure. No new canonical publication, sustained-rate, production source authority or billion-scale admission. Evidence: `out/native/ashlar_bucket_post_update_reads_r181/audited-summary.json`.


### r183–r184: two-client point reads and caller controls

Two independent persistent clients execute ten synchronized pairs of exact20-field LC1 reads using20 distinct updated identities and ten SELECT1 control pairs. Owned UUID/version is revalidated; expected values come from the already recorded independent immutable stage, avoiding another oracle scan. Result caching is disabled. Native query-start/execution-end windows overlap for all10 point and all10 control pairs; this does not prove simultaneous task execution or saturation. Polling outside timed calls adds idle gaps, so this is a burst-pair experiment, not steady load.

Point p95: engine197ms, caller443.288ms, compilation162ms,8files/403.803MB; remote reads0/20. SELECT1 p95: engine25ms, compilation26ms, caller149.983ms. Caller-minus-native-total residual p95 is124.784ms for points and97.532ms for controls; these are per-query differences, not subtraction of unrelated percentiles or a pure network measurement. Both point gates remain failed. Capacity-wait duration is absent from native histories; original summary incorrectly defaulted it to zero, and the separate audit explicitly records unknown while preserving the original evidence.

All native IDs succeeded and finalized. Total7.700GB reads/0 writes/0 spill fits10GB; the controller reserves1GB before each next pair and checks cumulative final telemetry between pairs. The reserve is an estimate, not an in-flight spending cap. No canonical publication, maintenance, resize or new compute occurred. Evidence: `out/native/ashlar_lc_concurrent_r183/audited-summary.json`.

Design implication: retain canonical hash LC, but do not advertise the provisional warm SLO as achieved. Candidate-file amplification persists for updated rows even with no remote reads. Caller/server residual and compilation consume significant budget; an in-region application measurement and reduced candidate bytes are separate qualification work. Native task saturation and capacity queueing remain unmeasured. Next investigate whether publication hot-file shape can reduce singleton amplification without adding mandatory full-table maintenance to every batch; measure that change together with raw/journal/current publication timing, not MERGE throughput alone.


### r185–r186: diagnose update hash-range overlap before rewriting

Two full20M narrow-column live-file groups pass at owned LC0/LC1 with2.522GB reads/0 writes/0 spill, within10GB. Live file count changes532 to522. The six new files contain exactly100k updated rows; five span99.9783–99.9959% of the256-bit hash domain and one spans49.0083%. Each of the30 prior sampled keys intersects18 pre-update live-file extrema and5–6 new hot-file extrema. Hot compressed candidate footprint is312.789–326.515MB; baseline footprint370.872–418.363MB. These are reconstructed full-precision live-row min/max extents and compressed file sizes, not stored Delta statistics, actual candidate counts or bytes scanned. DV filtering and stored-statistics truncation can change those relationships. The separate measured reader remains8files p95; do not substitute the reconstructed23–24 range overlaps for that measurement.

This identifies a specific testable write-shape hypothesis: random parallel hot output creates overlapping hash ranges even though the table has CLUSTER BY(lookup_hash). It does not establish that all read latency is attributable to that overlap. Evidence: `out/native/ashlar_lc_file_ranges_r185/summary.json`.

A no-mutation native EXPLAIN probe accepts `REPARTITION_BY_RANGE(6,lookup_hash)` on the20-column input and shows a Photon `rangepartitioning(...,6)` exchange. The MERGE EXPLAIN displays a command wrapper without the final runtime writer exchanges, so it does not prove range-aligned Delta output. Native version remains1; all probe histories finalize with0 read/write/spill. Evidence: `out/native/ashlar_lc_range_plan_r186/`. Spark documents the partitioning hint in its [primary performance documentation](https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html); actual Databricks writer behavior must be observed rather than inferred from that documentation.

#### Next controlled range-input update

Use one new owned shallow clone of exact canonical E23, matching LC/DV/rowTracking/compression/statistics and the already proved immutable stage0. Preserve the existing baseline/update result as control. Replace only the source SELECT with the6-range hint; full native tuple, lookup hash, old entity_version15 and r139-b1 eligibility stay unchanged. No insert/delete clauses, schema change, new source authority or source publication are introduced. Do not mutate LC1 or canonical tables and do not replay an uncertain MERGE.

Before execution revalidate canonical/stage IDs and versions, and reference the existing exact intended-stage evidence by immutable pins. Bound the new run to one100k MERGE,90s statement/600s controller admission,25GB reported reads and2GB reported remote writes, with at least8GB reserved for post-update narrow custody and changed-carrier proof. These are admission estimates, not in-flight spending caps. Close results before metric collection; check cumulative final metrics between phases. Stop before reader admission if the write shape or preservation check fails. Record native history/rows-copied/DVs/output bytes, and compare all20 changed fields using binary text/native typed values. Prove19.9M untouched-row physical custody only if the history shows zero copied rows and immutable unchanged files; otherwise a wider proof needs a separate justified budget.

Measure fresh live-file extents on the100k output and a small exact singleton cohort only after correctness. A hint that disappears or leaves nearly global hot ranges is a failed optimization hypothesis, not permission to claim improved maintenance. Even successful range shaping must be integrated with raw/journal/current validation and immutable publication to qualify freshness; a standalone MERGE cannot admit10k/s,100k/s burst, cold/service or1B/5B targets. Canonical hash LC remains the proposal, UC Delta remains selected, and UMF binding remains deferred.


### r187–r188: range-aligned update produces narrower hot files

The fresh owned shallow LC clone of immutable canonical E23 applies the same independently proved stage0 and eligibility/native tuple with only `REPARTITION_BY_RANGE(6,lookup_hash)` added to the source SELECT. Native MERGE version1 updates100k, copies0, inserts0/deletes0. All20 changed fields match;19.9M unchanged full identities/file paths/row positions pass symmetric custody, and20M globally unique IDs remain. Clone0/MERGE1 is the complete closed lineage. Audit verifies hash LC, DV/rowTracking and reader3/writer7. Canonical manifest remainsr139-b1/E23 and unchanged other vector pins; this owned derivative is unpublished.

Six new hot files cover15.9644–18.0914% hash-domain spans; each of the20 tested hashes intersects exactly one reconstructed new live-file range. This contrasts with five nearly global hot files in the prior update. Full-precision live extrema remain distinct from stored Delta statistics. Twenty exact uncached-result singleton reads have no remote reads: p95 engine104ms/caller365.700ms,3files/139.231MB. Prior same20-key control is179ms/433.354ms,8files/403.803MB. This operational improvement is measured, but the runs are sequential, not randomized causal evidence; validation/polling can warm data and change idle timing. Both provisional warm gates still fail, and the caller gap remains material.

MERGE caller6.903s, reported remote write326.548MB,0 copied rows; prior LC control4.110s/326.515MB. The new run's native metadata time5.573s is retained rather than treating6.903s as a pure shuffle penalty. Whole clone/update/exact/custody/range/read run uses7.770GB reported reads/326.548MB writes/0 spill, within25GB/2GB admission, with telemetry checks/reserves between phases. All native IDs succeeded/finalized, no mutation replay or full-table OPTIMIZE. Existing intended-stage proof is inherited by immutable pins rather than rereading all20M payloads. Evidence: `out/native/ashlar_lc_range_update_r187/audited-summary.json`.

Design consequence: range-aligned hot output is a viable physical tuning candidate alongside canonical hash LC, avoiding a required full-table rewrite for this measured update. Six ranges is an experiment setting, not a production constant or1B/5B partition count. Spark input hints can still be overridden at other scales/writer paths; qualify actual output extents rather than presence of a hint. Preserve all source/native eligibility and20-field semantics. Before adopting the writer setting, repeat with new bounded batches and integrate raw/journal/current validation and the existing publication barrier to measure its freshness effect. Standalone MERGE/read evidence admits neither sustained10k/s nor100k/s recovery, cold/production service p95, real source authority/fencing or billion scale. UMF binding remains deferred.

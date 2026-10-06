# Higher-entropy scale and GraphFrames iteration

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

The declared synthetic complete-edge wire retains all 20 canonical fields, with
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

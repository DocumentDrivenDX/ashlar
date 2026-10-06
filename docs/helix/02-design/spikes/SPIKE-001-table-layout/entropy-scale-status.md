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
the existing r85 version2 table. All 50 paired full-carrier results agree; all
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

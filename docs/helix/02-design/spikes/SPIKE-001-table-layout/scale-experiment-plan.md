# Next bounded scale experiment — prepared, not authorized to expand

Governed by CONTRACT-003/ADR-001 and the owner's instruction to limit time, money and effort. The earlier native 10M-edge experiments establish bounded pruning/preservation, not 1B-node/5B-edge capacity. The current warehouse is available; no Spark/graph runtime was found in the recorded inventory. No additional resources or scale writes have been started.

## Questions the next experiment must answer

Measure file/metadata growth, native hash/full-key pruning and maintained ingest cost on the actual 0.3 carrier shape, including cursor/origin references. Keep client/server/compile/engine timing separate. Retain exact fields, source history and actual-version publication. A capacity run cannot pass by dropping raw history or structural coverage to make current-table reads fast.

## Resource admission before submission

Require an explicit owner spending/time limit and effective account metering/pricing for the existing warehouse. The quoted rate for a separate optional warehouse is not evidence of this shared warehouse's incremental bill. Capture current compute settings, baseline storage/file sizes and concurrent-use state. Do not resize/stop shared compute. Exclude the billion-row run until measured throughput and complete temporary-storage/cost estimates justify it.

Bound each submitted phase by a persisted statement handle and a wall deadline. On deadline, request cancellation and recover the same handle until terminal; do not retry an unknown write. Cancellation does not guarantee no committed side effects: inspect table histories and exact row/digest evidence. Temporary tables remain recoverable; DROP/VACUUM requires separate authorization. Storage therefore continues after the experiment unless an explicitly authorized cleanup policy exists.

## Prepared workload sequence

1. Reuse recorded 10M-edge baseline without copying. Inventory actual files/bytes/versions and inspect its high-entropy scope; do not extrapolate repeated-payload compression.
2. Under the selected cap, run one fresh 0.3 edge-carrier phase with at most 20M rows, two publisher-origin fields and realistic distinct payloads. Record all omitted graph/source tables as scope exclusions. This phase is a physical sensitivity experiment, not full graph admission.
3. If the first phase finishes within the admitted cap and demonstrates acceptable pruning/file/ingest costs, prepare the next larger graph phase with full canonical, raw source, property journal, adjacency and publication accounting. Derive its resource estimate from measured bytes and measured throughput, then obtain the required larger resource bound before submission.
4. At any failed deadline, digest, identity, history, publication or cost condition, stop growth. Keep the measurements and prior published state; do not repeatedly run cached lookup variants.

The 20M-row cap is a proposed first sensitivity step, not permission or a hidden replacement for the full 1B/5B requirement. It cannot establish sustained 10k/s plus bursts. A later publication schedule must include staging, validation, maintenance, readers and source arrival clocks, then measure queue growth and freshness distributions. The actual producer/worker profile is currently unqualified, so source-freshness admission remains synthetic until real source evidence exists.

## Metrics and evidence

Store generation seed/entropy profile, native schema and complete version vectors, query text/parameters and handles, all fields/digests, actual active files/bytes, clustering/maintenance histories, result-cache state, remote I/O, queue/startup time, compile/engine/caller distributions, per-arrival processing/freshness and backlog, cancellation/recovery outcome and resource/billing evidence. Report omissions and unknowns alongside every claim.

Use the historical performance targets for comparison only under the owner's fixed Unity Catalog Delta direction; a failure is not an architecture veto. Do not claim that small correctness or arithmetic capacity sensitivity proves the requested billion-node runtime performance.


## Verified existing baseline metadata

[r77 read-only inventory](out/native/ashlar_scale_baseline_inventory_20261006_r77/summary.json) confirms stable latest edge version 5 before/after DESCRIBE DETAIL: 136 active files, 6,182,560,646 bytes (6.18 decimal GB), mean active file size 45,460,004.75 bytes (43.35 MiB). The 64MiB target is a tuning property, not an observed hard file size. These compressed bytes describe this specific historical carrier mix and exclude raw feed/history/other graph tables, retained Delta versions and 0.3 reference width. Do not multiply them into a billion-node storage claim. No full scan or larger write was performed.


## Owner disposition

On 2026-10-06 the owner selected small tests and continued design work, with commit/push after each iteration. This resolves the pending resource question without authorizing the proposed 20M-row phase or new compute. Keep the plan prepared for future scale qualification; continue implementation-ready table/protocol/mapping design and small correctness tests.


## Payload variety qualification

[r80](out/native/ashlar_payload_sample_20261006_r80/summary.json) reads 256 rows
from the existing edge version 5 with one unordered LIMIT query. All 256 property
texts differ, but every bag has exactly key 201 and exactly 2,058 UTF-8 bytes;
retained content is the same 28-byte text in all rows. The sample includes two
source authorities and two relationship types. Local gzip preserves about 56.94%
of joined property bytes and 1.10% of joined retained bytes. These are sample
observations, not Parquet/Zstd compression or a representative full-table audit.
LIMIT bounds returned rows, not independently measured physical scanning.

A future qualified workload must distinguish byte variety from semantic shape
variety. Declare the distribution of property counts/IDs, variable text lengths,
missing versus explicit null, integers above 2^53, exact decimal/time tokens,
nested retained content, revisions and source cursor width. Include structured
repeated values alongside distinct opaque text, and report proportions and seed.
Also vary source/type skew, endpoint degree skew and updated-key locality rather
than assuming uniform records represent a real consumer. Preserve every exact
carrier and native source origin under the selected profile. Character frequency
or distinct whole-bag count alone does not qualify this workload.

Before any larger write, generate and inspect a small corpus for these declared
shapes and measure its actual bytes. Resource estimates must separately account
for current carriers, raw input, property-event fanout, structural projections,
retained Delta versions and releases. The owner's small-test disposition still
excludes the prepared 20M phase; this observation creates no scale authorization.


## Small declared-shape corpus

[varied-corpus](out/varied-corpus/summary.json) supplies 64 objects, 192 edges and
256 complete synthetic raw envelopes, generated deterministically without compute.
Eight property shapes occur 32 times each: empty, explicit null, exact large
integers, decimal/exponent tokens, timezone text, escaped/Unicode variable text,
wide repeated categories and nested values. Opaque-bearing shapes vary text length;
retained nested content varies by carrier. Native tuple IDs repeat across source
and type, and signed-int64 extremes remain decimal strings in transport. Typed
endpoint closure, independent parallel edges, self-loops and isolates are checked.

Every raw envelope includes every canonical fixture field with matching exact
props_json/retained_json and a checked UTF-8 payload digest. This closes the earlier
synthetic fixture's missing-structural-field issue for this local corpus only.
The corpus matches 0.3 object/edge column names, but is not native Truss wire or
transaction history. Property journal events, revision prerequisites, seed/coverage
boundaries and real source authority are still absent. Synthetic source xid text
exceeds signed BIGINT; no scalar checkpoint is generated.

The next small native fidelity slice may load these declared carriers into private
0.3 tables and compare exact raw strings, endpoints, hash computation and source
references. It should use byte-preserving transport and actual versions. Its
result would remain correctness evidence; do not infer compression, throughput or
billion-scale capacity from fewer than a thousand synthetic records.

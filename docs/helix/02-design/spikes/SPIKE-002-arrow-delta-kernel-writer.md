---
ddx:
  id: SPIKE-002
  type: spike
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: ADR-001
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
  - id: TD-007
    kind: informed_by
  - id: TD-008
    kind: informed_by
---

# SPIKE-002: Arrow-backed Delta Kernel writer discriminator

## Question and boundary

Can a native Arrow producer append the canonical `.4-xxhash64` edge carrier
through a supported Delta transaction API, retaining its full schema, Delta
writer-v7 protocol/features, file statistics, exact Spark-compatible lookup hash,
and atomic transaction semantics, while improving the dominant edge materialization
cost? This is a local synthetic experiment proposal only. The selected Spark
ZSTD writer and its profile remain unchanged. It does not qualify Unity Catalog,
production authority, source/publication/ACK behavior, or the few-minute
end-to-end target.

Official Delta Kernel documentation describes appending existing-table data by
obtaining a snapshot and transaction, writing Parquet through an engine, registering
file metadata with `add_files`, and committing. The API collects statistics and
performs the Delta transaction; the connector does not handcraft log actions.
Its Arrow default engine and current table-creation documentation describe
`appendOnly`, `invariants`, `domainMetadata`, and a `clusteredTable` feature.
The retained Ashlar edge-table receipt instead names the writer feature
`clustering`. These names must not be treated as equivalent. This is a plausible
investigation path, not proof that a particular Kernel release admits Ashlar's
exact generated table and feature combination. Exact protocol and feature
compatibility is a hard pre-write gate.

References: [Delta Kernel append API](https://docs.delta.io/kernel/rust/writing/append.html),
[Kernel Arrow/default-engine installation](https://docs.delta.io/kernel/rust/getting_started/installation.html),
[feature-derived table requirements](https://docs.delta.io/kernel/rust/writing/create_table.html),
[domain metadata requirements](https://docs.delta.io/kernel/rust/writing/domain_metadata.html),
[Delta protocol compatibility](https://docs.delta.io/versioning/), and
[Spark `xxhash64` semantics](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html)
and [Spark's typed hash expression implementation](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/hash.scala).

## Proposed single discriminator

After review, run one sequential pair on an isolated local scratch table generated
from the exact pinned UMF `.4` edge model and checked DDL. Use the existing pinned
225M sample generator and source/profile in the [retained attempt-03 SQL](../../04-build/evidence/edge-write-attribution-20261023/attempt-03/insert.sql),
reduced to exactly 1,000,000 edges. One case uses the selected Spark 4.2/Delta 4.4.1 ZSTD writer; the other
uses one pinned Delta Kernel release with Arrow input and the same Parquet codec.
Both write to distinct disposable scratch locations. Do not touch, mutate, or
relabel the selected realization. The pair changes only the writer implementation.
Use at most `local[18]` / 18 CPUs, 24 GiB process memory, 2 GiB scratch output
per case, 300 seconds for the sequential pair, and zero retries. These are
proposed case ceilings, not an approval to consume resources.

The candidate must create a transaction from the table snapshot, get the logical
and physical write schemas from Delta Kernel, construct Arrow batches matching
the logical schema, and pass the Parquet file metadata/statistics returned by the
engine into the transaction's supported add-files and commit path. Keep full
Delta semantics and existing writer-v7 features; do not lower protocol, remove
fields or constraints, bypass the Delta transaction, or write log JSON directly.
Use the UMF-generated schema rather than a second hand-authored schema.

Ashlar's hash is Spark SQL `xxhash64(CAST(source_system AS STRING),
CAST(rel_type_id AS BIGINT), CAST(id AS BIGINT))`, seed 42, with signed BIGINT
output. Arrow's or another engine's generic xxHash function is not presumed
equivalent: Spark hashes each typed argument into the next seed. The candidate
must use a version-pinned Spark-compatible implementation or reject the path.
For this discriminator, compare every persisted `lookup_hash` against native
Spark SQL recomputation for all 1M candidate rows. Also compare the complete
persisted edge row bag and ordered logical schema, row count, Delta protocol and
exact feature/property set; inspect Parquet footer codec and file statistics,
and prove one atomic committed Delta version with no untracked data files.

Capture Arrow generation, Parquet write, Delta commit, Spark startup/DDL,
readback validation, and cleanup as separate monotonic intervals. Compare the
two writer intervals and total discriminator wall time; this small sample tests
the integration and early cost signal only. Do not extrapolate it as a 1B/5B
estimate. A favorable result would only justify presenting a separately bounded
larger local trial for owner review.

## Admission and kill criteria

Before the pair, pin the exact Delta Kernel/Arrow dependency versions and hashes,
table-model/DDL hashes, generator and SQL hashes, runtime identity, codec, scratch
paths, resource limits and cleanup ownership. Inspect the selected profile's
retained Delta protocol and metadata as the reference. Confirm that the candidate
Kernel officially supports every observed reader/writer feature by its exact
protocol name and semantics, including the retained `clustering` writer feature.
Documentation for `clusteredTable` alone does not satisfy this gate.
If the local API/package is unavailable, the feature list is incomplete, a feature
is experimental or requires manually appending protocol actions, or the exact
feature combination has no supported existing-table append path, stop before
writing and record the blocker. Do not infer support from the general Delta
protocol version number or from OSS Spark/Delta version parity.

Kill the candidate immediately on any mismatch in generated schema, values,
nullability/invariants, `lookup_hash`, row multiplicity, protocol/features,
statistics, codec, or committed transaction/readback. Also kill it if resource,
output, or wall limits are reached, cleanup cannot prove the owned scratch table
removed, or its write interval is not lower than the paired Spark write interval.
No failure authorizes retrying with relaxed semantics or a second candidate
variant. Retain both case receipts and exact scratch cleanup evidence.

## Interpretation

Delta Kernel provides a supported transaction abstraction worth a narrow local
integration test; the repository's `.venv` currently has no installed
`deltalake`, `pyarrow`, Spark, or Delta packages, so local dependency/API execution
has not been verified here. Official docs do not establish a speedup for Ashlar's
wide edge shape. Exact Spark hash parity is a central compatibility risk, and
one million rows cannot establish large-scale throughput or output geometry.
The retained ZSTD write-only projection is about 16.50 minutes for 1B nodes and
5B edges at the recorded 80% rate, before point reads, scans, graph action,
validation, and cleanup. The owner’s desired few-minute end-to-end target remains
open even if this discriminator passes; no full-scale run is authorized by this
proposal.

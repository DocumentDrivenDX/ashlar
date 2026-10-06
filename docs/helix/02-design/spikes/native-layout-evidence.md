---
ddx:
  id: ashlar.native-layout-evidence
  type: evidence
  activity: design
  status: draft
  authoring:
    home: repo
---

# Native Databricks layout evidence

Executed 2026-10-05 on dbw-aidev-cus, existing data-gateway serverless Photon
SQL warehouse, 2X-Small, one cluster. SQL channel 2026.38. CLI profile aidev-cus
handled authentication; credentials are not captured in evidence.

## Scope and limits

Synthetic fixtures; exact schema/run IDs and native statement IDs are retained.
Result cache is avoided using uuid() in read output and verified against query
history; disk cache remains enabled/observed. Warm running compute only. CLI
wall timing includes process/auth/network/response handling and is not optimized
production-driver latency. Native history separates execution, compilation and
server-total duration. Counts are full scans at the tested small scale, not
proof of interactive full-graph aggregation at 1B nodes.

The larger fixture is 3M nodes / 9M edges, not the 1B-node / 5B-edge planning
graph. Uniform repeated payloads compress unrealistically well. Fixed-layout
runs on shared existing compute do not prove cold IO, concurrency, ingestion
freshness, pruning at production file counts or external graph compatibility.
No full 1B-node support or p99 result is claimed.

## 30,000 nodes / 90,000 edges

Schema: `client_dev.ashlar_spike_20261005_7f2b2e`. 3 recorded repetitions per table/query, one excluded warmup. 0 result-cache hits. Result parity and endpoint checks passed.

| Table | Shape | n | Execution p95 ms | Compile p95 ms | Server total p95 ms | CLI wall p95 ms | Read/pruned files median |
| --- | --- | --- | --- | --- | --- | --- | --- |
| bag_l | lookup | 3 | 219 | 129 | 416 | 876.6 | 1 / 0 |
| bag_l | list | 3 | 232 | 251 | 524 | 1087.5 | 1 / 0 |
| bag_l | count | 3 | 252 | 132 | 424 | 1193.9 | 1 / 0 |
| bag_z | lookup | 3 | 222 | 142 | 401 | 912.0 | 1 / 2 |
| bag_z | list | 3 | 260 | 144 | 424 | 867.4 | 1 / 2 |
| bag_z | count | 3 | 311 | 139 | 486 | 881.2 | 1 / 2 |
| promoted_l | lookup | 3 | 242 | 145 | 427 | 894.4 | 1 / 0 |
| promoted_l | list | 3 | 249 | 145 | 430 | 869.9 | 1 / 0 |
| promoted_l | count | 3 | 311 | 158 | 523 | 1466.1 | 1 / 0 |
| type_a_l | lookup | 3 | 228 | 130 | 397 | 1002.0 | 1 / 0 |
| type_a_l | list | 3 | 218 | 305 | 566 | 996.5 | 1 / 0 |
| type_a_l | count | 3 | 257 | 128 | 424 | 836.7 | 1 / 0 |

## 3,000,000 nodes / 9,000,000 edges

Schema: `client_dev.ashlar_spike_20261005_9586d0`. 20 recorded repetitions per table/query, one excluded warmup. 0 result-cache hits. Result parity and endpoint checks passed.

| Table | Shape | n | Execution p95 ms | Compile p95 ms | Server total p95 ms | CLI wall p95 ms | Read/pruned files median |
| --- | --- | --- | --- | --- | --- | --- | --- |
| bag_l | lookup | 20 | 256 | 129 | 456 | 1053.7 | 1.0 / 0.0 |
| bag_l | list | 20 | 674 | 146 | 858 | 1466.9 | 1.0 / 0.0 |
| bag_l | count | 20 | 631 | 132 | 790 | 1454.2 | 1.0 / 0.0 |
| bag_z | lookup | 20 | 276 | 122 | 440 | 926.1 | 1.0 / 2.0 |
| bag_z | list | 20 | 510 | 110 | 677 | 1301.5 | 1.0 / 2.0 |
| bag_z | count | 20 | 545 | 167 | 725 | 1366.8 | 1.0 / 2.0 |
| promoted_l | lookup | 20 | 299 | 122 | 458 | 1031.3 | 1.0 / 0.0 |
| promoted_l | list | 20 | 264 | 122 | 440 | 896.1 | 1.0 / 0.0 |
| promoted_l | count | 20 | 317 | 142 | 476 | 948.0 | 1.0 / 0.0 |
| type_a_l | lookup | 20 | 247 | 116 | 394 | 844.7 | 1.0 / 0.0 |
| type_a_l | list | 20 | 209 | 112 | 367 | 890.3 | 1.0 / 0.0 |
| type_a_l | count | 20 | 261 | 118 | 402 | 1071.3 | 1.0 / 0.0 |

## Larger-run interpretation

At 3M nodes / 9M edges, singleton execution p95 is 256 ms for generic liquid,
276 ms for partitioned/Z-order, 299 ms for shared promoted and 247 ms for typed.
Server-total p95 is 394–458 ms; CLI wall p95 is 845–1054 ms. The provisional
100 ms execution / 250 ms caller gate remains failed. Removing CLI overhead
alone cannot close the server-total gap.

Typed list/count median execution is 192/217 ms versus bag-liquid 568/553 ms
(approximately 3.0×/2.5× faster). This supports scalar serving projections,
not a universal change to canonical per-type tables. Singleton differences do
not establish a statistically robust clustering winner from twenty samples.

Generic liquid and promoted scans read one file and prune zero files in the
median; the type-partitioned variant reads one and prunes two. Thus the highly
compressible fixture still does not exercise realistic many-file point pruning.
Measured read bytes came from IO cache (median 100%). No cold-data result,
concurrency result or billion-node extrapolation is supported.

## Native Contract probes

In the small-run schema all seven CONTRACT-003 CREATE TABLE statements succeeded.
Stored exact JSON strings recovered unchanged, including int64 max, a decimal
beyond common float precision, explicit null, nanosecond timestamp text/offset
and unknown nested retained content. These are carrier preservation tests, not
native typed-value or UMF adapter admission. Version-pinned lookup returned
version 1 after version 2 replaced current state. A duplicate synthetic key was
accepted by Delta and detected by a grouping check, confirming publisher-owned
semantic uniqueness. Full multi-table publication/recovery remains untested.

## Findings and decision gates

The initial proposed 100 ms execution / 250 ms caller singleton gates are not
satisfied by this warehouse/path. Do not raise targets silently or claim that
clustering guarantees point-read latency. The small run has too few files to
choose layout from pruning alone. Interpret larger-run metrics before selecting
identity clustering, typed projections or Z-order. A faster persistent client
can remove CLI overhead, but cannot erase measured warehouse compilation and
execution time.

Keep the native singleton path and generic canonical objects/edges. The current
ADR remains proposed; native execution evidence removes the DDL unknown but does
not settle billion-node physical layout. Next experiments: optimized persistent
SQL client, random identity working set larger than cache, more files/skew/types,
concurrency, and manifest/failure recovery. Advance scale only after tail-latency
and cost gates are reviewed.

## Raw evidence

[Native summary](SPIKE-001-table-layout/out/native-summary.json),
[run directories](SPIKE-001-table-layout/out/native/),
[SQL runner](SPIKE-001-table-layout/native_sql.py),
[Contract probe](SPIKE-001-table-layout/native_contract_probe.py).
Each run retains statements.jsonl, query-history.json, measurements.json and
run.json. The small schema also retains contract-probes.jsonl and its summary.
Tables remain isolated in the named schemas for inspection. No warehouse settings,
existing schemas or existing datasets were changed; no automatic schema/table
cleanup was performed.

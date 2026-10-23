---
ddx:
  id: ashlar.evidence.r05-codec-pair-attempt-03-snappy-output-cap-20261023
  type: evidence
  activity: build
  status: recorded
  authoring:
    home: repo
  links:
  - id: ashlar.evidence.r05-codec-snappy-edge-pair-plan-20261023
    kind: informed_by
  - id: ashlar.evidence.r05-codec-pair-attempt-02-zstd-baseline-20261023
    kind: informed_by
  - id: TD-007
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
---

# R05 codec-pair attempt 03: SNAPPY output threshold

**Outcome:** The single SNAPPY-only continuation was stopped at its 4,000,000,000-byte polled output threshold. It returned exit 1 after supervisor SIGTERM, reaped the leader, and found an empty process-group inventory. It is not a successful candidate and supplies no valid INSERT timing comparison. This closes the SNAPPY codec avenue under this experiment's resource bound; do not retry or broaden the codec matrix.

The retained one-use root is `/private/tmp/ashlar-r05-codec-pair-20261023-03/snappy`; keep its failed progress, log, partial Delta table, and supervisor receipt. Progress reached `plan_captured`, then `failed` during INSERT, followed by `spark_stopped`. There is no `insert_completed`, no Delta write commit, no footer validation, and no row/witness readback. The created Delta table's initial metadata/schema and `delta.parquet.compression.codec=snappy` readback were observed before INSERT. No candidate equivalence or timing conclusion follows.

The supervisor sampled a final output-tree size of 5,669,004,782 bytes, or 1,669,004,782 bytes (41.7%) above the 4,000,000,000-byte threshold. The retained partial table contains 18 Parquet files totaling 5,623,620,561 bytes; their codecs were not footer-verified. Diagnostics were 1,442,423 bytes. The monitor polls every 0.5 seconds; the recorded size is post-stop accounting, not the exact first crossing size. The threshold is not a filesystem quota: the monitor can observe a crossing only on a poll and data can still be written while SIGTERM is handled and the Spark group is reaped. The evidence does not isolate the overshoot between those effects. The runner log's `SparkContext was shut down`, append error, and subsequent scheduler shutdown errors are consistent with aborting in-flight INSERT after the supervisor limit; they are not evidence of a codec implementation defect.

| Retained file | SHA-256 |
| --- | --- |
| `out/progress.jsonl` | `5024cdb00d6fa5f8dc9526188d3183a12476281441a46546ae928901a41d6c29` |
| `runner.log` | `05fcbfdc79eeab53841b8c72b139d11da89bcce1d2662f200f8e7ffce331d459` |
| `supervisor-result.json` | `f095841f4ab29c07b0c9f2fe7ee95901e58c57d1333f31a63c55ba4505955bbc` |
| `_delta_log/00000000000000000000.json` | `fae557b5f7ddb2872d45afc36a4e2df61b4c620bfe57782360f44f4d9ba9e3e6` |
| Attempt-03 root `baseline-retention-receipt.json` | `4e27f671953d1cc8743ea3ef631fd03c77ca26a730bb37cb410d64889e7388cf` |

The separate baseline-retention receipt binds the original attempt-02 progress, log, supervisor result and both Delta-log hashes; it records that the exact attempt-02 warehouse was removed before the candidate started. [Attempt 02](codec-pair-attempt-02-zstd-baseline-20261023.md) remains the latest completed ZSTD INSERT observation: 25.962255 seconds for 225M rows, with full receipt readbacks. At 80% of that measured edge rate, the 5B-edge write arithmetic is 721.174 seconds; combining the retained 45M-node sample's 80%-rate 1B-node estimate of 268.877 seconds gives 990.050 seconds, or 16.50 minutes of writes alone. This remains above the 15-minute owner-review trigger and says nothing about the stronger few-minute end-to-end target. Startup, DDL, both exact point lookups, both full-table scans, graph export/action, validation, and cleanup are outside that write-only arithmetic. See the updated [R05 budget](README.md) and [codec avenue disposition](codec-snappy-edge-pair-plan-20261023.md).

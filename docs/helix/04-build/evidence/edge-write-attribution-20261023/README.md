# Edge-write attribution attempt (2026-10-23)

**State:** one bounded local `.4` edge materialization completed. The packet retains the harness, receipts and event evidence for a single edge-only sample. It reuses reviewed source commit `bc2accd883a49d34c282dc002af98b4b05ea9440`, wheel SHA-256 `18401e7052202515da90c5c7a87d775c8c33b82f82b9526f9802d615356b76af`, Spark 4.2.0, Delta 4.4.1, JDK 21 and the existing 18-core synthetic formulas. No node, graph-engine, cloud, 1B/5B, comparison or candidate work ran.

## Preflight outcomes

Two invocations stopped before any Spark job or data write; they remain preflight failures, not samples:

1. The initial sandbox invocation failed at Py4J startup because localhost socket binding was denied. Its supervisor record and log remain in `/private/tmp/ashlar-edge-write-attribution-20261023/`.
2. The escalated invocation passed loopback setup but stopped during `SparkContext` initialization because Spark requires the event-log base directory to exist. Its `FileNotFoundException`, progress record and supervisor result remain in `/private/tmp/ashlar-edge-write-attribution-20261023-attempt-02/`.

The event directory creation and static/inert parser fixes were reviewed before the third attempt. Spark 4.2's installed event writer naming was inspected from its bundled `spark-core_2.13-4.2.0.jar`; the reader accepts rolling `events_*` and `local-*` streams, and excludes CRC and appstatus sidecars.

## Bounded run and result

The one approved invocation was:

```sh
/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python -B docs/helix/04-build/evidence/edge-write-attribution-20261023/supervise.py
```

It completed with return code 0 in 48.339 seconds. The supervisor observed 2,591,601,070 output-tree bytes and 2,468,353 diagnostic bytes, below its polled 4,000,000,000 and 128,000,000 thresholds; no limit fired. These thresholds are monitored termination guards, not OS filesystem quotas. The table footer verified 225,000,000 rows, 18 ZSTD files and 2,569,031,961 active Parquet bytes, exactly matching the prior edge-only baseline. The process verified Python 3.12.14, PySpark 4.2.0, Delta 4.4.1, JDK 21.0.13.0.2, `local[18]`, 24g driver memory and 18 JVM processors. Per-file hashes, plan, SQL, progress, original and reparsed event summaries, raw event stream, run log and supervisor receipt are retained in [attempt-03](attempt-03/experiment.json) and [its hash manifest](attempt-03/SHA256SUMS.json).

The physical plan records outer SQL execution 8 as `AppendDataExecV1`; it has no direct job/stage link. Nested SQL execution 9 (`rootExecutionId` 8) owns job 4/stage 6, the 18-task materialization stage that reports all 225M output records and active bytes. Stage 6 spans 25.485 seconds; task durations range from 21.763 to 25.466 seconds (median 23.884 seconds). Summed executor run time is 424.703 task-seconds, summed executor CPU is 310.684 CPU-seconds, and summed JVM GC is 11.277 task-seconds. Stage span is 96.7% of the 26.354-second INSERT wall time; the 0.869-second remainder is residual overhead, not a measurement of Delta commit alone. CPU/run ratios do not attribute time to compression versus expression evaluation or IO. No JFR or profiler was collected.

`insert.sql` retains the exact executed SQL bytes whose SHA-256 is recorded in the progress receipt; `insert-sql-display.txt` preserves the captured file form with its trailing line feed. The first event summary did not classify the nested SQL from its description. The offline parser now recognizes the outer append plan and retains per-task metrics; the retained event stream was reparsed without starting Spark. The wrapper/child relation and its limits remain explicit in the receipt rather than being represented as a direct stage link.

## Harness and cleanup

`run.py` performs the `.4` edge-only INSERT with 225M sequential IDs, unchanged endpoint formulas and row order, `local[18]`, 18 shuffle/default partitions and ZSTD. It captures the physical plan, event log, progress checkpoints, Delta commits, table footer, active bytes and per-column chunks. The event parser records SQL/job/stage/task relationships, task timings and executor metrics. It does not use JFR/profiling and cannot identify the relative cost of expression evaluation, compression or IO.

`supervise.py` refuses an existing output root and launches one process group. It monitors a 300-second total child runtime, a 4,000,000,000-byte output tree and 128,000,000 diagnostic bytes at 0.5-second intervals, starts wall termination at 290 seconds, then allows up to eight seconds after SIGTERM before SIGKILL. It rechecks caps after child exit. Byte thresholds can overshoot between polls. There are no retries.

After the per-file hashes and footer evidence were copied and verified, only the owned private `out/warehouse` data directory was removed; event logs and compact receipts remain at the private attempt root. Failed preflight roots remain untouched. No candidate run, node build, graph-engine run, cloud workload or 1B/5B trial was performed.

The static check command is rerunnable after the retained attempt:

```sh
/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python -B docs/helix/04-build/evidence/edge-write-attribution-20261023/setup_check.py
```

It checks source ordering, inert event parsing, exact local runtime and artifact hashes, and free disk; it does not construct SparkContext. Add `--pre-run` before a new attempt to also require that the unique private attempt-03 output root is absent. That fresh-root condition was checked before the retained run; it is intentionally excluded from the default post-run validation.

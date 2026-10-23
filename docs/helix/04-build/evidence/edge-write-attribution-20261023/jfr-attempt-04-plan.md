# Attempt 04 preparation: bounded JFR window

**State:** prepared, not executed. The current attempt-03 measurement is retained unchanged. This file proposes one profiled repetition of the same 225M-edge local write; it is not a result or approval to run while another local campaign is active.

## Exact window and profile

`run_jfr.py` is a separate copy of the reviewed attempt-03 runner. Its SQL text and runtime setup are unchanged, and `insert.sql` contains the exact bytes passed to Spark. It starts JDK Flight Recorder immediately before `spark.sql(sql)` and issues `JFR.stop` in the `finally` immediately after that call returns or raises. Wall-clock timestamps bracket the `jcmd` start/stop calls; the run's monotonic INSERT timer brackets the actual call. Attempt 03 observed outer SQL execution 8 (`AppendDataExecV1`) with nested execution 9 (`rootExecutionId` 8) → job 4 → stage 6. Attempt 04 must retain and report its own actual IDs; the previous IDs are a guide for analysis, not assumed values. JFR scope therefore contains the full INSERT call, including command overhead, while the event log should identify the materialization stage inside it.

The selected JDK `profile` configuration has `jdk.ExecutionSample` at 10 ms and `jdk.ObjectAllocationSample` throttled to 300 samples/s, both with stack traces. JDK 21 JFR defaults to disk recording. Before starting, the runner sets `JFR.configure repositorypath` to `out/jfr-repository`, reads that configuration back and refuses if the returned path is outside monitored output. It sets an explicit `filename` under `out`, `disk=true` and `maxsize=32M`, then stops, hashes and summarizes the final recording. The 32 MiB retained recording and a possible coexisting 32 MiB repository ring leave room below the 128,000,000-byte diagnostics threshold for the existing event log, profile copy, logs and receipts; the supervisor still polls the entire non-warehouse output tree and can terminate on threshold. The 32 MiB max is a rolling retained-data limit, not a guarantee that every event fits; post-run analysis must verify JFR duration and event types/counts before treating the interval as adequate. Allocation results are sampled estimates, not an exhaustive allocation count. The profile covers the local Spark JVM, including driver and task threads. JFR overhead can alter timing; this profiled repetition must not be compared to attempt 03 as an unprofiled speed result. It may identify sampled CPU/allocation stack hotspots, but cannot by itself prove disk-vs-compression-vs-expression causality.

## Bounds and command

`supervise_jfr.py` targets a fresh `/private/tmp/ashlar-edge-write-attribution-20261023-attempt-04` and refuses reuse. It preserves the existing 300-second child limit, 4,000,000,000-byte output-tree threshold and 128,000,000-byte diagnostics threshold, monitored every 0.5 seconds with the same 290-second termination trigger and eight-second grace. The JFR maxsize is 32 MiB; a repository ring plus stopped recording could briefly coexist at up to roughly 64 MiB before event logs, profile copy and receipts. The whole non-warehouse output tree is included in the monitored diagnostics total. Thresholds are polled guards, not filesystem quotas. The run remains one sequential local edge write: 225M rows, 18 partitions, the same formulas/order, Spark 4.2.0, Delta 4.4.1, local[18], JDK 21 and ZSTD. It performs no node build, candidate change, graph-engine work or cloud operation.

After independent review and confirmation that the local resource slot is free, the proposed command is:

```sh
/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python -B docs/helix/04-build/evidence/edge-write-attribution-20261023/supervise_jfr.py
```

This preparation does not authorize execution. Do not run during another project's local PG/Spark campaign. Preserve a failed attempt without retry; if successful, hash the JFR and receipts, verify 225M rows/18 ZSTD files and the output budget, then remove only the private warehouse after evidence verification.

## Preparation checks

`jfr-prepare-check.py` is static/inert: it verifies the separate scripts parse, the exact SQL block matches attempt 03, the fresh root is selected, supervisor limits are unchanged, and the installed JDK profile contains the recorded CPU/allocation sample settings. It does not launch Spark, Java, `jcmd`, JFR or a data write.

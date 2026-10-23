# Per-column Parquet statistics pair — completed 2026-10-23

## Result

One sequential local comparison completed for the pinned canonical `.4`
225M-edge attempt-04 insert. The baseline used the reviewed writer defaults; the
candidate changed only
`spark.hadoop.parquet.column.statistics.enabled#source_delivery_id=false`.
Both cases used the same insert SQL bytes and generated carrier. Both produced
225,000,000 rows in 18 ZSTD Parquet files, with 36 row groups.

| Observation | Baseline | Candidate |
| --- | ---: | ---: |
| Insert elapsed time | 32.1088 s | 25.7974 s |
| Parquet bytes | 2,569,031,961 | 2,568,793,346 |
| `source_delivery_id` chunks with non-null statistics | 36/36 | 0/36 |
| `source_delivery_id` chunks with null-count metadata | 36/36 | 0/36 |

The candidate was 6.3114 seconds lower in this one unprofiled sequential pair;
that is an observation, not a general speed claim. The Parquet output saved
238,615 bytes. Its regular `source_delivery_id` statistics, including null-count
metadata, were absent in all 36 column chunks. The four selected lookup-key
statistics (`source_system`, `rel_type_id`, `id`, `lookup_hash`) remained present
in all 36 chunks. The configured Delta data-skipping columns and their typed
min/max/null-count keys and values remained equal on every one of the 18 Delta
AddFile actions in each case.

Each case returned the same exact one-row full-key lookup, including the complete
typed carrier and `source_delivery_id` value. The query used a full key predicate
including the native hash. `DataFrame.inputFiles()` reported one source file per
case; this is an input-file inventory, not a measurement of files read by the
executed scan. The result is a point-result equality check, not full-table value
equivalence or proof that a production lookup can omit delivery-id metadata.

This experiment does not change source configuration or promote the candidate
to a default. At 80% of the measured per-role throughput, the unchanged node
sample projects to 9.67956 s × (1B / 45M) / 0.8 = 268.877 s. The candidate edge
sample projects to 25.79739175 s × (5B / 225M) / 0.8 = 716.594 s. The combined
write-only estimate is 985.471 s (16.425 minutes), 85.471 s above the 15-minute
review threshold. The full-size run remains held. The pair does not establish
scale capacity, production latency, or a
generally faster profile. No JFR profile was taken.

## Execution profile and custody

The runner and prepared protocol are retained in this directory. The successful
execution used local Spark `local[18]`, macOS arm64, Python 3.12, PySpark/Spark
4.2.0, Delta 4.4.1, Parquet 1.17.0, and JDK 21.0.13 with a 24 GiB driver.
It used the task-local `ashlar_graph_toolkit-0.1.0.dev1` wheel and generated
carrier from source commit `bc2accd883a49d34c282dc002af98b4b05ea9440`; the insert
SQL was pinned byte-for-byte to retained attempt-04. The supervisor completed
both cases sequentially without retry in 235.634 seconds, within its 290-second
termination trigger and monitored 10 GB output / 128 MB diagnostic limits. It
recorded 5,185,073,179 output bytes and 47,247,872 diagnostic bytes. These were
polled limits, not filesystem quotas.

The first sandboxed attempt (r1) stopped before writing data because Py4J loopback
was denied with `EPERM`. The successful r2 run used elevated local execution
accepted by automatic sandbox review. No multi-gigabyte run output is copied
into the repository; the private r2 root is
`/private/tmp/ashlar-parquet-statistics-pair-20261023-r2`.

SHA-256 hashes below identify the compact result, receipts, and event-attribution
artifacts retained in that private root. The per-case event logs, Delta tables,
and Parquet files remain there for local inspection.

| Artifact | SHA-256 |
| --- | --- |
| Pair comparison | `9162c483e8aa060b564c60b78681e0b3497465a5da6427d403236b446f2ba413` |
| Supervisor receipt | `47688b31b1e97080f5624af41fcf845035b9a06c85c4a0ba45160cae4aa50ecd` |
| Baseline case summary | `de194d84882495f687e84bc02c74a05130b7c694b5db231a51888b9f4a183f62` |
| Candidate case summary | `221ba6be63d537371bd581970b1ba8aa8b11bd106e696609bef1969bddb4fcad` |
| Baseline selected Delta statistics | `caee8bb14a9ac48e324e7df366f90aa1f1561760ab2ef16d82b9d8adb22d54d4` |
| Candidate selected Delta statistics | `636c5b7e59aba06b7f9fa70dfaaf9401c0bf2c709f213229d1cc41168a28fcc0` |
| Baseline Parquet footer statistics | `9e47109c01bca3ee6fea51acd860f7d0088d966668f5ebaccf2a5c6584cfd270` |
| Candidate Parquet footer statistics | `86608725c538f23d1b32c3647c6d64d86d8679a9419614c407ed9fd56385226a` |
| Baseline exact-key lookup | `a88d5332be65fa0466f0319b73e61ccba68eb2be838dcc2c244f1944db3740dc` |
| Candidate exact-key lookup | `6842c8b376d6afa61928aa26f1ff06b66daea847efb0de0849222899bebdfb2c` |
| Baseline Spark event attribution | `101f45a152de419c982fa844df8506a64878342d130a29a3b81a28fcb17b8e59` |
| Candidate Spark event attribution | `8950d26cf71f8f8a4162b186a4457134147a611c044979e57a73ece8fff120e6` |

## Procedure and bounds

`run_case.py` verifies the runtime, wheel, generated carrier, insert SQL, and
Delta/Parquet JAR fingerprints before writing one case. It captures the writer
setting, SQL plan, every Parquet footer column chunk, selected Delta statistics,
the exact-key result, and Spark event-log task attribution. `run_pair.py` runs
baseline then candidate in fresh directories and stops on the first failure.
The successful pair used the bounded protocol below; it must not be treated as
authorization to repeat or scale the run.

```sh
/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python -B \
  docs/helix/04-build/evidence/parquet-field-statistics-paired-20261023/run_pair.py \
  --root /private/tmp/ashlar-parquet-statistics-pair-<unique-id>
```

The supervisor sampled its 300-second wall budget, 10,000,000,000 aggregate
output-byte limit, and 128,000,000 aggregate diagnostic-byte limit every
0.5 seconds, terminated at a 290-second trigger, checked again between cases,
and rechecked after child exit. Output bytes covered every file in the pair root;
diagnostic bytes covered every non-Parquet file, including logs and Delta logs.
The output directory was created with mode `0700`. The exact lookup capped
returned rows at two as a duplicate sentinel. No install, cloud action, retry,
or JFR capture was part of the successful pair.

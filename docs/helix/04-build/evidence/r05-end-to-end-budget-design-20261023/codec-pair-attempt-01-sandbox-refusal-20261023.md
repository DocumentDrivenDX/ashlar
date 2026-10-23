---
ddx:
  id: ashlar.evidence.r05-codec-pair-attempt-01-sandbox-refusal-20261023
  type: evidence
  activity: build
  status: recorded
  authoring:
    home: repo
  links:
  - id: ashlar.evidence.r05-codec-snappy-edge-pair-plan-20261023
    kind: informed_by
  - id: TD-007
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
---

# R05 codec-pair attempt 01: sandbox loopback refusal

**Outcome:** Failed during Spark startup before a Spark session, table, or write. The failure is an execution-environment refusal, not a codec or Delta result. No SNAPPY candidate was attempted. Do not reuse this one-use root.

The bounded supervisor started the pinned Python runner with the retained 225M-edge ZSTD case. The runner completed its Python, Spark/Delta package, JDK, JAR and generated-carrier admissions, then called `SparkSession.builder...getOrCreate()`. The Java gateway failed to bind its Py4J server to `127.0.0.1:0` with `java.net.SocketException: Operation not permitted`. `SparkContext` was not established: the progress record has `pre_spark_environment`, `admitted`, then `failed`, with no `spark_started`, `carrier_table_ready`, or `insert_completed` stage. Therefore no Delta table was created and no row write ran.

The supervisor recorded exit code 1 after 21.45 seconds, with 4,886 output/diagnostic bytes, no resource cap hit and `remaining_process_group_uncertain=false`. Its retained root is `/private/tmp/ashlar-r05-codec-pair-20261023`; preserve it as the attempt record. Receipt hashes:

| File | SHA-256 |
| --- | --- |
| `zstd/out/progress.jsonl` | `892a9f3f2e1abac0b307715db38bcd0b171758981e710bad05d5742b835a233a` |
| `zstd/runner.log` | `70f73157d17a700c27232e998f09f81efedf6e246c26f12c61884a201a3d498a` |
| `zstd/supervisor-result.json` | `98c363dfa7950e68aebaae40e415340708da9333e749197f73ce7e7c51d29d87` |

The failed root remains untouched. The reviewed pair supervisor now targets the fresh one-use root `/private/tmp/ashlar-r05-codec-pair-20261023-02`; it retains the same 300-second pair cap, 290-second termination threshold, per-case output/diagnostic caps, 12 GB free-space admission, runner hash, and expected INSERT hash. This only prepares a clean retry. Do not invoke it until Sol has reviewed the sandbox requirement and arranged an authorized execution context; the prior failure supplies no timing or codec evidence.

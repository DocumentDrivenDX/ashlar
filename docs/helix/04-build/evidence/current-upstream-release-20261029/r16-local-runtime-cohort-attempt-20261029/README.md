---
ddx:
  id: ashlar.evidence.r16-local-runtime-cohort-attempt-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: ashlar.evidence.truss051-r16-effectfree-candidate-20261029
      kind: informed_by
---

# R16 local runtime cohort — 2026-10-29

Status: **effect-free package/JAR preflight and one bounded local Spark/Delta
startup probe passed** for a fresh macOS arm64 cohort. This verifies selected
package versions, imports, dependency consistency, retained JAR bytes and JVM
class loading for the declared Spark/Delta startup configuration. It does not
qualify Delta table reads/writes, Truss source acceptance/feed, publication, ACK
or PostgreSQL. The single Spark session was stopped cleanly; no PostgreSQL
process nor source/native lifecycle operation was run.

## Cohort and package verification

The isolated runtime is
`/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv`, created with
Python 3.12.14. The host JDK is Homebrew OpenJDK 21.0.13.0.2 (`aarch64`). The
selected direct packages and required Truss native extra are installed:

| Distribution | Version | Artifact SHA-256 |
| --- | --- | --- |
| PySpark | 4.2.0 | source archive `pyspark-4.2.0.tar.gz`: `5ad689d53570ee1674193fd4f9bda065f0db3be9363a27d2a3406cc457b70b61` |
| Delta Spark Python | 4.4.1 | `delta_spark-4.4.1-py3-none-any.whl`: `224814c76071236dd98c23e8d01da9f43bec21af4129cae92822ca25e5becec7` |
| Truss | 0.0.51 `[native]` | `umf_truss-0.0.51-py3-none-any.whl`: `eb59141c48e94bb93ab7053e9e8725012eee1749e4432e9c71804d23abbdff57` |
| UMF Core | 0.10.3 | selected macOS arm64 wheel: `dc2586793e1492d1f825bd9e12f27d5db9f9330ae2343d9e7773c7052f1f6ea6` |
| UMF Access | 0.1.0.dev8 | `umf_access-0.1.0.dev8-py3-none-any.whl`: `1a21c1ddd59f6b23e9f0b33228280564af0e96bb4d13699b89616d5143e4fd23` |
| Weft | 0.4.56 | selected macOS arm64 wheel: `68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d` |
| psycopg / psycopg-binary | 3.3.6 | macOS arm64 binary wheel SHA-256: `612382ac3ed13651c7fa44b5fee9fbf7baaa2ddbc6f500391672682c5f1df9e0` |

The exact PySpark source archive was built and installed offline from the isolated
wheelhouse; its local SHA-256 matches the PyPI 4.2.0 file digest. The build
emitted PySpark 4.2.0 metadata and a `py2.py3-none-any` wheel. The Delta Spark
Python wheel SHA-256 also matches the PyPI 4.4.1 file digest. UMF Core, Access
and Weft wheel hashes match the public selected assets recorded in the [R01 pin
receipt](../r01-umf-weft-pins-20261029/README.md), and the Truss hash matches
the reviewed R16 candidate receipt. PyPI JSON metadata and the exact artifact
hash inventory are retained in the isolated cohort directory.

`pip check` reports `No broken requirements found`. Imports succeeded for
`pyspark`, `delta`, `truss`, `umf`, `umf_access`, `weft` and `psycopg`; the
Delta `configure_spark_with_delta_pip` API is importable. Exact installed
versions, import output, dependency check, wheelhouse, and artifact hashes are
retained under `/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/` in
`pip-freeze.txt`, `import-check.txt`, `pip-check.txt`, and
`artifact-sha256.txt`. PySpark's cached-index download initially failed under
the sandbox; the narrowly scoped unsandboxed download then resolved and staged
the exact sdist and platform-compatible dependencies. No incompatible wheel
was substituted.

## Bounded local Spark/Delta startup probe

The first sandboxed probe exited before SparkSession creation because Py4J could
not bind its localhost gateway socket (`Operation not permitted`). The same
probe then ran once with localhost binding enabled, under a 90-second outer
timeout. It used `local[1]`, Spark 4.2.0, JDK 21.0.13.0.2, the exact retained
Delta 4.4.1 JARs, `DeltaSparkSessionExtension`, and `DeltaCatalog`. Startup
returned Spark 4.2.0; `currentDatabase()` returned `default`; the extension and
catalog classes loaded. No table was read or written. `spark.stop()` returned,
and the outer process exited code 0 after 7.369 seconds. The startup log and
bounded runner are retained at
`/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/spark-startup-probe-unsandboxed.log`
and `run-spark-startup-probe.py`.

## Spark and Delta JAR custody

The installed PySpark `jars/` tree contains 323 JAR files (477,231,117 bytes).
Every filename, size and SHA-256 is in
`/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/spark-jars.sha256`
(manifest SHA-256 `0dde0bc25c6d2183615440789e14ebe9f89bc8a12e60b3f8b42b43da4743ca4a`).
The full installed set was byte-compared against the same 323 JAR entries in
the pinned PySpark source archive; there were no missing, extra or differing
entries. The `spark-core_2.13-4.2.0.jar` SHA-256 is
`8e0a03b66338a4a38c9089d9f798ab6a6c6583aefe93f910311704a5ec90ffc4`, and
`scala-library-2.13.18.jar` is
`4e85d96ff7bc7dc627985523c3541b9917aaa08e956391380c42db21a2c4e5a0`.

Delta 4.4.1's `delta-spark_2.13-4.4.1.jar` and its POM, plus the
`delta-storage-4.4.1.jar` and its POM, were fetched directly from Maven Central
into the cohort's `jars/` directory. JAR SHA-256 values are respectively
`2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be` and
`f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f`.
Their Maven coordinates and SHA-256 values are included in `artifact-sha256.txt`.
The PySpark 4.2.0 source archive and Delta Spark 4.4.1 wheel hashes both match
the corresponding PyPI JSON release digests retained beside that file.

The Delta Spark POM declares `spark-core_2.13`, `spark-sql_2.13` and
`spark-hive_2.13` 4.2.0 as provided, and Delta Storage 4.4.1. It also declares
Scala library 2.13.17, while the selected Spark 4.2.0 bundle contains Scala
2.13.18. This exact POM/runtime patch-version difference remains a compatibility risk.
The bounded startup loaded the selected extension and catalog classes with the
Spark-bundled Scala 2.13.18 runtime, but did not exercise Delta table semantics
or writes.

## Retained size and limit

`du -sk` accounting for the complete retained cohort at capture was:

| Component | KiB |
| --- | ---: |
| Isolated Python venv | 690,880 |
| Downloaded package wheelhouse, including the 4.2.0 source archive | 467,000 |
| Verified selected UMF/Weft/Truss wheel cache | 20,852 |
| Delta 4.4.1 JARs and POMs | 9,428 |
| Hash manifests, PyPI metadata, checks, scripts and startup logs | 88 |
| **Total retained cohort** | **1,188,248** |

The 2 GiB R16 retained-plus-attempt ceiling is 2,097,152 KiB, leaving 908,904
KiB of headroom at this capture. The temporary duplicate extraction used for
byte comparison was removed after the installed PySpark JAR set matched it
exactly. No source lifecycle operation was attempted.

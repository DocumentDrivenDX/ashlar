# UMF 0.5.6 candidate Delta 4 one-table acceptance attempt (2026-10-23)

This records one bounded attempt to exercise the Spark Delta 4 DDL realization
used by Ashlar's local bridge, before any Truss lifecycle. The attempt did not
reach Spark session creation or execute DDL. PySpark 4.2.0 and Delta Python
package 4.4.1 were present in the isolated Truss R4 venv, but the Delta Spark
JAR coordinate was not cached. Ivy could not resolve it because
`repo1.maven.org` and `repos.spark-packages.org` did not resolve in this
sandbox. The Java gateway exited during dependency resolution.

No table, row, publication, ACK, or Spark action was created. The attempt ran
once with a 180-second process-group deadline, `local[1]`, 1 GiB driver memory,
and a 64 MiB cap checked on retained temporary files. It ended in 2.5 seconds
and retained 48 KiB before cleanup. No dependency or DDL retry was made. The
only retained attempt artifacts are [the runner](attempt.py) and [the captured
log](run.log). The temporary directory owned by this attempt was removed after
these files were copied here. No R5 packet was changed.

The source candidate was UMF `9a11ae5c04c23f0cc0ae1d61a27c4d7a64819b08`.
The intended SQL and writer setting are from the pinned bridge's inert browser
probe: direct path `CREATE TABLE`, `CLUSTER BY (id)`, properties
`delta.dataSkippingStatsColumns=id,caption` and
`delta.deletedFileRetentionDuration=interval 14 days`, with
`spark.sql.parquet.compression.codec=zstd`. Since Spark did not start, no
observed table property or writer setting is claimed. The probe is proposal
evidence only.

Exact custody hashes:

| Input | SHA-256 |
| --- | --- |
| Candidate `src/adapters/delta/ddl.ts` | `8e4f694b7e3ecbdc0548f06b4db0a67339ea9319d3b70939003d4a0d18856051` |
| Candidate `src/model/document.ts` | `6ea8f43e4bcd3b5d7259329e6768dd563e066d7169a23bee75e88f054f348ddb` |
| Bridge realization manifest | `6fe91c59732342314263728c13114660e43f9150be8797639b5897fb9fe4c6b3` |
| Bridge role semantics | `1844cd4f2e3501e546aeb54327948b87d0860dd1e330d4acc1c9ed56a075b5f4` |
| Bridge build-input inventory | `9ac1f3c0412a7f4b617e15ff0581b1e424fac9d6da0d8aeb4535371ee550feff` |
| Bridge IIFE | `b686d8aa7ce55047f2c5bd9269a39cbd3ca43228ce70ae738a207108da200ce5` |
| Inert bridge browser probe | `6ed23ab0b582bc941cc011931f3f82d357a8594efa19970ba54d80e582da9f35` |
| Isolated R4 runtime install receipt | `c8e3056973fbea931dda98c47f1b67fc4ba651583e148183f281a2e0058cae55` |
| Isolated R4 runtime preflight | `6a9e35679a73350646e5e8af32be67b0c9b2eb30e2a5d124f3dcbe758a6250df` |
| Attempt runner | `b0fdc48543867c6ee635700326b2a3457920f3984c3591c0ebbbc32e95f993bc` |
| Captured failed attempt log | `51bea6e750866fdf104bfc72ce8a387af7e38c28c79fc0280d2e402e39e003c7` |

The R4 runtime profile reported PySpark 4.2.0, `delta-spark` 4.4.1, UMF core
0.10.0, and UMF access 0.1.0.dev7. The authorized fresh inventory before launch
showed no Java or Spark process; JDK 21.0.13.0.2 was selected explicitly from
`/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home`.

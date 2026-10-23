# UMF 0.5.6 candidate Delta 4 one-table acceptance attempts (2026-10-23)

This packet records four bounded attempts to exercise the Spark Delta 4 DDL
realization used by Ashlar's local bridge, before any Truss lifecycle.
Attempt-01 used `configure_spark_with_delta_pip`; it failed before Spark session
creation because Ivy attempted Maven resolution and the configured repositories
did not resolve in the sandbox. Its [runner](attempt.py), [captured
log](run.log), and receipt are retained at the packet root.

Attempt-02 supplied the exact pinned local Delta 4.4.1 JARs through `spark.jars`
with no `spark.jars.packages` request. Spark launched, but Py4J could not bind
to `127.0.0.1` (`Operation not permitted`) during SparkContext startup. It did
not create a Spark session or execute DDL. Its separate [attempt directory](attempt-02/)
retains the runner, supervisor, log and JSON receipt. Per instruction, there
was no retry after this failure.

Attempt-03 repeated the offline configuration under the specifically approved
loopback escalation and created a Spark session. Its pre-DDL probe used
`java.lang.Class.forName`, which failed to find the Delta extension through the
application classloader; this did not test Spark's user classloader. The runner
stopped before DDL, and there was no retry. Its separate [attempt directory](attempt-03/)
retains the runner, supervisor, log and receipt. Attempts 01–03 created no table
or row. Attempt-03 used the same
`local[1]` and 1 GiB driver with a 180-second deadline and 256 MiB owned-temp
cap.

Attempt-04 removed the invalid application-classloader probe and ran the
candidate DDL and one-row insert under the approved loopback escalation. The
direct-path Delta table was created and read back the exact row. Delta reported
clustering column `id`, properties `delta.dataSkippingStatsColumns=id,caption`
and `delta.deletedFileRetentionDuration=interval 14 days`, with one file and
848 active bytes. The writer setting read back as
`spark.sql.parquet.compression.codec=zstd`; the produced file is named
`.zstd.parquet`. The Delta log reports Spark 4.2.0 / Delta 4.4.1, protocol
reader 1 / writer 7, and versions 0 (CREATE TABLE) and 1 (WRITE). See the
[attempt-04 receipt and retained Delta output](attempt-04/). This is one native
DDL/write acceptance on the recorded local profile. It does not qualify the
whole eight-role bridge, a Truss lifecycle, ACK, managed Unity Catalog, or any
broader support profile. No R5 packet was changed.

The source candidate was UMF `9a11ae5c04c23f0cc0ae1d61a27c4d7a64819b08`.
The DDL shape and writer setting were sourced from the pinned bridge's inert
browser probe: direct-path `CREATE TABLE`, `CLUSTER BY (id)`, properties
`delta.dataSkippingStatsColumns=id,caption` and
`delta.deletedFileRetentionDuration=interval 14 days`, with
`spark.sql.parquet.compression.codec=zstd`. Attempt-04 bound the direct path to
its owned temporary root and executed this DDL shape natively; the observed
values are recorded in its result and Delta log.

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
| Attempt-01 captured log | `51bea6e750866fdf104bfc72ce8a387af7e38c28c79fc0280d2e402e39e003c7` |
| Attempt-02 Delta Spark JAR | `2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be` |
| Attempt-02 Delta storage JAR | `f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f` |
| Attempt-02 runner | `3df6157c614b0fdadee27670992408534aefaa207074f1906411addb98478c6e` |
| Attempt-02 supervisor | `ad3d91760f4d965bdcce09158fcc6fa7e2df378ce67bc10e5d55a08e777177d3` |
| Attempt-02 captured log | `af17b482af2f2b0407cc428b77498a9cf3d6278894c1b905fdcfe5e7a9752bb7` |
| Attempt-03 runner | `f079c606907797ae886aa06533817a0dac7717eba5e3f53ae76a17d6e266376e` |
| Attempt-03 supervisor | `cfc5f1a0ebceda5626f90a77f5ae500430aac10f293c3101cff108cc8d970516` |
| Attempt-03 captured log | `ab30c162b34c4dc8608515efb4124efdc4db13f4ba4da2de443c08491f224f80` |
| Attempt-04 runner | `c0581337e6e688a9e3b8469823aa48b972d30720bd0b046e9a054db6fae224af` |
| Attempt-04 supervisor | `3afb8fc9de1244acc94d6f1babfcb1258836d8183ec1b5e2ead2cf0e2e7a7949` |
| Attempt-04 captured log | `55ae0242a21a9b64a186add0afdc4b573b1344e325036cd261707e0490ef2d08` |
| Attempt-04 native result | `973090c8e29db1f9435ca7b54a38e86cdc580aab5f51ebc60b878cc2c0d820f4` |
| Attempt-04 CREATE TABLE Delta log | `e0533256c65df41e7289e3a0b13031ea06901b4333a6b73b8317fbcb217458a8` |
| Attempt-04 WRITE Delta log | `8b0f7dde83972da0eba61fd1d5a4831f288069ba854d712755f2981f54068161` |
| Attempt-04 Parquet output | `4a09a14c64cd0e7c4fe21adb39fb8675c6f0c641d0068aac616869d890280b8f` |

Attempts 01–03 did not reach DDL or observe table properties or a native writer
setting. Attempt-04 is the first native evidence for this candidate proposal
shape, with the direct path rebound to the owned test directory and the terminal
semicolon omitted. It does not execute the generated bridge SQL byte-for-byte.
Its recorded DDL, one-row result, observed properties and effective
writer configuration appear in [the native result](attempt-04/native-result.json)
with the supporting [Delta commit log](attempt-04/delta-log/). It remains a
bounded local acceptance rather than evidence for downstream lifecycle behavior.

The R4 runtime profile reported PySpark 4.2.0, `delta-spark` 4.4.1, UMF core
0.10.0, and UMF access 0.1.0.dev7. The authorized fresh inventory before launch
showed no Java or Spark process; JDK 21.0.13.0.2 was selected explicitly from
`/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home`.

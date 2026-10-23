# Attempt-04: local Delta 4 DDL and one-row write passed

This single escalated local run used Spark 4.2.0, Delta 4.4.1, Scala 2.13.18,
JDK 21.0.13.0.2, `local[1]`, a 1 GiB driver, a 180-second deadline and a
256 MiB owned-temp cap. It supplied the SHA-pinned `delta-spark_4.2_2.13` and
`delta-storage` JARs through `spark.jars`; `spark.jars.packages` was empty.
The expired application-classloader probe from attempt-03 was removed.

The candidate bridge DDL shape, with only the direct path bound to this attempt's
owned root, created a Delta table clustered by `id`, with
`delta.dataSkippingStatsColumns=id,caption` and
`delta.deletedFileRetentionDuration=interval 14 days`. One row was inserted and
read back exactly (`id=1`, `caption=one-row-acceptance`). Spark reported both
table properties and `clusteringColumns=[id]`; the table had one 848-byte file.
The writer configuration read back as
`spark.sql.parquet.compression.codec=zstd`; Delta's add-file action references
a `.zstd.parquet` output file. The retained Delta log records CREATE TABLE at
version 0 and WRITE at version 1, engine Spark 4.2.0 / Delta 4.4.1, and protocol
reader 1 / writer 7 with writer features `domainMetadata`, `clustering`,
`invariants`, and `appendOnly`.

The run passed all acceptance assertions in 8.82 seconds. The supervisor
observed 9,649,983 bytes against the 268,435,456-byte cap. See
[native result](native-result.json), [supervisor result](supervisor-result.json),
[Delta commits](delta-log/), and the retained [one-row Parquet file](part-00000-e0186d4d-6a7d-41ba-afb6-595bfb70d153-c000.zstd.parquet).
This qualifies only this one-table DDL/write on the recorded local runtime. It
does not exercise the complete eight-role bridge or Truss lifecycle, ACK,
managed Unity Catalog, or broader support profiles.

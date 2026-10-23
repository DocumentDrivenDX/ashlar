# Offline Parquet write-path review (2026-10-23)

This is a static follow-up to the one bounded [attempt-04 JFR sample](../edge-write-attribution-20261023/attempt-04/README.md). No Spark process or data write was run for this review, and no carrier or writer configuration was changed.

## Observed path

Filtering the retained `jdk.ExecutionSample` events to the outer INSERT interval (6,729 samples) yields 629 samples at `Binary.lexicographicCompare`. The complete stack is:

`Binary.lexicographicCompare` → `PrimitiveComparator.compareBinary` → `BinaryStatistics.updateStats` → `ColumnValueCollector.write` → `ColumnWriterBase.write` → `MessageColumnIORecordConsumer.addBinary` → Spark `ParquetWriteSupport.makeWriter$10`.

The 310 in-window `UTF8String.getBytes` samples flow through the same Parquet writer lambda and then `writeFields`/`consumeField`. This byte conversion is part of encoding the retained string value; turning off column statistics alone should not remove it. The JFR stack does not name the active field. The 629 comparison samples pass through `BinaryStatistics.updateStats`, but do not identify a column or dictionary path. The sample therefore cannot establish how much work comes from any one field or prove a bottleneck.

## Narrow candidate

In the retained synthetic SQL, edge `source_delivery_id` is `CAST(id AS STRING)`, a changing, high-cardinality value. The selected `.4` Delta statistics list is `lookup_hash,source_system,rel_type_id,id`; it does not select `source_delivery_id`. The field itself remains required data and must retain its exact value for source custody. Do not remove, normalize, hash or re-encode it.

Apache Parquet 1.17.0's installed `ParquetOutputFormat` exposes column-specific statistics configuration. Its configuration parser forms keys as the base setting, `#`, and the column path; `ParquetProperties.getStatisticsEnabled(ColumnDescriptor)` resolves that column override before the global default. The narrow candidate is therefore the Hadoop writer option:

```text
parquet.column.statistics.enabled#source_delivery_id=false
```

In a Spark builder this would be passed through the Hadoop-property bridge as `spark.hadoop.parquet.column.statistics.enabled#source_delivery_id=false`, then checked in the actual writer job configuration. This disables regular Parquet column statistics for that column while leaving statistics enabled for other columns. Do not assume any particular statistic, including null counts, remains available for the disabled field. The change does not avoid `UTF8String.getBytes()` for the value being written; it may avoid per-row binary statistics comparisons and updates. Readers filtering on `source_delivery_id` may lose row-group skipping. Since this field is not in the selected Delta data-skipping list and supported exact singleton lookup uses the full typed key and lookup hash, it is a narrowly scoped candidate, not a proven harmless change. Confirm Delta log statistics remain intact for all selected fields rather than assuming the two statistics layers are independent.

The current local writer profile does not include this option. Any adoption must bind the exact setting into a separately versioned local realization/installation identity and preserve prior receipts. This is a candidate for measurement, not a configuration recommendation or authorization to change the active profile.

## One paired check if selected

Run exactly one baseline/candidate pair on the same pinned local Spark 4.2.0, Delta 4.4.1, JDK 21, `local[18]`, 18 partitions, ZSTD, canonical `.4` schema, ordered 225M-edge fixture and SQL. Use separate fresh paths and one aggregate supervisor bound of 300 seconds, 10 GB output and 128 MB diagnostics. The two 2.57 GB data outputs do not fit together under the existing 4 GB single-run output guard. The candidate changes only the per-column Parquet statistics setting above. Run the pair sequentially with no competing Spark work, no retries and no full-size trial. If JFR samples are compared, both runs must use the same JFR profile and settings; do not compare profiled elapsed time with unprofiled timing.

Require exact schema, row count and generated field/value checks in both outputs. Inspect every Parquet footer: candidate must disable regular column statistics for `source_delivery_id`, while retaining them for `source_system`, `rel_type_id`, `id`, and `lookup_hash`. Do not require null-count metadata for the disabled field. Compare Delta log statistics for the selected `.4` data-skipping columns, and run the same exact-key lookup to verify equal identity/result and equal or better files/bytes read. Separately record the loss of `source_delivery_id` row-group pruning in a predicate check if that access path is supported. Collect comparable write-stage task time, bytes, `BinaryStatistics.updateStats` samples and allocation samples; only retain the setting if the field-level metadata effect is proven and the measured benefit is stable enough to matter. If the per-column override is ignored, selected Delta stats change, or supported lookup pruning regresses, refuse the candidate.

## Local artifacts inspected

- Attempt-04 exact SQL and plan, JFR recording/event stream, footer receipt and hashes in the linked packet above.
- PySpark's installed Parquet jars: `parquet-column-1.17.0.jar` SHA-256 `2fcd38628897c3b547b92d0abf7200ec84e9eb3fb9e8450b196142e4d5229044`; `parquet-hadoop-1.17.0.jar` SHA-256 `c47b0a875951fa191fadf8c5d13742e8938bdda9779b2d8fec5c5345ac9e0184`; Spark SQL 4.2.0 jar SHA-256 `ec0a0d699c6c57300ee47ba5d54b4d1f71d3e5401fb90a2bcdcd83b942084dbf`.
- The contract boundary for preserving source delivery identity and the selected stats list remains [TD-007](../../../02-design/technical-designs/TD-007-native-xxhash64-lookup-layout.md). Its open requirement to remove only redundant statistics while preserving source/ACK custody still governs any future change.

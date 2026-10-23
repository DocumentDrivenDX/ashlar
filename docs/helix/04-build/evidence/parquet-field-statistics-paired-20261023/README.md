# Per-column Parquet statistics pair — prepared, not run

This packet prepares one sequential local comparison of the exact canonical `.4` 225M-edge write from attempt-04. The baseline uses the existing writer defaults. The sole candidate change is `spark.hadoop.parquet.column.statistics.enabled#source_delivery_id=false`; the source data, generated carrier, SQL bytes, runtime, Delta-selected statistics, partitions, and output format remain pinned to the reviewed attempt-04 profile.

The candidate tests whether omitting regular Parquet column statistics for the high-cardinality `source_delivery_id` field changes write cost while preserving the exact one-row full-key result. It does not prove every stored value, claim that null-count metadata remains available, change Delta data-skipping statistics, or establish a generally faster profile. Every footer and row-group column chunk is inspected; the candidate is refused unless the delivery-id regular statistics are absent from every chunk and selected key statistics remain present in every chunk. The pair compares aggregate statistics-presence counters for all footer columns, exact typed min/max/null metadata for the four selected key columns, the exact full-key result, selected Delta statistics, and the 18-file/ZSTD/225M-row invariants. Each case must expose 18 Delta AddFile actions totaling 225M records, with exact selected `minValues`, `maxValues`, and `nullCount` keys and typed values on every action.

`run_case.py` performs one case and captures the actual writer setting, SQL plan, per-file footer statistics, Delta-selected statistics, exact-key result, and event-log task attribution. `run_pair.py` runs baseline then candidate in fresh directories and stops on the first failure. It never retries. Both programs are preparation only; neither was executed for this packet.

Before a future run, an operator must confirm the local Spark slot is idle and use a new, absent root beneath `/private/tmp`:

```sh
/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python -B \
  docs/helix/04-build/evidence/parquet-field-statistics-paired-20261023/run_pair.py \
  --root /private/tmp/ashlar-parquet-statistics-pair-<unique-id>
```

The supervisor bounds the sequential pair to a 290-second termination trigger within a 300-second total budget, 10,000,000,000 aggregate output bytes, and 128,000,000 aggregate diagnostic bytes. It samples these limits every 0.5 seconds, checks again after baseline before starting candidate, and rechecks after child exit; they are monitored limits, not filesystem quotas. The output limit counts every file in the pair root. The diagnostic limit counts every non-Parquet artifact, including Delta transaction logs, plans, event logs, JSON evidence, progress and child logs; the Parquet data bytes are counted separately toward the aggregate output limit. A newly created root is mode 0700. No Spark run, installation, cloud action, JFR profile, or retry is part of this preparation.

The exact-key lookup caps returned results at two rows as a duplicate sentinel. It records `DataFrame.inputFiles()` as a source-file inventory and retains the executed plan; `inputFiles()` is not treated as proof of which files the executed scan read. The semantic gate is exact one-row equality, not a full-table value proof or file-pruning claim. A successful pair would still be a single unprofiled local observation, not a scale result or a release qualification. The currently selected 1B-node/5B-edge performance gate remains held.

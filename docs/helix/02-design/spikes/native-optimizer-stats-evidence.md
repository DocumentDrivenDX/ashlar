# Targeted optimizer statistics and pinned validation

2026-10-05 workspace date (native UTC records extend into October 6); SPIKE-001, unchanged dbw-aidev-cus warehouse. Fixed edge version 16, journal 17 and stage r17_3. This comparison retains every shared post-state predicate and the existing two-broadcast hint.

All four validation calls return 300,000 rows, 300,000 distinct typed identities and zero mismatches. Before caller durations are 4.096s / 4.471s; after are 4.250s / 4.043s. Two observations per phase cannot establish a population percentile or a robust improvement. No integrated ingest gate is admitted.

Targeted catalog optimizer statistics collection costs 1.610s for edge identity/hash columns and 1.033s for journal identity/origin columns, 2.643s total. These are `ANALYZE ... COMPUTE STATISTICS FOR COLUMNS` commands, not Delta data-skipping statistics collection. Both commands finish successfully; no graph data writes or global predictive-optimization changes occur.

The follow-up read-only inspection confirms catalog statistics exist (`Manual Analyze`, row counts 10,019,981 edge and 2,820,023 journal). The current-table initial plan reports full statistics for both tables; the otherwise matched historical plan reports both missing. Latest Delta versions remain edge 16 / journal 17. Thus this trial demonstrates current-catalog statistics availability without historical-plan recognition on this runtime and shape. It does not establish a general time-travel guarantee or show the actual uncommitted transaction plan. Some reported column extrema reflect earlier values; statistics are optimizer inputs, not correctness evidence. Exact snapshot checks remain the correctness oracle.

Harnesses: `SPIKE-001-table-layout/native_optimizer_stats.py` and `native_stats_inspect.py`. Complete query records, four caller observations, initial before/after and current/pinned plans, catalog-statistics JSON and terminal summaries are under `out/native/ashlar_optimizer_stats_20261005_r25/` and `out/native/ashlar_stats_inspect_20261005_r26/`. No graph rows are replayed.

Retain the existing broadcast shape. Next measure post-ingest singleton pruning and a controlled reclustering comparison, accounting for maintenance time before any integrated-rate claim. Freshness, caller latency, sustained/burst ingest, billion-node scale and external-engine compatibility remain open. Goal active; UMF deferred.

Official syntax reference: [SHOW STATISTICS](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-show-statistics), which describes current optimizer statistics and excludes temporal table specifications. The current-versus-pinned plan result above is measured workspace evidence.

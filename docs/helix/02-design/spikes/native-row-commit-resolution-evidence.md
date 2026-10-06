# Native row-commit version resolution

2026-10-05; SPIKE-001 read-only evidence on existing dbw-aidev-cus warehouse and existing table features. No protocol feature or global setting was changed.

At pinned canonical 14 and journal 15, all recovered r17 markers report one row_commit_version: respectively **14** and **15**, matching independent complete-history/content resolution. Initial aggregate calls take **579ms** and **472ms** caller time. Strict follow-up requires expected total count, matching non-null metadata count, equal min/max and exactly one distinct version for all 300k rows in each table. The reusable `resolve_row_commit_versions.py` refuses missing/null/mixed versions rather than guessing previous+one or latest. The strict follow-up terminal result passes for both complete 300k-row groups.

[Databricks documentation](https://learn.microsoft.com/en-us/azure/databricks/delta/row-tracking) defines hidden row_commit_version as the version of last row insert/update and notes row-tracking protocol compatibility implications. Actual selected transaction behavior is proved here; rewriting maintenance after this commit, restore/clone, disabled tracking and later logical updates are not. Independent native identity remains canonical; row tracking IDs are not substituted for Truss IDs or external endpoints.

Original native detail/aggregate evidence `SPIKE-001-table-layout/out/native/ashlar_row_commit_20261005_r20/`; strict evidence `out/native/ashlar_row_commit_strict_20261005_r20/`. Calls are single samples, not latency percentiles or integrated freshness results. This can reduce candidate version-resolution queries but still needs exact changed/untouched validation and an enforced pending-publication barrier. Deleted entities require journal/tombstone evidence; this is a property-batch subset, not every lifecycle case.

Next test metadata preservation across actual maintenance and an atomic pending-batch barrier, then use strict resolved vectors in integrated publications. Preserve external-engine protocol limits and defer UMF binding. Caller/read/ingest/full-scale gates remain open; goal active.

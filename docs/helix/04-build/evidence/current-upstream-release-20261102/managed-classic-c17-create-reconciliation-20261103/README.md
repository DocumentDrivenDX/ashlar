# Managed classic C17 create refusals — 2026-11-03

These records document two failed, bounded classic-trial CREATE attempts for the
same reviewed nonce. They do not establish classic execution qualification.

- The first attempt reached the CLI, which refused its positional Spark version
  alongside `--json` before submitting a CREATE request. Its original state and
  receipt are retained as written; the appended pre-API reconciliation records
  the exact parser diagnostic and the stopped watchdog.
- The corrected retry reached the workspace backend, which refused the request
  because the organization had no associated worker environments. It returned
  no cluster ID. A fresh post-terminal cluster inventory was an empty list;
  the appended reconciliation records the inventory digest and stopped
  watchdog.

Neither attempt created a cluster or started a task, Spark, Delta, source, or
ACK operation. The first receipt's ambiguous cleanup label is preserved as
historical output and clarified by its append-only reconciliation; the second
attempt is classified as a backend refusal, not a pre-API parser refusal.

`SHA256SUMS.json` binds the copied state, receipts, stop markers, reconciliations
and empty inventory to their original private local files. The raw private
query-history capture is not included in this evidence tree.

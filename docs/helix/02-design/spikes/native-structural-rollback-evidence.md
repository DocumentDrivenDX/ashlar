# Structural rollback evidence

2026-10-05; SPIKE-001 bounded synthetic fault test on unchanged dbw-aidev-cus warehouse.

The first r6 transaction attempted to write the ordinary publication manifest alongside catalog-managed graph tables. Databricks returned terminal TRANSACTION_NOT_SUPPORTED.WRITE_NON_CATALOG_MANAGED_TABLE before the expected SIGNAL. This is a rejected test, not successful fault injection. Independent comparison of all seven pre-r6/pre-r7 table versions confirms no committed change. Original rejection evidence remains retained.

The revised r7 test keeps the manifest outside the atomic transaction, matching the current publication protocol. BEGIN ATOMIC deletes all 20,001 hub canonical edges and adjacency rows, their degree group, all 20,021 structural journal records and 20 tombstones, inserts a receipt, then deliberately SIGNALs SQLSTATE 45000 with a unique fault marker. The terminal server error contains that marker. Caller failure duration **5.201s**.

All six participating Delta table versions remain unchanged; the manifest version also remains unchanged. Hub canonical/adjacency counts and degree are still 20,001, all structural journals/tombstones remain, the attempted receipt is absent, and accepted r3-2 descriptor full-row readback is identical. Same terminal statement evidence is inspected; no unknown write is blindly retried. Unchanged versions plus affected-content checks establish rollback for these operations on this fixture. This does not establish publisher fencing, response-loss resolution, cancellation, crash recovery, retries or descriptor atomicity with graph data.

Harness: `SPIKE-001-table-layout/native_structural_rollback.py` (current r7 scope). Raw original rejection under `out/native/ashlar_structural_rollback_20261005_r6/`; successful injected-failure controls under `out/native/ashlar_structural_rollback_20261005_r7/`. Next receipt-based publication recovery after a committed data transaction but absent descriptor, with explicit versions and idempotency/fencing limits; also concurrent pinned structural reads. Latency, sustained rate, full scale and graph engine compatibility remain unproven. Goal active.
